"""Example WebSocket relay, upload side-channel, approvals, and LangChain bridge."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import websockets
from aiohttp import web
from pydantic import BaseModel, Field, TypeAdapter, create_model

from labos.mcp.protocol import (
    Attachment,
    Message,
    ToolApprovalRequest,
    ToolApprovalResponse,
    ToolAttachmentChunk,
    ToolAttachmentEnd,
    ToolAttachmentStart,
    ToolCall,
    ToolDefinition,
    ToolError,
    ToolEvent,
    ToolFatal,
    ToolHeartbeat,
    ToolLog,
    ToolProgress,
    ToolResult,
    ToolsRegister,
    ToolsUpdate,
    ToolUploadUrlRequest,
    ToolUploadUrlResponse,
)


VALID_KEYS = {"dev-key-123": "user-1"}


@dataclass
class DeviceSession:
    device_id: str
    user_id: str
    ws: Any
    tools: dict[str, ToolDefinition] = field(default_factory=dict)
    pending: dict[str, asyncio.Future[Any]] = field(default_factory=dict)
    last_heartbeat: datetime | None = None
    healthy: bool = True


class Relay:
    def __init__(self) -> None:
        self.connections: dict[str, DeviceSession] = {}
        self._device_ready: dict[str, asyncio.Event] = {}
        self.uploads: dict[str, bytes] = {}
        self.attachments: dict[str, bytes | str] = {}
        self._chunk_buffers: dict[str, bytearray] = {}

    async def handler(self, ws: Any) -> None:
        user_id = self._authenticate(ws)
        if user_id is None:
            await ws.close(code=1008, reason="Invalid API key")
            return

        raw_register = await ws.recv()
        message = TypeAdapter(Message).validate_python(json.loads(raw_register))
        if not isinstance(message, ToolsRegister):
            await ws.close(code=1002, reason="Expected tools.register")
            return

        session = DeviceSession(device_id=message.device_id, user_id=user_id, ws=ws)
        session.tools = {tool.name: tool for tool in message.tools}
        self.connections[message.device_id] = session
        self._device_ready.setdefault(message.device_id, asyncio.Event()).set()
        print(f"registered {message.device_id}: {', '.join(session.tools)}", flush=True)

        try:
            async for raw_message in ws:
                await self._handle_device_message(session, raw_message)
        finally:
            self.connections.pop(message.device_id, None)

    async def wait_for_device(self, device_id: str, timeout: float = 30.0) -> DeviceSession:
        if device_id in self.connections:
            return self.connections[device_id]

        event = self._device_ready.setdefault(device_id, asyncio.Event())
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return self.connections[device_id]

    async def wait_for_tool(self, device_id: str, tool_name: str, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            session = await self.wait_for_device(device_id)
            if tool_name in session.tools:
                return
            await asyncio.sleep(0.05)
        raise TimeoutError(f"Timed out waiting for {tool_name}")

    async def call_tool(
        self,
        device_id: str,
        tool_name: str,
        args: dict[str, Any],
        *,
        timeout: float = 30.0,
        call_id: str | None = None,
    ) -> Any:
        session = self.connections[device_id]
        if not session.healthy and tool_name != "simulate_fatal":
            raise RuntimeError(f"Device {device_id} is unhealthy")

        actual_call_id = call_id or uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        session.pending[actual_call_id] = future

        try:
            await session.ws.send(
                json.dumps(
                    ToolCall(
                        id=actual_call_id,
                        tool=tool_name,
                        arguments=args,
                        timeout_ms=int(timeout * 1000),
                    ).model_dump(mode="json", exclude_none=True)
                )
            )
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            await session.ws.send(json.dumps({"type": "tool.cancel", "id": actual_call_id, "reason": "timeout"}))
            raise
        finally:
            session.pending.pop(actual_call_id, None)

    def langchain_tools_for_device(self, device_id: str) -> list[Any]:
        """Return LangChain StructuredTool wrappers for a connected device."""
        from langchain_core.tools import StructuredTool

        session = self.connections[device_id]
        tools = []
        for definition in session.tools.values():
            args_schema = _pydantic_model_from_json_schema(definition.name, definition.input_schema)

            async def remote_tool(_tool_name: str = definition.name, **kwargs: Any) -> Any:
                return await self.call_tool(device_id, _tool_name, kwargs)

            tools.append(
                StructuredTool.from_function(
                    coroutine=remote_tool,
                    name=definition.name,
                    description=definition.description,
                    args_schema=args_schema,
                )
            )
        return tools

    async def _handle_device_message(self, session: DeviceSession, raw_message: str | bytes) -> None:
        message = TypeAdapter(Message).validate_python(json.loads(raw_message))
        if isinstance(message, ToolResult):
            future = session.pending.get(message.id)
            if future is not None and not future.done():
                future.set_result(message.result)
        elif isinstance(message, ToolError):
            future = session.pending.get(message.id)
            if future is not None and not future.done():
                future.set_exception(RuntimeError(message.error.message))
        elif isinstance(message, ToolProgress):
            print(f"progress {message.id}: {message.value:.0%} {message.message or ''}", flush=True)
        elif isinstance(message, ToolLog):
            print(f"{message.level.upper()}: {message.message} {message.fields}", flush=True)
        elif isinstance(message, ToolEvent):
            print(f"event {message.name}: {message.payload}", flush=True)
        elif isinstance(message, ToolFatal):
            session.healthy = False
            print(f"FATAL {message.code}: {message.message} {message.fields}", flush=True)
        elif isinstance(message, ToolHeartbeat):
            session.last_heartbeat = message.timestamp
            print(f"heartbeat {session.device_id}", flush=True)
        elif isinstance(message, ToolsUpdate):
            session.tools = {tool.name: tool for tool in message.tools}
            print(f"tools updated {session.device_id}: {', '.join(session.tools)}", flush=True)
        elif isinstance(message, ToolAttachmentStart):
            self._handle_attachment_start(message)
        elif isinstance(message, ToolAttachmentChunk):
            self._chunk_buffers.setdefault(message.attachment_id, bytearray()).extend(base64.b64decode(message.data_base64))
        elif isinstance(message, ToolAttachmentEnd):
            self.attachments[message.attachment_id] = bytes(self._chunk_buffers.pop(message.attachment_id, b""))
            print(f"attachment chunked {message.attachment_id}", flush=True)
        elif isinstance(message, ToolUploadUrlRequest):
            token = uuid.uuid4().hex
            await session.ws.send(
                json.dumps(
                    ToolUploadUrlResponse(
                        id=message.id,
                        request_id=message.request_id,
                        url=f"http://localhost:8766/uploads/{token}",
                    ).model_dump(mode="json", exclude_none=True)
                )
            )
            self.attachments[message.attachment.id] = f"pending-upload:{token}"
        elif isinstance(message, ToolApprovalRequest):
            approved = await self._prompt_approval(message)
            await session.ws.send(
                json.dumps(
                    ToolApprovalResponse(
                        request_id=message.request_id,
                        approved=approved,
                        approver="stdin",
                    ).model_dump(mode="json", exclude_none=True)
                )
            )

    def _handle_attachment_start(self, message: ToolAttachmentStart) -> None:
        if message.data_base64 is not None:
            self.attachments[message.attachment.id] = base64.b64decode(message.data_base64)
            print(f"attachment inline {message.attachment.id}", flush=True)
        elif message.url is not None:
            self.attachments[message.attachment.id] = message.url
            print(f"attachment uploaded {message.attachment.id}: {message.url}", flush=True)
        else:
            self._chunk_buffers[message.attachment.id] = bytearray()
            print(f"attachment chunk start {message.attachment.id}", flush=True)

    async def _prompt_approval(self, message: ToolApprovalRequest) -> bool:
        # Demo-only HITL path. A real backend should enqueue this request to the website UI.
        answer = await asyncio.to_thread(input, f"APPROVE [{message.risk}] {message.prompt} (y/N): ")
        return answer.strip().lower() in {"y", "yes"}

    def _authenticate(self, ws: Any) -> str | None:
        headers = _headers(ws)
        auth_header = headers.get("authorization", "")
        if not auth_header.lower().startswith("bearer "):
            return None
        return VALID_KEYS.get(auth_header.split(" ", 1)[1])


async def run_manual_demo(relay: Relay, device_id: str = "demo-device") -> None:
    await relay.wait_for_device(device_id)
    await relay.wait_for_tool(device_id, "multiply")
    print("manual relay demo: add ->", await relay.call_tool(device_id, "add", {"a": 7, "b": 5}), flush=True)
    print("manual relay demo: multiply ->", await relay.call_tool(device_id, "multiply", {"a": 3, "b": 4}), flush=True)
    print(
        "manual relay demo: read_temperature ->",
        await relay.call_tool(device_id, "read_temperature", {"units": "F"}),
        flush=True,
    )
    print("manual relay demo: scan_inline ->", await relay.call_tool(device_id, "scan_inline", {}), flush=True)
    print("manual relay demo: scan_chunked ->", await relay.call_tool(device_id, "scan_chunked", {}), flush=True)
    print("manual relay demo: scan_upload ->", await relay.call_tool(device_id, "scan_upload", {}), flush=True)
    print("manual relay demo: approval ->", await relay.call_tool(device_id, "move_to", {"x": 1.0, "y": 2.0}), flush=True)
    print("manual relay demo: idempotent ->", await relay.call_tool(device_id, "add", {"a": 1, "b": 2}, call_id="same-id"), flush=True)
    print("manual relay demo: idempotent ->", await relay.call_tool(device_id, "add", {"a": 999, "b": 999}, call_id="same-id"), flush=True)
    print("manual relay demo: long_running ->", await relay.call_tool(device_id, "long_running", {"steps": 3}), flush=True)
    try:
        await relay.call_tool(device_id, "simulate_fatal", {})
    except RuntimeError as exc:
        print(f"manual relay demo: fatal -> {exc}", flush=True)


async def run_langchain_demo(relay: Relay, device_id: str = "demo-device") -> None:
    """Example LangChain agent relay. Requires `pip install -e '.[examples]'` and `OPENAI_API_KEY`."""
    from langchain.agents import create_agent
    from langchain_openai import ChatOpenAI

    await relay.wait_for_device(device_id)
    agent = create_agent(
        model=ChatOpenAI(model="gpt-4o-mini"),
        tools=relay.langchain_tools_for_device(device_id),
    )
    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": "What is 7 plus 5? Then read the temperature in F."}]}
    )
    print(result, flush=True)


async def main() -> None:
    relay = Relay()
    runner = await _start_http_server(relay)
    try:
        async with websockets.serve(relay.handler, "localhost", 8765):
            print("relay listening on ws://localhost:8765/remote", flush=True)
            print("upload side-channel listening on http://localhost:8766", flush=True)
            if os.getenv("LABOS_EXAMPLE_LANGCHAIN"):
                await run_langchain_demo(relay)
            elif os.getenv("LABOS_EXAMPLE_RUN_ONCE"):
                await run_manual_demo(relay)
            else:
                await asyncio.Future()
    finally:
        await runner.cleanup()


async def _start_http_server(relay: Relay) -> web.AppRunner:
    # Demo-only upload side-channel. Production should authenticate these routes,
    # persist to object storage, enforce quotas, and use short-lived signed URLs.
    app = web.Application(client_max_size=10 * 1024 * 1024)

    async def put_upload(request: web.Request) -> web.Response:
        token = request.match_info["token"]
        relay.uploads[token] = await request.read()
        return web.Response(text="ok")

    async def get_upload(request: web.Request) -> web.Response:
        token = request.match_info["token"]
        data = relay.uploads.get(token)
        if data is None:
            raise web.HTTPNotFound()
        return web.Response(body=data)

    async def devices(_request: web.Request) -> web.Response:
        now = datetime.now(timezone.utc)
        return web.json_response(
            {
                device_id: {
                    "healthy": session.healthy,
                    "tools": list(session.tools),
                    "heartbeat_age_seconds": (
                        None if session.last_heartbeat is None else (now - session.last_heartbeat).total_seconds()
                    ),
                }
                for device_id, session in relay.connections.items()
            }
        )

    app.router.add_put("/uploads/{token}", put_upload)
    app.router.add_get("/uploads/{token}", get_upload)
    app.router.add_get("/devices", devices)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 8766)
    await site.start()
    return runner


def _headers(ws: Any) -> dict[str, str]:
    if hasattr(ws, "request") and getattr(ws.request, "headers", None) is not None:
        return {key.lower(): value for key, value in ws.request.headers.items()}
    if hasattr(ws, "request_headers"):
        return {key.lower(): value for key, value in ws.request_headers.items()}
    return {}


def _pydantic_model_from_json_schema(name: str, schema: dict[str, Any]) -> type[BaseModel]:
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    fields: dict[str, tuple[Any, Any]] = {}

    for field_name, field_schema in properties.items():
        annotation = _annotation_from_schema(field_schema)
        default = ... if field_name in required else field_schema.get("default", None)
        fields[field_name] = (annotation, Field(default=default, description=field_schema.get("description")))

    return create_model(f"{name.title().replace('_', '')}Args", **fields)


def _annotation_from_schema(schema: dict[str, Any]) -> Any:
    if "enum" in schema:
        return Literal[tuple(schema["enum"])]  # type: ignore[valid-type]

    schema_type = schema.get("type")
    if schema_type == "integer":
        return int
    if schema_type == "number":
        return float
    if schema_type == "boolean":
        return bool
    if schema_type == "array":
        return list[Any]
    if schema_type == "object":
        return dict[str, Any]
    return str if schema_type == "string" else Any


if __name__ == "__main__":
    asyncio.run(main())
