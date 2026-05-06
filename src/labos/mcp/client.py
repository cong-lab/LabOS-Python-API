"""Remote MCP-style WebSocket client."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import socket
import threading
import uuid
from collections.abc import Awaitable, Callable
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeVar

import websockets
from pydantic import TypeAdapter, ValidationError

from labos.mcp.context import Context
from labos.mcp.protocol import (
    Message,
    ToolCall,
    ToolCancel,
    ToolDefinition,
    ToolError,
    ToolErrorPayload,
    ToolHeartbeat,
    ToolApprovalRequest,
    ToolApprovalResponse,
    ToolUploadUrlRequest,
    ToolUploadUrlResponse,
    ToolResult,
    ToolsRegister,
    ToolsUpdate,
)
from labos.mcp.schema import build_input_model, build_tool_definition, get_context_parameter

ToolFn = TypeVar("ToolFn", bound=Callable[..., Any])


@dataclass(frozen=True)
class _ToolRegistration:
    fn: Callable[..., Any]
    definition: ToolDefinition
    input_model: type
    context_parameter: str | None
    is_async: bool


class RemoteMCP:
    """Register local tools and expose them through a remote WebSocket relay."""

    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        *,
        device_id: str | None = None,
        name: str = "labos-device",
        reconnect: bool = True,
        ping_interval: float = 20.0,
        heartbeat_interval: float = 15.0,
        idempotency_cache_size: int = 256,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        dotenv = _load_dotenv()
        resolved_url = _resolve_config_value(url, "LABOS_URL", dotenv)
        resolved_api_key = _resolve_config_value(api_key, "LABOS_API_KEY", dotenv)
        if resolved_url is None:
            raise ValueError("RemoteMCP requires a url argument or LABOS_URL in the environment or .env file.")
        if resolved_api_key is None:
            raise ValueError("RemoteMCP requires an api_key argument or LABOS_API_KEY in the environment or .env file.")

        self.url = resolved_url
        self.api_key = resolved_api_key
        self.device_id = _resolve_config_value(device_id, "LABOS_DEVICE_ID", dotenv) or _default_device_id()
        self.name = name
        self.reconnect = reconnect
        self.ping_interval = ping_interval
        self.heartbeat_interval = heartbeat_interval
        self.idempotency_cache_size = idempotency_cache_size
        self.metadata = metadata or {}
        self._registry: dict[str, _ToolRegistration] = {}
        self._inflight: dict[str, asyncio.Task[None]] = {}
        self._contexts: dict[str, Context] = {}
        self._active_ws: Any | None = None
        self._idempotency_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._running_call_ids: set[str] = set()
        self._inflight_replay_ws: dict[str, list[Any]] = {}
        self._approval_requests: dict[str, asyncio.Future[ToolApprovalResponse]] = {}
        self._upload_url_requests: dict[str, asyncio.Future[ToolUploadUrlResponse]] = {}
        self._startup_hooks: list[Callable[[], Any]] = []
        self._shutdown_hooks: list[Callable[[], Any]] = []
        self._connect_hooks: list[Callable[[Any], Any]] = []
        self._disconnect_hooks: list[Callable[[Any], Any]] = []

    @property
    def tools(self) -> list[ToolDefinition]:
        return [registration.definition for registration in self._registry.values()]

    def tool(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        risk: Literal["low", "medium", "high"] = "low",
        user_confirmation: bool = False,
    ) -> Callable[[ToolFn], ToolFn]:
        def decorator(fn: ToolFn) -> ToolFn:
            self._register_tool(fn, name=name, description=description, risk=risk, user_confirmation=user_confirmation)
            return fn

        return decorator

    async def add_tool(
        self,
        fn: ToolFn,
        *,
        name: str | None = None,
        description: str | None = None,
        risk: Literal["low", "medium", "high"] = "low",
        user_confirmation: bool = False,
    ) -> ToolFn:
        self._register_tool(fn, name=name, description=description, risk=risk, user_confirmation=user_confirmation)
        await self._send_tools_update()
        return fn

    async def remove_tool(self, name: str) -> None:
        self._registry.pop(name, None)
        await self._send_tools_update()

    def on_startup(self, fn: ToolFn) -> ToolFn:
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: ToolFn) -> ToolFn:
        self._shutdown_hooks.append(fn)
        return fn

    def on_connect(self, fn: ToolFn) -> ToolFn:
        self._connect_hooks.append(fn)
        return fn

    def on_disconnect(self, fn: ToolFn) -> ToolFn:
        self._disconnect_hooks.append(fn)
        return fn

    def run(self) -> None:
        asyncio.run(self.run_async())

    async def run_async(self) -> None:
        await self._call_startup_hooks()
        try:
            backoff = 1.0
            while True:
                try:
                    await self._connect_once()
                    backoff = 1.0
                    if not self.reconnect:
                        return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if not self.reconnect:
                        raise
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            await self._call_shutdown_hooks()

    async def _connect_once(self) -> None:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        async with _connect_websocket(self.url, headers=headers, ping_interval=self.ping_interval) as ws:
            heartbeat_task: asyncio.Task[None] | None = None
            try:
                self._active_ws = ws
                await self._register_tools(ws)
                await self._call_connect_hooks(ws)
                heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                async for raw_message in ws:
                    await self._handle_message(raw_message, ws)
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    await asyncio.gather(heartbeat_task, return_exceptions=True)
                await self._cancel_inflight()
                await self._call_disconnect_hooks(ws)
                if self._active_ws is ws:
                    self._active_ws = None

    async def _register_tools(self, ws: Any) -> None:
        message = ToolsRegister(
            device_id=self.device_id,
            name=self.name,
            tools=self.tools,
            metadata=self.metadata,
        )
        await _send_json(ws, message.model_dump(mode="json", exclude_none=True))

    async def _handle_message(self, raw_message: str | bytes, ws: Any) -> None:
        payload = raw_message if isinstance(raw_message, dict) else json.loads(raw_message)
        message = TypeAdapter(Message).validate_python(payload)

        if isinstance(message, ToolCall):
            task = asyncio.create_task(self._handle_tool_call(message, ws))
            self._inflight[message.id] = task
            task.add_done_callback(lambda _task, call_id=message.id: self._inflight.pop(call_id, None))
        elif isinstance(message, ToolCancel):
            task = self._inflight.get(message.id)
            context = self._contexts.get(message.id)
            if context is not None:
                context.cancel()
            if task is not None:
                task.cancel()
        elif isinstance(message, ToolApprovalResponse):
            future = self._approval_requests.pop(message.request_id, None)
            if future is not None and not future.done():
                future.set_result(message)
        elif isinstance(message, ToolUploadUrlResponse):
            future = self._upload_url_requests.pop(message.request_id, None)
            if future is not None and not future.done():
                future.set_result(message)

    async def _handle_tool_call(self, call: ToolCall, ws: Any) -> None:
        if call.id in self._idempotency_cache:
            await _send_json(ws, self._idempotency_cache[call.id])
            self._idempotency_cache.move_to_end(call.id)
            return
        if call.id in self._running_call_ids:
            self._inflight_replay_ws.setdefault(call.id, []).append(ws)
            return

        registration = self._registry.get(call.tool)
        if registration is None:
            await self._send_error(ws, call.id, "not_found", f"Unknown tool: {call.tool}", cache=True)
            return

        self._running_call_ids.add(call.id)
        cancel_event = threading.Event()
        context = Context(
            call_id=call.id,
            ws=ws,
            cancel_event=cancel_event,
            approval_resolver=lambda request, timeout, prompt: self._send_approval_request(ws, request, timeout, prompt),
            upload_url_resolver=lambda request: self._send_upload_url_request(ws, request),
        )
        self._contexts[call.id] = context
        try:
            validated = registration.input_model.model_validate(call.arguments)
            kwargs = validated.model_dump()
            if registration.context_parameter is not None:
                kwargs[registration.context_parameter] = context

            execution = self._execute_tool(registration, kwargs)
            if call.timeout_ms is not None:
                result = await asyncio.wait_for(execution, timeout=call.timeout_ms / 1000)
            else:
                result = await execution

            payload = ToolResult(id=call.id, result=result).model_dump(mode="json")
            self._cache_response(call.id, payload)
            await self._send_response_to_call_waiters(ws, call.id, payload)
        except asyncio.CancelledError:
            context.cancel()
            await self._send_error(ws, call.id, "cancelled", "Tool call was cancelled", cache=True)
        except TimeoutError:
            context.cancel()
            await self._send_error(ws, call.id, "timeout", "Tool call timed out", cache=True)
        except ValidationError as exc:
            await self._send_error(ws, call.id, "invalid_arguments", str(exc), details={"errors": exc.errors()}, cache=True)
        except Exception as exc:
            await self._send_error(ws, call.id, "tool_error", str(exc), cache=True)
        finally:
            self._contexts.pop(call.id, None)
            self._running_call_ids.discard(call.id)

    async def _execute_tool(self, registration: _ToolRegistration, kwargs: dict[str, Any]) -> Any:
        if registration.is_async:
            return await registration.fn(**kwargs)
        return await asyncio.to_thread(registration.fn, **kwargs)

    async def _cancel_inflight(self) -> None:
        tasks = list(self._inflight.values())
        for context in list(self._contexts.values()):
            context.cancel()
        self._inflight.clear()
        self._contexts.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_error(
        self,
        ws: Any,
        call_id: str,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        cache: bool = False,
    ) -> None:
        error = ToolError(id=call_id, error=ToolErrorPayload(code=code, message=message, details=details))
        payload = error.model_dump(mode="json", exclude_none=True)
        if cache:
            self._cache_response(call_id, payload)
        await self._send_response_to_call_waiters(ws, call_id, payload)

    def _register_tool(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None,
        description: str | None,
        risk: Literal["low", "medium", "high"],
        user_confirmation: bool,
    ) -> None:
        definition = build_tool_definition(
            fn,
            name=name,
            description=description,
            risk=risk,
            user_confirmation=user_confirmation,
        )
        self._registry[definition.name] = _ToolRegistration(
            fn=fn,
            definition=definition,
            input_model=build_input_model(fn),
            context_parameter=get_context_parameter(fn),
            is_async=inspect.iscoroutinefunction(fn),
        )

    async def _send_tools_update(self) -> None:
        if self._active_ws is None:
            return
        message = ToolsUpdate(device_id=self.device_id, name=self.name, tools=self.tools, metadata=self.metadata)
        await _send_json(self._active_ws, message.model_dump(mode="json", exclude_none=True))

    async def _heartbeat_loop(self, ws: Any) -> None:
        while True:
            await _send_json(ws, ToolHeartbeat().model_dump(mode="json"))
            await asyncio.sleep(self.heartbeat_interval)

    async def _wait_for_approval_response(self, request_id: str, timeout: float, prompt: str) -> ToolApprovalResponse:
        future: asyncio.Future[ToolApprovalResponse] = asyncio.get_running_loop().create_future()
        self._approval_requests[request_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            self._approval_requests.pop(request_id, None)
            raise

    async def _send_approval_request(
        self,
        ws: Any,
        request: ToolApprovalRequest,
        timeout: float,
        prompt: str,
    ) -> ToolApprovalResponse:
        future: asyncio.Future[ToolApprovalResponse] = asyncio.get_running_loop().create_future()
        self._approval_requests[request.request_id] = future
        await _send_json(ws, request.model_dump(mode="json"))
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            self._approval_requests.pop(request.request_id, None)
            raise

    async def _send_upload_url_request(self, ws: Any, request: ToolUploadUrlRequest) -> ToolUploadUrlResponse:
        future: asyncio.Future[ToolUploadUrlResponse] = asyncio.get_running_loop().create_future()
        self._upload_url_requests[request.request_id] = future
        await _send_json(ws, request.model_dump(mode="json"))
        try:
            return await future
        finally:
            self._upload_url_requests.pop(request.request_id, None)

    async def _wait_for_upload_url_response(self, request_id: str) -> ToolUploadUrlResponse:
        future: asyncio.Future[ToolUploadUrlResponse] = asyncio.get_running_loop().create_future()
        self._upload_url_requests[request_id] = future
        try:
            return await future
        finally:
            self._upload_url_requests.pop(request_id, None)

    def _cache_response(self, call_id: str, payload: dict[str, Any]) -> None:
        self._idempotency_cache[call_id] = payload
        self._idempotency_cache.move_to_end(call_id)
        while len(self._idempotency_cache) > self.idempotency_cache_size:
            self._idempotency_cache.popitem(last=False)

    async def _send_response_to_call_waiters(self, ws: Any, call_id: str, payload: dict[str, Any]) -> None:
        await _send_json(ws, payload)
        replay_targets = self._inflight_replay_ws.pop(call_id, [])
        for replay_ws in replay_targets:
            await _send_json(replay_ws, payload)

    async def _call_startup_hooks(self) -> None:
        await _call_hooks(self._startup_hooks)

    async def _call_shutdown_hooks(self) -> None:
        await _call_hooks(self._shutdown_hooks)

    async def _call_connect_hooks(self, ws: Any) -> None:
        await _call_hooks(self._connect_hooks, ws)

    async def _call_disconnect_hooks(self, ws: Any) -> None:
        await _call_hooks(self._disconnect_hooks, ws)


async def _send_json(ws: Any, payload: dict[str, Any]) -> None:
    send: Callable[[str], Awaitable[None]] = ws.send
    await send(json.dumps(payload))


async def _call_hooks(hooks: list[Callable[..., Any]], *args: Any) -> None:
    for hook in hooks:
        result = hook(*args)
        if inspect.isawaitable(result):
            await result


def _connect_websocket(url: str, *, headers: dict[str, str], ping_interval: float) -> Any:
    try:
        return websockets.connect(url, additional_headers=headers, ping_interval=ping_interval)
    except TypeError as exc:
        if "additional_headers" not in str(exc):
            raise
        return websockets.connect(url, extra_headers=headers, ping_interval=ping_interval)


def _default_device_id() -> str:
    return f"{socket.gethostname()}-{uuid.getnode():x}"


def _resolve_config_value(explicit: str | None, env_var: str, dotenv: dict[str, str]) -> str | None:
    if explicit is not None:
        return explicit
    return os.environ.get(env_var) or dotenv.get(env_var)


def _load_dotenv(path: str | Path = ".env") -> dict[str, str]:
    dotenv_path = Path(path)
    if not dotenv_path.is_file():
        return {}

    values: dict[str, str] = {}
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].lstrip()

        key, separator, raw_value = stripped.partition("=")
        if not separator:
            continue

        key = key.strip()
        if not key:
            continue

        values[key] = _parse_dotenv_value(raw_value.strip())
    return values


def _parse_dotenv_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value.split(" #", 1)[0].rstrip()
