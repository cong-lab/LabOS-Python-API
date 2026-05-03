"""Per-call context helpers for remote tool functions."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import mimetypes
import threading
import urllib.request
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from labos.mcp.exceptions import ApprovalTimeoutError, ToolFatalError
from labos.mcp.protocol import (
    Attachment,
    Message,
    ToolApprovalRequest,
    ToolApprovalResponse,
    ToolAttachmentChunk,
    ToolAttachmentEnd,
    ToolAttachmentStart,
    ToolEvent,
    ToolFatal,
    ToolLog,
    ToolProgress,
    ToolUploadUrlRequest,
    ToolUploadUrlResponse,
)

UploadFn = Callable[[str, bytes, dict[str, str]], Awaitable[None]]
ApprovalResolver = Callable[[ToolApprovalRequest, float, str], Awaitable[ToolApprovalResponse]]
UploadUrlResolver = Callable[[ToolUploadUrlRequest], Awaitable[ToolUploadUrlResponse]]


class Context:
    """Context injected into tools that opt in with a `Context` parameter."""

    def __init__(
        self,
        call_id: str,
        ws: Any,
        *,
        deadline: datetime | None = None,
        cancel_event: threading.Event | None = None,
        chunk_size: int = 64 * 1024,
        inline_threshold: int = 256 * 1024,
        chunked_threshold: int = 50 * 1024 * 1024,
        upload: UploadFn | None = None,
        approval_resolver: ApprovalResolver | None = None,
        upload_url_resolver: UploadUrlResolver | None = None,
    ) -> None:
        self.call_id = call_id
        self._ws = ws
        self.deadline = deadline
        self._cancel_event = cancel_event or threading.Event()
        self._cancel_callbacks: list[Callable[[], None]] = []
        self._chunk_size = chunk_size
        self._inline_threshold = inline_threshold
        self._chunked_threshold = chunked_threshold
        self._upload = upload or _default_upload
        self._approval_resolver = approval_resolver
        self._upload_url_resolver = upload_url_resolver

    @property
    def cancelled(self) -> bool:
        return self._cancel_event.is_set()

    def on_cancel(self, callback: Callable[[], None]) -> None:
        self._cancel_callbacks.append(callback)

    def cancel(self) -> None:
        if self._cancel_event.is_set():
            return
        self._cancel_event.set()
        for callback in list(self._cancel_callbacks):
            callback()

    async def progress(self, value: float, message: str | None = None) -> None:
        payload = ToolProgress(id=self.call_id, value=value, message=message)
        await self._send(payload.model_dump(mode="json", exclude_none=True))

    async def log(self, level: str, message: str, **fields: Any) -> None:
        payload = ToolLog(id=self.call_id, level=level, message=message, fields=fields)  # type: ignore[arg-type]
        await self._send(payload.model_dump(mode="json", exclude_none=True))

    async def debug(self, message: str, **fields: Any) -> None:
        await self.log("debug", message, **fields)

    async def info(self, message: str, **fields: Any) -> None:
        await self.log("info", message, **fields)

    async def warning(self, message: str, **fields: Any) -> None:
        await self.log("warning", message, **fields)

    async def error(self, message: str, **fields: Any) -> None:
        await self.log("error", message, **fields)

    async def fatal(self, code: str, message: str, **fields: Any) -> None:
        payload = ToolFatal(id=self.call_id, code=code, message=message, fields=fields)
        await self._send(payload.model_dump(mode="json", exclude_none=True))
        raise ToolFatalError(message)

    async def emit(self, name: str, payload: dict[str, Any] | None = None) -> None:
        event = ToolEvent(id=self.call_id, name=name, payload=payload or {})
        await self._send(event.model_dump(mode="json", exclude_none=True))

    async def send_file(
        self,
        path: str | Path,
        *,
        name: str | None = None,
        mime_type: str | None = None,
        transport: str = "auto",
    ) -> Attachment:
        file_path = Path(path)
        data = file_path.read_bytes()
        return await self.send_bytes(
            data,
            name=name or file_path.name,
            mime_type=mime_type or mimetypes.guess_type(file_path.name)[0] or "application/octet-stream",
            transport=transport,
        )

    async def send_bytes(
        self,
        data: bytes,
        *,
        name: str,
        mime_type: str,
        transport: str = "auto",
        request_id: str | None = None,
    ) -> Attachment:
        attachment = Attachment(
            id=uuid.uuid4().hex,
            name=name,
            mime_type=mime_type,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )
        selected_transport = self._select_transport(len(data), transport)

        if selected_transport == "inline":
            await self._send(
                ToolAttachmentStart(
                    id=self.call_id,
                    attachment=attachment,
                    data_base64=base64.b64encode(data).decode("ascii"),
                ).model_dump(mode="json", exclude_none=True)
            )
        elif selected_transport == "chunked":
            await self._send(ToolAttachmentStart(id=self.call_id, attachment=attachment).model_dump(mode="json"))
            for seq, offset in enumerate(range(0, len(data), self._chunk_size)):
                chunk = data[offset : offset + self._chunk_size]
                await self._send(
                    ToolAttachmentChunk(
                        id=self.call_id,
                        attachment_id=attachment.id,
                        seq=seq,
                        data_base64=base64.b64encode(chunk).decode("ascii"),
                    ).model_dump(mode="json")
                )
            await self._send(ToolAttachmentEnd(id=self.call_id, attachment_id=attachment.id).model_dump(mode="json"))
        elif selected_transport == "upload":
            upload_response = await self._request_upload_url(attachment, request_id=request_id)
            await self._upload(upload_response.url, data, upload_response.headers)
            await self._send(
                ToolAttachmentStart(
                    id=self.call_id,
                    attachment=attachment,
                    url=upload_response.url,
                ).model_dump(mode="json", exclude_none=True)
            )
        else:
            raise ValueError(f"Unknown attachment transport: {transport}")

        return attachment

    async def request_approval(self, prompt: str, *, risk: str = "medium", timeout: float = 60.0) -> bool:
        request_id = uuid.uuid4().hex
        request = ToolApprovalRequest(
            id=self.call_id,
            request_id=request_id,
            prompt=prompt,
            risk=risk,  # type: ignore[arg-type]
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=timeout),
        )

        try:
            if self._approval_resolver is not None:
                return (await self._approval_resolver(request, timeout, prompt)).approved
            await self._send(request.model_dump(mode="json"))
            while True:
                response = TypeAdapter(Message).validate_python(json.loads(await asyncio.wait_for(self._ws.recv(), timeout)))
                if isinstance(response, ToolApprovalResponse) and response.request_id == request_id:
                    return response.approved
        except TimeoutError as exc:
            raise ApprovalTimeoutError(f"Approval request timed out: {prompt}") from exc

    def _select_transport(self, size_bytes: int, transport: str) -> str:
        if transport != "auto":
            return transport
        if size_bytes <= self._inline_threshold:
            return "inline"
        if size_bytes <= self._chunked_threshold:
            return "chunked"
        return "upload"

    async def _request_upload_url(self, attachment: Attachment, *, request_id: str | None = None) -> ToolUploadUrlResponse:
        actual_request_id = request_id or uuid.uuid4().hex
        request = ToolUploadUrlRequest(id=self.call_id, request_id=actual_request_id, attachment=attachment)

        if self._upload_url_resolver is not None:
            return await self._upload_url_resolver(request)

        await self._send(request.model_dump(mode="json"))
        while True:
            response = TypeAdapter(Message).validate_python(json.loads(await self._ws.recv()))
            if isinstance(response, ToolUploadUrlResponse) and response.request_id == actual_request_id:
                return response

    async def _send(self, payload: dict[str, Any]) -> None:
        await self._ws.send(json.dumps(payload))


async def _default_upload(url: str, data: bytes, headers: dict[str, str]) -> None:
    def upload() -> None:
        request = urllib.request.Request(url, data=data, headers=headers, method="PUT")
        with urllib.request.urlopen(request, timeout=60):
            pass

    await asyncio.to_thread(upload)
