"""Wire protocol models for LabOS remote tools."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


class ProtocolModel(BaseModel):
    """Base model configured for strict protocol payloads."""

    model_config = {"extra": "forbid"}


class ToolDefinition(ProtocolModel):
    name: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None = None
    risk: Literal["low", "medium", "high"] = "low"
    user_confirmation: bool = False


class Attachment(ProtocolModel):
    id: str
    name: str
    mime_type: str
    size_bytes: int
    sha256: str | None = None


class ToolsRegister(ProtocolModel):
    type: Literal["tools.register"] = "tools.register"
    device_id: str
    name: str | None = None
    tools: list[ToolDefinition]
    metadata: dict[str, Any] = Field(default_factory=dict)


class ToolCall(ProtocolModel):
    type: Literal["tool.call"] = "tool.call"
    id: str
    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    timeout_ms: int | None = None


class ToolResult(ProtocolModel):
    type: Literal["tool.result"] = "tool.result"
    id: str
    status: Literal["ok"] = "ok"
    result: Any = None
    attachments: list[Attachment] = Field(default_factory=list)


class ToolErrorPayload(ProtocolModel):
    code: str = "tool_error"
    message: str
    details: dict[str, Any] | None = None


class ToolError(ProtocolModel):
    type: Literal["tool.error"] = "tool.error"
    id: str
    status: Literal["error"] = "error"
    error: ToolErrorPayload


class ToolProgress(ProtocolModel):
    type: Literal["tool.progress"] = "tool.progress"
    id: str
    value: float = Field(ge=0.0, le=1.0)
    message: str | None = None


class ToolLog(ProtocolModel):
    type: Literal["tool.log"] = "tool.log"
    id: str | None = None
    level: Literal["debug", "info", "warning", "error"] = "info"
    message: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    fields: dict[str, Any] = Field(default_factory=dict)


class ToolFatal(ProtocolModel):
    type: Literal["tool.fatal"] = "tool.fatal"
    id: str | None = None
    code: str
    message: str
    fields: dict[str, Any] = Field(default_factory=dict)


class ToolEvent(ProtocolModel):
    type: Literal["tool.event"] = "tool.event"
    id: str | None = None
    name: str
    payload: dict[str, Any] = Field(default_factory=dict)


class ToolAttachmentStart(ProtocolModel):
    type: Literal["tool.attachment.start"] = "tool.attachment.start"
    id: str
    attachment: Attachment
    data_base64: str | None = None
    url: str | None = None


class ToolAttachmentChunk(ProtocolModel):
    type: Literal["tool.attachment.chunk"] = "tool.attachment.chunk"
    id: str
    attachment_id: str
    seq: int
    data_base64: str


class ToolAttachmentEnd(ProtocolModel):
    type: Literal["tool.attachment.end"] = "tool.attachment.end"
    id: str
    attachment_id: str


class ToolUploadUrlRequest(ProtocolModel):
    type: Literal["tool.upload_url.request"] = "tool.upload_url.request"
    id: str
    request_id: str
    attachment: Attachment


class ToolUploadUrlResponse(ProtocolModel):
    type: Literal["tool.upload_url.response"] = "tool.upload_url.response"
    id: str | None = None
    request_id: str
    url: str
    headers: dict[str, str] = Field(default_factory=dict)


class ToolApprovalRequest(ProtocolModel):
    type: Literal["tool.approval.request"] = "tool.approval.request"
    id: str
    request_id: str
    prompt: str
    risk: Literal["low", "medium", "high"] = "medium"
    expires_at: datetime


class ToolApprovalResponse(ProtocolModel):
    type: Literal["tool.approval.response"] = "tool.approval.response"
    request_id: str
    approved: bool
    approver: str | None = None


class ToolHeartbeat(ProtocolModel):
    type: Literal["tool.heartbeat"] = "tool.heartbeat"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    fields: dict[str, Any] = Field(default_factory=dict)


class ToolCancel(ProtocolModel):
    type: Literal["tool.cancel"] = "tool.cancel"
    id: str
    reason: str | None = None


class ToolsUpdate(ProtocolModel):
    type: Literal["tools.update"] = "tools.update"
    device_id: str
    name: str | None = None
    tools: list[ToolDefinition]
    metadata: dict[str, Any] = Field(default_factory=dict)


class Hello(ProtocolModel):
    type: Literal["hello"] = "hello"
    session_token: str | None = None


Message = Annotated[
    ToolsRegister
    | ToolsUpdate
    | ToolCall
    | ToolResult
    | ToolError
    | ToolProgress
    | ToolLog
    | ToolFatal
    | ToolEvent
    | ToolAttachmentStart
    | ToolAttachmentChunk
    | ToolAttachmentEnd
    | ToolUploadUrlRequest
    | ToolUploadUrlResponse
    | ToolApprovalRequest
    | ToolApprovalResponse
    | ToolHeartbeat
    | ToolCancel
    | Hello,
    Field(discriminator="type"),
]
