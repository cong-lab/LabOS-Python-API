from datetime import datetime, timezone

from pydantic import TypeAdapter

from labos.mcp.protocol import (
    Attachment,
    Message,
    ToolApprovalRequest,
    ToolEvent,
    ToolHeartbeat,
    ToolLog,
    ToolResult,
)


def test_message_parser_accepts_new_protocol_messages() -> None:
    message = TypeAdapter(Message).validate_python(
        {
            "type": "tool.log",
            "id": "call-1",
            "level": "warning",
            "message": "overheating",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fields": {"temp_c": 92.4},
        }
    )

    assert isinstance(message, ToolLog)
    assert message.level == "warning"
    assert message.fields == {"temp_c": 92.4}


def test_tool_result_defaults_to_empty_attachments() -> None:
    result = ToolResult(id="call-1", result={"ok": True})

    assert result.attachments == []
    assert result.model_dump()["attachments"] == []


def test_tool_result_serializes_attachment_payload() -> None:
    attachment = Attachment(id="att-1", name="scan.png", mime_type="image/png", size_bytes=3)
    result = ToolResult(id="call-1", result={"summary": "ok"}, attachments=[attachment])

    assert result.model_dump(mode="json")["attachments"] == [
        {"id": "att-1", "name": "scan.png", "mime_type": "image/png", "size_bytes": 3, "sha256": None}
    ]


def test_event_approval_and_heartbeat_parse_through_message_union() -> None:
    event = TypeAdapter(Message).validate_python({"type": "tool.event", "id": "call-1", "name": "sample.loaded"})
    approval = TypeAdapter(Message).validate_python(
        {
            "type": "tool.approval.request",
            "id": "call-1",
            "request_id": "approval-1",
            "prompt": "Move robot?",
            "risk": "high",
            "expires_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    heartbeat = TypeAdapter(Message).validate_python({"type": "tool.heartbeat"})

    assert isinstance(event, ToolEvent)
    assert event.payload == {}
    assert isinstance(approval, ToolApprovalRequest)
    assert isinstance(heartbeat, ToolHeartbeat)
