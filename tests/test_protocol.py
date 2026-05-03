from pydantic import TypeAdapter

from datetime import datetime, timezone

from labos.mcp.protocol import Attachment, Message, ToolCall, ToolLog, ToolResult


def test_message_parser_accepts_tool_call() -> None:
    message = TypeAdapter(Message).validate_python(
        {
            "type": "tool.call",
            "id": "call-1",
            "tool": "add",
            "arguments": {"a": 1, "b": 2},
            "timeout_ms": 1000,
        }
    )

    assert isinstance(message, ToolCall)
    assert message.tool == "add"
    assert message.arguments == {"a": 1, "b": 2}


def test_tool_result_serializes_protocol_shape() -> None:
    message = ToolResult(id="call-1", result=3)

    assert message.model_dump() == {
        "type": "tool.result",
        "id": "call-1",
        "status": "ok",
        "result": 3,
        "attachments": [],
    }


def test_message_parser_accepts_tool_log() -> None:
    message = TypeAdapter(Message).validate_python(
        {
            "type": "tool.log",
            "level": "warning",
            "message": "overheating",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    assert isinstance(message, ToolLog)


def test_tool_result_serializes_attachments() -> None:
    attachment = Attachment(id="att-1", name="data.csv", mime_type="text/csv", size_bytes=12)
    message = ToolResult(id="call-1", result={"summary": "ok"}, attachments=[attachment])

    assert message.model_dump()["attachments"][0]["id"] == "att-1"
