import json
import asyncio

from labos.mcp import Context, RemoteMCP
from labos.mcp.protocol import ToolCall


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        raise StopAsyncIteration


def test_tool_decorator_registers_schema_from_function() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    [tool] = mcp.tools

    assert tool.name == "add"
    assert tool.description == "Add two numbers."
    assert tool.input_schema["required"] == ["a", "b"]
    assert tool.output_schema["type"] == "integer"


async def test_handle_tool_call_sends_result_for_sync_tool() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    ws = FakeWebSocket()

    @mcp.tool()
    def add(a: int, b: int) -> int:
        """Add two numbers."""
        return a + b

    await mcp._handle_tool_call(ToolCall(id="call-1", tool="add", arguments={"a": 2, "b": 3}), ws)

    assert json.loads(ws.messages[0]) == {
        "type": "tool.result",
        "id": "call-1",
        "status": "ok",
        "result": 5,
        "attachments": [],
    }


async def test_handle_tool_call_injects_context_and_streams_progress() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    ws = FakeWebSocket()

    @mcp.tool()
    async def long_running(steps: int, ctx: Context) -> str:
        """Report progress."""
        await ctx.progress(1 / steps, "started")
        return "done"

    await mcp._handle_tool_call(ToolCall(id="call-1", tool="long_running", arguments={"steps": 2}), ws)

    assert json.loads(ws.messages[0]) == {
        "type": "tool.progress",
        "id": "call-1",
        "value": 0.5,
        "message": "started",
    }
    assert json.loads(ws.messages[1]) == {
        "type": "tool.result",
        "id": "call-1",
        "status": "ok",
        "result": "done",
        "attachments": [],
    }


async def test_handle_tool_call_sends_error_for_unknown_tool() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    ws = FakeWebSocket()

    await mcp._handle_tool_call(ToolCall(id="call-1", tool="missing", arguments={}), ws)

    payload = json.loads(ws.messages[0])
    assert payload["type"] == "tool.error"
    assert payload["id"] == "call-1"
    assert payload["error"]["code"] == "not_found"


async def test_handle_tool_call_enforces_timeout_ms() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    ws = FakeWebSocket()

    @mcp.tool()
    async def slow() -> str:
        """Run slowly."""
        await asyncio.sleep(1)
        return "done"

    await mcp._handle_tool_call(ToolCall(id="call-1", tool="slow", timeout_ms=1), ws)

    payload = json.loads(ws.messages[0])
    assert payload["type"] == "tool.error"
    assert payload["error"]["code"] == "timeout"


async def test_connect_once_falls_back_to_legacy_websockets_header_keyword(monkeypatch) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    calls: list[dict[str, object]] = []

    class FakeConnection:
        async def __aenter__(self) -> FakeWebSocket:
            return FakeWebSocket()

        async def __aexit__(self, *args: object) -> None:
            return None

    def fake_connect(*args: object, **kwargs: object) -> FakeConnection:
        calls.append(kwargs)
        if "additional_headers" in kwargs:
            raise TypeError("unexpected keyword argument 'additional_headers'")
        return FakeConnection()

    monkeypatch.setattr("labos.mcp.client.websockets.connect", fake_connect)

    await mcp._connect_once()

    assert "additional_headers" in calls[0]
    assert calls[1]["extra_headers"] == {"Authorization": "Bearer dev-key"}


def test_constructor_reads_defaults_from_dotenv(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LABOS_URL", raising=False)
    monkeypatch.delenv("LABOS_API_KEY", raising=False)
    monkeypatch.delenv("LABOS_DEVICE_ID", raising=False)
    tmp_path.joinpath(".env").write_text(
        "\n".join(
            [
                "LABOS_URL=ws://dotenv.example/remote",
                "LABOS_API_KEY=dotenv-key",
                "LABOS_DEVICE_ID=dotenv-device",
            ]
        ),
        encoding="utf-8",
    )

    mcp = RemoteMCP()

    assert mcp.url == "ws://dotenv.example/remote"
    assert mcp.api_key == "dotenv-key"
    assert mcp.device_id == "dotenv-device"


def test_constructor_arguments_override_dotenv(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LABOS_URL", raising=False)
    monkeypatch.delenv("LABOS_API_KEY", raising=False)
    monkeypatch.delenv("LABOS_DEVICE_ID", raising=False)
    tmp_path.joinpath(".env").write_text(
        "\n".join(
            [
                "LABOS_URL=ws://dotenv.example/remote",
                "LABOS_API_KEY=dotenv-key",
                "LABOS_DEVICE_ID=dotenv-device",
            ]
        ),
        encoding="utf-8",
    )

    mcp = RemoteMCP(url="ws://argument.example/remote", api_key="argument-key", device_id="argument-device")

    assert mcp.url == "ws://argument.example/remote"
    assert mcp.api_key == "argument-key"
    assert mcp.device_id == "argument-device"
