import json

from labos.mcp import Context


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, message: str) -> None:
        self.messages.append(message)


async def test_context_progress_sends_tool_progress_message() -> None:
    ws = FakeWebSocket()
    ctx = Context(call_id="call-1", ws=ws)

    await ctx.progress(0.5, "halfway")

    assert json.loads(ws.messages[0]) == {
        "type": "tool.progress",
        "id": "call-1",
        "value": 0.5,
        "message": "halfway",
    }
