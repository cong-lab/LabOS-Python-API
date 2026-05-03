import asyncio
import json
import time

from labos.mcp import Context, RemoteMCP
from labos.mcp.protocol import ToolCall, ToolCancel


async def test_sync_tool_can_poll_context_cancelled(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")

    @mcp.tool()
    def blocking(ctx: Context) -> str:
        while not ctx.cancelled:
            time.sleep(0.01)
        return "cancelled"

    task = asyncio.create_task(mcp._handle_tool_call(ToolCall(id="call-1", tool="blocking"), fake_ws))
    await asyncio.sleep(0.03)
    task.cancel()
    await task

    assert json.loads(fake_ws.messages[0])["error"]["code"] == "cancelled"


async def test_on_cancel_callback_invoked_once(make_context) -> None:
    calls = 0
    ctx = make_context()

    def callback() -> None:
        nonlocal calls
        calls += 1

    ctx.on_cancel(callback)
    ctx.cancel()
    ctx.cancel()

    assert ctx.cancelled is True
    assert calls == 1


async def test_tool_cancel_message_cancels_inflight_sync_tool(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")

    @mcp.tool()
    def blocking(ctx: Context) -> str:
        while not ctx.cancelled:
            time.sleep(0.01)
        return "cancelled"

    await mcp._handle_message(
        '{"type":"tool.call","id":"call-1","tool":"blocking","arguments":{}}',
        fake_ws,
    )
    await asyncio.sleep(0.03)
    await mcp._handle_message(ToolCancel(id="call-1").model_dump_json(), fake_ws)
    await asyncio.sleep(0.03)

    assert json.loads(fake_ws.messages[0])["error"]["code"] == "cancelled"
