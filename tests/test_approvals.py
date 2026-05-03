import asyncio

import pytest

from labos.mcp.exceptions import ApprovalTimeoutError
from labos.mcp import RemoteMCP, Context
from labos.mcp.protocol import ToolCall


async def test_request_approval_resolves_true(make_context, fake_ws) -> None:
    ctx = make_context()

    async def responder() -> None:
        while not fake_ws.messages:
            await asyncio.sleep(0)
        request = fake_ws.json_messages()[0]
        fake_ws.queue_json(
            {
                "type": "tool.approval.response",
                "request_id": request["request_id"],
                "approved": True,
                "approver": "operator",
            }
        )

    task = asyncio.create_task(responder())
    assert await ctx.request_approval("Move robot?", risk="high", timeout=0.5) is True
    await task


async def test_request_approval_resolves_false(make_context, fake_ws) -> None:
    ctx = make_context()

    async def responder() -> None:
        while not fake_ws.messages:
            await asyncio.sleep(0)
        request = fake_ws.json_messages()[0]
        fake_ws.queue_json({"type": "tool.approval.response", "request_id": request["request_id"], "approved": False})

    task = asyncio.create_task(responder())
    assert await ctx.request_approval("Move robot?", timeout=0.5) is False
    await task


async def test_request_approval_times_out(make_context) -> None:
    ctx = make_context()

    with pytest.raises(ApprovalTimeoutError):
        await ctx.request_approval("Move robot?", timeout=0.01)


async def test_client_registers_approval_future_before_sending_request(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")

    @mcp.tool()
    async def guarded(ctx: Context) -> str:
        approved = await ctx.request_approval("Move robot?", timeout=0.5)
        return "approved" if approved else "denied"

    task = asyncio.create_task(mcp._handle_tool_call(ToolCall(id="call-1", tool="guarded"), fake_ws))
    while not fake_ws.messages:
        await asyncio.sleep(0)
    request = fake_ws.json_messages()[0]

    assert request["type"] == "tool.approval.request"
    assert request["request_id"] in mcp._approval_requests

    await mcp._handle_message(
        {
            "type": "tool.approval.response",
            "request_id": request["request_id"],
            "approved": True,
        },
        fake_ws,
    )
    await task

    assert fake_ws.json_messages()[-1]["result"] == "approved"
