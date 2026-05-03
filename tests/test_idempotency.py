import json
import asyncio

from labos.mcp import RemoteMCP
from labos.mcp.protocol import ToolCall


async def test_duplicate_tool_call_returns_cached_result_without_reexecution(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    calls = 0

    @mcp.tool()
    def add(a: int, b: int) -> int:
        nonlocal calls
        calls += 1
        return a + b

    call = ToolCall(id="call-1", tool="add", arguments={"a": 1, "b": 2})
    await mcp._handle_tool_call(call, fake_ws)
    await mcp._handle_tool_call(call, fake_ws)

    assert calls == 1
    assert [json.loads(message)["result"] for message in fake_ws.messages] == [3, 3]


async def test_idempotency_cache_eviction_reexecutes_tool(fake_ws) -> None:
    mcp = RemoteMCP(
        url="ws://localhost:8765/remote",
        api_key="dev-key",
        device_id="device-1",
        idempotency_cache_size=1,
    )
    calls = 0

    @mcp.tool()
    def count() -> int:
        nonlocal calls
        calls += 1
        return calls

    await mcp._handle_tool_call(ToolCall(id="call-1", tool="count"), fake_ws)
    await mcp._handle_tool_call(ToolCall(id="call-2", tool="count"), fake_ws)
    await mcp._handle_tool_call(ToolCall(id="call-1", tool="count"), fake_ws)

    assert [json.loads(message)["result"] for message in fake_ws.messages] == [1, 2, 3]


async def test_duplicate_inflight_tool_call_executes_once(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")
    calls = 0
    release = asyncio.Event()

    @mcp.tool()
    async def slow() -> int:
        nonlocal calls
        calls += 1
        await release.wait()
        return 42

    call = ToolCall(id="call-1", tool="slow")
    first = asyncio.create_task(mcp._handle_tool_call(call, fake_ws))
    await asyncio.sleep(0)
    second = asyncio.create_task(mcp._handle_tool_call(call, fake_ws))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert calls == 1
    assert [json.loads(message)["result"] for message in fake_ws.messages] == [42, 42]
