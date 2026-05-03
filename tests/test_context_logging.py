import json

import pytest

from labos.mcp.exceptions import ToolFatalError


async def test_context_warning_sends_tool_log(make_context, fake_ws) -> None:
    ctx = make_context()

    await ctx.warning("overheating", temp_c=92.4)

    assert fake_ws.json_messages()[0]["type"] == "tool.log"
    assert fake_ws.json_messages()[0]["level"] == "warning"
    assert fake_ws.json_messages()[0]["fields"] == {"temp_c": 92.4}


async def test_context_info_debug_error_levels_round_trip(make_context, fake_ws) -> None:
    ctx = make_context()

    await ctx.debug("debug")
    await ctx.info("info")
    await ctx.error("error")

    assert [message["level"] for message in fake_ws.json_messages()] == ["debug", "info", "error"]


async def test_context_fatal_sends_tool_fatal_and_raises(make_context, fake_ws) -> None:
    ctx = make_context()

    with pytest.raises(ToolFatalError):
        await ctx.fatal("collision", "crashed into object", speed=0.4)

    message = json.loads(fake_ws.messages[0])
    assert message["type"] == "tool.fatal"
    assert message["code"] == "collision"
    assert message["fields"] == {"speed": 0.4}


async def test_context_emit_sends_tool_event(make_context, fake_ws) -> None:
    ctx = make_context()

    await ctx.emit("sample.loaded", {"slot": 3})

    assert fake_ws.json_messages()[0] == {
        "type": "tool.event",
        "id": "call-1",
        "name": "sample.loaded",
        "payload": {"slot": 3},
    }
