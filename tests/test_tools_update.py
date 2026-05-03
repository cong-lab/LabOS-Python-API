from labos.mcp import RemoteMCP


async def test_add_and_remove_tool_update_local_registry_when_disconnected() -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")

    def add(a: int, b: int) -> int:
        return a + b

    await mcp.add_tool(add)
    assert [tool.name for tool in mcp.tools] == ["add"]

    await mcp.remove_tool("add")
    assert mcp.tools == []


async def test_add_and_remove_tool_emit_tools_update_when_connected(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", device_id="device-1")
    mcp._active_ws = fake_ws

    def add(a: int, b: int) -> int:
        return a + b

    await mcp.add_tool(add)
    await mcp.remove_tool("add")

    messages = fake_ws.json_messages()
    assert messages[0]["type"] == "tools.update"
    assert messages[0]["tools"][0]["name"] == "add"
    assert messages[1]["type"] == "tools.update"
    assert messages[1]["tools"] == []
