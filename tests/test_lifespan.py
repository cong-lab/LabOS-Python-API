from labos.mcp import RemoteMCP


async def test_startup_and_shutdown_hooks_fire_around_run_async(monkeypatch) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", reconnect=False)
    events: list[str] = []

    @mcp.on_startup
    async def startup() -> None:
        events.append("startup")

    @mcp.on_shutdown
    async def shutdown() -> None:
        events.append("shutdown")

    async def connect_once() -> None:
        events.append("connect_once")

    monkeypatch.setattr(mcp, "_connect_once", connect_once)

    await mcp.run_async()

    assert events == ["startup", "connect_once", "shutdown"]


async def test_connect_and_disconnect_hooks_receive_websocket(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")
    events: list[tuple[str, object]] = []

    @mcp.on_connect
    async def connected(ws) -> None:
        events.append(("connect", ws))

    @mcp.on_disconnect
    async def disconnected(ws) -> None:
        events.append(("disconnect", ws))

    await mcp._call_connect_hooks(fake_ws)
    await mcp._call_disconnect_hooks(fake_ws)

    assert events == [("connect", fake_ws), ("disconnect", fake_ws)]
