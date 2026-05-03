import asyncio

from labos.mcp import RemoteMCP


async def test_heartbeat_task_emits_message_and_cancels_cleanly(fake_ws) -> None:
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key", heartbeat_interval=0.01)

    task = asyncio.create_task(mcp._heartbeat_loop(fake_ws))
    await asyncio.sleep(0.03)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    messages = fake_ws.json_messages()
    assert any(message["type"] == "tool.heartbeat" for message in messages)
