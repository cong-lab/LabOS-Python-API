from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import pytest

from labos.mcp import Context


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.inbound: asyncio.Queue[str] = asyncio.Queue()

    async def send(self, message: str) -> None:
        self.messages.append(message)

    async def recv(self) -> str:
        return await self.inbound.get()

    async def receive_json(self) -> dict[str, Any]:
        return json.loads(await self.recv())

    def queue_json(self, payload: dict[str, Any]) -> None:
        self.inbound.put_nowait(json.dumps(payload))

    def json_messages(self) -> list[dict[str, Any]]:
        return [json.loads(message) for message in self.messages]

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        try:
            return self.inbound.get_nowait()
        except asyncio.QueueEmpty:
            raise StopAsyncIteration


@pytest.fixture
def fake_ws() -> FakeWebSocket:
    return FakeWebSocket()


@pytest.fixture
def make_context(fake_ws: FakeWebSocket) -> Callable[..., Context]:
    def factory(call_id: str = "call-1", **kwargs: Any) -> Context:
        return Context(call_id=call_id, ws=fake_ws, **kwargs)

    return factory
