"""Example LabOS remote tool client.

Run this after starting `examples/example_server.py`.
"""

from __future__ import annotations

import asyncio
import time
from typing import Literal

from labos.mcp import Context, RemoteMCP


mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key-123", device_id="demo-device", heartbeat_interval=1)


@mcp.on_startup
async def open_hardware() -> None:
    print("opening demo hardware...")


@mcp.on_shutdown
async def close_hardware() -> None:
    print("closing demo hardware...")


@mcp.on_connect
async def add_runtime_tool(_ws) -> None:
    await mcp.add_tool(multiply, description="Multiply two numbers added after initial registration.")


def multiply(a: int, b: int) -> int:
    return a * b


@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers."""
    return a + b


@mcp.tool(risk="low")
def read_temperature(units: Literal["C", "F"] = "C") -> float:
    """Read the current temperature from the lab sensor."""
    return 21.5 if units == "C" else 70.7


@mcp.tool()
async def long_running(steps: int, ctx: Context) -> str:
    """Demo of streaming progress."""
    for index in range(steps):
        await ctx.progress((index + 1) / steps, f"step {index + 1}/{steps}")
        await asyncio.sleep(0.25)
    return "done"


@mcp.tool()
async def scan_inline(ctx: Context) -> dict:
    """Capture a tiny scan and report a warning."""
    await ctx.info("starting inline scan")
    await ctx.warning("lamp temperature high", temp_c=92.4)
    attachment = await ctx.send_bytes(b"inline scan", name="inline.txt", mime_type="text/plain", transport="inline")
    await ctx.emit("scan.complete", {"transport": "inline", "attachment_id": attachment.id})
    return {"summary": "ok", "attachments": [attachment.id]}


@mcp.tool()
async def scan_chunked(ctx: Context) -> dict:
    """Capture a chunked binary payload."""
    attachment = await ctx.send_bytes(
        b"chunked scan payload",
        name="chunked.bin",
        mime_type="application/octet-stream",
        transport="chunked",
    )
    return {"summary": "ok", "attachments": [attachment.id]}


@mcp.tool()
async def scan_upload(ctx: Context) -> dict:
    """Capture an uploaded binary payload."""
    attachment = await ctx.send_bytes(
        b"uploaded scan payload",
        name="uploaded.bin",
        mime_type="application/octet-stream",
        transport="upload",
    )
    return {"summary": "ok", "attachments": [attachment.id]}


@mcp.tool(risk="high", user_confirmation=True)
async def move_to(x: float, y: float, ctx: Context) -> str:
    """Move a robot after operator approval."""
    if not await ctx.request_approval(f"Move arm to ({x}, {y})?", risk="high", timeout=30):
        raise PermissionError("rejected by operator")
    return f"moved to ({x}, {y})"


@mcp.tool()
def long_blocking(ctx: Context) -> int:
    """Demo of sync cancellation via ctx.cancelled."""
    iterations = 0
    while not ctx.cancelled:
        time.sleep(0.05)
        iterations += 1
    return iterations


@mcp.tool()
async def simulate_fatal(ctx: Context) -> str:
    """Report a fatal robot condition."""
    await ctx.fatal("collision", "crashed into object", axis="x")
    return "unreachable"


if __name__ == "__main__":
    mcp.run()
