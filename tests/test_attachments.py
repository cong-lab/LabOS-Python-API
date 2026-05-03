import base64
import asyncio


async def test_send_bytes_inline_emits_attachment_start(make_context, fake_ws) -> None:
    ctx = make_context()

    attachment = await ctx.send_bytes(b"abc", name="scan.txt", mime_type="text/plain", transport="inline")

    [message] = fake_ws.json_messages()
    assert attachment.id == message["attachment"]["id"]
    assert message["type"] == "tool.attachment.start"
    assert message["data_base64"] == base64.b64encode(b"abc").decode("ascii")


async def test_send_bytes_chunked_emits_start_chunks_and_end(make_context, fake_ws) -> None:
    ctx = make_context(chunk_size=2)

    attachment = await ctx.send_bytes(b"abcde", name="frame.bin", mime_type="application/octet-stream", transport="chunked")

    messages = fake_ws.json_messages()
    assert [message["type"] for message in messages] == [
        "tool.attachment.start",
        "tool.attachment.chunk",
        "tool.attachment.chunk",
        "tool.attachment.chunk",
        "tool.attachment.end",
    ]
    reassembled = b"".join(base64.b64decode(message["data_base64"]) for message in messages[1:4])
    assert reassembled == b"abcde"
    assert messages[-1]["attachment_id"] == attachment.id


async def test_send_bytes_upload_requests_url_and_emits_attachment_reference(make_context, fake_ws) -> None:
    uploaded: list[tuple[str, bytes, dict[str, str]]] = []

    async def upload(url: str, data: bytes, headers: dict[str, str]) -> None:
        uploaded.append((url, data, headers))

    ctx = make_context(upload=upload)
    fake_ws.queue_json(
        {
            "type": "tool.upload_url.response",
            "request_id": "upload-1",
            "url": "http://localhost/upload",
            "headers": {"x-demo": "1"},
        }
    )

    attachment = await ctx.send_bytes(
        b"payload",
        name="payload.bin",
        mime_type="application/octet-stream",
        transport="upload",
        request_id="upload-1",
    )

    assert uploaded == [("http://localhost/upload", b"payload", {"x-demo": "1"})]
    assert fake_ws.json_messages()[0]["type"] == "tool.upload_url.request"
    assert fake_ws.json_messages()[1]["type"] == "tool.attachment.start"
    assert fake_ws.json_messages()[1]["url"] == "http://localhost/upload"
    assert attachment.id == fake_ws.json_messages()[1]["attachment"]["id"]


async def test_send_file_reads_temp_file(tmp_path, make_context, fake_ws) -> None:
    file_path = tmp_path / "scan.bin"
    file_path.write_bytes(b"scan")
    ctx = make_context()

    attachment = await ctx.send_file(file_path, mime_type="application/octet-stream", transport="inline")

    assert attachment.name == "scan.bin"
    assert fake_ws.json_messages()[0]["attachment"]["size_bytes"] == 4


async def test_client_registers_upload_future_before_sending_request(fake_ws) -> None:
    from labos.mcp import Context, RemoteMCP
    from labos.mcp.protocol import ToolCall

    uploaded: list[tuple[str, bytes, dict[str, str]]] = []
    mcp = RemoteMCP(url="ws://localhost:8765/remote", api_key="dev-key")

    async def upload(url: str, data: bytes, headers: dict[str, str]) -> None:
        uploaded.append((url, data, headers))

    @mcp.tool()
    async def capture(ctx: Context) -> str:
        ctx._upload = upload
        attachment = await ctx.send_bytes(
            b"payload",
            name="payload.bin",
            mime_type="application/octet-stream",
            transport="upload",
        )
        return attachment.id

    task = asyncio.create_task(mcp._handle_tool_call(ToolCall(id="call-1", tool="capture"), fake_ws))
    while not fake_ws.messages:
        await asyncio.sleep(0)
    request = fake_ws.json_messages()[0]

    assert request["type"] == "tool.upload_url.request"
    assert request["request_id"] in mcp._upload_url_requests

    await mcp._handle_message(
        {
            "type": "tool.upload_url.response",
            "request_id": request["request_id"],
            "url": "http://localhost/upload",
            "headers": {"x-demo": "1"},
        },
        fake_ws,
    )
    await task

    assert uploaded == [("http://localhost/upload", b"payload", {"x-demo": "1"})]
