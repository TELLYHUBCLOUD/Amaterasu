from asyncio import CancelledError, create_task, gather, sleep
from base64 import b64encode
from contextlib import contextmanager
from json import loads
from os import getenv
from pathlib import Path
from struct import error as StructError
from time import monotonic

from httpx import AsyncClient, AsyncHTTPTransport, ConnectError, ConnectTimeout, HTTPError, Timeout
from pyrogram.raw.types import InputFileBig
from pyrogram.file_id import FileId, FileType


class HostUploadUncertain(RuntimeError):
    pass


class GoStreamInterrupted(OSError):
    pass


class GoFloodWait(Exception):
    def __init__(self, seconds, source):
        super().__init__(f"Telegram cooldown ({seconds}s)")
        self.seconds = seconds
        self.source = source


def record_go_flood(client, seconds):
    if not isinstance(seconds, int) or seconds <= 0:
        return
    until = monotonic() + seconds
    client._amaterasu_go_flood_until = max(
        until, getattr(client, "_amaterasu_go_flood_until", 0),
    )
    client._amaterasu_send_flood_until = max(
        until, getattr(client, "_amaterasu_send_flood_until", 0),
    )
    for name in ("_amaterasu_download_pacer", "_amaterasu_upload_pacer"):
        if (pacer := getattr(client, name, None)) is not None:
            pacer.flood_until = max(until, pacer.flood_until)


async def wait_go_cooldown(client):
    while (delay := getattr(client, "_amaterasu_go_flood_until", 0) - monotonic()) > 0:
        await sleep(delay)


def retry_hoster_error(error):
    return isinstance(error, Exception) and not isinstance(error, HostUploadUncertain)


@contextmanager
def hoster_receipt(status):
    try:
        yield
    except Exception as error:
        if 200 <= status < 300:
            raise HostUploadUncertain(
                f"Hoster accepted the upload (HTTP {status}), but its result could not be confirmed"
            ) from error
        raise


async def cancel_hoster(uploader):
    if task := getattr(uploader, "_go_hoster_task", None):
        task.cancel()
        await gather(task, return_exceptions=True)


def bot_auth(client):
    token = getattr(client, "bot_token", None)
    if not token or getattr(client, "proxy", None):
        return None
    return {"app_id": client.api_id, "app_hash": client.api_hash, "token": token}


def media_request(client, file_id, size, start, end):
    auth = bot_auth(client)
    if auth is None:
        return None
    location = FileId.decode(file_id) if isinstance(file_id, str) else file_id
    if location.file_type == FileType.CHAT_PHOTO:
        return None
    return {
        **auth,
        "dc": location.dc_id,
        "kind": "photo" if location.file_type == FileType.PHOTO else "document",
        "id": location.media_id,
        "access_hash": location.access_hash,
        "file_reference": b64encode(location.file_reference or b"").decode("ascii"),
        "thumb_size": location.thumbnail_size or "",
        "size": size,
        "start": start,
        "end": end,
    }


def go_client(read_timeout=15):
    socket = getenv("AMATERASU_GO_SOCKET", "/tmp/amaterasu_go.sock")
    return AsyncClient(
        transport=AsyncHTTPTransport(uds=socket),
        base_url="http://amaterasu-go",
        timeout=Timeout(read_timeout, connect=1),
    )


async def stream_range(client, file_id, size, start, end):
    remaining = end - start + 1
    sent = 0
    try:
        if getattr(client, "_amaterasu_go_flood_until", 0) > monotonic():
            return
        payload = media_request(client, file_id, size, start, end)
        if payload is None:
            return
        async with go_client() as sidecar:
            async with sidecar.stream("POST", "/v1/stream", json=payload) as response:
                if response.status_code == 429:
                    record_go_flood(client, int(response.headers.get("retry-after", "0")))
                    return
                if (
                    response.status_code != 206
                    or response.headers.get("content-range") != f"bytes {start}-{end}/{size}"
                    or response.headers.get("content-length") != str(remaining)
                ):
                    return
                async for chunk in response.aiter_bytes():
                    if len(chunk) > remaining - sent:
                        raise OSError("Go sent bytes beyond the requested range")
                    sent += len(chunk)
                    yield chunk
                if sent != remaining:
                    raise OSError("Go ended the range early")
    except (HTTPError, OSError, ValueError, StructError) as error:
        if 0 < sent < remaining:
            # The range may have failed after its HTTP headers were committed.
            try:
                async with go_client() as sidecar:
                    status = await sidecar.post("/v1/cooldown", json=payload, timeout=1)
                if status.status_code == 429:
                    record_go_flood(client, int(status.headers.get("retry-after", "0")))
            except (HTTPError, OSError, ValueError):
                pass
            raise GoStreamInterrupted("Go range interrupted") from error


async def recover_go_range(source, first, start, end, resume, on_failure):
    offset = start + len(first)
    yield first
    try:
        async for chunk in source:
            offset += len(chunk)
            yield chunk
    except GoStreamInterrupted:
        if offset <= end:
            on_failure(offset)
            async for chunk in resume(offset):
                yield chunk


async def run_task(task, payload, progress, *, nonrepeatable=False):
    try:
        async with go_client(3600 if nonrepeatable else 30) as sidecar:
            async with sidecar.stream("POST", f"/v1/tasks/{task}", json=payload) as response:
                if response.status_code != 200:
                    if not nonrepeatable or response.status_code in {400, 404}:
                        return None
                else:
                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        event = loads(line[6:])
                        status = event.get("status")
                        if status == "running":
                            await progress(event["processed_bytes"], event["total_bytes"])
                        elif status == "complete":
                            return event["result"]
                        elif status == "cooldown":
                            raise GoFloodWait(event["retry_after"], event["source"])
                        elif status == "failed":
                            break
    except (ConnectError, ConnectTimeout):
        return None
    except (HTTPError, OSError, ValueError, KeyError):
        if nonrepeatable:
            raise HostUploadUncertain("Go hoster upload outcome is unknown") from None
    if nonrepeatable:
        raise HostUploadUncertain("Go hoster upload did not complete")
    return None


async def upload_parts(client, filename, progress):
    auth = bot_auth(client)
    if not auth or not isinstance(filename, str):
        return None
    if getattr(client, "_amaterasu_go_flood_until", 0) > monotonic():
        return None
    path = Path(filename).absolute()
    try:
        if not path.is_file() or not 10 * 1024**2 < path.stat().st_size <= 2 * 1024**3:
            return None
    except OSError:
        return None
    try:
        result = await run_task("hyperul", {**auth, "path": str(path)}, progress)
    except GoFloodWait as wait:
        record_go_flood(client, wait.seconds)
        return None
    if result is not None:
        return InputFileBig(id=result["id"], parts=result["parts"], name=result["name"])
    return None


async def upload_hoster(provider, url, path, uploader, callback, *,
                        field="", filename="", fields=None, authorization=""):
    if getenv("USE_GO_ENGINE", "true").strip().lower() not in {"1", "true", "yes", "on"}:
        return None

    async def report(current, _total):
        if uploader.listener.is_cancelled:
            raise CancelledError()
        callback(current)

    file = Path(path).absolute()
    if uploader.listener.is_cancelled:
        raise CancelledError()
    transfer = create_task(run_task("uphoster", {
        "provider": provider, "url": url, "path": str(file), "field": field,
        "filename": filename, "fields": fields, "authorization": authorization,
    }, report, nonrepeatable=True))
    uploader._go_hoster_task = transfer
    try:
        result = await transfer
    finally:
        uploader._go_hoster_task = None
    if uploader.listener.is_cancelled:
        raise CancelledError()
    if result is not None and 200 <= result["code"] < 300:
        try:
            callback(file.stat().st_size)
        except OSError:
            pass
    return result
