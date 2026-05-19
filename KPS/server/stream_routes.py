# KPS/server/stream_routes.py

import asyncio
import re
import secrets
import subprocess
import time
from urllib.parse import quote, unquote

from aiohttp import web

from KPS import __version__, StartTime
from KPS.bot import StreamBot, multi_clients, work_loads
from KPS.server.exceptions import FileNotFound, InvalidHash
from KPS.utils.custom_dl import ByteStreamer
from KPS.utils.logger import logger
from KPS.utils.probe import probe_tracks
from KPS.utils.render_template import render_page
from KPS.utils.time_format import get_readable_time

routes = web.RouteTableDef()

SECURE_HASH_LENGTH = 6
CHUNK_SIZE = 1024 * 1024
MAX_CONCURRENT_PER_CLIENT = 8
RANGE_REGEX = re.compile(r"bytes=(?P<start>\d*)-(?P<end>\d*)")
PATTERN_HASH_FIRST = re.compile(
    rf"^([a-zA-Z0-9_-]{{{SECURE_HASH_LENGTH}}})(\d+)(?:/.*)?$")
PATTERN_ID_FIRST = re.compile(r"^(\d+)(?:/.*)?$")
VALID_HASH_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

streamers = {}

# In-memory cache for extracted WebVTT subtitles:
# {(message_id, secure_hash, subtitle_index): bytes}
_subtitle_cache: dict[tuple[int, str, int], bytes] = {}
_subtitle_locks: dict[tuple[int, str, int], asyncio.Lock] = {}


def get_streamer(client_id: int) -> ByteStreamer:
    if client_id not in streamers:
        streamers[client_id] = ByteStreamer(multi_clients[client_id])
    return streamers[client_id]


def parse_media_request(path: str, query: dict) -> tuple[int, str]:
    clean_path = unquote(path).strip('/')

    match = PATTERN_HASH_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(2))
            secure_hash = match.group(1)
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    match = PATTERN_ID_FIRST.match(clean_path)
    if match:
        try:
            message_id = int(match.group(1))
            secure_hash = query.get("hash", "").strip()
            if (len(secure_hash) == SECURE_HASH_LENGTH and
                    VALID_HASH_REGEX.match(secure_hash)):
                return message_id, secure_hash
            else:
                raise InvalidHash("Invalid or missing hash in query parameter")
        except ValueError as e:
            raise InvalidHash(f"Invalid message ID format in path: {e}") from e

    raise InvalidHash("Invalid URL structure or missing hash")


def select_optimal_client() -> tuple[int, ByteStreamer]:
    if not work_loads:
        raise web.HTTPInternalServerError(
            text=("No available clients to handle the request. "
                  "Please try again later."))

    available_clients = [
        (cid, load) for cid, load in work_loads.items()
        if load < MAX_CONCURRENT_PER_CLIENT]

    if available_clients:
        client_id = min(available_clients, key=lambda x: x[1])[0]
    else:
        client_id = min(work_loads, key=work_loads.get)

    return client_id, get_streamer(client_id)


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    match = RANGE_REGEX.match(range_header)
    if not match:
        raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")

    start_str = match.group("start")
    end_str = match.group("end")
    if start_str:
        start = int(start_str)
        end = int(end_str) if end_str else file_size - 1
    else:
        if not end_str:
            raise web.HTTPBadRequest(text=f"Invalid range header: {range_header}")
        suffix_len = int(end_str)
        if suffix_len <= 0:
            raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{file_size}"})
        start = max(file_size - suffix_len, 0)
        end = file_size - 1

    if start < 0 or end >= file_size or start > end:
        raise web.HTTPRequestRangeNotSatisfiable(
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    return start, end


@routes.get("/", allow_head=True)

async def root_redirect(request):
    raise web.HTTPFound("https://telegram.me/MRVIOLETSTREAMBOT")


@routes.get("/status", allow_head=True)
async def status_endpoint(request):
    uptime = time.time() - StartTime
    total_load = sum(work_loads.values())

    workload_distribution = {str(k): v for k, v in sorted(work_loads.items())}

    return web.json_response({
        "server": {
            "status": "operational",
            "version": __version__,
            "uptime": get_readable_time(uptime)
        },
        "telegram_bot": {
            # "username": f"@{StreamBot.username}",
            "active_clients": len(multi_clients)
        },
        "resources": {
            "total_workload": total_load,
            "workload_distribution": workload_distribution

        }
    })


@routes.get(r"/watch/MRVIOLETSTREAMBOT-{path:.+}", allow_head=True)
async def media_preview(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        rendered_page = await render_page(
            message_id, secure_hash, requested_action='stream')
        return web.Response(text=rendered_page, content_type='text/html')

    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in preview: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:

        error_id = secrets.token_hex(6)
        logger.error(f"Preview error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


@routes.get(r"/api/tracks/MRVIOLETSTREAMBOT-{path:.+}")
async def media_tracks(request: web.Request):
    """Return audio/video/subtitle tracks for a media file as JSON."""
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1
        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found in info.")
            if file_info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash(
                    "Provided hash does not match file's unique ID.")

            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound(
                    "File size is reported as zero or unavailable.")

            tracks = await probe_tracks(
                streamer, message_id, secure_hash, file_size)
            return web.json_response(tracks)
        finally:
            work_loads[client_id] -= 1

    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in /api/tracks: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"/api/tracks error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


@routes.get(r"/remux/MRVIOLETSTREAMBOT-{path:.+}")
async def media_remux(request: web.Request):
    """Stream the file remuxed with a selected audio track (FFmpeg -c copy).

    Query params:
        audio: zero-based audio stream index (default 0)
    """
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        try:
            audio_track = int(request.query.get("audio", "0"))
        except ValueError:
            raise web.HTTPBadRequest(text="Invalid 'audio' query parameter")
        if audio_track < 0 or audio_track > 31:
            raise web.HTTPBadRequest(text="audio index out of range")

        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1

        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found in info.")
            if file_info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash(
                    "Provided hash does not match file's unique ID.")

            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound(
                    "File size is reported as zero or unavailable.")

            filename = (
                file_info.get('file_name') or f"file_{secrets.token_hex(4)}")
            base_name = (
                filename.rsplit('.', 1)[0] if '.' in filename else filename)
            remux_filename = f"{base_name}.mp4"

            headers = {
                "Content-Type": "video/mp4",
                "Content-Disposition":
                    f"inline; filename*=UTF-8''{quote(remux_filename)}",
                "Cache-Control": "no-cache",
                "Accept-Ranges": "none",
                "Connection": "keep-alive",
            }

            response = web.StreamResponse(status=200, headers=headers)
            await response.prepare(request)

            try:
                await _pipe_remux(
                    streamer, message_id, file_size, audio_track, response)
            finally:
                try:
                    await response.write_eof()
                except (ConnectionResetError, asyncio.CancelledError):
                    pass
            return response
        finally:
            work_loads[client_id] -= 1

    except web.HTTPException:
        raise
    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in /remux: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"/remux error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


@routes.get(r"/sub/MRVIOLETSTREAMBOT-{path:.+}")
async def media_subtitle(request: web.Request):
    """Extract the Nth subtitle stream as WebVTT for use in a <track>.

    Query params:
        subtitle: zero-based subtitle stream index (default 0)
    """
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        try:
            sub_track = int(request.query.get("subtitle", "0"))
        except ValueError:
            raise web.HTTPBadRequest(text="Invalid 'subtitle' query parameter")
        if sub_track < 0 or sub_track > 31:
            raise web.HTTPBadRequest(text="subtitle index out of range")

        cache_key = (message_id, secure_hash, sub_track)
        cached = _subtitle_cache.get(cache_key)
        if cached is not None:
            return web.Response(
                body=cached,
                content_type="text/vtt",
                charset="utf-8",
                headers={"Cache-Control": "public, max-age=3600"},
            )

        client_id, streamer = select_optimal_client()
        work_loads[client_id] += 1
        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found in info.")
            if file_info['unique_id'][:SECURE_HASH_LENGTH] != secure_hash:
                raise InvalidHash(
                    "Provided hash does not match file's unique ID.")

            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound(
                    "File size is reported as zero or unavailable.")

            lock = _subtitle_locks.setdefault(cache_key, asyncio.Lock())
            async with lock:
                cached = _subtitle_cache.get(cache_key)
                if cached is None:
                    cached = await _extract_subtitle_to_vtt(
                        streamer, message_id, file_size, sub_track)
                    _subtitle_cache[cache_key] = cached
            return web.Response(
                body=cached,
                content_type="text/vtt",
                charset="utf-8",
                headers={"Cache-Control": "public, max-age=3600"},
            )
        finally:
            work_loads[client_id] -= 1

    except web.HTTPException:
        raise
    except (InvalidHash, FileNotFound) as e:
        logger.debug(
            f"Client error in /sub: {type(e).__name__} - {e}",
            exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"/sub error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"Server error occurred: {error_id}") from e


async def _pipe_remux(
    streamer: ByteStreamer,
    message_id: int,
    file_size: int,
    audio_track: int,
    response: web.StreamResponse,
) -> None:
    """Run ffmpeg with Telegram bytes on stdin, fragmented MP4 to client."""
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", "pipe:0",
        "-map", "0:v:0?",
        "-map", f"0:a:{audio_track}?",
        "-c", "copy",
        "-f", "mp4",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof",
        "pipe:1",
    ]
    logger.info(
        "Starting ffmpeg remux: msg=%s audio=%s size=%.1fMB",
        message_id, audio_track, file_size / 1024 / 1024,
    )

    process = await asyncio.to_thread(
        subprocess.Popen,
        ffmpeg_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def feed_ffmpeg() -> None:
        bytes_fed = 0
        try:
            async for chunk in streamer.stream_file(
                message_id, offset=0, limit=file_size
            ):
                if not chunk:
                    continue
                if process.poll() is not None:
                    break
                await asyncio.to_thread(process.stdin.write, bytes(chunk))
                bytes_fed += len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            logger.warning("remux feed error msg=%s: %s", message_id, e)
        finally:
            try:
                if process.poll() is None:
                    process.stdin.close()
            except Exception:
                pass
            logger.debug(
                "remux feed done msg=%s fed=%.1fMB",
                message_id, bytes_fed / 1024 / 1024,
            )

    feed_task = asyncio.create_task(feed_ffmpeg())

    bytes_sent = 0
    read_size = 256 * 1024
    try:
        while True:
            chunk = await asyncio.to_thread(process.stdout.read, read_size)
            if not chunk:
                break
            try:
                await response.write(chunk)
            except (ConnectionResetError, asyncio.CancelledError):
                break
            bytes_sent += len(chunk)
    finally:
        feed_task.cancel()
        try:
            if process.poll() is None:
                process.kill()
            await asyncio.to_thread(process.wait, 5)
        except Exception:
            pass
        if process.returncode not in (None, 0):
            stderr = b""
            try:
                stderr = process.stderr.read() or b""
            except Exception:
                pass
            logger.warning(
                "ffmpeg exited %s msg=%s: %s",
                process.returncode,
                message_id,
                stderr.decode(errors='replace')[:500],
            )
        try:
            await feed_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        logger.info(
            "remux done msg=%s sent=%.1fMB",
            message_id, bytes_sent / 1024 / 1024,
        )


async def _extract_subtitle_to_vtt(
    streamer: ByteStreamer,
    message_id: int,
    file_size: int,
    subtitle_index: int,
) -> bytes:
    """Pipe Telegram bytes through ffmpeg to extract one subtitle as WebVTT."""
    ffmpeg_cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", "pipe:0",
        "-map", f"0:s:{subtitle_index}",
        "-c:s", "webvtt",
        "-f", "webvtt",
        "pipe:1",
    ]
    logger.info(
        "Starting subtitle extraction: msg=%s sub=%s size=%.1fMB",
        message_id, subtitle_index, file_size / 1024 / 1024,
    )

    process = await asyncio.to_thread(
        subprocess.Popen,
        ffmpeg_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def feed_ffmpeg() -> None:
        try:
            async for chunk in streamer.stream_file(
                message_id, offset=0, limit=file_size
            ):
                if not chunk:
                    continue
                if process.poll() is not None:
                    break
                await asyncio.to_thread(process.stdin.write, bytes(chunk))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            logger.warning("sub feed error msg=%s: %s", message_id, e)
        finally:
            try:
                if process.poll() is None:
                    process.stdin.close()
            except Exception:
                pass

    feed_task = asyncio.create_task(feed_ffmpeg())
    try:
        stdout = await asyncio.to_thread(process.stdout.read)
    finally:
        try:
            await feed_task
        except Exception:
            pass
        try:
            if process.poll() is None:
                process.kill()
            await asyncio.to_thread(process.wait, 5)
        except Exception:
            pass

    if process.returncode not in (None, 0):
        stderr = b""
        try:
            stderr = process.stderr.read() or b""
        except Exception:
            pass
        logger.warning(
            "subtitle ffmpeg exited %s msg=%s: %s",
            process.returncode,
            message_id,
            stderr.decode(errors='replace')[:500],
        )

    if not stdout:
        # Always return a valid (empty) WebVTT so the <track> doesn't 500.
        return b"WEBVTT\n\n"
    return stdout


@routes.get(r"/MRVIOLETSTREAMBOT-{path:.+}", allow_head=True)
async def media_delivery(request: web.Request):
    try:
        path = request.match_info["path"]
        message_id, secure_hash = parse_media_request(path, request.query)

        client_id, streamer = select_optimal_client()

        work_loads[client_id] += 1

        try:
            file_info = await streamer.get_file_info(message_id)
            if not file_info.get('unique_id'):
                raise FileNotFound("File unique ID not found in info.")

            if (file_info['unique_id'][:SECURE_HASH_LENGTH] !=
                    secure_hash):
                raise InvalidHash(
                    "Provided hash does not match file's unique ID.")

            file_size = file_info.get('file_size', 0)
            if file_size == 0:
                raise FileNotFound(
                    "File size is reported as zero or unavailable.")

            range_header = request.headers.get("Range", "")
            start, end = parse_range_header(range_header, file_size)
            content_length = end - start + 1

            if start == 0 and end == file_size - 1:
                range_header = ""

            mime_type = (
                file_info.get('mime_type') or 'application/octet-stream')
            filename = (
                file_info.get('file_name') or f"file_{secrets.token_hex(4)}")

            headers = {
                "Content-Type": mime_type,
                "Content-Length": str(content_length),
                "Content-Disposition": (
                    f"inline; filename*=UTF-8''{quote(filename)}"),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000",
                "Connection": "keep-alive"
            }

            if range_header:
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

            if request.method == 'HEAD':
                work_loads[client_id] -= 1
                return web.Response(
                    status=206 if range_header else 200,
                    headers=headers
                )

            async def stream_generator():
                try:
                    bytes_sent = 0
                    bytes_to_skip = start % CHUNK_SIZE

                    async for chunk in streamer.stream_file(
                            message_id, offset=start, limit=content_length):
                        if bytes_to_skip > 0:
                            if len(chunk) <= bytes_to_skip:
                                bytes_to_skip -= len(chunk)
                                continue
                            chunk = chunk[bytes_to_skip:]
                            bytes_to_skip = 0

                        remaining = content_length - bytes_sent
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]

                        if chunk:
                            yield chunk
                            bytes_sent += len(chunk)

                        if bytes_sent >= content_length:
                            break
                finally:
                    work_loads[client_id] -= 1
            return web.Response(
                status=206 if range_header else 200,
                body=stream_generator(),
                headers=headers
            )

        except (FileNotFound, InvalidHash):
            work_loads[client_id] -= 1
            raise
        except Exception as e:
            work_loads[client_id] -= 1
            error_id = secrets.token_hex(6)
            logger.error(
                f"Stream error {error_id}: {e}",
                exc_info=True)
            raise web.HTTPInternalServerError(
                text=f"Server error during streaming: {error_id}") from e

    except (InvalidHash, FileNotFound) as e:
        logger.debug(f"Client error: {type(e).__name__} - {e}", exc_info=True)
        raise web.HTTPNotFound(text="Resource not found") from e
    except Exception as e:
        error_id = secrets.token_hex(6)
        logger.error(f"Server error {error_id}: {e}", exc_info=True)
        raise web.HTTPInternalServerError(
            text=f"An unexpected server error occurred: {error_id}") from e
