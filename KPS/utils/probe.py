# KPS/utils/probe.py

"""
Track Probe Service - Discovers audio/video/subtitle tracks in media files.

Downloads the first few MB of a Telegram file (via ByteStreamer) into a temp
file, runs ``ffprobe -show_streams`` on it, and returns structured track info.

Results are cached in-memory per ``(message_id, secure_hash)`` to avoid
re-probing the same file on every page load.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
from typing import TYPE_CHECKING, Any

from KPS.utils.logger import logger

if TYPE_CHECKING:
    from KPS.utils.custom_dl import ByteStreamer

# 5 MB is enough for the header / metadata of most container formats
PROBE_DOWNLOAD_SIZE = 5 * 1024 * 1024

# In-memory cache: {(message_id, secure_hash): tracks_info_dict}
_probe_cache: dict[tuple[int, str], dict[str, Any]] = {}
_probe_locks: dict[tuple[int, str], asyncio.Lock] = {}

# ISO 639-2/B -> display name mapping (matches FileToLink1 reference)
_LANG_NAMES = {
    'eng': 'English', 'hin': 'Hindi', 'jpn': 'Japanese',
    'kor': 'Korean', 'spa': 'Spanish', 'fre': 'French',
    'fra': 'French', 'ger': 'German', 'deu': 'German',
    'ita': 'Italian', 'por': 'Portuguese', 'rus': 'Russian',
    'ara': 'Arabic', 'chi': 'Chinese', 'zho': 'Chinese',
    'tam': 'Tamil', 'tel': 'Telugu', 'kan': 'Kannada',
    'mal': 'Malayalam', 'ben': 'Bengali', 'mar': 'Marathi',
    'guj': 'Gujarati', 'pan': 'Punjabi', 'urd': 'Urdu',
    'tha': 'Thai', 'vie': 'Vietnamese', 'ind': 'Indonesian',
    'tur': 'Turkish', 'pol': 'Polish', 'dut': 'Dutch',
    'nld': 'Dutch', 'swe': 'Swedish', 'nor': 'Norwegian',
    'dan': 'Danish', 'fin': 'Finnish', 'cze': 'Czech',
    'ces': 'Czech', 'gre': 'Greek', 'ell': 'Greek',
    'heb': 'Hebrew', 'rum': 'Romanian', 'ron': 'Romanian',
    'hun': 'Hungarian', 'fil': 'Filipino', 'und': 'Unknown',
}

# Text-based subtitle codecs (convertible to WebVTT for browser <track>)
TEXT_SUB_CODECS = {
    'subrip', 'srt', 'ass', 'ssa', 'webvtt', 'vtt', 'mov_text', 'text',
    'microdvd', 'jacosub', 'sami', 'realtext', 'stl', 'pjs', 'mpl2',
}

# Bitmap-based codecs that can NOT be converted to WebVTT
BITMAP_SUB_CODECS = {
    'hdmv_pgs_subtitle', 'pgs', 'dvb_subtitle', 'dvb_teletext',
    'dvd_subtitle', 'xsub',
}


def _empty_result(error: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "video_tracks": [],
        "audio_tracks": [],
        "subtitle_tracks": [],
        "has_multiple_audio": False,
        "has_subtitles": False,
    }
    if error:
        result["error"] = error
    return result


async def probe_tracks(
    streamer: ByteStreamer,
    message_id: int,
    secure_hash: str,
    file_size: int,
) -> dict[str, Any]:
    """Probe a Telegram-hosted media file for audio/video/subtitle tracks."""
    cache_key = (message_id, secure_hash)
    cached = _probe_cache.get(cache_key)
    if cached is not None and not cached.get('error'):
        return cached

    lock = _probe_locks.setdefault(cache_key, asyncio.Lock())
    async with lock:
        cached = _probe_cache.get(cache_key)
        if cached is not None and not cached.get('error'):
            return cached

        result = await _probe_to_tempfile(streamer, message_id, file_size)
        if not result.get('error'):
            _probe_cache[cache_key] = result
        return result


async def _probe_to_tempfile(
    streamer: ByteStreamer,
    message_id: int,
    file_size: int,
) -> dict[str, Any]:
    """Download a small head of the file, then run ffprobe on it."""
    temp_path: str | None = None
    try:
        fd, temp_path = tempfile.mkstemp(suffix=".bin")
        os.close(fd)

        probe_size = min(PROBE_DOWNLOAD_SIZE, file_size)
        downloaded = 0
        try:
            with open(temp_path, 'wb') as f:
                async for chunk in streamer.stream_file(
                    message_id, offset=0, limit=probe_size
                ):
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded >= probe_size:
                        break
        except Exception as dl_err:
            logger.warning(
                "Probe download failed for msg %s: %s: %s",
                message_id, type(dl_err).__name__, dl_err,
            )
            return _empty_result(
                f"Download failed: {type(dl_err).__name__}: {dl_err}"
            )

        logger.debug(
            "Probe download complete: %d KB for msg %s",
            downloaded // 1024, message_id,
        )

        if downloaded == 0:
            return _empty_result("Downloaded 0 bytes from Telegram")

        return await _run_ffprobe(temp_path)
    except Exception as e:
        logger.error(
            "Track probing failed for msg %s: %s: %s",
            message_id, type(e).__name__, e, exc_info=True,
        )
        return _empty_result(f"{type(e).__name__}: {e or 'Unknown error'}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def _run_ffprobe(file_path: str) -> dict[str, Any]:
    """Run ffprobe on a file and parse the output into structured track info."""
    cmd = [
        'ffprobe',
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_streams',
        '-show_format',
        file_path,
    ]

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            timeout=30.0,
            check=False,
        )
    except FileNotFoundError:
        return _empty_result("ffprobe not found on PATH (install ffmpeg)")
    except Exception as e:
        logger.error("ffprobe execution failed: %s", e)
        return _empty_result(f"ffprobe execution failed: {e}")

    if result.returncode != 0:
        error_msg = result.stderr.decode(errors='replace').strip()
        logger.warning("ffprobe failed (code %s): %s", result.returncode, error_msg)
        return _empty_result(f"ffprobe error: {error_msg or 'non-zero exit'}")

    try:
        probe_data = json.loads(result.stdout.decode(errors='replace'))
    except json.JSONDecodeError as e:
        logger.error("Failed to parse ffprobe output: %s", e)
        return _empty_result("Failed to parse ffprobe output")

    return _parse_probe_data(probe_data)


def _parse_probe_data(probe_data: dict[str, Any]) -> dict[str, Any]:
    """Parse raw ffprobe JSON into organized track lists."""
    video_tracks: list[dict[str, Any]] = []
    audio_tracks: list[dict[str, Any]] = []
    subtitle_tracks: list[dict[str, Any]] = []

    audio_index = 0
    subtitle_index = 0

    for stream in probe_data.get('streams', []):
        codec_type = stream.get('codec_type', '')
        tags = stream.get('tags') or {}
        language = (tags.get('language') or 'und').lower()
        title = tags.get('title', '')

        if codec_type == 'video':
            video_tracks.append({
                "index": stream.get('index', 0),
                "codec": stream.get('codec_name', 'unknown'),
                "width": stream.get('width', 0),
                "height": stream.get('height', 0),
                "fps": _parse_fps(stream.get('r_frame_rate', '0/1')),
                "language": language,
                "title": title,
            })
        elif codec_type == 'audio':
            audio_tracks.append({
                "index": stream.get('index', 0),
                "audio_index": audio_index,
                "codec": stream.get('codec_name', 'unknown'),
                "channels": stream.get('channels', 2),
                "channel_layout": stream.get('channel_layout', ''),
                "sample_rate": stream.get('sample_rate', ''),
                "bit_rate": stream.get('bit_rate', ''),
                "language": language,
                "title": title,
                "label": _build_audio_label(language, title, stream),
            })
            audio_index += 1
        elif codec_type == 'subtitle':
            codec = stream.get('codec_name', 'unknown').lower()
            kind = (
                'text' if codec in TEXT_SUB_CODECS
                else 'bitmap' if codec in BITMAP_SUB_CODECS
                else 'unknown'
            )
            subtitle_tracks.append({
                "index": stream.get('index', 0),
                "subtitle_index": subtitle_index,
                "codec": codec,
                "kind": kind,
                "language": language,
                "title": title,
                "label": _build_subtitle_label(language, title, codec, kind),
            })
            subtitle_index += 1

    return {
        "video_tracks": video_tracks,
        "audio_tracks": audio_tracks,
        "subtitle_tracks": subtitle_tracks,
        "has_multiple_audio": len(audio_tracks) > 1,
        "has_subtitles": len(subtitle_tracks) > 0,
    }


def _lang_display(language: str) -> str:
    if not language:
        return 'Unknown'
    return _LANG_NAMES.get(language.lower(), language.upper())


def _should_show_title(title: str, language: str, lang_display: str) -> bool:
    """Skip titles that are just the language repeated."""
    if not title:
        return False
    t = title.strip().lower()
    if not t:
        return False
    if t == language.lower() or t == lang_display.lower():
        return False
    return True


def _build_audio_label(language: str, title: str, stream: dict[str, Any]) -> str:
    """Build a user-friendly label like ``English (AAC 5.1)``."""
    lang_display = _lang_display(language)
    codec = (stream.get('codec_name') or '').upper()
    channels = stream.get('channels', 2) or 2

    if channels >= 8:
        ch_label = '7.1'
    elif channels >= 6:
        ch_label = '5.1'
    elif channels == 2:
        ch_label = 'Stereo'
    elif channels == 1:
        ch_label = 'Mono'
    else:
        ch_label = f'{channels}ch'

    parts = [lang_display]
    if _should_show_title(title, language, lang_display):
        parts.append(f'- {title}')
    if codec:
        parts.append(f'({codec} {ch_label})')
    else:
        parts.append(f'({ch_label})')

    return ' '.join(parts)


def _build_subtitle_label(
    language: str, title: str, codec: str, kind: str,
) -> str:
    """Build a user-friendly label for a subtitle track."""
    lang_display = _lang_display(language)
    parts = [lang_display]
    if _should_show_title(title, language, lang_display):
        parts.append(f'- {title}')
    if kind == 'bitmap':
        parts.append(f'({codec.upper()}, image-based)')
    elif codec:
        parts.append(f'({codec.upper()})')
    return ' '.join(parts)


def _parse_fps(fps_str: str) -> float:
    """Parse ffmpeg frame rate string like '24000/1001' into a float."""
    try:
        if '/' in fps_str:
            num, den = fps_str.split('/')
            denom = float(den)
            if denom == 0:
                return 0.0
            return round(float(num) / denom, 2)
        return float(fps_str)
    except (ValueError, ZeroDivisionError):
        return 0.0
