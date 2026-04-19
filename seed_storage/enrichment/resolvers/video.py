"""Video resolver — download → ffmpeg audio extraction → transcription.

Handles generic video URLs (not YouTube). Downloads to temp file, extracts
audio with ffmpeg, then transcribes with Whisper or AssemblyAI.
Temp files are always cleaned up in the finally block.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_VIDEO_EXTENSIONS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".wmv", ".m4v"}
_TIMEOUT_DOWNLOAD = 120.0
_TIMEOUT_TRANSCRIBE = 300.0  # Whisper can be slow


def _has_video_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in _VIDEO_EXTENSIONS)


def _get_transcription_backend() -> str:
    try:
        from seed_storage.config import settings

        return settings.TRANSCRIPTION_BACKEND
    except Exception:  # noqa: BLE001
        return "whisper"


class VideoResolver(BaseResolver):
    """Downloads video, extracts audio, and transcribes using Whisper/AssemblyAI."""

    def can_handle(self, url: str) -> bool:
        return _has_video_extension(url)

    async def resolve(self, url: str) -> ResolvedContent:
        try:
            return await self._resolve_internal(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("VideoResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))

    async def _resolve_internal(self, url: str) -> ResolvedContent:
        import asyncio

        loop = asyncio.get_event_loop()

        # Download video
        video_tmp = await self._download_video(url)
        audio_tmp = None

        try:
            # Extract audio with ffmpeg
            audio_tmp = await loop.run_in_executor(None, self._extract_audio, video_tmp)

            # Transcribe
            backend = _get_transcription_backend()
            if backend == "assemblyai":
                transcript = await loop.run_in_executor(
                    None, self._transcribe_assemblyai, audio_tmp
                )
            else:
                transcript = await loop.run_in_executor(None, self._transcribe_whisper, audio_tmp)
        finally:
            # Always clean up temp files
            for tmp in (video_tmp, audio_tmp):
                if tmp is not None:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:  # noqa: BLE001
                        pass

        return ResolvedContent(
            source_url=url,
            content_type="video",
            title=None,
            text=transcript or "",
            transcript=transcript,
            summary=None,
            expansion_urls=[],
            metadata={"transcription_backend": _get_transcription_backend()},
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )

    async def _download_video(self, url: str) -> Path:
        """Download video to a temp file. Returns Path to the temp file."""
        suffix = Path(urlparse(url).path).suffix or ".mp4"
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=_TIMEOUT_DOWNLOAD,
            ) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    with open(tmp_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=65536):
                            f.write(chunk)
        except Exception:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass
            raise

        return tmp_path

    def _extract_audio(self, video_path: Path) -> Path:
        """Use ffmpeg to extract audio to a WAV file."""
        import subprocess

        audio_tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        audio_path = Path(audio_tmp.name)
        audio_tmp.close()

        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-i",
                    str(video_path),
                    "-ar",
                    "16000",
                    "-ac",
                    "1",
                    "-f",
                    "wav",
                    "-y",
                    str(audio_path),
                ],
                capture_output=True,
                timeout=120,
            )
            if result.returncode != 0:
                stderr = result.stderr.decode("utf-8", errors="replace")
                if "invalid data" in stderr.lower() or "no such decoder" in stderr.lower():
                    raise ValueError(f"Unsupported video codec: {stderr[:200]}")
                raise ValueError(f"ffmpeg failed (exit {result.returncode}): {stderr[:200]}")
        except subprocess.TimeoutExpired:
            raise ValueError("ffmpeg audio extraction timed out")
        except FileNotFoundError:
            raise ValueError("ffmpeg not found — install ffmpeg")

        return audio_path

    def _transcribe_whisper(self, audio_path: Path) -> str:
        """Transcribe audio file using OpenAI Whisper (local model)."""
        import whisper  # type: ignore[import-untyped]

        model = whisper.load_model("base")
        result = model.transcribe(str(audio_path), fp16=False)
        return result.get("text", "").strip()

    def _transcribe_assemblyai(self, audio_path: Path) -> str:
        """Transcribe audio file using AssemblyAI API."""
        import assemblyai as aai  # type: ignore[import-untyped]

        try:
            from seed_storage.config import settings

            aai.settings.api_key = settings.ASSEMBLYAI_API_KEY
        except Exception:  # noqa: BLE001
            pass

        transcriber = aai.Transcriber()
        transcript = transcriber.transcribe(str(audio_path))
        if transcript.error:
            raise ValueError(f"AssemblyAI transcription error: {transcript.error}")
        return transcript.text or ""
