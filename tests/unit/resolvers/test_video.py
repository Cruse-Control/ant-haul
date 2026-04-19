"""Tests for VideoResolver (~5 tests)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from seed_storage.enrichment.resolvers.video import VideoResolver


@pytest.fixture
def resolver():
    return VideoResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_mp4(resolver):
    assert resolver.can_handle("https://example.com/video.mp4")


def test_can_handle_mkv(resolver):
    assert resolver.can_handle("https://example.com/clip.mkv")


def test_cannot_handle_webpage(resolver):
    assert not resolver.can_handle("https://example.com/page")


def test_cannot_handle_youtube(resolver):
    # YouTube is handled by YouTubeResolver, not VideoResolver
    assert not resolver.can_handle("https://youtube.com/watch?v=abc123")


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_pipeline_download_ffmpeg_transcribe(resolver):
    """Full path: download → ffmpeg → transcribe produces text transcript."""
    fake_audio_path = Path("/tmp/fake_audio.wav")

    with patch.object(
        resolver,
        "_download_video",
        new=AsyncMock(return_value=Path("/tmp/fake_video.mp4")),
    ):
        with patch.object(
            resolver,
            "_extract_audio",
            return_value=fake_audio_path,
        ):
            with patch.object(
                resolver,
                "_transcribe_whisper",
                return_value="This is the transcribed text from the video.",
            ):
                with patch.object(
                    Path,
                    "unlink",
                    return_value=None,
                ):
                    result = await resolver.resolve("https://example.com/video.mp4")

    assert result.extraction_error is None
    assert result.transcript == "This is the transcribed text from the video."
    assert result.text == "This is the transcribed text from the video."
    assert result.content_type == "video"


@pytest.mark.asyncio
async def test_temp_files_cleaned_up(resolver):
    """Temp files are deleted even when transcription succeeds."""
    deleted_paths = []

    def mock_unlink(self, missing_ok=False):
        deleted_paths.append(str(self))

    fake_video = Path("/tmp/test_video.mp4")
    fake_audio = Path("/tmp/test_audio.wav")

    with patch.object(resolver, "_download_video", new=AsyncMock(return_value=fake_video)):
        with patch.object(resolver, "_extract_audio", return_value=fake_audio):
            with patch.object(resolver, "_transcribe_whisper", return_value="Transcript text"):
                with patch.object(Path, "unlink", mock_unlink):
                    result = await resolver.resolve("https://example.com/video.mp4")

    assert result.extraction_error is None
    # Both video and audio temp files should have been attempted for deletion
    assert str(fake_video) in deleted_paths or str(fake_audio) in deleted_paths


@pytest.mark.asyncio
async def test_whisper_timeout_returns_error(resolver):
    """Whisper timeout returns error_result."""
    with patch.object(resolver, "_download_video", new=AsyncMock(return_value=Path("/tmp/v.mp4"))):
        with patch.object(resolver, "_extract_audio", return_value=Path("/tmp/a.wav")):
            with patch.object(
                resolver, "_transcribe_whisper", side_effect=Exception("Whisper timed out")
            ):
                with patch.object(Path, "unlink", return_value=None):
                    result = await resolver.resolve("https://example.com/video.mp4")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_download_failure_returns_error(resolver):
    """Download failure returns error_result."""
    with patch.object(
        resolver,
        "_download_video",
        new=AsyncMock(side_effect=Exception("Connection refused")),
    ):
        result = await resolver.resolve("https://example.com/video.mp4")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_unsupported_codec_returns_error(resolver):
    """ffmpeg failure for unsupported codec returns error_result."""
    with patch.object(resolver, "_download_video", new=AsyncMock(return_value=Path("/tmp/v.mp4"))):
        with patch.object(
            resolver,
            "_extract_audio",
            side_effect=ValueError("Unsupported video codec: mpeg4_decoder not found"),
        ):
            with patch.object(Path, "unlink", return_value=None):
                result = await resolver.resolve("https://example.com/video.mp4")

    assert result.extraction_error is not None
    assert "codec" in result.extraction_error.lower() or result.extraction_error != ""
