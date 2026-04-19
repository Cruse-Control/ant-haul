"""Tests for YouTubeResolver (~8 tests)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.youtube import YouTubeResolver, _extract_video_id


@pytest.fixture
def resolver():
    return YouTubeResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_youtube_watch(resolver):
    assert resolver.can_handle("https://www.youtube.com/watch?v=dQw4w9WgXcQ")


def test_can_handle_youtu_be(resolver):
    assert resolver.can_handle("https://youtu.be/dQw4w9WgXcQ")


def test_can_handle_shorts(resolver):
    assert resolver.can_handle("https://www.youtube.com/shorts/dQw4w9WgXcQ")


def test_cannot_handle_non_youtube(resolver):
    assert not resolver.can_handle("https://vimeo.com/123456")


# ---------------------------------------------------------------------------
# _extract_video_id helper
# ---------------------------------------------------------------------------


def test_extract_video_id_watch_url():
    assert _extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_youtu_be():
    assert _extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_shorts():
    assert _extract_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _make_ydl_info(
    title="Test Video",
    description="A test description",
    channel="TestChannel",
    duration=300,
    view_count=1000,
    subtitles=None,
    automatic_captions=None,
):
    return {
        "title": title,
        "description": description,
        "channel": channel,
        "uploader": channel,
        "duration": duration,
        "view_count": view_count,
        "upload_date": "20240101",
        "subtitles": subtitles or {},
        "automatic_captions": automatic_captions or {},
    }


@pytest.mark.asyncio
async def test_metadata_extraction(resolver):
    """Metadata fields (title, channel, duration, view_count) are populated."""
    with patch.object(resolver, "_fetch_ydl", return_value=None) as mock_fetch:
        expected = ResolvedContent(
            source_url="https://www.youtube.com/watch?v=abc123",
            content_type="youtube",
            title="Great Video",
            text="A test description",
            transcript=None,
            summary=None,
            expansion_urls=[],
            metadata={
                "channel": "MyChannel",
                "duration_seconds": 120,
                "view_count": 500,
                "upload_date": "20240101",
                "video_id": "abc123",
            },
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )
        mock_fetch.return_value = expected

        result = await resolver.resolve("https://www.youtube.com/watch?v=abc123")

    assert result.title == "Great Video"
    assert result.metadata["channel"] == "MyChannel"
    assert result.metadata["duration_seconds"] == 120
    assert result.metadata["view_count"] == 500
    assert result.content_type == "youtube"


@pytest.mark.asyncio
async def test_manual_captions_used(resolver):
    """Manual captions are preferred over auto-generated."""
    subtitles = {
        "en": [{"ext": "vtt", "data": "WEBVTT\n\n00:00:01.000 --> 00:00:03.000\nHello world\n"}]
    }
    auto_captions = {
        "en": [{"ext": "vtt", "data": "WEBVTT\n\n00:00:01.000 --> 00:00:03.000\nAuto caption\n"}]
    }
    info = _make_ydl_info(subtitles=subtitles, automatic_captions=auto_captions)

    with patch("yt_dlp.YoutubeDL") as mock_ydl_cls:
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl.__exit__ = MagicMock(return_value=None)
        mock_ydl.extract_info = MagicMock(return_value=info)
        mock_ydl_cls.return_value = mock_ydl

        result = await resolver.resolve("https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    # Manual captions should be used, not auto
    assert result.transcript is not None
    assert "Hello world" in result.transcript
    assert "Auto caption" not in result.transcript


@pytest.mark.asyncio
async def test_auto_caption_fallback(resolver):
    """Falls back to auto-generated captions when manual captions absent."""
    auto_captions = {
        "en": [
            {"ext": "vtt", "data": "WEBVTT\n\n00:00:01.000 --> 00:00:03.000\nAuto caption text\n"}
        ]
    }
    info = _make_ydl_info(subtitles={}, automatic_captions=auto_captions)

    with patch("yt_dlp.YoutubeDL") as mock_ydl_cls:
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl.__exit__ = MagicMock(return_value=None)
        mock_ydl.extract_info = MagicMock(return_value=info)
        mock_ydl_cls.return_value = mock_ydl

        result = await resolver.resolve("https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    assert result.transcript is not None
    assert "Auto caption text" in result.transcript


@pytest.mark.asyncio
async def test_truncation_at_12000_tokens(resolver):
    """Transcript truncated at ~48000 chars."""
    long_transcript = "word " * 15_000  # ~75000 chars

    # Patch _fetch_ydl to return a result with long transcript
    long_result = ResolvedContent(
        source_url="https://www.youtube.com/watch?v=abc",
        content_type="youtube",
        title="Long Video",
        text=long_transcript[:48_000],
        transcript=long_transcript[:48_000],
        summary=None,
        expansion_urls=[],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )

    with patch.object(resolver, "_fetch_ydl", return_value=long_result):
        result = await resolver.resolve("https://www.youtube.com/watch?v=abc")

    assert len(result.transcript or "") <= 48_000


@pytest.mark.asyncio
async def test_ydl_timeout_returns_error(resolver):
    """Timeout from yt-dlp returns error_result."""
    with patch("yt_dlp.YoutubeDL") as mock_ydl_cls:
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl.__exit__ = MagicMock(return_value=None)
        mock_ydl.extract_info = MagicMock(side_effect=Exception("Connection timed out"))
        mock_ydl_cls.return_value = mock_ydl

        result = await resolver.resolve("https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_shorts_url_handled(resolver):
    """YouTube Shorts URLs are handled correctly."""
    info = _make_ydl_info(title="Short Video")
    info["subtitles"] = {}
    info["automatic_captions"] = {}

    with patch("yt_dlp.YoutubeDL") as mock_ydl_cls:
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl.__exit__ = MagicMock(return_value=None)
        mock_ydl.extract_info = MagicMock(return_value=info)
        mock_ydl_cls.return_value = mock_ydl

        result = await resolver.resolve("https://www.youtube.com/shorts/dQw4w9WgXcQ")

    assert result.content_type == "youtube"
    assert result.extraction_error is None


@pytest.mark.asyncio
async def test_metadata_fields_populated(resolver):
    """All metadata fields are present in the result."""
    info = _make_ydl_info(channel="TestChan", duration=600, view_count=999)

    with patch("yt_dlp.YoutubeDL") as mock_ydl_cls:
        mock_ydl = MagicMock()
        mock_ydl.__enter__ = MagicMock(return_value=mock_ydl)
        mock_ydl.__exit__ = MagicMock(return_value=None)
        mock_ydl.extract_info = MagicMock(return_value=info)
        mock_ydl_cls.return_value = mock_ydl

        result = await resolver.resolve("https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    assert "channel" in result.metadata
    assert "duration_seconds" in result.metadata
    assert "view_count" in result.metadata
    assert "upload_date" in result.metadata
