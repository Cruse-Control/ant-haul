"""Tests for ImageResolver (~5 tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from seed_storage.enrichment.resolvers.image import ImageResolver


@pytest.fixture
def resolver():
    return ImageResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_jpg(resolver):
    assert resolver.can_handle("https://example.com/photo.jpg")


def test_can_handle_png(resolver):
    assert resolver.can_handle("https://example.com/image.png")


def test_can_handle_webp(resolver):
    assert resolver.can_handle("https://example.com/pic.webp")


def test_cannot_handle_non_image(resolver):
    assert not resolver.can_handle("https://example.com/document.pdf")


def test_cannot_handle_webpage(resolver):
    assert not resolver.can_handle("https://example.com/page")


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------

FAKE_IMAGE_BYTES = b"\xff\xd8\xff\xe0" + b"\x00" * 100  # Minimal JPEG header


@pytest.mark.asyncio
async def test_vision_llm_called(resolver):
    """Vision LLM is called and its response populates summary and text."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {"content-type": "image/jpeg"}
    mock_response.content = FAKE_IMAGE_BYTES

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(
            resolver, "_call_vision_llm", new=AsyncMock(return_value="A red apple on a table")
        ):
            result = await resolver.resolve("https://example.com/photo.jpg")

    assert result.extraction_error is None
    assert result.summary == "A red apple on a table"
    assert result.text == "A red apple on a table"
    assert result.content_type == "image"


@pytest.mark.asyncio
async def test_inaccessible_url_returns_error(resolver):
    """HTTP error on image download returns error_result."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "404", request=MagicMock(), response=MagicMock(status_code=404)
            )
        )
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/photo.jpg")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_timeout_returns_error(resolver):
    """Timeout returns error_result."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/photo.jpg")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_wrong_content_type_returns_error(resolver):
    """Content-type mismatch (e.g., text/html) returns error_result."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {"content-type": "text/html"}
    mock_response.content = b"<html>Not an image</html>"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/photo.jpg")

    assert result.extraction_error is not None


@pytest.mark.asyncio
async def test_summary_populated(resolver):
    """summary field is non-None when vision LLM succeeds."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {"content-type": "image/png"}
    mock_response.content = FAKE_IMAGE_BYTES

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(
            resolver, "_call_vision_llm", new=AsyncMock(return_value="A beautiful sunset")
        ):
            result = await resolver.resolve("https://example.com/sunset.png")

    assert result.summary is not None
    assert len(result.summary) > 0
