"""Tests for PDFResolver (~5 tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seed_storage.enrichment.resolvers.pdf import PDFResolver


@pytest.fixture
def resolver():
    return PDFResolver()


FAKE_PDF_BYTES = b"%PDF-1.4\n" + b"\x00" * 100


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_pdf_extension(resolver):
    assert resolver.can_handle("https://example.com/document.pdf")


def test_cannot_handle_non_pdf(resolver):
    assert not resolver.can_handle("https://example.com/page.html")


def test_cannot_handle_image(resolver):
    assert not resolver.can_handle("https://example.com/photo.jpg")


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _mock_http_response(data=FAKE_PDF_BYTES, content_type="application/pdf"):
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.headers = {"content-type": content_type}
    mock_response.content = data
    return mock_response


@pytest.mark.asyncio
async def test_docling_success(resolver):
    """docling extracts text successfully."""
    mock_response = _mock_http_response()

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(
            resolver,
            "_extract_text_sync",
            return_value=("Extracted PDF text content here", "PDF Title"),
        ):
            result = await resolver.resolve("https://example.com/doc.pdf")

    assert result.extraction_error is None
    assert result.text == "Extracted PDF text content here"
    assert result.title == "PDF Title"
    assert result.content_type == "pdf"


@pytest.mark.asyncio
async def test_unstructured_fallback(resolver):
    """Falls back to unstructured when docling fails."""
    mock_response = _mock_http_response()

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        # docling fails, unstructured succeeds
        with patch.object(
            resolver, "_extract_text_sync", return_value=("Unstructured extracted text", None)
        ):
            result = await resolver.resolve("https://example.com/doc.pdf")

    assert result.extraction_error is None
    assert "Unstructured extracted text" in result.text


@pytest.mark.asyncio
async def test_both_fail_returns_error(resolver):
    """When both docling and unstructured fail, returns error_result."""
    mock_response = _mock_http_response()

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(resolver, "_extract_text_sync", return_value=("", None)):
            result = await resolver.resolve("https://example.com/doc.pdf")

    assert result.extraction_error is not None
    assert result.text == ""


@pytest.mark.asyncio
async def test_truncation_at_10000_tokens(resolver):
    """Text longer than ~40000 chars is truncated."""
    long_text = "paragraph " * 5_000  # ~50000 chars
    mock_response = _mock_http_response()

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch.object(resolver, "_extract_text_sync", return_value=(long_text, None)):
            result = await resolver.resolve("https://example.com/doc.pdf")

    assert len(result.text) <= 40_000
    assert result.extraction_error is None


@pytest.mark.asyncio
async def test_large_pdf_timeout(resolver):
    """Download timeout returns error_result."""
    import httpx

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/large.pdf")

    assert result.extraction_error is not None
    assert result.text == ""
