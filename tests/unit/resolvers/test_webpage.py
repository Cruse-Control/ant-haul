"""Tests for WebpageResolver (~8 tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from seed_storage.enrichment.resolvers.webpage import WebpageResolver, _extract_links


@pytest.fixture
def resolver():
    return WebpageResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_http(resolver):
    assert resolver.can_handle("http://example.com/page")


def test_can_handle_https(resolver):
    assert resolver.can_handle("https://example.com/page")


def test_cannot_handle_non_http(resolver):
    assert not resolver.can_handle("ftp://example.com/file.txt")


# ---------------------------------------------------------------------------
# Successful extraction
# ---------------------------------------------------------------------------

SAMPLE_HTML = """
<html><head><title>Test Page</title></head><body>
<p>This is some article text about interesting things.</p>
<a href="https://linked.com/page">link</a>
<a href="/relative/path">relative</a>
</body></html>
"""


@pytest.mark.asyncio
async def test_trafilatura_success(resolver):
    """trafilatura extracts text successfully."""
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status = MagicMock()
    mock_response.url = "https://example.com/page"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch("trafilatura.extract", return_value="Article text about interesting things"):
            with patch("trafilatura.extract_metadata", return_value=MagicMock(title="Test Page")):
                result = await resolver.resolve("https://example.com/page")

    assert result.extraction_error is None
    assert result.content_type == "webpage"
    assert "interesting things" in result.text
    assert result.title == "Test Page"


@pytest.mark.asyncio
async def test_readability_fallback(resolver):
    """Falls back to readability when trafilatura returns None."""
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status = MagicMock()
    mock_response.url = "https://example.com/page"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch("trafilatura.extract", return_value=None):
            with patch("trafilatura.extract_metadata", return_value=None):
                mock_doc = MagicMock()
                mock_doc.title.return_value = "Test Page"
                # readability summary returns HTML — the resolver strips tags
                # Use enough text so the 50-char minimum passes
                mock_doc.summary.return_value = "<div><p>Readable content here with enough text to pass the minimum length check.</p></div>"

                with patch("readability.Document", return_value=mock_doc):
                    result = await resolver.resolve("https://example.com/page")

    assert result.extraction_error is None
    assert "Readable content" in result.text


@pytest.mark.asyncio
async def test_both_fail_returns_error_result(resolver):
    """When both trafilatura and readability fail, returns error_result."""
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status = MagicMock()
    mock_response.url = "https://example.com/page"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch("trafilatura.extract", return_value=None):
            with patch("trafilatura.extract_metadata", return_value=None):
                with patch("readability.Document", side_effect=Exception("readability failed")):
                    result = await resolver.resolve("https://example.com/page")

    assert result.extraction_error is not None
    assert result.text == ""
    assert result.source_url == "https://example.com/page"


@pytest.mark.asyncio
async def test_truncation_at_8000_tokens(resolver):
    """Text longer than ~32000 chars is truncated."""
    long_text = "word " * 10_000  # ~50000 chars
    mock_response = MagicMock()
    mock_response.text = SAMPLE_HTML
    mock_response.raise_for_status = MagicMock()
    mock_response.url = "https://example.com/page"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch("trafilatura.extract", return_value=long_text):
            with patch("trafilatura.extract_metadata", return_value=None):
                result = await resolver.resolve("https://example.com/page")

    assert len(result.text) <= 32_000
    assert result.extraction_error is None


@pytest.mark.asyncio
async def test_expansion_urls_collected(resolver):
    """Links from the page are collected in expansion_urls."""
    html_with_links = """
    <html><body>
    <a href="https://other.com/a">link a</a>
    <a href="https://other.com/b">link b</a>
    </body></html>
    """
    mock_response = MagicMock()
    mock_response.text = html_with_links
    mock_response.raise_for_status = MagicMock()
    mock_response.url = "https://example.com/page"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        with patch("trafilatura.extract", return_value="Some text content"):
            with patch("trafilatura.extract_metadata", return_value=None):
                result = await resolver.resolve("https://example.com/page")

    assert "https://other.com/a" in result.expansion_urls
    assert "https://other.com/b" in result.expansion_urls


@pytest.mark.asyncio
async def test_timeout_returns_error_result(resolver):
    """Timeout returns error_result with appropriate message."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/page")

    assert result.extraction_error is not None
    assert "timed out" in result.extraction_error.lower()
    assert result.text == ""


@pytest.mark.asyncio
async def test_ssl_error_returns_error_result(resolver):
    """SSL/connection errors return error_result."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=Exception("SSL certificate verify failed"))
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/page")

    assert result.extraction_error is not None
    assert result.text == ""


# ---------------------------------------------------------------------------
# _extract_links helper
# ---------------------------------------------------------------------------


def test_extract_links_absolute():
    html = '<a href="https://other.com/page">link</a>'
    links = _extract_links(html, "https://example.com")
    assert "https://other.com/page" in links


def test_extract_links_relative():
    html = '<a href="/relative/path">link</a>'
    links = _extract_links(html, "https://example.com")
    assert "https://example.com/relative/path" in links


def test_extract_links_deduplicates():
    html = '<a href="https://other.com/page">1</a><a href="https://other.com/page">2</a>'
    links = _extract_links(html, "https://example.com")
    assert links.count("https://other.com/page") == 1
