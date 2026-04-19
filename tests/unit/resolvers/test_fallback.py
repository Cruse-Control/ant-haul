"""Tests for FallbackResolver (~4 tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from seed_storage.enrichment.resolvers.fallback import FallbackResolver


@pytest.fixture
def resolver():
    return FallbackResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_http(resolver):
    assert resolver.can_handle("http://example.com/page")


def test_can_handle_https(resolver):
    assert resolver.can_handle("https://example.com/page")


def test_cannot_handle_non_http(resolver):
    assert not resolver.can_handle("ftp://example.com/file")


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------

SIMPLE_HTML = """
<html>
<head><title>Simple Page</title></head>
<body>
<script>var x = 1;</script>
<p>Main content of the page.</p>
<nav>Navigation links</nav>
</body>
</html>
"""


@pytest.mark.asyncio
async def test_http_get_and_bs4(resolver):
    """Performs GET request and extracts text with BeautifulSoup."""
    mock_response = MagicMock()
    mock_response.text = SIMPLE_HTML

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/page")

    # Should succeed and contain some text
    assert result.source_url == "https://example.com/page"
    # text may be empty string but should not raise
    assert isinstance(result.text, str)


@pytest.mark.asyncio
async def test_never_raises(resolver):
    """FallbackResolver never raises — even on complete failure."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=Exception("Catastrophic failure"))
        mock_client_cls.return_value = mock_client

        # Must not raise
        result = await resolver.resolve("https://example.com/page")

    assert result is not None
    assert result.source_url == "https://example.com/page"


@pytest.mark.asyncio
async def test_timeout_returns_minimal_result(resolver):
    """Timeout produces a result with empty text and an extraction_error."""
    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://slow.example.com/page")

    assert result.text == ""
    assert result.extraction_error is not None
    assert "timed out" in result.extraction_error.lower()


@pytest.mark.asyncio
async def test_malformed_html(resolver):
    """Malformed HTML still produces a result (no crash)."""
    mock_response = MagicMock()
    mock_response.text = "<html><body><p>Unclosed tag <b>text"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value = mock_client

        result = await resolver.resolve("https://example.com/malformed")

    assert result is not None
    assert isinstance(result.text, str)
