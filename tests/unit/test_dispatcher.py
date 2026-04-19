"""Tests for ContentDispatcher (~15 tests)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from seed_storage.enrichment.dispatcher import ContentDispatcher
from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_resolved(url: str, content_type="webpage", text="extracted text") -> ResolvedContent:
    return ResolvedContent(
        source_url=url,
        content_type=content_type,
        title="Test Title",
        text=text,
        transcript=None,
        summary=None,
        expansion_urls=[],
        metadata={},
        extraction_error=None,
        resolved_at=datetime.now(tz=UTC),
    )


class _MockResolver(BaseResolver):
    """A simple mock resolver for testing."""

    def __init__(self, handles: bool, result: ResolvedContent | None = None, raises: bool = False):
        self._handles = handles
        self._result = result
        self._raises = raises
        self.resolve_called_with: list[str] = []

    def can_handle(self, url: str) -> bool:
        return self._handles

    async def resolve(self, url: str) -> ResolvedContent:
        self.resolve_called_with.append(url)
        if self._raises:
            raise RuntimeError("Resolver exploded!")
        if self._result is not None:
            return self._result
        return _make_resolved(url)


# ---------------------------------------------------------------------------
# Routing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_twitter_url_routing():
    """twitter.com URLs go to TwitterResolver."""
    dispatcher = ContentDispatcher()
    with patch(
        "seed_storage.enrichment.resolvers.twitter.TwitterResolver.resolve",
        new=AsyncMock(
            return_value=ResolvedContent.error_result("https://twitter.com/user/status/123", "stub")
        ),
    ):
        result = await dispatcher.dispatch("https://twitter.com/user/status/123")
    assert result.source_url == "https://twitter.com/user/status/123"


@pytest.mark.asyncio
async def test_x_com_routing():
    """x.com URLs go to TwitterResolver."""
    dispatcher = ContentDispatcher()
    with patch(
        "seed_storage.enrichment.resolvers.twitter.TwitterResolver.resolve",
        new=AsyncMock(
            return_value=ResolvedContent.error_result("https://x.com/user/status/123", "stub")
        ),
    ):
        result = await dispatcher.dispatch("https://x.com/user/status/123")
    assert result.source_url == "https://x.com/user/status/123"


@pytest.mark.asyncio
async def test_youtube_url_routing():
    """youtube.com URLs go to YouTubeResolver."""
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    mock_result = _make_resolved(url, content_type="youtube")

    with patch(
        "seed_storage.enrichment.resolvers.youtube.YouTubeResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "youtube"


@pytest.mark.asyncio
async def test_github_url_routing():
    """github.com repo URLs go to GitHubResolver."""
    url = "https://github.com/owner/repo"
    mock_result = _make_resolved(url, content_type="github")

    with patch(
        "seed_storage.enrichment.resolvers.github.GitHubResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "github"


@pytest.mark.asyncio
async def test_image_url_routing():
    """Image URLs go to ImageResolver."""
    url = "https://example.com/photo.jpg"
    mock_result = _make_resolved(url, content_type="image")

    with patch(
        "seed_storage.enrichment.resolvers.image.ImageResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "image"


@pytest.mark.asyncio
async def test_pdf_url_routing():
    """PDF URLs go to PDFResolver."""
    url = "https://example.com/doc.pdf"
    mock_result = _make_resolved(url, content_type="pdf")

    with patch(
        "seed_storage.enrichment.resolvers.pdf.PDFResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "pdf"


@pytest.mark.asyncio
async def test_video_url_routing():
    """Video URLs go to VideoResolver."""
    url = "https://example.com/clip.mp4"
    mock_result = _make_resolved(url, content_type="video")

    with patch(
        "seed_storage.enrichment.resolvers.video.VideoResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "video"


@pytest.mark.asyncio
async def test_generic_webpage_routing():
    """Generic HTTP URLs go to WebpageResolver."""
    url = "https://blog.example.com/some-article"
    mock_result = _make_resolved(url, content_type="webpage")

    with patch(
        "seed_storage.enrichment.resolvers.webpage.WebpageResolver.resolve",
        new=AsyncMock(return_value=mock_result),
    ):
        dispatcher = ContentDispatcher()
        result = await dispatcher.dispatch(url)

    assert result.content_type == "webpage"


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_twitter_before_webpage():
    """TwitterResolver comes before WebpageResolver in priority order."""
    url = "https://twitter.com/u/status/1"
    twitter_result = ResolvedContent.error_result(url, "stub")

    twitter_called = False
    webpage_called = False

    async def mock_twitter_resolve(self, u):
        nonlocal twitter_called
        twitter_called = True
        return twitter_result

    async def mock_webpage_resolve(self, u):
        nonlocal webpage_called
        webpage_called = True
        return _make_resolved(u, content_type="webpage")

    with patch(
        "seed_storage.enrichment.resolvers.twitter.TwitterResolver.resolve",
        new=mock_twitter_resolve,
    ):
        with patch(
            "seed_storage.enrichment.resolvers.webpage.WebpageResolver.resolve",
            new=mock_webpage_resolve,
        ):
            dispatcher = ContentDispatcher()
            await dispatcher.dispatch(url)

    assert twitter_called, "TwitterResolver should have been called"
    assert not webpage_called, "WebpageResolver should NOT have been called"


# ---------------------------------------------------------------------------
# Custom resolver list tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_custom_resolver_list():
    """ContentDispatcher accepts a custom resolver list."""
    url = "https://custom.com/page"
    expected = _make_resolved(url, text="custom result")
    mock_resolver = _MockResolver(handles=True, result=expected)

    dispatcher = ContentDispatcher(resolvers=[mock_resolver])
    result = await dispatcher.dispatch(url)

    assert result.text == "custom result"
    assert url in mock_resolver.resolve_called_with


@pytest.mark.asyncio
async def test_first_matching_resolver_wins():
    """First resolver in list that can_handle wins."""
    url = "https://example.com"
    first_result = _make_resolved(url, text="first")
    second_result = _make_resolved(url, text="second")

    first = _MockResolver(handles=True, result=first_result)
    second = _MockResolver(handles=True, result=second_result)

    dispatcher = ContentDispatcher(resolvers=[first, second])
    result = await dispatcher.dispatch(url)

    assert result.text == "first"
    assert not second.resolve_called_with  # second should never be called


@pytest.mark.asyncio
async def test_skips_non_matching_resolver():
    """Non-matching resolver is skipped."""
    url = "https://example.com"
    expected = _make_resolved(url, text="matched")
    non_matching = _MockResolver(handles=False)
    matching = _MockResolver(handles=True, result=expected)

    dispatcher = ContentDispatcher(resolvers=[non_matching, matching])
    result = await dispatcher.dispatch(url)

    assert result.text == "matched"
    assert not non_matching.resolve_called_with


@pytest.mark.asyncio
async def test_no_matching_resolver_returns_error():
    """When no resolver matches, returns error_result."""
    url = "ftp://legacy.example.com/file"  # nothing handles ftp

    dispatcher = ContentDispatcher(resolvers=[])
    result = await dispatcher.dispatch(url)

    assert result.extraction_error is not None
    assert result.source_url == url


@pytest.mark.asyncio
async def test_resolver_exception_returns_error_result():
    """If resolver raises unexpectedly, dispatcher catches it and returns error_result."""
    url = "https://example.com"
    exploding = _MockResolver(handles=True, raises=True)

    dispatcher = ContentDispatcher(resolvers=[exploding])
    result = await dispatcher.dispatch(url)

    assert result.extraction_error is not None
    assert result.source_url == url
    assert result.text == ""


@pytest.mark.asyncio
async def test_multiple_urls_independent():
    """Dispatching multiple URLs independently works correctly."""
    url1 = "https://example.com/a"
    url2 = "https://example.com/b"

    results1 = _make_resolved(url1, text="result1")
    results2 = _make_resolved(url2, text="result2")

    call_order = []

    async def mock_resolve(url):
        call_order.append(url)
        if url == url1:
            return results1
        return results2

    resolver = _MockResolver(handles=True)
    resolver.resolve = mock_resolve  # type: ignore[method-assign]

    dispatcher = ContentDispatcher(resolvers=[resolver])
    r1 = await dispatcher.dispatch(url1)
    r2 = await dispatcher.dispatch(url2)

    assert r1.text == "result1"
    assert r2.text == "result2"


# ---------------------------------------------------------------------------
# resolved_at stamping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_stamps_resolved_at():
    """Dispatcher stamps resolved_at on the returned content."""
    url = "https://example.com"
    before = datetime.now(tz=UTC)

    mock_result = _make_resolved(url)
    mock_resolver = _MockResolver(handles=True, result=mock_result)

    dispatcher = ContentDispatcher(resolvers=[mock_resolver])
    result = await dispatcher.dispatch(url)

    after = datetime.now(tz=UTC)

    assert result.resolved_at is not None
    assert before <= result.resolved_at <= after
