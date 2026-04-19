"""Tests for TwitterResolver — stub (~2 tests)."""

from __future__ import annotations

import pytest

from seed_storage.enrichment.resolvers.twitter import TwitterResolver


@pytest.fixture
def resolver():
    return TwitterResolver()


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_twitter_com(resolver):
    assert resolver.can_handle("https://twitter.com/user/status/123456")


def test_can_handle_x_com(resolver):
    assert resolver.can_handle("https://x.com/user/status/123456")


def test_can_handle_mobile_twitter(resolver):
    assert resolver.can_handle("https://mobile.twitter.com/user/status/123")


def test_cannot_handle_non_twitter(resolver):
    assert not resolver.can_handle("https://example.com/page")


# ---------------------------------------------------------------------------
# Resolution — stub returns error_result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_twitter_returns_error_result(resolver):
    """twitter.com URL returns error_result (stub)."""
    result = await resolver.resolve("https://twitter.com/user/status/123456")

    assert result.extraction_error is not None
    assert result.text == ""
    assert result.source_url == "https://twitter.com/user/status/123456"


@pytest.mark.asyncio
async def test_x_com_returns_error_result(resolver):
    """x.com URL returns error_result (stub)."""
    result = await resolver.resolve("https://x.com/elonmusk/status/9999999999")

    assert result.extraction_error is not None
    assert result.text == ""
    assert result.source_url == "https://x.com/elonmusk/status/9999999999"
