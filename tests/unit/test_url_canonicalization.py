"""Unit tests for seed_storage.dedup.canonicalize_url and url_hash (~20 tests)."""

import hashlib

from seed_storage.dedup import canonicalize_url, url_hash

# ---------------------------------------------------------------------------
# Tracking param stripping
# ---------------------------------------------------------------------------


def test_strip_utm_source():
    url = "https://example.com/page?utm_source=google"
    assert "utm_source" not in canonicalize_url(url)


def test_strip_utm_medium():
    url = "https://example.com/page?utm_medium=cpc"
    assert "utm_medium" not in canonicalize_url(url)


def test_strip_utm_campaign():
    url = "https://example.com/page?utm_campaign=spring_sale"
    assert "utm_campaign" not in canonicalize_url(url)


def test_strip_utm_content():
    url = "https://example.com/page?utm_content=banner"
    assert "utm_content" not in canonicalize_url(url)


def test_strip_utm_term():
    url = "https://example.com/page?utm_term=running+shoes"
    assert "utm_term" not in canonicalize_url(url)


def test_strip_fbclid():
    url = "https://example.com/page?fbclid=IwAR1234567890"
    assert "fbclid" not in canonicalize_url(url)


def test_strip_ref():
    url = "https://example.com/page?ref=homepage"
    assert "ref" not in canonicalize_url(url)


def test_strip_si():
    url = "https://open.spotify.com/track/abc?si=xyz"
    assert "si" not in canonicalize_url(url)


def test_strip_t_param():
    url = "https://twitter.com/user/status/123?t=abc123"
    assert "t=" not in canonicalize_url(url)


def test_strip_s_param():
    url = "https://twitter.com/user/status/123?s=20"
    assert "s=" not in canonicalize_url(url)


def test_strip_multiple_tracking_params_at_once():
    url = "https://example.com/page?utm_source=fb&fbclid=xyz&ref=menu&keep=1"
    result = canonicalize_url(url)
    assert "utm_source" not in result
    assert "fbclid" not in result
    assert "ref" not in result
    assert "keep=1" in result


# ---------------------------------------------------------------------------
# Scheme and host normalization
# ---------------------------------------------------------------------------


def test_lowercase_scheme():
    assert canonicalize_url("HTTP://Example.com/path").startswith("http://")


def test_lowercase_host():
    result = canonicalize_url("https://EXAMPLE.COM/path")
    assert "example.com" in result


def test_preserve_path_case():
    result = canonicalize_url("https://example.com/MyPage/SubDir")
    assert "/MyPage/SubDir" in result


# ---------------------------------------------------------------------------
# Query param sorting
# ---------------------------------------------------------------------------


def test_sort_query_params():
    url1 = "https://example.com/?b=2&a=1"
    url2 = "https://example.com/?a=1&b=2"
    assert canonicalize_url(url1) == canonicalize_url(url2)


# ---------------------------------------------------------------------------
# Trailing slash and fragment removal
# ---------------------------------------------------------------------------


def test_remove_trailing_slash():
    assert canonicalize_url("https://example.com/page/") == "https://example.com/page"


def test_remove_fragment():
    result = canonicalize_url("https://example.com/page#section")
    assert "#" not in result


def test_preserve_root_path():
    """Root path '/' should not be mangled."""
    result = canonicalize_url("https://example.com/")
    assert result == "https://example.com/"


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_idempotent():
    url = "https://example.com/page?utm_source=google&keep=yes"
    once = canonicalize_url(url)
    twice = canonicalize_url(once)
    assert once == twice


# ---------------------------------------------------------------------------
# YouTube / Twitter normalization
# ---------------------------------------------------------------------------


def test_youtube_strips_si():
    url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&si=abcXYZ"
    result = canonicalize_url(url)
    assert "si" not in result
    assert "v=dQw4w9WgXcQ" in result


def test_twitter_strips_s_and_t():
    url = "https://twitter.com/user/status/1234567890?s=20&t=abc"
    result = canonicalize_url(url)
    assert "s=" not in result
    assert "t=" not in result


# ---------------------------------------------------------------------------
# Malformed / edge cases
# ---------------------------------------------------------------------------


def test_malformed_url_returns_original():
    bad = "not a url at all"
    assert canonicalize_url(bad) == bad


def test_url_hash_is_sha256_of_canonical():
    url = "https://example.com/path?utm_source=x"
    canonical = canonicalize_url(url)
    expected = hashlib.sha256(canonical.encode()).hexdigest()
    assert url_hash(url) == expected


def test_url_hash_consistent_across_tracking_variants():
    base = "https://example.com/article"
    with_tracking = "https://example.com/article?utm_source=nl&utm_medium=email"
    assert url_hash(base) == url_hash(with_tracking)
