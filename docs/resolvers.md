# Adding a New Content Resolver

This guide walks through adding a resolver for a new content type to the seed-storage enrichment pipeline.

## Overview

Content resolvers live in `seed_storage/enrichment/resolvers/`. Each resolver handles a specific type of URL and returns a `ResolvedContent` object. The `ContentDispatcher` selects the first resolver whose `can_handle()` returns `True`.

## Step 1: Create the resolver file

Create `seed_storage/enrichment/resolvers/mytype.py`:

```python
"""seed_storage/enrichment/resolvers/mytype.py — MyType content resolver."""

from __future__ import annotations

import logging

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

# Token truncation limit (adjust per content type)
_MAX_TOKENS = 8_000
_CHARS_PER_TOKEN = 4


class MyTypeResolver(BaseResolver):
    """Resolves MyType URLs.

    Handles: example.com/content/ URLs.
    """

    def can_handle(self, url: str) -> bool:
        """Return True if this resolver can handle the URL.

        Must be synchronous — no network I/O allowed here.
        """
        return "example.com" in url.lower()

    async def resolve(self, url: str) -> ResolvedContent:
        """Fetch and extract content. Never raises — returns error_result on failure."""
        try:
            import httpx

            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url)
                response.raise_for_status()

            text = _extract_text(response.text)

            # Truncate to token budget
            max_chars = _MAX_TOKENS * _CHARS_PER_TOKEN
            if len(text) > max_chars:
                text = text[:max_chars]

            return ResolvedContent(
                source_url=url,
                content_type="webpage",  # use the closest existing ContentType
                title=_extract_title(response.text),
                text=text,
                transcript=None,
                summary=None,
                expansion_urls=_extract_links(response.text, url),
                metadata={"status_code": response.status_code},
                extraction_error=None,
                resolved_at=__import__("datetime").datetime.now(
                    tz=__import__("datetime").timezone.utc
                ),
            )

        except Exception as exc:
            logger.warning("MyTypeResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))


def _extract_text(html: str) -> str:
    """Extract clean text from HTML."""
    ...


def _extract_title(html: str) -> str | None:
    """Extract page title."""
    ...


def _extract_links(html: str, base_url: str) -> list[str]:
    """Extract absolute URLs from HTML links."""
    ...
```

## Step 2: Implement `can_handle()`

`can_handle()` is called synchronously in the dispatcher routing loop. Rules:
- **No network I/O** — pattern-match on URL structure only
- **Return True conservatively** — if uncertain, let a higher-priority resolver handle it
- **Order matters** — more specific resolvers must be registered before generic ones (e.g., YouTube before Webpage)

Common patterns:

```python
# Host matching
def can_handle(self, url: str) -> bool:
    from urllib.parse import urlparse
    host = urlparse(url).netloc.lower()
    return host in ("example.com", "www.example.com")

# Extension matching
def can_handle(self, url: str) -> bool:
    from urllib.parse import urlparse
    path = urlparse(url).path.lower()
    return path.endswith((".ext1", ".ext2"))

# Path prefix matching
def can_handle(self, url: str) -> bool:
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.netloc == "api.example.com" and parsed.path.startswith("/v2/")
```

## Step 3: Implement `resolve()`

`resolve()` must:
- Be `async`
- **Never raise** — catch all exceptions and return `ResolvedContent.error_result(url, str(exc))`
- Populate `expansion_urls` with secondary URLs found in the content (these feed the frontier)
- Set `resolved_at` — the dispatcher will overwrite this, but set it anyway for correctness

`ResolvedContent` fields:

| Field | Type | Description |
|-------|------|-------------|
| `source_url` | `str` | The original URL |
| `content_type` | `ContentType` | One of: `"webpage"`, `"youtube"`, `"video"`, `"image"`, `"pdf"`, `"github"`, `"tweet"`, `"unknown"` |
| `title` | `str \| None` | Page/content title |
| `text` | `str` | Clean extracted text (empty string on failure, not None) |
| `transcript` | `str \| None` | For video/audio content |
| `summary` | `str \| None` | For image descriptions (from vision LLM) |
| `expansion_urls` | `list[str]` | Secondary URLs found in the content |
| `metadata` | `dict` | Source-specific extras |
| `extraction_error` | `str \| None` | None on success, error message on failure |
| `resolved_at` | `datetime` | UTC timestamp (overwritten by dispatcher) |

**Truncation:** Apply a character budget to prevent oversized graph episodes:
- Webpage: 8,000 tokens (~32,000 chars)
- YouTube transcript: 12,000 tokens (~48,000 chars)
- PDF: 10,000 tokens (~40,000 chars)

## Step 4: Register in the dispatcher

Edit `seed_storage/enrichment/dispatcher.py`:

```python
from seed_storage.enrichment.resolvers.mytype import MyTypeResolver

_DEFAULT_RESOLVERS: list[BaseResolver] = [
    TwitterResolver(),
    YouTubeResolver(),
    GitHubResolver(),
    ImageResolver(),
    PDFResolver(),
    VideoResolver(),
    MyTypeResolver(),    # <── insert before WebpageResolver if more specific
    WebpageResolver(),
    FallbackResolver(),
]
```

Position your resolver **before** any resolver it should take precedence over.

## Step 5: Write unit tests

Create `tests/unit/resolvers/test_mytype.py`. Mock all HTTP calls — no real network in unit tests.

```python
"""Unit tests for MyTypeResolver."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seed_storage.enrichment.resolvers.mytype import MyTypeResolver


class TestCanHandle:
    def test_matches_example_com(self):
        r = MyTypeResolver()
        assert r.can_handle("https://example.com/some/path") is True

    def test_does_not_match_other_domain(self):
        r = MyTypeResolver()
        assert r.can_handle("https://other.com/path") is False

    def test_does_not_match_similar_domain(self):
        r = MyTypeResolver()
        assert r.can_handle("https://notexample.com/path") is False


class TestResolve:
    @pytest.mark.asyncio
    async def test_successful_resolution(self):
        r = MyTypeResolver()
        mock_response = MagicMock()
        mock_response.text = "<html><title>Test</title><p>Content here</p></html>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            result = await r.resolve("https://example.com/page")

        assert result.extraction_error is None
        assert result.text  # non-empty
        assert result.source_url == "https://example.com/page"

    @pytest.mark.asyncio
    async def test_http_error_returns_error_result(self):
        r = MyTypeResolver()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=Exception("Connection refused")
            )
            result = await r.resolve("https://example.com/bad")

        assert result.extraction_error is not None
        assert result.text == ""
        assert result.source_url == "https://example.com/bad"

    @pytest.mark.asyncio
    async def test_truncation_at_token_limit(self):
        r = MyTypeResolver()
        long_content = "word " * 10_000  # well over 8000 tokens
        mock_response = MagicMock()
        mock_response.text = f"<html><p>{long_content}</p></html>"
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )
            result = await r.resolve("https://example.com/long")

        # Should be truncated
        assert len(result.text) <= 8_000 * 4 + 100  # small tolerance
```

Expected test count: ~5–8 tests per resolver (success, fallback/error paths, truncation, edge cases).

## Step 6: Update dispatcher tests

Add test cases to `tests/unit/test_dispatcher.py` for the new URL pattern:

```python
def test_dispatches_example_com_to_mytype_resolver():
    dispatcher = ContentDispatcher()
    resolver = dispatcher._pick_resolver("https://example.com/page")
    assert isinstance(resolver, MyTypeResolver)
```

## Step 7: Add dependencies

If your resolver requires new packages, add them to `pyproject.toml` under `[project.dependencies]`.

Note: `pyproject.toml` is owned by config-agent. If modifying in a feature branch, document the dependency in your branch README for the merge.

## Checklist

- [ ] `can_handle()` is synchronous, no network I/O, pattern-matches URL
- [ ] `resolve()` is `async`, never raises, returns `error_result` on failure
- [ ] `text` field is always `str` (empty string on failure, never `None`)
- [ ] `expansion_urls` populated with secondary URLs found in content
- [ ] Token truncation applied before returning
- [ ] Resolver registered in `dispatcher.py` at the correct priority position
- [ ] Unit tests cover: success, HTTP error, truncation, timeout, edge cases
- [ ] Dispatcher routing test added for new URL pattern
- [ ] Dependencies added to `pyproject.toml` if needed

## Dispatcher priority order

From highest to lowest priority (first match wins):

| Priority | Resolver | File | `can_handle` strategy | Key dependency |
|----------|----------|------|----------------------|----------------|
| 1 | TwitterResolver | `twitter.py` | host in twitter.com/x.com | None (stub — always returns error) |
| 2 | YouTubeResolver | `youtube.py` | host in youtube.com/youtu.be | yt-dlp |
| 3 | GitHubResolver | `github.py` | host == github.com + path depth | httpx + GitHub API |
| 4 | ImageResolver | `image.py` | path extension in image set | httpx + vision LLM |
| 5 | PDFResolver | `pdf.py` | path ends with .pdf | docling / unstructured |
| 6 | VideoResolver | `video.py` | path extension in video set | yt-dlp + ffmpeg + whisper |
| 7 | WebpageResolver | `webpage.py` | scheme in http/https | trafilatura + readability-lxml |
| 8 | FallbackResolver | `fallback.py` | scheme in http/https (always True) | httpx + BeautifulSoup |

## Resolver quirks and implementation notes

### Twitter/X (stub)
Returns `ResolvedContent.error_result()` for all twitter.com and x.com URLs. Real extraction is out of scope for Phase A. Do not implement real Twitter extraction without confirming this is in scope.

### YouTube
Uses yt-dlp to fetch video metadata and transcript. Prefers manual captions over auto-generated. Falls back to Whisper transcription if no captions are available. Truncates transcript at 12,000 tokens.

### GitHub
Fetches repository metadata and README via the GitHub REST API. Uses `GITHUB_TOKEN` if set (allows access to private repos and higher rate limits). Returns metadata + README text.

### Image
Downloads the image and calls the vision LLM (configured via `VISION_PROVIDER`, defaults to `LLM_PROVIDER`). Returns the LLM description in both `summary` and `text` fields.

### PDF
Uses docling as the primary extractor. Falls back to unstructured if docling fails. Both are heavy imports — mock them in unit tests. Truncates at 10,000 tokens.

### Video
Downloads to a temp file, runs ffmpeg for audio extraction, then transcribes with Whisper (or AssemblyAI if `TRANSCRIPTION_BACKEND=assemblyai`). The temp file is always cleaned up in a `finally` block regardless of outcome.

### Webpage
Uses trafilatura as the primary extractor (best at article extraction). Falls back to readability-lxml. If both fail, returns `error_result()`. Does NOT fall back to the FallbackResolver — WebpageResolver handles generic HTTP(S) URLs before Fallback.

### Fallback
Best-effort HTML extraction via BeautifulSoup. Never raises. Always returns at least a minimal `ResolvedContent`. Used only when no other resolver matches.

## `source_description` format for content episodes

When a resolver's output is ingested via `ingest_episode`, the `source_description` on the Episodic node follows this format:

```
content_from_{source_type.title()}_{source_channel}:{content_type}
```

Examples:
- `content_from_Discord_general:youtube`
- `content_from_Discord_imessages:webpage`
- `content_from_Expansion_unknown:pdf`

Note: the channel name is separated by `_` (underscore), NOT `#`. This is intentional and differs from message episodes which use `#`. See `seed_storage/worker/tasks.py` `_source_description_content()`.
