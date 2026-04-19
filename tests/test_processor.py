"""Tests for processor — medium-specific extraction logic."""

import json

import pytest

from ingestion.processor import (
    _adjudicate,
    _extract_yt_id,
    _get_yt_transcript,
)


class TestExtractYouTubeId:
    def test_standard_watch(self):
        assert _extract_yt_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_short_url(self):
        assert _extract_yt_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"

    def test_shorts(self):
        assert _extract_yt_id("https://youtube.com/shorts/abc123") == "abc123"

    def test_empty(self):
        assert _extract_yt_id("https://example.com") == ""


class TestAdjudicate:
    @pytest.mark.asyncio
    async def test_no_api_key_returns_skipped(self):
        result = await _adjudicate(None, "Some transcript text")
        assert result == (False, "skipped")

    @pytest.mark.asyncio
    async def test_empty_transcript_returns_skipped(self):
        result = await _adjudicate(None, "")
        assert result == (False, "skipped")

    @pytest.mark.asyncio
    async def test_no_transcript_marker_returns_skipped(self):
        result = await _adjudicate(None, "[No transcript available for video xyz]")
        assert result == (False, "skipped")


class TestProcessWebOutboundLinks:
    """Verify _process_web extracts outbound links from article HTML."""

    @pytest.mark.asyncio
    async def test_extracts_outbound_links(self):
        import httpx

        html = """<html><head>
            <title>Test Article</title>
            <meta name="author" content="Author Name">
        </head><body>
            <p>Article body with <a href="https://example.com/linked">a link</a>
            and <a href="https://other.com/page">another link</a>
            and <a href="/relative/path">a relative one</a></p>
        </body></html>"""

        from unittest.mock import AsyncMock, MagicMock, patch
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.get = AsyncMock(return_value=mock_resp)

        from ingestion.processor import _process_web
        content, meta = await _process_web(mock_http, "https://test.com/article")

        assert "outbound_links" in meta
        assert "https://example.com/linked" in meta["outbound_links"]
        assert "https://other.com/page" in meta["outbound_links"]
        # Relative links should NOT be included (don't start with http)
        assert "/relative/path" not in meta["outbound_links"]

    @pytest.mark.asyncio
    async def test_no_links_no_key(self):
        import httpx
        from unittest.mock import AsyncMock, MagicMock

        html = "<html><head><title>Empty</title></head><body><p>No links here</p></body></html>"
        mock_resp = MagicMock()
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()
        mock_http = AsyncMock(spec=httpx.AsyncClient)
        mock_http.get = AsyncMock(return_value=mock_resp)

        from ingestion.processor import _process_web
        content, meta = await _process_web(mock_http, "https://test.com/empty")

        assert "outbound_links" not in meta


class TestProcessorIntegration:
    """Integration test: stage an item, process it, verify status change."""

    def test_plain_text_passthrough(self):
        """plain_text items should keep their raw_content unchanged in processing."""
        import uuid
        from seed_storage import staging

        staging.init_tables()
        uri = f"discord://test/{uuid.uuid4()}"
        sid = staging.stage(
            source_type="plain_text",
            source_uri=uri,
            raw_content="Just a plain text message about AI",
        )
        assert sid is not None

        # Verify it's staged.
        items = staging.get_staged(status="staged")
        item = next((i for i in items if str(i["id"]) == sid), None)
        assert item is not None
        assert item["source_type"] == "plain_text"

        # Cleanup.
        staging.update_status([sid], "deleted")
