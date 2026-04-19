"""Tests for discord_touch — emoji mapping, encoding, graceful handling."""

import pytest

from ingestion.discord_touch import STAGE_EMOJI, _encode_emoji, react


class TestStageEmoji:
    def test_all_stages_have_emoji(self):
        for stage in ["staged", "processed", "enriched", "loaded", "failed", "submodule", "deduped"]:
            assert stage in STAGE_EMOJI
            assert len(STAGE_EMOJI[stage]) > 0

    def test_no_duplicate_emoji(self):
        values = list(STAGE_EMOJI.values())
        assert len(values) == len(set(values))


class TestEncodeEmoji:
    def test_encodes_unicode(self):
        encoded = _encode_emoji("\U0001f4e5")  # 📥
        assert "%" in encoded  # URL-encoded

    def test_roundtrip(self):
        from urllib.parse import unquote
        emoji = "\U0001f9e0"  # 🧠
        assert unquote(_encode_emoji(emoji)) == emoji


class TestReact:
    @pytest.mark.asyncio
    async def test_no_metadata_returns_silently(self):
        """Items without discord_msg_id should not raise."""
        await react({"metadata": {}}, "processed")

    @pytest.mark.asyncio
    async def test_no_token_returns_silently(self, monkeypatch):
        """Missing bot token should not raise."""
        monkeypatch.delenv("DISCORD_BOT_ANT_FARM_TOKEN", raising=False)
        item = {"metadata": {"discord_msg_id": "123", "discord_channel_id": "456"}}
        await react(item, "processed")

    @pytest.mark.asyncio
    async def test_unknown_status_returns_silently(self):
        """Unknown status should not raise."""
        item = {"metadata": {"discord_msg_id": "123", "discord_channel_id": "456"}}
        await react(item, "nonexistent_status")

    @pytest.mark.asyncio
    async def test_string_metadata_parsed(self):
        """Metadata stored as JSON string should be handled."""
        import json
        item = {"metadata": json.dumps({"discord_msg_id": "123", "discord_channel_id": "456"})}
        await react(item, "processed")  # Should not raise

    @pytest.mark.asyncio
    async def test_none_metadata(self):
        """None metadata should not raise."""
        await react({"metadata": None}, "loaded")
