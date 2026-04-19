"""Unit tests for seed_storage/ingestion/bot.py.

All Discord I/O and Redis are mocked. Tests cover:
- Configured channel processed, non-configured ignored
- Empty content + no attachments → skipped
- Bot author → skipped
- raw_payload shape matches Contract 1
- source_type / source_id / source_channel correctness
- Attachments extracted from message
- metadata fields: channel_id, author_id, guild_id
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_discord_message(
    *,
    channel_id: str = "111",
    channel_name: str = "general",
    author_bot: bool = False,
    author_name: str = "TestUser",
    author_id: str = "222",
    message_id: str = "333",
    content: str = "Hello, world!",
    attachments: list | None = None,
    guild_id: str = "444",
    guild: bool = True,
) -> MagicMock:
    """Return a MagicMock shaped like a discord.Message."""
    msg = MagicMock()
    msg.id = message_id
    msg.content = content
    msg.created_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    msg.channel.id = channel_id
    msg.channel.name = channel_name

    msg.author.bot = author_bot
    msg.author.display_name = author_name
    msg.author.id = author_id

    if attachments is None:
        msg.attachments = []
    else:
        msg.attachments = attachments

    if guild:
        msg.guild = MagicMock()
        msg.guild.id = guild_id
    else:
        msg.guild = None

    return msg


def _make_attachment(url: str) -> MagicMock:
    a = MagicMock()
    a.url = url
    return a


@pytest.fixture(autouse=True)
def _patch_discord_client(monkeypatch):
    """Prevent discord.Client.__init__ from doing real network setup."""
    import discord
    monkeypatch.setattr(discord.Client, "__init__", lambda self, **kwargs: None)


@pytest.fixture
def mock_enrich():
    with patch("seed_storage.ingestion.bot._enrich_message") as m:
        yield m


@pytest.fixture
def bot():
    from seed_storage.ingestion.bot import SeedBot
    return SeedBot(redis_client=None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestOnMessage:
    async def test_configured_channel_is_processed(self, bot, mock_enrich):
        msg = _make_discord_message(channel_id="111")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = ["111"]
            await bot.on_message(msg)
        mock_enrich.delay.assert_called_once()

    async def test_non_configured_channel_is_ignored(self, bot, mock_enrich):
        msg = _make_discord_message(channel_id="999")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = ["111", "222"]
            await bot.on_message(msg)
        mock_enrich.delay.assert_not_called()

    async def test_no_channel_filter_processes_all_channels(self, bot, mock_enrich):
        """When DISCORD_CHANNEL_IDS is empty, all channels are processed."""
        msg = _make_discord_message(channel_id="999")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        mock_enrich.delay.assert_called_once()

    async def test_empty_content_and_no_attachments_is_skipped(self, bot, mock_enrich):
        msg = _make_discord_message(content="", attachments=[])
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        mock_enrich.delay.assert_not_called()

    async def test_bot_author_is_skipped(self, bot, mock_enrich):
        msg = _make_discord_message(author_bot=True)
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        mock_enrich.delay.assert_not_called()

    async def test_empty_content_with_attachment_is_processed(self, bot, mock_enrich):
        """No content but has attachment → should NOT be skipped."""
        att = _make_attachment("https://cdn.discordapp.com/1/2/file.png")
        msg = _make_discord_message(content="", attachments=[att])
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        mock_enrich.delay.assert_called_once()

    async def test_raw_payload_shape_matches_contract(self, bot, mock_enrich):
        """All Contract 1 fields must be present in the enqueued payload."""
        msg = _make_discord_message()
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        payload = mock_enrich.delay.call_args[0][0]
        required = {
            "source_type", "source_id", "source_channel", "author",
            "content", "timestamp", "attachments", "metadata",
        }
        assert required.issubset(set(payload.keys()))

    async def test_source_type_is_discord(self, bot, mock_enrich):
        msg = _make_discord_message()
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["source_type"] == "discord"

    async def test_source_id_is_message_snowflake(self, bot, mock_enrich):
        msg = _make_discord_message(message_id="987654321")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["source_id"] == "987654321"

    async def test_source_channel_is_channel_name(self, bot, mock_enrich):
        msg = _make_discord_message(channel_name="announcements")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["source_channel"] == "announcements"

    async def test_attachments_extracted(self, bot, mock_enrich):
        att = _make_attachment("https://cdn.discordapp.com/a/b/file.png")
        msg = _make_discord_message(attachments=[att])
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["attachments"] == ["https://cdn.discordapp.com/a/b/file.png"]

    async def test_metadata_includes_channel_id_author_id_guild_id(self, bot, mock_enrich):
        msg = _make_discord_message(channel_id="100", author_id="200", guild_id="300")
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        meta = mock_enrich.delay.call_args[0][0]["metadata"]
        assert meta["channel_id"] == "100"
        assert meta["author_id"] == "200"
        assert meta["guild_id"] == "300"

    async def test_guild_id_empty_string_when_no_guild(self, bot, mock_enrich):
        """DMs have no guild; guild_id should be empty string."""
        msg = _make_discord_message(guild=False)
        with patch("seed_storage.ingestion.bot.settings") as s:
            s.discord_channel_ids = []
            await bot.on_message(msg)
        meta = mock_enrich.delay.call_args[0][0]["metadata"]
        assert meta["guild_id"] == ""
