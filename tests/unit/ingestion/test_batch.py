"""Unit tests for seed_storage/ingestion/batch.py.

All file I/O uses tmp_path. enrich_message.delay() is mocked. Tests cover:
- JSON parsed correctly
- raw_payload shape matches Contract 1
- --offset skips first N messages
- 5000 cap (verified via monkeypatched BATCH_CAP)
- Progress logging every 100 messages (verified via monkeypatched cap)
- Failure logged on malformed entry
- Summary dict format
- Empty file → zero summary
- Malformed entry → skip and continue
- source_type is always "discord"
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_discord_message(
    msg_id: str = "1",
    content: str = "Hello",
    author_id: str = "10",
    author_name: str = "Alice",
    is_bot: bool = False,
    attachments: list | None = None,
    timestamp: str = "2024-01-01T12:00:00+00:00",
) -> dict:
    """Build a minimal DiscordChatExporter message dict."""
    return {
        "id": msg_id,
        "timestamp": timestamp,
        "content": content,
        "author": {
            "id": author_id,
            "name": author_name,
            "nickname": author_name,
            "isBot": is_bot,
        },
        "attachments": attachments or [],
    }


def _make_export(messages: list[dict], channel_name: str = "general",
                 channel_id: str = "100", guild_id: str = "200") -> dict:
    """Build a minimal DiscordChatExporter export dict."""
    return {
        "guild": {"id": guild_id, "name": "Test Server"},
        "channel": {"id": channel_id, "name": channel_name},
        "messages": messages,
    }


def _write_export(tmp_path: Path, data: dict, filename: str = "export.json") -> Path:
    f = tmp_path / filename
    f.write_text(json.dumps(data), encoding="utf-8")
    return f


@pytest.fixture
def mock_enrich():
    with patch("seed_storage.ingestion.batch._enrich_message") as m:
        yield m


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestImportFile:
    def test_json_parsed_correctly(self, mock_enrich, tmp_path):
        export = _make_export([_make_discord_message("1", content="Hello")])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f)
        assert result["enqueued"] == 1
        mock_enrich.delay.assert_called_once()

    def test_raw_payload_shape_matches_contract(self, mock_enrich, tmp_path):
        export = _make_export([_make_discord_message("42", content="Test content")])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        import_file(f)
        payload = mock_enrich.delay.call_args[0][0]
        required = {
            "source_type", "source_id", "source_channel", "author",
            "content", "timestamp", "attachments", "metadata",
        }
        assert required.issubset(set(payload.keys()))

    def test_source_type_is_discord(self, mock_enrich, tmp_path):
        export = _make_export([_make_discord_message("1", content="Hi")])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        import_file(f)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["source_type"] == "discord"

    def test_source_id_is_message_id(self, mock_enrich, tmp_path):
        export = _make_export([_make_discord_message("99999", content="Hello")])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        import_file(f)
        payload = mock_enrich.delay.call_args[0][0]
        assert payload["source_id"] == "99999"

    def test_offset_skips_first_n_messages(self, mock_enrich, tmp_path):
        messages = [_make_discord_message(str(i), content=f"msg{i}") for i in range(5)]
        export = _make_export(messages)
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f, offset=3)
        assert result["enqueued"] == 2
        assert mock_enrich.delay.call_count == 2
        # Verify the first enqueued message is msg3, not msg0
        first_payload = mock_enrich.delay.call_args_list[0][0][0]
        assert first_payload["source_id"] == "3"

    def test_batch_cap_limits_enqueued(self, mock_enrich, tmp_path, monkeypatch):
        monkeypatch.setattr("seed_storage.ingestion.batch.BATCH_CAP", 3)
        messages = [_make_discord_message(str(i), content=f"content {i}") for i in range(5)]
        export = _make_export(messages)
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f)
        assert result["enqueued"] == 3
        assert mock_enrich.delay.call_count == 3

    def test_progress_logged_every_100(self, mock_enrich, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr("seed_storage.ingestion.batch.BATCH_CAP", 250)
        messages = [_make_discord_message(str(i), content=f"msg {i}") for i in range(201)]
        export = _make_export(messages)
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        with caplog.at_level(logging.INFO, logger="seed_storage.ingestion.batch"):
            import_file(f)
        progress_logs = [r for r in caplog.records if "Progress:" in r.message]
        assert len(progress_logs) >= 2  # at 100 and 200

    def test_failure_logged_on_malformed_entry(self, mock_enrich, tmp_path, caplog):
        # Missing 'id' key makes the entry malformed
        bad_msg = {"timestamp": "2024-01-01T12:00:00+00:00", "content": "oops",
                   "author": {"id": "1", "name": "X", "isBot": False}, "attachments": []}
        # Remove 'id' to force a KeyError in _parse_message
        del bad_msg  # recreate without id
        bad_msg = {"timestamp": "2024-01-01T12:00:00+00:00", "content": "oops",
                   "author": {"name": "X", "isBot": False}, "attachments": []}
        # 'id' is required — omit it
        bad_msg_no_id = {"timestamp": "2024-01-01T12:00:00+00:00", "content": "oops",
                         "author": {"name": "X", "isBot": False}, "attachments": []}
        good_msg = _make_discord_message("1", content="good")
        export = _make_export([bad_msg_no_id, good_msg])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        with caplog.at_level(logging.WARNING, logger="seed_storage.ingestion.batch"):
            result = import_file(f)
        assert result["failed"] == 1
        assert result["enqueued"] == 1
        assert any("Malformed" in r.message for r in caplog.records)

    def test_summary_format(self, mock_enrich, tmp_path):
        export = _make_export([_make_discord_message("1", content="Hi")])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f)
        assert set(result.keys()) == {"total", "enqueued", "skipped", "failed"}
        assert isinstance(result["total"], int)
        assert isinstance(result["enqueued"], int)
        assert isinstance(result["skipped"], int)
        assert isinstance(result["failed"], int)

    def test_empty_file_returns_zero_summary(self, mock_enrich, tmp_path):
        f = tmp_path / "empty.json"
        f.write_text("", encoding="utf-8")
        from seed_storage.ingestion.batch import import_file
        result = import_file(f)
        assert result == {"total": 0, "enqueued": 0, "skipped": 0, "failed": 0}
        mock_enrich.delay.assert_not_called()

    def test_malformed_entry_skip_and_continue(self, mock_enrich, tmp_path):
        """A malformed entry is skipped; subsequent valid entries are still processed."""
        bad = {"content": "no id", "author": {"name": "A", "isBot": False},
               "timestamp": "2024-01-01T12:00:00+00:00", "attachments": []}
        good1 = _make_discord_message("1", content="first valid")
        good2 = _make_discord_message("2", content="second valid")
        export = _make_export([good1, bad, good2])
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f)
        assert result["enqueued"] == 2
        assert result["failed"] == 1

    def test_metadata_includes_required_fields(self, mock_enrich, tmp_path):
        export = _make_export(
            [_make_discord_message("1", content="Hello", author_id="55")],
            channel_id="100",
            guild_id="200",
        )
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        import_file(f)
        meta = mock_enrich.delay.call_args[0][0]["metadata"]
        assert "channel_id" in meta
        assert "author_id" in meta
        assert "guild_id" in meta
        assert meta["channel_id"] == "100"
        assert meta["author_id"] == "55"
        assert meta["guild_id"] == "200"

    def test_total_reflects_full_file_count(self, mock_enrich, tmp_path):
        """total in summary counts all messages in file, not just processed ones."""
        messages = [_make_discord_message(str(i), content=f"msg{i}") for i in range(10)]
        export = _make_export(messages)
        f = _write_export(tmp_path, export)
        from seed_storage.ingestion.batch import import_file
        result = import_file(f, offset=3)
        assert result["total"] == 10  # full file count
        assert result["enqueued"] == 7  # only after offset
