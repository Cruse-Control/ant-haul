"""Unit tests for seed_storage.worker.dead_letters."""

import json
from unittest.mock import MagicMock, patch

import pytest

from seed_storage.worker.dead_letters import (
    DEAD_LETTERS_KEY,
    dead_letter,
    list_dead_letters,
    replay_all,
    replay_one,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_redis():
    return MagicMock()


def _make_entry(**overrides) -> str:
    """Serialize a minimal dead-letter entry to JSON."""
    base = {
        "task_name": "enrich_message",
        "payload": {"source_id": "discord:123", "content": "hello"},
        "source_id": "discord:123",
        "exception_type": "ValueError",
        "exception_message": "bad input",
        "traceback": "Traceback ...",
        "retries": 3,
        "failed_at": "2026-04-18T00:00:00+00:00",
    }
    base.update(overrides)
    return json.dumps(base)


# ---------------------------------------------------------------------------
# dead_letter: RPUSH stores entry
# ---------------------------------------------------------------------------

def test_rpush_stores_entry():
    """dead_letter() calls RPUSH on DEAD_LETTERS_KEY with a JSON-encoded entry."""
    mock_redis_client = MagicMock()

    with patch("seed_storage.worker.dead_letters.redis_lib.from_url", return_value=mock_redis_client), \
         patch("seed_storage.worker.dead_letters.settings") as mock_settings:
        mock_settings.REDIS_URL = "redis://localhost/2"

        exc = ValueError("something broke")
        dead_letter("enrich_message", {"source_id": "discord:99"}, exc, retries=2)

    mock_redis_client.rpush.assert_called_once()
    key, raw = mock_redis_client.rpush.call_args[0]
    assert key == DEAD_LETTERS_KEY
    entry = json.loads(raw)
    assert entry["task_name"] == "enrich_message"


# ---------------------------------------------------------------------------
# dead_letter: all required fields present
# ---------------------------------------------------------------------------

def test_all_required_fields():
    """The stored entry contains all required metadata fields."""
    mock_redis_client = MagicMock()

    with patch("seed_storage.worker.dead_letters.redis_lib.from_url", return_value=mock_redis_client), \
         patch("seed_storage.worker.dead_letters.settings") as mock_settings:
        mock_settings.REDIS_URL = "redis://localhost/2"

        exc = RuntimeError("oops")
        dead_letter("ingest_episode", {"url": "https://example.com"}, exc, retries=1)

    _, raw = mock_redis_client.rpush.call_args[0]
    entry = json.loads(raw)

    for field in ("task_name", "payload", "source_id", "exception_type",
                  "exception_message", "traceback", "retries", "failed_at"):
        assert field in entry, f"Missing field: {field}"

    assert entry["exception_type"] == "RuntimeError"
    assert entry["retries"] == 1


# ---------------------------------------------------------------------------
# dead_letter: source_id extraction from payload
# ---------------------------------------------------------------------------

def test_source_id_extracted_from_source_id_field():
    """source_id is read directly from payload["source_id"] when present."""
    mock_redis_client = MagicMock()

    with patch("seed_storage.worker.dead_letters.redis_lib.from_url", return_value=mock_redis_client), \
         patch("seed_storage.worker.dead_letters.settings") as mock_settings:
        mock_settings.REDIS_URL = "redis://localhost/2"

        dead_letter("enrich_message", {"source_id": "discord:42"}, ValueError("x"), 0)

    _, raw = mock_redis_client.rpush.call_args[0]
    assert json.loads(raw)["source_id"] == "discord:42"


def test_source_id_fallback_to_url():
    """source_id falls back to payload["url"] when source_id is absent."""
    mock_redis_client = MagicMock()

    with patch("seed_storage.worker.dead_letters.redis_lib.from_url", return_value=mock_redis_client), \
         patch("seed_storage.worker.dead_letters.settings") as mock_settings:
        mock_settings.REDIS_URL = "redis://localhost/2"

        dead_letter("expand_from_frontier", {"url": "https://example.com/page"}, ValueError("x"), 0)

    _, raw = mock_redis_client.rpush.call_args[0]
    assert json.loads(raw)["source_id"] == "https://example.com/page"


def test_source_id_fallback_unknown():
    """source_id is '<unknown>' when neither source_id nor url are in payload."""
    mock_redis_client = MagicMock()

    with patch("seed_storage.worker.dead_letters.redis_lib.from_url", return_value=mock_redis_client), \
         patch("seed_storage.worker.dead_letters.settings") as mock_settings:
        mock_settings.REDIS_URL = "redis://localhost/2"

        dead_letter("scan_frontier", {}, ValueError("x"), 0)

    _, raw = mock_redis_client.rpush.call_args[0]
    assert json.loads(raw)["source_id"] == "<unknown>"


# ---------------------------------------------------------------------------
# list_dead_letters: LRANGE without consuming
# ---------------------------------------------------------------------------

def test_list_without_consuming(mock_redis):
    """list_dead_letters uses LRANGE and never calls LPOP."""
    mock_redis.lrange.return_value = [_make_entry(), _make_entry(task_name="ingest_episode")]

    count, entries = list_dead_letters(mock_redis)

    mock_redis.lrange.assert_called_once_with(DEAD_LETTERS_KEY, 0, -1)
    mock_redis.lpop.assert_not_called()
    assert count == 2
    assert entries[0]["task_name"] == "enrich_message"
    assert entries[1]["task_name"] == "ingest_episode"


# ---------------------------------------------------------------------------
# replay_one: LPOP returns entry
# ---------------------------------------------------------------------------

def test_replay_one_lpop(mock_redis):
    """replay_one calls LPOP and returns (task_name, payload)."""
    mock_redis.lpop.return_value = _make_entry()

    result = replay_one(mock_redis)

    mock_redis.lpop.assert_called_once_with(DEAD_LETTERS_KEY)
    assert result is not None
    task_name, payload = result
    assert task_name == "enrich_message"
    assert payload["source_id"] == "discord:123"


def test_replay_one_empty_queue_returns_none(mock_redis):
    """replay_one returns None when the queue is empty."""
    mock_redis.lpop.return_value = None

    result = replay_one(mock_redis)

    assert result is None


def test_unknown_task_name_logs_warning(mock_redis, caplog):
    """replay_one logs a WARNING when task_name is missing from the entry."""
    entry_no_task = json.dumps({
        "task_name": "",
        "payload": {},
        "retries": 0,
        "failed_at": "2026-04-18T00:00:00+00:00",
    })
    mock_redis.lpop.return_value = entry_no_task

    with caplog.at_level("WARNING", logger="seed_storage.worker.dead_letters"):
        result = replay_one(mock_redis)

    assert result is not None
    assert any("unknown task_name" in r.message.lower() or "task_name" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# replay_all: pops all entries
# ---------------------------------------------------------------------------

def test_replay_all(mock_redis):
    """replay_all pops every entry and returns a list of (task_name, payload)."""
    entries = [
        _make_entry(task_name="enrich_message"),
        _make_entry(task_name="ingest_episode"),
    ]
    # LPOP returns entries in order, then None to signal empty
    mock_redis.lpop.side_effect = [entries[0], entries[1], None]

    results = replay_all(mock_redis)

    assert len(results) == 2
    assert results[0][0] == "enrich_message"
    assert results[1][0] == "ingest_episode"
    assert mock_redis.lpop.call_count == 3
