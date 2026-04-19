"""Tests for conversation threading logic."""

from datetime import datetime, timezone, timedelta

from ingestion.threader import group_into_threads, _parse_embedded_time, _extract_speakers


def _make_item(channel, minutes_offset, text="test message", item_id="1"):
    base = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    return {
        "id": item_id,
        "channel": channel,
        "raw_content": text,
        "created_at": (base + timedelta(minutes=minutes_offset)).isoformat(),
        "source_type": "plain_text",
    }


class TestGroupIntoThreads:
    def test_three_messages_within_gap(self):
        items = [
            _make_item("imessages", 0, item_id="a"),
            _make_item("imessages", 1, item_id="b"),
            _make_item("imessages", 2, item_id="c"),
        ]
        threads = group_into_threads(items, gap_minutes=5)
        assert len(threads) == 1
        assert len(threads[0]) == 3

    def test_gap_splits_into_two_threads(self):
        items = [
            _make_item("imessages", 0, item_id="a"),
            _make_item("imessages", 1, item_id="b"),
            _make_item("imessages", 10, item_id="c"),  # 10 min gap
        ]
        threads = group_into_threads(items, gap_minutes=5)
        assert len(threads) == 2
        assert len(threads[0]) == 2
        assert len(threads[1]) == 1

    def test_different_channels_split(self):
        items = [
            _make_item("imessages", 0, item_id="a"),
            _make_item("granola-flynn", 1, item_id="b"),
        ]
        threads = group_into_threads(items, gap_minutes=5)
        assert len(threads) == 2

    def test_single_message(self):
        items = [_make_item("imessages", 0)]
        threads = group_into_threads(items, gap_minutes=5)
        assert len(threads) == 1
        assert len(threads[0]) == 1

    def test_empty_input(self):
        assert group_into_threads([], gap_minutes=5) == []


class TestParseEmbeddedTime:
    def test_imessage_format(self):
        text = "[Apr 01, 2026  15:22] **Flynn A. Cruse**: hello"
        dt = _parse_embedded_time(text)
        assert dt is not None
        assert dt.month == 4
        assert dt.day == 1
        assert dt.hour == 15
        assert dt.minute == 22

    def test_no_timestamp(self):
        assert _parse_embedded_time("just a regular message") is None


class TestExtractSpeakers:
    def test_single_speaker(self):
        text = "[Apr 01, 2026  15:22] **Flynn A. Cruse**: hello"
        assert _extract_speakers(text) == ["Flynn A. Cruse"]

    def test_multiple_speakers(self):
        text = "**Flynn A. Cruse**: hi\n**Me**: hello back"
        speakers = _extract_speakers(text)
        assert "Flynn A. Cruse" in speakers
        assert "Me" in speakers

    def test_no_speakers(self):
        assert _extract_speakers("plain text") == []

    def test_deduplication(self):
        text = "**Flynn**: msg1\n**Flynn**: msg2"
        assert _extract_speakers(text) == ["Flynn"]
