"""Unit tests for seed_storage.enrichment.models."""
import pytest
from datetime import datetime, timezone

from seed_storage.enrichment.models import ResolvedContent, ContentType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(
    source_url="https://example.com",
    content_type="webpage",
    title="Test Title",
    text="body text",
    transcript=None,
    summary=None,
    expansion_urls=None,
    metadata=None,
    extraction_error=None,
    resolved_at=None,
) -> ResolvedContent:
    return ResolvedContent(
        source_url=source_url,
        content_type=content_type,
        title=title,
        text=text,
        transcript=transcript,
        summary=summary,
        expansion_urls=expansion_urls or [],
        metadata=metadata or {},
        extraction_error=extraction_error,
        resolved_at=resolved_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# ResolvedContent construction
# ---------------------------------------------------------------------------

def test_basic_construction():
    rc = _make()
    assert rc.source_url == "https://example.com"
    assert rc.content_type == "webpage"
    assert rc.title == "Test Title"
    assert rc.text == "body text"
    assert rc.expansion_urls == []
    assert rc.metadata == {}
    assert rc.extraction_error is None


def test_optional_fields_default_none():
    rc = _make(transcript=None, summary=None)
    assert rc.transcript is None
    assert rc.summary is None


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

def test_to_dict_round_trip_types():
    rc = _make(
        expansion_urls=["https://a.com", "https://b.com"],
        metadata={"key": "value"},
        transcript="hello",
        summary="image desc",
    )
    d = rc.to_dict()
    assert isinstance(d["resolved_at"], str), "resolved_at must be ISO string"
    assert isinstance(d["expansion_urls"], list)
    assert isinstance(d["metadata"], dict)


def test_to_dict_datetime_iso_format():
    dt = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    rc = _make(resolved_at=dt)
    d = rc.to_dict()
    assert "2024-06-15" in d["resolved_at"]
    assert "10:30:00" in d["resolved_at"]


def test_to_dict_none_fields_preserved():
    rc = _make(title=None, transcript=None, summary=None, extraction_error=None)
    d = rc.to_dict()
    assert d["title"] is None
    assert d["transcript"] is None
    assert d["summary"] is None
    assert d["extraction_error"] is None


def test_to_dict_expansion_urls_is_copy():
    original = ["https://a.com"]
    rc = _make(expansion_urls=original)
    d = rc.to_dict()
    d["expansion_urls"].append("https://extra.com")
    assert rc.expansion_urls == original, "to_dict must return a copy"


# ---------------------------------------------------------------------------
# from_dict
# ---------------------------------------------------------------------------

def test_from_dict_round_trip():
    rc = _make(
        expansion_urls=["https://a.com"],
        metadata={"k": 1},
        transcript="tx",
        summary="sm",
        extraction_error=None,
    )
    d = rc.to_dict()
    rc2 = ResolvedContent.from_dict(d)
    assert rc2.source_url == rc.source_url
    assert rc2.content_type == rc.content_type
    assert rc2.title == rc.title
    assert rc2.text == rc.text
    assert rc2.transcript == rc.transcript
    assert rc2.summary == rc.summary
    assert rc2.expansion_urls == rc.expansion_urls
    assert rc2.metadata == rc.metadata
    assert rc2.extraction_error == rc.extraction_error
    assert rc2.resolved_at == rc.resolved_at


def test_from_dict_ignores_unknown_keys():
    d = _make().to_dict()
    d["future_field"] = "something"
    rc = ResolvedContent.from_dict(d)
    assert rc.source_url == "https://example.com"


def test_from_dict_unknown_content_type_becomes_unknown():
    d = _make().to_dict()
    d["content_type"] = "totally_new_type"
    rc = ResolvedContent.from_dict(d)
    assert rc.content_type == "unknown"


def test_from_dict_accepts_datetime_object():
    dt = datetime(2024, 3, 10, tzinfo=timezone.utc)
    d = _make(resolved_at=dt).to_dict()
    d["resolved_at"] = dt  # pass datetime directly instead of ISO string
    rc = ResolvedContent.from_dict(d)
    assert rc.resolved_at == dt


def test_from_dict_missing_optional_fields_default():
    d = {
        "source_url": "https://x.com",
        "content_type": "github",
        "resolved_at": "2024-01-01T00:00:00+00:00",
    }
    rc = ResolvedContent.from_dict(d)
    assert rc.text == ""
    assert rc.expansion_urls == []
    assert rc.metadata == {}
    assert rc.title is None
    assert rc.transcript is None
    assert rc.summary is None
    assert rc.extraction_error is None


# ---------------------------------------------------------------------------
# error_result factory
# ---------------------------------------------------------------------------

def test_error_result_fields():
    rc = ResolvedContent.error_result("https://bad.com", "timeout")
    assert rc.source_url == "https://bad.com"
    assert rc.extraction_error == "timeout"
    assert rc.text == ""
    assert rc.content_type == "unknown"
    assert rc.title is None
    assert rc.transcript is None
    assert rc.summary is None
    assert rc.expansion_urls == []
    assert rc.metadata == {}


def test_error_result_resolved_at_is_utc_now():
    before = datetime.now(tz=timezone.utc)
    rc = ResolvedContent.error_result("https://x.com", "err")
    after = datetime.now(tz=timezone.utc)
    assert before <= rc.resolved_at <= after


def test_error_result_empty_error_string():
    rc = ResolvedContent.error_result("https://x.com", "")
    assert rc.extraction_error == ""


def test_error_result_to_dict_serializable():
    rc = ResolvedContent.error_result("https://fail.com", "network error")
    d = rc.to_dict()
    assert isinstance(d["resolved_at"], str)
    assert d["extraction_error"] == "network error"
