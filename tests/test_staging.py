"""Integration tests for staging module — requires PostgreSQL at port 30433."""

import uuid

import pytest

from seed_storage import staging


@pytest.fixture(autouse=True)
def ensure_tables():
    """Ensure staging tables exist before tests."""
    staging.init_tables()


@pytest.fixture
def test_uri():
    return f"https://test.example.com/{uuid.uuid4()}"


@pytest.fixture
def staged(test_uri):
    """Stage one item, yield its ID, clean up."""
    sid = staging.stage(
        source_type="web",
        source_uri=test_uri,
        raw_content="Integration test content.",
        author="test-bot",
        channel="test-channel",
    )
    yield sid
    # Cleanup.
    try:
        staging.update_status([sid], "deleted")
    except Exception:
        pass


class TestStage:
    def test_stage_returns_uuid(self, staged):
        assert staged is not None
        uuid.UUID(staged)  # Validates it's a real UUID.

    def test_stage_dedup(self, test_uri):
        sid1 = staging.stage(source_type="web", source_uri=test_uri, raw_content="First")
        sid2 = staging.stage(source_type="web", source_uri=test_uri, raw_content="Duplicate")
        assert sid1 is not None
        assert sid2 is None  # Deduped.
        staging.update_status([sid1], "deleted")

    def test_word_count(self, test_uri):
        staging.stage(source_type="web", source_uri=test_uri, raw_content="one two three four five")
        items = staging.get_staged(status="staged", limit=1000)
        item = next(i for i in items if i["source_uri"] == test_uri)
        assert item["word_count"] == 5
        assert item["token_estimate"] == 6  # int(5 * 1.33)
        staging.update_status([str(item["id"])], "deleted")


class TestGetStaged:
    def test_get_staged_by_status(self, staged):
        items = staging.get_staged(status="staged")
        uris = [i["source_uri"] for i in items]
        item = next(i for i in items if str(i["id"]) == staged)
        assert item is not None


class TestUpdateContent:
    def test_update_content(self, staged):
        staging.update_content(staged, "Updated content", metadata={"key": "val"}, status="processed")
        items = staging.get_staged(status="processed")
        item = next((i for i in items if str(i["id"]) == staged), None)
        assert item is not None
        assert item["raw_content"] == "Updated content"
        assert item["word_count"] == 2


class TestUpdateStatus:
    def test_batch_update(self, staged):
        staging.update_status([staged], "processed")
        items = staging.get_staged(status="processed")
        ids = [str(i["id"]) for i in items]
        assert staged in ids


class TestSummary:
    def test_summary_structure(self):
        result = staging.summary()
        assert "by_type" in result
        assert "total_tokens" in result
        assert "total_items" in result


class TestCountByStatus:
    def test_count_returns_dict(self):
        result = staging.count_by_status()
        assert isinstance(result, dict)
