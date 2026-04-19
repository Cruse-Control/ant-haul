"""Tests for watcher capture logic — tests classifier routing and staging calls.

Note: We test the classifier + staging integration rather than mocking Discord,
since the watcher's core logic is just: extract URLs → classify → stage.
"""

import uuid

import pytest

from ingestion.classifier import Platform, classify, extract_urls
from seed_storage import staging


@pytest.fixture(autouse=True)
def ensure_tables():
    staging.init_tables()


class TestWatcherRouting:
    """Verify that the watcher's classify → stage flow works for each platform."""

    def test_instagram_url_stages_correctly(self):
        url = "https://instagram.com/reel/TEST123/"
        uri = f"{url}?t={uuid.uuid4()}"  # Unique for dedup.
        category = classify(url)
        assert category == Platform.INSTAGRAM
        sid = staging.stage(source_type=category.value, source_uri=uri, raw_content="Test IG reel")
        assert sid is not None
        staging.update_status([sid], "deleted")

    def test_youtube_url_stages_correctly(self):
        uri = f"https://youtube.com/watch?v=test{uuid.uuid4().hex[:8]}"
        category = classify(uri)
        assert category == Platform.YOUTUBE
        sid = staging.stage(source_type=category.value, source_uri=uri, raw_content="Test YT video")
        assert sid is not None
        staging.update_status([sid], "deleted")

    def test_github_url_stages_correctly(self):
        uri = f"https://github.com/test-org/test-repo-{uuid.uuid4().hex[:8]}"
        category = classify(uri)
        assert category == Platform.GITHUB
        sid = staging.stage(source_type=category.value, source_uri=uri, raw_content="Test repo")
        assert sid is not None
        staging.update_status([sid], "deleted")

    def test_web_url_stages_correctly(self):
        uri = f"https://blog.example.com/{uuid.uuid4()}"
        category = classify(uri)
        assert category == Platform.WEB
        sid = staging.stage(source_type=category.value, source_uri=uri, raw_content="Test blog")
        assert sid is not None
        staging.update_status([sid], "deleted")

    def test_plain_text_stages_with_discord_uri(self):
        """Messages with no URLs get a discord:// URI and plain_text type."""
        msg_id = uuid.uuid4()
        uri = f"discord://123/456/{msg_id}"
        sid = staging.stage(
            source_type=Platform.PLAIN_TEXT.value,
            source_uri=uri,
            raw_content="Just chatting about AI today",
            author="test-user",
            channel="test-channel",
        )
        assert sid is not None
        items = staging.get_staged(status="staged")
        item = next((i for i in items if str(i["id"]) == sid), None)
        assert item is not None
        assert item["source_type"] == "plain_text"
        staging.update_status([sid], "deleted")

    def test_multiple_urls_each_staged(self):
        """A message with multiple URLs should stage each one separately."""
        text = "Check https://github.com/foo/bar and https://youtube.com/watch?v=abc"
        urls = extract_urls(text)
        assert len(urls) == 2

        sids = []
        for url in urls:
            category = classify(url)
            uri = f"{url}?dedup={uuid.uuid4().hex[:8]}"
            sid = staging.stage(source_type=category.value, source_uri=uri, raw_content=text)
            if sid:
                sids.append(sid)

        assert len(sids) == 2
        for sid in sids:
            staging.update_status([sid], "deleted")

    def test_duplicate_url_not_staged_twice(self):
        uri = f"https://example.com/{uuid.uuid4()}"
        sid1 = staging.stage(source_type="web", source_uri=uri, raw_content="First")
        sid2 = staging.stage(source_type="web", source_uri=uri, raw_content="Duplicate")
        assert sid1 is not None
        assert sid2 is None
        staging.update_status([sid1], "deleted")
