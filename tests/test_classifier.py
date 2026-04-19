"""Unit tests for URL classifier — no external dependencies."""

import pytest

from ingestion.classifier import Platform, classify, extract_urls, clean_url


class TestClassify:
    def test_instagram_reel(self):
        assert classify("https://www.instagram.com/reel/ABC123/") == Platform.INSTAGRAM

    def test_instagram_image_post(self):
        assert classify("https://instagram.com/p/XYZ789/") == Platform.INSTAGRAM_IMAGE

    def test_instagram_image_with_index(self):
        assert classify("https://www.instagram.com/p/DWF4/?img_index=3") == Platform.INSTAGRAM_IMAGE

    def test_instagram_reels(self):
        assert classify("https://www.instagram.com/reels/DEF456/") == Platform.INSTAGRAM

    def test_youtube_watch(self):
        assert classify("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == Platform.YOUTUBE

    def test_youtube_short(self):
        assert classify("https://youtube.com/shorts/abc123") == Platform.YOUTUBE

    def test_youtu_be(self):
        assert classify("https://youtu.be/dQw4w9WgXcQ") == Platform.YOUTUBE

    def test_x_status(self):
        assert classify("https://x.com/kaborycz/status/1234567890") == Platform.X_TWITTER

    def test_twitter_status(self):
        assert classify("https://twitter.com/karpathy/status/9876543210") == Platform.X_TWITTER

    def test_github_repo(self):
        assert classify("https://github.com/Cruse-Control/seed-storage") == Platform.GITHUB

    def test_github_deep(self):
        assert classify("https://github.com/anthropics/claude-code/tree/main/src") == Platform.GITHUB

    def test_web_fallback(self):
        assert classify("https://latentpatterns.com/principles") == Platform.WEB

    def test_web_substack(self):
        assert classify("https://karpathy.substack.com/p/some-post") == Platform.WEB

    def test_web_no_path(self):
        assert classify("https://example.com") == Platform.WEB

    # New platform types
    def test_discord_invite(self):
        assert classify("https://discord.com/channels/123/456") == Platform.DISCORD_LINK

    def test_discord_gg(self):
        assert classify("https://discord.gg/Dy3TmK6Npq") == Platform.DISCORD_LINK

    def test_spotify(self):
        assert classify("https://open.spotify.com/playlist/abc") == Platform.MEDIA_LINK

    def test_tiktok(self):
        assert classify("https://www.tiktok.com/@user/video/123") == Platform.MEDIA_LINK

    def test_apple_music(self):
        assert classify("https://music.apple.com/us/album/abc") == Platform.MEDIA_LINK


class TestCleanUrl:
    def test_strips_utm(self):
        url = "https://example.com/post?utm_source=twitter&utm_medium=social&id=123"
        assert clean_url(url) == "https://example.com/post?id=123"

    def test_strips_igsh(self):
        url = "https://instagram.com/reel/ABC/?igsh=abc123"
        assert clean_url(url) == "https://instagram.com/reel/ABC/"

    def test_strips_fbclid(self):
        url = "https://example.com/page?fbclid=abc123"
        assert clean_url(url) == "https://example.com/page"

    def test_preserves_meaningful_params(self):
        url = "https://youtube.com/watch?v=abc123"
        assert clean_url(url) == "https://youtube.com/watch?v=abc123"

    def test_strips_trailing_paren(self):
        url = "https://discord.com/channels/123/456)"
        assert clean_url(url) == "https://discord.com/channels/123/456"

    def test_no_params(self):
        url = "https://example.com/page"
        assert clean_url(url) == "https://example.com/page"


class TestExtractUrls:
    def test_single_url(self):
        urls = extract_urls("Check this out https://github.com/foo/bar cool right?")
        assert urls == ["https://github.com/foo/bar"]

    def test_multiple_urls(self):
        text = "See https://x.com/a/status/1 and https://youtube.com/watch?v=abc"
        urls = extract_urls(text)
        assert len(urls) == 2

    def test_no_urls(self):
        assert extract_urls("Just a plain text message with no links") == []

    def test_empty(self):
        assert extract_urls("") == []

    def test_strips_tracking_on_extract(self):
        urls = extract_urls("Check https://example.com/post?utm_source=twitter&id=5")
        assert urls == ["https://example.com/post?id=5"]
