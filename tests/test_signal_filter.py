"""Unit tests for signal filter — no external dependencies."""

from ingestion.signal_filter import is_noise


class TestIsNoise:
    # Should be filtered (noise).
    def test_ant_keeper_notify(self):
        assert is_noise("[NOTIFY] task instagram-video-analyzer failed") is True

    def test_queen_ant(self):
        assert is_noise("QUEEN_ANT [NOTIFY] daemon crashed") is True

    def test_alert_emoji(self):
        assert is_noise("⚠️ task failed after 3 retries") is True

    def test_single_word_fluff(self):
        assert is_noise("ok") is True
        assert is_noise("thanks") is True
        assert is_noise("lol") is True
        assert is_noise("LOL") is True
        assert is_noise("Nice") is True

    def test_too_short(self):
        assert is_noise("hi") is True
        assert is_noise("ok") is True
        assert is_noise("") is True

    def test_error_log_line(self):
        assert is_noise("[ERROR] connection refused") is True

    # Should pass through (not noise).
    def test_url_context(self):
        assert is_noise("Check out this project https://github.com/foo/bar") is False

    def test_substantive_message(self):
        assert is_noise("I think we should use Graphiti for the knowledge graph") is False

    def test_conversation_fragment(self):
        assert is_noise("That reminds me of what Karpathy said about knowledge bases") is False

    def test_question(self):
        assert is_noise("How does the adjudicator decide if visuals matter?") is False

    def test_six_chars(self):
        assert is_noise("agreed") is False  # 6 chars, passes min length, not in fluff list
