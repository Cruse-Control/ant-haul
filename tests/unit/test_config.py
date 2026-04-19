"""Unit tests for seed_storage.config.Settings.

All tests construct fresh Settings() instances with controlled env vars via
monkeypatch — no real infrastructure or credentials required.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from seed_storage.config import Settings, _read_file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_settings(env: dict[str, str], env_file: str | None = None) -> Settings:
    """Create a Settings instance with only the provided env vars (no .env)."""
    with patch.dict(os.environ, env, clear=True):
        if env_file is None:
            return Settings(_env_file=None)
        return Settings(_env_file=env_file)


# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_neo4j_uri_default(self):
        s = make_settings({})
        assert s.NEO4J_URI == "bolt://neo4j.ant-keeper.svc:7687"

    def test_redis_url_default(self):
        s = make_settings({})
        assert s.REDIS_URL == "redis://redis.ant-keeper.svc:6379/2"

    def test_llm_provider_default(self):
        s = make_settings({})
        assert s.LLM_PROVIDER == "openai"

    def test_daily_budget_default(self):
        s = make_settings({})
        assert s.DAILY_LLM_BUDGET == 5.00

    def test_hard_depth_ceiling_default(self):
        s = make_settings({})
        assert s.HARD_DEPTH_CEILING == 5

    def test_max_expansion_breadth_default(self):
        s = make_settings({})
        assert s.MAX_EXPANSION_BREADTH == 20

    def test_frontier_auto_enabled_default(self):
        s = make_settings({})
        assert s.FRONTIER_AUTO_ENABLED is False

    def test_group_id_default(self):
        s = make_settings({})
        assert s.GROUP_ID == "seed-storage"

    def test_transcription_backend_default(self):
        s = make_settings({})
        assert s.TRANSCRIPTION_BACKEND == "whisper"

    def test_vision_provider_defaults_to_llm_provider(self):
        s = make_settings({"LLM_PROVIDER": "openai"})
        assert s.VISION_PROVIDER == "openai"


# ---------------------------------------------------------------------------
# File-mode credential loading
# ---------------------------------------------------------------------------


class TestFileCredentials:
    def test_neo4j_password_from_file(self, tmp_path):
        secret_file = tmp_path / "neo4j_password"
        secret_file.write_text("super-secret-neo4j-pw\n")

        s = make_settings({"NEO4J_PASSWORD_PATH": str(secret_file)})
        assert s.NEO4J_PASSWORD == "super-secret-neo4j-pw"

    def test_discord_token_from_file(self, tmp_path):
        token_file = tmp_path / "discord_token"
        token_file.write_text("MTIzNDU2Nzg5.token.here")

        s = make_settings({"DISCORD_BOT_TOKEN_PATH": str(token_file)})
        assert s.DISCORD_BOT_TOKEN == "MTIzNDU2Nzg5.token.here"

    def test_webhook_url_from_file(self, tmp_path):
        hook_file = tmp_path / "webhook_url"
        hook_file.write_text("https://discord.com/api/webhooks/123/abc")

        s = make_settings({"DISCORD_ALERTS_WEBHOOK_PATH": str(hook_file)})
        assert s.DISCORD_ALERTS_WEBHOOK_URL == "https://discord.com/api/webhooks/123/abc"

    def test_github_token_from_file(self, tmp_path):
        tok_file = tmp_path / "github_token"
        tok_file.write_text("ghp_example_token_12345")

        s = make_settings({"GITHUB_TOKEN_PATH": str(tok_file)})
        assert s.GITHUB_TOKEN == "ghp_example_token_12345"

    def test_file_cred_strips_trailing_whitespace(self, tmp_path):
        secret_file = tmp_path / "neo4j_password"
        secret_file.write_text("  password-with-spaces  \n")

        s = make_settings({"NEO4J_PASSWORD_PATH": str(secret_file)})
        assert s.NEO4J_PASSWORD == "password-with-spaces"

    def test_direct_env_takes_precedence_over_file(self, tmp_path):
        """When both direct env var and path are set, direct env wins."""
        secret_file = tmp_path / "neo4j_password"
        secret_file.write_text("from-file")

        s = make_settings(
            {
                "NEO4J_PASSWORD": "from-env",
                "NEO4J_PASSWORD_PATH": str(secret_file),
            }
        )
        assert s.NEO4J_PASSWORD == "from-env"

    def test_missing_file_path_leaves_field_empty(self, tmp_path):
        nonexistent = str(tmp_path / "does_not_exist")
        s = make_settings({"NEO4J_PASSWORD_PATH": nonexistent})
        assert s.NEO4J_PASSWORD == ""


# ---------------------------------------------------------------------------
# Missing credentials → ValueError (via validate_credentials)
# ---------------------------------------------------------------------------


class TestValidateCredentials:
    def test_missing_llm_api_key_raises(self):
        s = make_settings({"LLM_PROVIDER": "openai", "OPENAI_API_KEY": ""})
        with pytest.raises(ValueError, match="LLM_PROVIDER"):
            s.validate_credentials()

    def test_missing_openai_key_for_embeddings_raises(self):
        """Even if using anthropic provider, OPENAI_API_KEY must be set for embeddings."""
        s = make_settings(
            {
                "LLM_PROVIDER": "anthropic",
                "ANTHROPIC_API_KEY": "sk-ant-test",
                "OPENAI_API_KEY": "",
            }
        )
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            s.validate_credentials()

    def test_missing_discord_token_raises(self):
        s = make_settings(
            {
                "LLM_PROVIDER": "openai",
                "OPENAI_API_KEY": "sk-test",
                "DISCORD_BOT_TOKEN": "",
            }
        )
        with pytest.raises(ValueError, match="DISCORD_BOT_TOKEN"):
            s.validate_credentials()

    def test_all_credentials_present_does_not_raise(self):
        s = make_settings(
            {
                "LLM_PROVIDER": "openai",
                "OPENAI_API_KEY": "sk-test-key",
                "DISCORD_BOT_TOKEN": "Bot my.token.here",
            }
        )
        s.validate_credentials()  # must not raise


# ---------------------------------------------------------------------------
# LLM_API_KEY resolution per provider
# ---------------------------------------------------------------------------


class TestLlmApiKeyResolution:
    def test_openai_key_returned_for_openai_provider(self):
        s = make_settings({"LLM_PROVIDER": "openai", "OPENAI_API_KEY": "sk-openai-abc"})
        assert s.llm_api_key == "sk-openai-abc"

    def test_anthropic_key_returned_for_anthropic_provider(self):
        s = make_settings({"LLM_PROVIDER": "anthropic", "ANTHROPIC_API_KEY": "sk-ant-abc"})
        assert s.llm_api_key == "sk-ant-abc"

    def test_groq_key_returned_for_groq_provider(self):
        s = make_settings({"LLM_PROVIDER": "groq", "GROQ_API_KEY": "gsk_abc123"})
        assert s.llm_api_key == "gsk_abc123"

    def test_llm_api_key_empty_when_no_key_set(self):
        s = make_settings({"LLM_PROVIDER": "openai"})
        assert s.llm_api_key == ""


# ---------------------------------------------------------------------------
# DISCORD_CHANNEL_IDS parsing
# ---------------------------------------------------------------------------


class TestDiscordChannelIds:
    def test_comma_separated_ids_parsed(self):
        s = make_settings({"DISCORD_CHANNEL_IDS": "111,222,333"})
        assert s.discord_channel_ids == ["111", "222", "333"]

    def test_empty_string_returns_empty_list(self):
        s = make_settings({})
        assert s.discord_channel_ids == []

    def test_whitespace_around_ids_stripped(self):
        s = make_settings({"DISCORD_CHANNEL_IDS": " 111 , 222 , 333 "})
        assert s.discord_channel_ids == ["111", "222", "333"]

    def test_single_id_parsed(self):
        s = make_settings({"DISCORD_CHANNEL_IDS": "123456789"})
        assert s.discord_channel_ids == ["123456789"]

    def test_trailing_comma_ignored(self):
        s = make_settings({"DISCORD_CHANNEL_IDS": "111,222,"})
        assert s.discord_channel_ids == ["111", "222"]


# ---------------------------------------------------------------------------
# TRANSCRIPTION_BACKEND validation
# ---------------------------------------------------------------------------


class TestTranscriptionBackend:
    def test_invalid_backend_raises_value_error(self):
        with pytest.raises(ValidationError, match="TRANSCRIPTION_BACKEND"):
            make_settings({"TRANSCRIPTION_BACKEND": "invalid_backend"})

    def test_assemblyai_backend_accepted(self):
        s = make_settings({"TRANSCRIPTION_BACKEND": "assemblyai"})
        assert s.TRANSCRIPTION_BACKEND == "assemblyai"

    def test_whisper_backend_accepted(self):
        s = make_settings({"TRANSCRIPTION_BACKEND": "whisper"})
        assert s.TRANSCRIPTION_BACKEND == "whisper"


# ---------------------------------------------------------------------------
# VISION_PROVIDER defaults
# ---------------------------------------------------------------------------


class TestVisionProvider:
    def test_vision_provider_defaults_to_llm_provider(self):
        s = make_settings({"LLM_PROVIDER": "openai"})
        assert s.VISION_PROVIDER == "openai"

    def test_vision_provider_explicit_value_preserved(self):
        s = make_settings({"LLM_PROVIDER": "openai", "VISION_PROVIDER": "anthropic"})
        assert s.VISION_PROVIDER == "anthropic"

    def test_vision_provider_inherits_groq(self):
        s = make_settings({"LLM_PROVIDER": "groq"})
        assert s.VISION_PROVIDER == "groq"


# ---------------------------------------------------------------------------
# LLM_PROVIDER validation
# ---------------------------------------------------------------------------


class TestLlmProviderValidation:
    def test_invalid_llm_provider_raises(self):
        with pytest.raises(ValidationError, match="LLM_PROVIDER"):
            make_settings({"LLM_PROVIDER": "gemini"})

    def test_valid_providers_accepted(self):
        for provider in ("openai", "anthropic", "groq"):
            s = make_settings({"LLM_PROVIDER": provider})
            assert s.LLM_PROVIDER == provider


# ---------------------------------------------------------------------------
# Env var precedence over .env file
# ---------------------------------------------------------------------------


class TestEnvPrecedence:
    def test_env_var_beats_dotenv(self, tmp_path):
        dotenv = tmp_path / ".env"
        dotenv.write_text("REDIS_URL=redis://from-dotenv/0\n")

        with patch.dict(os.environ, {"REDIS_URL": "redis://from-env/2"}, clear=True):
            s = Settings(_env_file=str(dotenv))

        assert s.REDIS_URL == "redis://from-env/2"

    def test_dotenv_value_used_when_no_env_override(self, tmp_path):
        dotenv = tmp_path / ".env"
        dotenv.write_text("NEO4J_USER=dotenv-user\n")

        with patch.dict(os.environ, {}, clear=True):
            s = Settings(_env_file=str(dotenv))

        assert s.NEO4J_USER == "dotenv-user"


# ---------------------------------------------------------------------------
# _read_file helper
# ---------------------------------------------------------------------------


class TestReadFileHelper:
    def test_reads_existing_file(self, tmp_path):
        f = tmp_path / "secret"
        f.write_text("my-secret\n")
        assert _read_file(str(f)) == "my-secret"

    def test_returns_empty_for_missing_file(self, tmp_path):
        assert _read_file(str(tmp_path / "nonexistent")) == ""
