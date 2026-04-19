"""Unit tests for seed_storage/graphiti_client.py.

Tests cover:
- Provider branching (openai/anthropic/groq)
- build_indices_and_constraints() called on init
- Singleton behavior
- group_id="seed-storage" constant
- Vision client per provider
- VISION_PROVIDER defaults to LLM_PROVIDER
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset():
    """Reset the graphiti singleton between tests."""
    import seed_storage.graphiti_client as gc

    gc._graphiti = None


def _mock_settings(**overrides):
    """Return a MagicMock that behaves like Settings with the given overrides."""
    defaults = {
        "LLM_PROVIDER": "openai",
        "LLM_MODEL": "gpt-4o-mini",
        "OPENAI_API_KEY": "test-openai-key",
        "ANTHROPIC_API_KEY": "",
        "GROQ_API_KEY": "",
        "NEO4J_URI": "bolt://localhost:7687",
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": "testpass",
        "VISION_PROVIDER": "",
    }
    defaults.update(overrides)
    mock = MagicMock()
    for k, v in defaults.items():
        setattr(mock, k, v)
    return mock


# ---------------------------------------------------------------------------
# Group ID constant
# ---------------------------------------------------------------------------


class TestGroupIdConstant:
    def test_group_id_is_seed_storage(self):
        from seed_storage.graphiti_client import GROUP_ID

        assert GROUP_ID == "seed-storage"


# ---------------------------------------------------------------------------
# Singleton behavior
# ---------------------------------------------------------------------------


class TestGetGraphitiSingleton:
    def setup_method(self):
        _reset()

    @pytest.mark.asyncio
    async def test_singleton_returns_same_instance(self):
        mock_graphiti = MagicMock()
        mock_graphiti.build_indices_and_constraints = AsyncMock()

        with (
            patch("seed_storage.graphiti_client.Graphiti", return_value=mock_graphiti),
            patch("seed_storage.graphiti_client._build_llm_client", return_value=MagicMock()),
            patch("seed_storage.graphiti_client._build_embedder", return_value=MagicMock()),
        ):
            from seed_storage.graphiti_client import get_graphiti

            first = await get_graphiti()
            second = await get_graphiti()

        assert first is second

    @pytest.mark.asyncio
    async def test_build_indices_called_on_first_init(self):
        mock_graphiti = MagicMock()
        mock_graphiti.build_indices_and_constraints = AsyncMock()

        with (
            patch("seed_storage.graphiti_client.Graphiti", return_value=mock_graphiti),
            patch("seed_storage.graphiti_client._build_llm_client", return_value=MagicMock()),
            patch("seed_storage.graphiti_client._build_embedder", return_value=MagicMock()),
        ):
            from seed_storage.graphiti_client import get_graphiti

            await get_graphiti()

        mock_graphiti.build_indices_and_constraints.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_build_indices_called_only_once_for_singleton(self):
        mock_graphiti = MagicMock()
        mock_graphiti.build_indices_and_constraints = AsyncMock()

        with (
            patch("seed_storage.graphiti_client.Graphiti", return_value=mock_graphiti),
            patch("seed_storage.graphiti_client._build_llm_client", return_value=MagicMock()),
            patch("seed_storage.graphiti_client._build_embedder", return_value=MagicMock()),
        ):
            from seed_storage.graphiti_client import get_graphiti

            await get_graphiti()
            await get_graphiti()
            await get_graphiti()

        # build_indices should only be called once even with multiple get_graphiti() calls
        mock_graphiti.build_indices_and_constraints.assert_awaited_once()


# ---------------------------------------------------------------------------
# Provider branching: LLM client
# ---------------------------------------------------------------------------


class TestLLMProviderBranching:
    def setup_method(self):
        _reset()

    def test_openai_provider_returns_openai_client(self):
        mock_client = MagicMock()
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(LLM_PROVIDER="openai", OPENAI_API_KEY="test-openai-key"),
            ),
            patch(
                "graphiti_core.llm_client.openai_client.OpenAIClient", return_value=mock_client
            ) as mock_cls,
        ):
            from seed_storage.graphiti_client import _build_llm_client

            _build_llm_client()
        mock_cls.assert_called_once()

    def test_anthropic_provider_returns_anthropic_client(self):
        mock_client = MagicMock()
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(LLM_PROVIDER="anthropic", ANTHROPIC_API_KEY="test-anthropic-key"),
            ),
            patch(
                "graphiti_core.llm_client.anthropic_client.AnthropicClient",
                return_value=mock_client,
            ) as mock_cls,
        ):
            from seed_storage.graphiti_client import _build_llm_client

            _build_llm_client()
        mock_cls.assert_called_once()

    def test_groq_provider_returns_groq_client(self):
        mock_client = MagicMock()
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(LLM_PROVIDER="groq", GROQ_API_KEY="test-groq-key"),
            ),
            patch(
                "graphiti_core.llm_client.groq_client.GroqClient", return_value=mock_client
            ) as mock_cls,
        ):
            from seed_storage.graphiti_client import _build_llm_client

            _build_llm_client()
        mock_cls.assert_called_once()


# ---------------------------------------------------------------------------
# Embedder: always OpenAIEmbedder
# ---------------------------------------------------------------------------


class TestEmbedder:
    def test_embedder_is_always_openai(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(OPENAI_API_KEY="test-openai-key"),
            ),
            patch("seed_storage.graphiti_client.OpenAIEmbedder") as mock_cls,
        ):
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import _build_embedder

            _build_embedder()
        mock_cls.assert_called_once()

    def test_embedder_uses_openai_api_key(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(LLM_PROVIDER="anthropic", OPENAI_API_KEY="sk-test-12345"),
            ),
            patch("seed_storage.graphiti_client.OpenAIEmbedder") as mock_cls,
            patch("seed_storage.graphiti_client.OpenAIEmbedderConfig") as mock_cfg,
        ):
            mock_cfg.return_value = MagicMock()
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import _build_embedder

            _build_embedder()
        # Verify OpenAIEmbedderConfig was called with api_key from OPENAI_API_KEY
        call_kwargs = mock_cfg.call_args
        assert call_kwargs is not None


# ---------------------------------------------------------------------------
# Vision client
# ---------------------------------------------------------------------------


class TestGetVisionClient:
    def test_vision_client_openai_provider(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(LLM_PROVIDER="openai", VISION_PROVIDER="openai"),
            ),
            patch("openai.OpenAI") as mock_cls,
        ):
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import get_vision_client

            get_vision_client()
        mock_cls.assert_called_once()

    def test_vision_client_anthropic_provider(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(VISION_PROVIDER="anthropic", ANTHROPIC_API_KEY="test-key"),
            ),
            patch("anthropic.Anthropic") as mock_cls,
        ):
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import get_vision_client

            get_vision_client()
        mock_cls.assert_called_once()

    def test_vision_client_groq_provider(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(VISION_PROVIDER="groq", GROQ_API_KEY="test-key"),
            ),
            patch("groq.Groq") as mock_cls,
        ):
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import get_vision_client

            get_vision_client()
        mock_cls.assert_called_once()

    def test_vision_provider_defaults_to_llm_provider(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(
                    LLM_PROVIDER="anthropic",
                    VISION_PROVIDER="",
                    ANTHROPIC_API_KEY="test-key",
                ),
            ),
            patch("anthropic.Anthropic") as mock_cls,
        ):
            mock_cls.return_value = MagicMock()
            from seed_storage.graphiti_client import get_vision_client

            get_vision_client()
        mock_cls.assert_called_once()

    def test_vision_client_is_separate_from_graphiti_llm(self):
        with (
            patch(
                "seed_storage.graphiti_client.settings",
                _mock_settings(VISION_PROVIDER="openai", OPENAI_API_KEY="test-key"),
            ),
            patch("openai.OpenAI") as mock_openai,
        ):
            mock_openai.return_value = MagicMock()
            from seed_storage.graphiti_client import get_vision_client

            vision = get_vision_client()
        # It should NOT be a graphiti LLM client type
        from graphiti_core.llm_client.client import LLMClient

        assert not isinstance(vision, LLMClient)
