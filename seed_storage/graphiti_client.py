"""Graphiti client singleton with provider branching and vision client factory.

All add_episode() calls must use group_id="seed-storage". Never per-channel.
"""

from __future__ import annotations

from typing import Any

from graphiti_core import Graphiti
from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
from graphiti_core.llm_client.config import LLMConfig

from seed_storage.config import settings

GROUP_ID = "seed-storage"

_graphiti: Graphiti | None = None


def _build_llm_client():
    """Build the Graphiti LLM client based on settings.LLM_PROVIDER.

    Provider branching:
    - openai  → OpenAIClient
    - anthropic → AnthropicClient
    - groq    → GroqClient
    """
    provider = settings.LLM_PROVIDER
    model = settings.LLM_MODEL

    if provider == "anthropic":
        from graphiti_core.llm_client.anthropic_client import AnthropicClient

        return AnthropicClient(config=LLMConfig(api_key=settings.ANTHROPIC_API_KEY, model=model))

    if provider == "groq":
        from graphiti_core.llm_client.groq_client import GroqClient

        return GroqClient(config=LLMConfig(api_key=settings.GROQ_API_KEY, model=model))

    # default: openai
    from graphiti_core.llm_client.openai_client import OpenAIClient

    return OpenAIClient(config=LLMConfig(api_key=settings.OPENAI_API_KEY, model=model))


def _build_embedder() -> OpenAIEmbedder:
    """Build the embedder. Always OpenAIEmbedder (requires OPENAI_API_KEY)."""
    return OpenAIEmbedder(
        config=OpenAIEmbedderConfig(
            api_key=settings.OPENAI_API_KEY,
            embedding_model="text-embedding-3-small",
        )
    )


async def get_graphiti() -> Graphiti:
    """Return the Graphiti singleton, initializing on first call.

    Calls build_indices_and_constraints() on first init.
    Provider branching: openai→OpenAIClient, anthropic→AnthropicClient, groq→GroqClient.
    Embedder: always OpenAIEmbedder (requires OPENAI_API_KEY regardless of LLM_PROVIDER).
    """
    global _graphiti
    if _graphiti is not None:
        return _graphiti

    _graphiti = Graphiti(
        uri=settings.NEO4J_URI,
        user=settings.NEO4J_USER,
        password=settings.NEO4J_PASSWORD,
        llm_client=_build_llm_client(),
        embedder=_build_embedder(),
    )

    await _graphiti.build_indices_and_constraints()
    return _graphiti


def reset_graphiti() -> None:
    """Reset the singleton (used in tests)."""
    global _graphiti
    _graphiti = None


def get_vision_client() -> Any:
    """Return an SDK client for VISION_PROVIDER (defaults to LLM_PROVIDER).

    Used by the image resolver. Separate from the Graphiti LLM client.
    Returns the native SDK client (openai.OpenAI, anthropic.Anthropic, or groq.Groq).
    """
    provider = settings.VISION_PROVIDER or settings.LLM_PROVIDER

    if provider == "anthropic":
        import anthropic

        return anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

    if provider == "groq":
        import groq

        return groq.Groq(api_key=settings.GROQ_API_KEY)

    # default: openai
    import openai

    return openai.OpenAI(api_key=settings.OPENAI_API_KEY)
