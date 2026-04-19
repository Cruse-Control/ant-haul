"""Embedding client — defaults to Gemini Embedding 2, configurable via env vars."""

from __future__ import annotations

from . import config

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    if config.EMBEDDING_PROVIDER == "openai":
        from openai import OpenAI
        kwargs = {"api_key": config.EMBEDDING_API_KEY}
        if config.EMBEDDING_BASE_URL:
            kwargs["base_url"] = config.EMBEDDING_BASE_URL
        _client = ("openai", OpenAI(**kwargs))
    else:
        from google import genai
        _client = ("gemini", genai.Client(api_key=config.EMBEDDING_API_KEY))
    return _client


async def embed_text(text: str) -> list[float]:
    """Embed a text string. Returns vector of EMBEDDING_DIMS dimensions."""
    provider, client = _get_client()
    if provider == "openai":
        result = client.embeddings.create(model=config.EMBEDDING_MODEL, input=text)
        return result.data[0].embedding
    result = client.models.embed_content(
        model=config.EMBEDDING_MODEL,
        contents=text,
    )
    return result.embeddings[0].values


async def embed_texts(texts: list[str]) -> list[list[float]]:
    """Batch embed multiple texts."""
    provider, client = _get_client()
    if provider == "openai":
        result = client.embeddings.create(model=config.EMBEDDING_MODEL, input=texts)
        return [d.embedding for d in result.data]
    result = client.models.embed_content(
        model=config.EMBEDDING_MODEL,
        contents=texts,
    )
    return [e.values for e in result.embeddings]
