"""Image resolver — vision LLM description.

Downloads the image and passes it to the configured vision LLM to produce
a text description. Provider is controlled by VISION_PROVIDER config.
"""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".svg"}
_IMAGE_CONTENT_TYPES = {
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/tiff",
    "image/svg+xml",
}
_TIMEOUT = 30.0
_VISION_PROMPT = (
    "Describe this image in detail. Include: main subjects, setting/context, "
    "any visible text, colors, and overall composition. Be factual and concise."
)


def _has_image_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in _IMAGE_EXTENSIONS)


class ImageResolver(BaseResolver):
    """Resolves image URLs using a vision LLM for description."""

    def can_handle(self, url: str) -> bool:
        return _has_image_extension(url)

    async def resolve(self, url: str) -> ResolvedContent:
        try:
            return await self._resolve_internal(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ImageResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))

    async def _resolve_internal(self, url: str) -> ResolvedContent:
        # Download image
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=_TIMEOUT,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").split(";")[0].strip()
            if content_type and content_type not in _IMAGE_CONTENT_TYPES:
                raise ValueError(f"Unexpected content-type for image URL: {content_type!r}")

            image_data = response.content
            if not image_data:
                raise ValueError("Empty image response")

            # Determine MIME type for base64 encoding
            mime_type = content_type if content_type in _IMAGE_CONTENT_TYPES else "image/jpeg"

        # Call vision LLM
        summary = await self._call_vision_llm(url, image_data, mime_type)

        return ResolvedContent(
            source_url=url,
            content_type="image",
            title=None,
            text=summary,
            transcript=None,
            summary=summary,
            expansion_urls=[],
            metadata={"mime_type": mime_type, "size_bytes": len(image_data)},
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )

    async def _call_vision_llm(self, url: str, image_data: bytes, mime_type: str) -> str:
        """Call the configured vision LLM and return the description."""
        try:
            from seed_storage.config import settings

            vision_provider = settings.VISION_PROVIDER or settings.LLM_PROVIDER
        except Exception:  # noqa: BLE001
            vision_provider = "openai"

        b64_image = base64.b64encode(image_data).decode("utf-8")

        if vision_provider in ("openai",):
            return await self._call_openai_vision(b64_image, mime_type)
        elif vision_provider == "anthropic":
            return await self._call_anthropic_vision(b64_image, mime_type)
        else:
            # Default to OpenAI-compatible
            return await self._call_openai_vision(b64_image, mime_type)

    async def _call_openai_vision(self, b64_image: str, mime_type: str) -> str:
        import openai  # type: ignore[import-untyped]

        try:
            from seed_storage.config import settings

            api_key = settings.OPENAI_API_KEY
            model = settings.LLM_MODEL
        except Exception:  # noqa: BLE001
            api_key = ""
            model = "gpt-4o-mini"

        client = openai.AsyncOpenAI(api_key=api_key)
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{b64_image}",
                            },
                        },
                        {"type": "text", "text": _VISION_PROMPT},
                    ],
                }
            ],
            max_tokens=500,
        )
        return response.choices[0].message.content or ""

    async def _call_anthropic_vision(self, b64_image: str, mime_type: str) -> str:
        import anthropic  # type: ignore[import-untyped]

        try:
            from seed_storage.config import settings

            api_key = settings.ANTHROPIC_API_KEY
        except Exception:  # noqa: BLE001
            api_key = ""

        client = anthropic.AsyncAnthropic(api_key=api_key)
        message = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": b64_image,
                            },
                        },
                        {"type": "text", "text": _VISION_PROMPT},
                    ],
                }
            ],
        )
        return message.content[0].text if message.content else ""
