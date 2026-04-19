"""PDF resolver — docling with unstructured fallback.

Downloads the PDF and extracts text. Truncates at 10 000 tokens (~40 000 chars).
"""

from __future__ import annotations

import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_MAX_CHARS = 40_000  # ~10 000 tokens
_TIMEOUT = 60.0  # PDFs can be large
_PDF_EXTENSIONS = {".pdf"}


def _has_pdf_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in _PDF_EXTENSIONS)


def _is_pdf_content_type(content_type: str) -> bool:
    return "application/pdf" in content_type or "application/x-pdf" in content_type


class PDFResolver(BaseResolver):
    """Resolves PDF URLs using docling with unstructured fallback."""

    def can_handle(self, url: str) -> bool:
        return _has_pdf_extension(url)

    async def resolve(self, url: str) -> ResolvedContent:
        try:
            return await self._resolve_internal(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("PDFResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))

    async def _resolve_internal(self, url: str) -> ResolvedContent:
        # Download PDF
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=_TIMEOUT,
        ) as client:
            response = await client.get(url)
            response.raise_for_status()

            pdf_data = response.content
            if not pdf_data:
                raise ValueError("Empty PDF response")

        # Write to temp file for docling/unstructured
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(pdf_data)

        try:
            text, title = await self._extract_text(tmp_path)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass

        if not text:
            return ResolvedContent.error_result(url, "Could not extract text from PDF")

        if len(text) > _MAX_CHARS:
            text = text[:_MAX_CHARS]

        return ResolvedContent(
            source_url=url,
            content_type="pdf",
            title=title,
            text=text,
            transcript=None,
            summary=None,
            expansion_urls=[],
            metadata={"size_bytes": len(pdf_data)},
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )

    async def _extract_text(self, path: Path) -> tuple[str, str | None]:
        """Extract text from PDF. Returns (text, title)."""
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._extract_text_sync, path)

    def _extract_text_sync(self, path: Path) -> tuple[str, str | None]:
        # --- Attempt 1: docling ---
        try:
            from docling.document_converter import DocumentConverter  # type: ignore[import-untyped]

            converter = DocumentConverter()
            result = converter.convert(str(path))
            if result and result.document:
                text = result.document.export_to_text()
                title = None
                # Try to get title from metadata
                meta = getattr(result.document, "meta", None)
                if meta:
                    title = getattr(meta, "title", None)
                if text and len(text.strip()) > 20:
                    return text.strip(), title
        except Exception as exc:  # noqa: BLE001
            logger.debug("docling failed: %s", exc)

        # --- Attempt 2: unstructured ---
        try:
            from unstructured.partition.pdf import partition_pdf  # type: ignore[import-untyped]

            elements = partition_pdf(filename=str(path))
            text = "\n".join(str(el) for el in elements if str(el).strip())
            if text and len(text.strip()) > 20:
                return text.strip(), None
        except Exception as exc:  # noqa: BLE001
            logger.debug("unstructured failed: %s", exc)

        return "", None
