"""Audible resolver — metadata scrape + DRM removal + Groq/Whisper transcription.

Pipeline:
  1. Scrape Audible product page (title, author, narrator, duration, ASIN, cover)
  2. Download AAX file via `audible` library (requires credentials)
  3. Convert AAX → MP3 via ffmpeg + activation bytes (DRM removal)
  4. Transcribe via Groq Whisper API (fast, ~$0.02/hr) or local whisper fallback
  5. Return ResolvedContent with full transcript + rich metadata

DRM Notice:
  AAX DRM removal is technically gray under DMCA §1201 even for owned content.
  This code is opt-in: it only runs when AUDIBLE_ACTIVATION_BYTES is set AND
  the file is actually downloaded (requires AUDIBLE_EMAIL + AUDIBLE_PASSWORD).
  By configuring these vars you are making your own legal assessment.

Configuration (all optional — controls which path is taken):
  AUDIBLE_EMAIL             Audible account email
  AUDIBLE_PASSWORD          Audible account password
  AUDIBLE_ACTIVATION_BYTES  Pre-computed 4-byte hex activation bytes
  AUDIBLE_LOCALE            Marketplace locale (default: us)
  GROQ_API_KEY              Used for fast Whisper API transcription
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_AUDIBLE_HOSTS = {"audible.com", "www.audible.com", "amzn.to", "www.amzn.to"}
_ASIN_RE = re.compile(r"/pd/(?:[^/]+/)?([A-Z0-9]{10})")
_MAX_TRANSCRIPT_CHARS = 200_000  # ~50k tokens — plenty for a book


def _extract_asin(url: str) -> str | None:
    """Extract Audible ASIN from product URL."""
    m = _ASIN_RE.search(url)
    return m.group(1) if m else None


class AudibleResolver(BaseResolver):
    """Resolves Audible book URLs: scrape metadata + optionally transcribe audio."""

    def can_handle(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.hostname in _AUDIBLE_HOSTS

    async def resolve(self, url: str) -> ResolvedContent:
        try:
            return await self._resolve_internal(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("AudibleResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))

    async def _resolve_internal(self, url: str) -> ResolvedContent:
        # Step 1: Resolve amzn.to short links
        resolved_url = await _resolve_redirect(url)
        asin = _extract_asin(resolved_url)

        # Step 2: Scrape product page metadata
        meta = await _scrape_product_page(resolved_url)
        title = meta.get("title", "")
        author = meta.get("author", "")
        narrator = meta.get("narrator", "")
        duration_str = meta.get("duration", "")
        summary_blurb = meta.get("summary", "")
        cover_url = meta.get("cover_url", "")

        activation_bytes = os.environ.get("AUDIBLE_ACTIVATION_BYTES", "").strip()
        audible_email = os.environ.get("AUDIBLE_EMAIL", "").strip()
        audible_password = os.environ.get("AUDIBLE_PASSWORD", "").strip()

        transcript: str | None = None
        processing_path = "metadata_only"

        # Step 3: Attempt DRM download + transcription if credentials present
        if activation_bytes and audible_email and audible_password and asin:
            logger.info("AudibleResolver: attempting DRM path for ASIN %s", asin)
            try:
                loop = asyncio.get_event_loop()
                transcript = await loop.run_in_executor(
                    None, _download_and_transcribe, asin, activation_bytes, audible_email, audible_password
                )
                processing_path = "drm_transcription"
            except Exception as exc:
                logger.warning("AudibleResolver: DRM path failed for %s: %s", asin, exc)
                # Fall through to metadata-only

        # Build text body
        text_parts = []
        if summary_blurb:
            text_parts.append(f"[Publisher Summary]\n{summary_blurb}")
        if transcript:
            text_parts.append(f"[Transcript]\n{transcript[:_MAX_TRANSCRIPT_CHARS]}")
        text = "\n\n".join(text_parts)

        metadata: dict = {
            "asin": asin,
            "author": author,
            "narrator": narrator,
            "duration": duration_str,
            "cover_url": cover_url,
            "processing_path": processing_path,
            "audible_url": resolved_url,
            "source_type": "audiobook",
        }
        if author:
            metadata["speakers"] = [{"name": author, "role": "author", "platform": "audible"}]
        if narrator and narrator != author:
            metadata["speakers"].append({"name": narrator, "role": "narrator", "platform": "audible"})

        return ResolvedContent(
            source_url=resolved_url,
            content_type="audiobook",
            title=title or None,
            text=text,
            transcript=transcript[:_MAX_TRANSCRIPT_CHARS] if transcript else None,
            summary=summary_blurb or None,
            expansion_urls=[],
            metadata=metadata,
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _resolve_redirect(url: str) -> str:
    """Follow HTTP redirects to resolve amzn.to short links."""
    parsed = urlparse(url)
    if parsed.hostname not in ("amzn.to", "www.amzn.to"):
        return url
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as http:
            resp = await http.get(url, headers={"User-Agent": "Mozilla/5.0"})
            return str(resp.url)
    except Exception:
        return url


async def _scrape_product_page(url: str) -> dict:
    """Scrape Audible product page for title, author, narrator, duration, summary."""
    result: dict = {}
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as http:
            resp = await http.get(url, headers=headers)
            if resp.status_code != 200:
                logger.warning("Audible page returned %d for %s", resp.status_code, url)
                return result
            html = resp.text

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        # Title
        title_el = (
            soup.select_one("h1.bc-heading")
            or soup.select_one("h1[data-automation-id='title']")
            or soup.select_one("h1")
        )
        if title_el:
            result["title"] = title_el.get_text(strip=True)

        # Author
        author_el = soup.select_one(".authorLabel a") or soup.select_one("[data-automation-id='author'] a")
        if author_el:
            result["author"] = author_el.get_text(strip=True)

        # Narrator
        narrator_el = soup.select_one(".narratorLabel a") or soup.select_one("[data-automation-id='narrator'] a")
        if narrator_el:
            result["narrator"] = narrator_el.get_text(strip=True)

        # Duration — look for "X hrs and Y mins" pattern
        duration_match = re.search(r"(\d+\s*hrs?\s*(?:and\s*)?\d*\s*mins?|\d+\s*mins?)", html)
        if duration_match:
            result["duration"] = duration_match.group(1).strip()

        # Summary / publisher description
        summary_el = (
            soup.select_one(".productPublisherSummary .bc-text")
            or soup.select_one("[data-automation-id='product-desc']")
            or soup.select_one(".summary")
        )
        if summary_el:
            result["summary"] = summary_el.get_text(separator=" ", strip=True)[:4000]

        # Cover image
        cover_el = soup.select_one("img.bc-pub-block[src*='images-amazon']") or soup.select_one(".hero-content img")
        if cover_el:
            result["cover_url"] = cover_el.get("src", "")

        logger.info(
            "AudibleResolver scraped: title=%r author=%r duration=%r",
            result.get("title"), result.get("author"), result.get("duration")
        )
    except Exception as exc:
        logger.warning("AudibleResolver scrape failed for %s: %s", url, exc)

    return result


def _download_and_transcribe(
    asin: str,
    activation_bytes: str,
    email: str,
    password: str,
) -> str:
    """Download AAX, convert to MP3, transcribe. Returns transcript text.

    This is CPU-bound and blocking — call via run_in_executor.
    """
    locale = os.environ.get("AUDIBLE_LOCALE", "us")

    with tempfile.TemporaryDirectory() as tmpdir:
        aax_path = Path(tmpdir) / f"{asin}.aax"
        mp3_path = Path(tmpdir) / f"{asin}.mp3"

        # Download AAX via audible library
        _download_aax(email, password, locale, asin, aax_path)

        # Convert AAX → MP3 via ffmpeg (DRM removal with activation bytes)
        _convert_aax_to_mp3(aax_path, mp3_path, activation_bytes)

        # Transcribe
        return _transcribe(mp3_path)


def _download_aax(email: str, password: str, locale: str, asin: str, dest: Path) -> None:
    """Download AAX file for owned title via audible library."""
    try:
        import audible  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "audible library not installed. Add 'audible' to pyproject.toml dependencies."
        ) from exc

    logger.info("AudibleResolver: downloading ASIN %s", asin)
    with audible.Client(email, password, locale=locale) as client:
        # Get download URL for the AAX format
        library, _ = client.get(
            "library",
            num_results=1000,
            response_groups="product_desc,pdf_url,media,product_attrs",
            asin=asin,
        )
        items = library.get("items", [])
        if not items:
            raise ValueError(f"ASIN {asin} not found in library — do you own this title?")

        item = items[0]
        download_url = item.get("content_delivery_type")
        if not download_url:
            # Fallback: use download_link
            dl_info = client.get(f"content/{asin}/licenserequest", body={
                "quality": "Normal",
                "num_active_licenses": 1,
                "response_groups": "last_position_heard,pdf_url,content_reference,chapter_info",
            })
            download_url = (
                dl_info.get("content_license", {})
                       .get("content_metadata", {})
                       .get("content_url", {})
                       .get("offline_url", "")
            )

        if not download_url:
            raise ValueError(f"Could not obtain download URL for ASIN {asin}")

        import httpx as _httpx
        with _httpx.stream("GET", download_url, follow_redirects=True, timeout=3600) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)

    logger.info("AudibleResolver: downloaded %s (%.1f MB)", asin, dest.stat().st_size / 1e6)


def _convert_aax_to_mp3(aax_path: Path, mp3_path: Path, activation_bytes: str) -> None:
    """Convert AAX → MP3 using ffmpeg with activation bytes for DRM removal."""
    cmd = [
        "ffmpeg", "-y",
        "-activation_bytes", activation_bytes,
        "-i", str(aax_path),
        "-codec:a", "libmp3lame",
        "-b:a", "128k",
        "-ac", "1",           # mono — sufficient for speech
        "-ar", "22050",       # 22kHz sample rate
        str(mp3_path),
    ]
    logger.info("AudibleResolver: converting AAX → MP3")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed (activation_bytes may be wrong): {result.stderr[-500:]}"
        )
    logger.info("AudibleResolver: converted to MP3 (%.1f MB)", mp3_path.stat().st_size / 1e6)


def _transcribe(mp3_path: Path) -> str:
    """Transcribe MP3 via Groq API or local whisper fallback."""
    groq_key = os.environ.get("GROQ_API_KEY", "").strip()

    if groq_key:
        return _transcribe_groq(mp3_path, groq_key)
    else:
        return _transcribe_local_whisper(mp3_path)


def _transcribe_groq(mp3_path: Path, groq_key: str) -> str:
    """Transcribe via Groq Whisper API — fast (~realtime) and cheap (~$0.02/hr)."""
    import httpx as _httpx

    logger.info("AudibleResolver: transcribing via Groq Whisper (%s)", mp3_path.name)
    file_size = mp3_path.stat().st_size
    # Groq has a 25MB file size limit per request — chunk if needed
    if file_size > 24 * 1024 * 1024:
        return _transcribe_groq_chunked(mp3_path, groq_key)

    with open(mp3_path, "rb") as f:
        resp = _httpx.post(
            "https://api.groq.com/openai/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {groq_key}"},
            data={"model": "whisper-large-v3-turbo", "response_format": "text"},
            files={"file": (mp3_path.name, f, "audio/mpeg")},
            timeout=300,
        )
    resp.raise_for_status()
    text = resp.text.strip()
    logger.info("AudibleResolver: Groq transcription complete (%d chars)", len(text))
    return text


def _transcribe_groq_chunked(mp3_path: Path, groq_key: str) -> str:
    """Split audio into 20-min chunks and transcribe each via Groq."""
    import httpx as _httpx

    logger.info("AudibleResolver: large file — chunking for Groq transcription")
    tmpdir = Path(tempfile.mkdtemp())
    chunk_pattern = str(tmpdir / "chunk_%03d.mp3")

    # Split into 20-minute segments
    split_cmd = [
        "ffmpeg", "-y", "-i", str(mp3_path),
        "-f", "segment", "-segment_time", "1200",
        "-c", "copy", chunk_pattern,
    ]
    subprocess.run(split_cmd, capture_output=True, timeout=600, check=True)

    chunks = sorted(tmpdir.glob("chunk_*.mp3"))
    logger.info("AudibleResolver: transcribing %d chunks via Groq", len(chunks))

    transcripts = []
    for chunk in chunks:
        with open(chunk, "rb") as f:
            resp = _httpx.post(
                "https://api.groq.com/openai/v1/audio/transcriptions",
                headers={"Authorization": f"Bearer {groq_key}"},
                data={"model": "whisper-large-v3-turbo", "response_format": "text"},
                files={"file": (chunk.name, f, "audio/mpeg")},
                timeout=300,
            )
        resp.raise_for_status()
        transcripts.append(resp.text.strip())
        logger.info("AudibleResolver: chunk %s done (%d chars)", chunk.name, len(transcripts[-1]))

    return "\n\n".join(transcripts)


def _transcribe_local_whisper(mp3_path: Path) -> str:
    """Fallback: transcribe via local openai-whisper model (already in Dockerfile)."""
    logger.info("AudibleResolver: transcribing via local whisper (no GROQ_API_KEY set)")
    try:
        import whisper  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("openai-whisper not installed") from exc

    model = whisper.load_model("base")
    result = model.transcribe(str(mp3_path), fp16=False)
    text = result.get("text", "")
    logger.info("AudibleResolver: local whisper done (%d chars)", len(text))
    return text
