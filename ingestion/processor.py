"""Step 2: Process staged items by medium.

Reads items with status='staged', performs medium-specific extraction
(transcript, README, readability, etc.), updates with processed content.

Run as: python -m ingestion.processor
Or register as an ant-keeper scheduled task.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, urlparse

import httpx
from anthropic import AsyncAnthropic

from ingestion import discord_touch
from seed_storage import staging

log = logging.getLogger("processor")

# Find yt-dlp in the same venv as this Python, or fall back to PATH.
_YTDLP = shutil.which("yt-dlp", path=os.path.dirname(sys.executable)) or "yt-dlp"

ADJUDICATOR_SYSTEM = """\
You analyze transcripts from video content to decide if the video's visuals \
carry important information beyond what's spoken.

Say YES if the content likely shows: code/terminals, diagrams/flowcharts, \
UI demos, screen recordings, data visualizations, physical tutorials, \
or significant text overlays not in the spoken audio.

Say NO if: talking head, speech conveys everything, visuals are decorative.

Respond ONLY with valid JSON: {"visuals_important": true/false, "reason": "one sentence"}\
"""


async def process_batch(limit: int = 50, concurrency: int = 3):
    """Process a batch of staged items with bounded concurrency."""
    items = staging.get_staged(status="staged", limit=limit)
    if not items:
        log.info("No staged items to process")
        return

    log.info("Processing %d staged items (concurrency=%d)", len(items), concurrency)

    async with httpx.AsyncClient(timeout=120) as http:
        from seed_storage import config

        anthropic = None
        if config.LLM_API_KEY:
            anthropic = AsyncAnthropic(api_key=config.LLM_API_KEY)

        analyzer_url = os.environ.get("ANALYZER_BASE_URL", "http://localhost:8000")

        sem = asyncio.Semaphore(concurrency)

        async def _process_guarded(item):
            async with sem:
                await process_one(item, http, anthropic, analyzer_url)

        tasks = [asyncio.create_task(_process_guarded(item)) for item in items]
        await asyncio.gather(*tasks, return_exceptions=True)


async def process_one(
    item: dict,
    http: httpx.AsyncClient,
    anthropic: AsyncAnthropic | None = None,
    analyzer_url: str = "",
) -> None:
    """Process a single staged item — extract content by medium type.

    Updates the staging table directly (status → 'processed' or 'failed').
    Can be called from process_batch() or express_ingest().
    """
    item_id = str(item["id"])
    source_type = item["source_type"]
    source_uri = item["source_uri"]

    # Preserve Discord commentary for URL entries — the processor replaces
    # raw_content with extracted content, but the original message has context.
    original_text = (item.get("raw_content") or "").strip()
    has_discord_context = (
        source_type not in ("plain_text", "conversation_thread", "discord_link", "media_link")
        and original_text
        and original_text != source_uri
        and len(original_text) > len(source_uri) + 5
    )

    try:
        if source_type == "instagram":
            content, meta = await _process_instagram(http, anthropic, analyzer_url, source_uri)
        elif source_type == "youtube":
            content, meta = await _process_youtube(http, anthropic, analyzer_url, source_uri)
        elif source_type == "github":
            content, meta = await _process_github(http, source_uri)
            # Non-blocking: try to add as submodule to inspirational-materials.
            try:
                from ingestion.submodule_adder import add_submodule
                sub_result = add_submodule(source_uri, description=content[:500], push=True, create_pr=True)
                meta["submodule_status"] = sub_result.get("status", "unknown")
                if sub_result.get("path"):
                    meta["submodule_path"] = sub_result["path"]
                log.info("Submodule add for %s: %s", source_uri, sub_result.get("status"))
            except Exception:
                log.debug("Submodule add failed for %s (non-blocking)", source_uri, exc_info=True)
        elif source_type == "x_twitter":
            content, meta = await _process_x(http, source_uri)
        elif source_type == "web":
            content, meta = await _process_web(http, source_uri)
        elif source_type == "instagram_image":
            content, meta = await _process_instagram_image(http, source_uri)
        elif source_type == "conversation_thread":
            # Threaded conversations already have merged content — passthrough.
            content = item["raw_content"]
            meta = item.get("metadata") or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
        elif source_type in ("plain_text", "discord_link", "media_link"):
            content = item["raw_content"]
            meta = {}
        else:
            log.warning("Unknown source_type '%s' for %s", source_type, source_uri)
            content = item["raw_content"]
            meta = {}

        # Stash Discord commentary in metadata so the loader can include it.
        if has_discord_context:
            commentary = original_text.replace(source_uri, "").strip()
            if len(commentary) > 5:
                meta["discord_context"] = commentary

        staging.update_content(item_id, content, metadata=meta, status="processed")
        log.info("Processed [%s] %s", source_type, source_uri)
        await discord_touch.react(item, "processed")

    except Exception:
        # Context-only fallback: store the Discord message as content.
        raw = item.get("raw_content", "")
        if raw and len(raw.strip()) > 10:
            log.warning("HTTP extraction failed for [%s] %s — falling back to Discord context", source_type, source_uri)
            staging.update_content(
                item_id, raw,
                metadata={"fetch_status": "context_only"},
                status="processed",
            )
            await discord_touch.react(item, "processed")
        else:
            log.exception("Failed to process [%s] %s (no context to fall back to)", source_type, source_uri)
            staging.update_status([item_id], "failed")
            await discord_touch.react(item, "failed", error_msg=f"Extraction failed for {source_uri}")


# ── Medium-specific extractors ─────────────────────────────────────


async def _process_instagram(
    http: httpx.AsyncClient, anthropic: AsyncAnthropic | None, base: str, url: str
) -> tuple[str, dict]:
    """Instagram reel/video extraction via yt-dlp (supports Instagram natively)."""
    content, meta = _ytdlp_extract(url)
    if content and not content.startswith("[yt-dlp failed"):
        return content, meta

    # Fallback: try instaloader for caption at least.
    shortcode = _extract_instagram_shortcode(url)
    if shortcode:
        caption_content, caption_meta = _instaloader_extract(shortcode)
        if caption_content and "could not" not in caption_content.lower():
            return caption_content, caption_meta

    raise RuntimeError(f"Instagram extraction failed for {url}")


async def _process_instagram_image(
    http: httpx.AsyncClient, url: str
) -> tuple[str, dict]:
    """Extract Instagram image post caption via instaloader with auth."""
    shortcode = _extract_instagram_shortcode(url)
    if not shortcode:
        return f"[Instagram image post — could not parse shortcode] {url}", {}

    content, meta = _instaloader_extract(shortcode)
    if content and "could not" not in content.lower():
        return content, meta

    # Fallback: try yt-dlp which also supports Instagram posts.
    content, meta = _ytdlp_extract(url)
    if content and not content.startswith("[yt-dlp failed"):
        return content, meta

    raise RuntimeError(f"Instagram image extraction failed for {url}")


def _instaloader_extract(shortcode: str) -> tuple[str, dict]:
    """Extract Instagram post caption via instaloader with auth.

    Runs in a thread with a 30s timeout to prevent instaloader's internal
    retry loop from hanging forever on 403/429 responses.
    """
    import concurrent.futures

    def _do_extract():
        import instaloader

        loader = instaloader.Instaloader(
            max_connection_attempts=1,  # Don't retry on 403/429.
            request_timeout=15,
        )

        # Authenticate if credentials available.
        username = os.environ.get("INSTAGRAM_USERNAME", "")
        password = os.environ.get("INSTAGRAM_PASSWORD", "")
        if username and password:
            try:
                loader.login(username, password)
            except Exception:
                log.debug("Instaloader login failed, trying without auth")

        post = instaloader.Post.from_shortcode(loader.context, shortcode)

        caption = (post.caption or "").strip()
        owner = post.owner_username or ""
        likes = post.likes
        date = post.date_utc

        if caption:
            content = f"Instagram post by @{owner}:\n\n{caption}"
        else:
            content = f"Instagram post by @{owner} (no caption)"

        meta: dict = {}
        if owner:
            meta["author"] = f"@{owner}"
            meta["speakers"] = [{"name": f"@{owner}", "role": "author", "platform": "instagram"}]
        if date:
            meta["published_at"] = date.isoformat()
        if likes:
            meta["engagement"] = {"likes": likes}
        return content, meta

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_extract)
            return future.result(timeout=30)
    except concurrent.futures.TimeoutError:
        log.warning("Instaloader timed out for shortcode %s", shortcode)
    except Exception:
        log.warning("Instaloader failed for shortcode %s", shortcode, exc_info=True)

    return f"[Instagram post — could not extract] {shortcode}", {}


def _extract_instagram_shortcode(url: str) -> str:
    """Extract Instagram shortcode from /p/SHORTCODE/ or /reel/SHORTCODE/ URLs."""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    for i, p in enumerate(parts):
        if p in ("p", "reel", "reels") and i + 1 < len(parts):
            return parts[i + 1]
    return ""


async def _process_youtube(
    http: httpx.AsyncClient, anthropic: AsyncAnthropic | None, base: str, url: str
) -> tuple[str, dict]:
    """YouTube extraction via yt-dlp (handles videos, Shorts, and live streams)."""
    video_id = _extract_yt_id(url)

    # Primary: yt-dlp for transcript + metadata.
    content, meta = _ytdlp_extract(url)

    # If yt-dlp got us nothing useful, try YouTube Data API for title+description.
    if not content or content.startswith("[yt-dlp failed"):
        channel_title, published_at, video_title, video_desc = await _fetch_yt_metadata(http, video_id)
        if video_title or video_desc:
            content = f"# {video_title}\n\n{video_desc}" if video_desc else f"# {video_title}"
            meta["fetch_status"] = "metadata_fallback"
            if channel_title:
                meta["speakers"] = [{"name": channel_title, "role": "creator", "platform": "youtube"}]
            if published_at:
                meta["published_at"] = published_at
        else:
            raise RuntimeError(f"YouTube extraction failed for {url}")

    # Adjudicator + visual analysis disabled — no video analysis service available.
    meta["adjudicator_decision"] = False
    meta["adjudicator_reason"] = "disabled"

    return content, meta


def _ytdlp_extract(url: str) -> tuple[str, dict]:
    """Extract content from a URL using yt-dlp (YouTube, Instagram, Twitter, etc.).

    Returns (content_text, metadata_dict). Falls back gracefully.
    """
    try:
        # Get metadata + subtitles via yt-dlp.
        result = subprocess.run(
            [
                _YTDLP,
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",
                "--sub-langs", "en.*,en",
                "--sub-format", "json3",
                "--dump-json",
                "--no-warnings",
                "--no-playlist",
                url,
            ],
            capture_output=True, text=True, timeout=60,
        )

        if result.returncode != 0:
            log.debug("yt-dlp failed for %s: %s", url, result.stderr[:200])
            return f"[yt-dlp failed for {url}]", {}

        info = json.loads(result.stdout)
        title = info.get("title", "")
        description = info.get("description", "")
        uploader = info.get("uploader", "") or info.get("channel", "")
        upload_date = info.get("upload_date", "")
        duration = info.get("duration", 0)
        view_count = info.get("view_count", 0)
        like_count = info.get("like_count", 0)

        # Try to get subtitles/transcript.
        transcript = ""
        subs = info.get("subtitles", {}) or {}
        auto_subs = info.get("automatic_captions", {}) or {}

        # Prefer manual subs, then auto.
        sub_data = None
        for lang in ["en", "en-US", "en-GB"]:
            if lang in subs:
                sub_data = subs[lang]
                break
        if not sub_data:
            for lang in ["en", "en-orig", "en-US", "en-GB"]:
                if lang in auto_subs:
                    sub_data = auto_subs[lang]
                    break

        # If we have subtitle URLs, try to download them.
        if sub_data:
            for fmt in sub_data:
                sub_url = fmt.get("url", "")
                if sub_url and "json3" in fmt.get("ext", ""):
                    try:
                        import httpx as _httpx
                        r = _httpx.get(sub_url, timeout=15)
                        if r.status_code == 200:
                            sub_json = r.json()
                            events = sub_json.get("events", [])
                            parts = []
                            for e in events:
                                segs = e.get("segs", [])
                                for s in segs:
                                    t = s.get("utf8", "").strip()
                                    if t and t != "\n":
                                        parts.append(t)
                            transcript = " ".join(parts)
                            break
                    except Exception:
                        pass

        # Build content.
        parts = []
        if title:
            parts.append(f"# {title}")
        if uploader:
            parts.append(f"By: {uploader}")
        if transcript:
            parts.append(f"\n## Transcript\n\n{transcript}")
        elif description:
            parts.append(f"\n## Description\n\n{description}")

        content = "\n".join(parts) if parts else ""

        meta: dict = {}
        if uploader:
            platform = "youtube" if "youtube" in url or "youtu.be" in url else "instagram" if "instagram" in url else "web"
            meta["speakers"] = [{"name": uploader, "role": "creator", "platform": platform}]
        if upload_date and len(upload_date) == 8:
            meta["published_at"] = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        if like_count:
            meta["engagement"] = {"likes": like_count, "views": view_count}
        if duration:
            meta["duration_seconds"] = duration

        return content, meta

    except subprocess.TimeoutExpired:
        log.warning("yt-dlp timed out for %s", url)
    except Exception:
        log.debug("yt-dlp extract failed for %s", url, exc_info=True)

    return f"[yt-dlp failed for {url}]", {}


async def _process_github(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """API → README + metadata, with auth token for rate limits and private repos."""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return f"[Could not parse GitHub repo from {url}]", {}

    owner, repo = parts[0], parts[1]
    api = f"https://api.github.com/repos/{owner}/{repo}"

    # Use auth token if available (60 → 5000 req/hr).
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get("GITHUB_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    resp = await http.get(api, headers=headers)
    resp.raise_for_status()
    meta_data = resp.json()

    description = meta_data.get("description", "") or ""
    stars = meta_data.get("stargazers_count", 0)
    language = meta_data.get("language", "") or ""
    topics = meta_data.get("topics", [])

    # Fetch README + high-signal config files (CLAUDE.md, AGENTS.md).
    docs = {}
    for doc_path in ["readme", "contents/CLAUDE.md", "contents/AGENTS.md"]:
        try:
            r = await http.get(f"{api}/{doc_path}", headers=headers)
            if r.status_code == 200:
                data = r.json()
                text = base64.b64decode(data.get("content", "")).decode("utf-8", errors="replace")
                name = data.get("name", doc_path.split("/")[-1])
                docs[name] = text
        except Exception:
            pass

    content = f"# {owner}/{repo}\n\n{description}\n\nStars: {stars} | Language: {language}"
    if topics:
        content += f"\nTopics: {', '.join(topics)}"
    for name, text in docs.items():
        content += f"\n\n---\n## {name}\n\n{text}"

    return content, {
        "stars": stars,
        "language": language,
        "topics": topics,
        "published_at": meta_data.get("created_at", ""),
        "speakers": [{"name": meta_data.get("owner", {}).get("login", owner), "role": "maintainer", "platform": "github"}],
    }


async def _process_x(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """Tweet extraction via FxTwitter API (free, no auth required)."""
    username, tweet_id = _extract_tweet_info(url)
    if not tweet_id:
        return f"[Could not parse tweet ID from {url}]", {}

    # FxTwitter requires username in the path and a User-Agent header.
    fx_path = f"{username}/status/{tweet_id}" if username else f"i/status/{tweet_id}"
    resp = await http.get(
        f"https://api.fxtwitter.com/{fx_path}",
        timeout=30,
        headers={"User-Agent": "SeedStorage/1.0", "Accept": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    tweet = data.get("tweet", {})

    author = tweet.get("author", {})
    author_name = author.get("name", "")
    author_handle = f"@{author.get('screen_name', '')}"
    text = tweet.get("text", "")

    content_parts = [f"Tweet by {author_name} ({author_handle}):", "", text]

    # Include media descriptions if present.
    media = tweet.get("media", {})
    if media:
        for photo in media.get("photos", []):
            alt = photo.get("altText", "")
            if alt:
                content_parts.append(f"\n[Image: {alt}]")
        for video in media.get("videos", []):
            dur = video.get("duration", 0)
            content_parts.append(f"\n[Video: {dur}s]")

    # Include quote tweet if present.
    quote = tweet.get("quote", {})
    if quote:
        q_author = quote.get("author", {})
        q_name = q_author.get("name", "")
        q_handle = f"@{q_author.get('screen_name', '')}"
        content_parts.append(f"\nQuoting {q_name} ({q_handle}):")
        content_parts.append(quote.get("text", ""))

    content = "\n".join(content_parts)

    # If content is very thin (media-only tweet), try yt-dlp for video description.
    if len(text.strip()) < 10:
        ytdlp_content, _ = _ytdlp_extract(url.replace("fxtwitter.com", "x.com"))
        if ytdlp_content and not ytdlp_content.startswith("[yt-dlp"):
            content += f"\n\n{ytdlp_content}"

    meta: dict = {
        "author": author_handle,
        "author_name": author_name,
        "tweet_id": tweet_id,
        "speakers": [{"name": author_name or author_handle, "role": "author", "platform": "x.com"}],
    }
    created_at = tweet.get("created_at")
    if created_at:
        meta["published_at"] = created_at
    likes = tweet.get("likes", 0)
    retweets = tweet.get("retweets", 0)
    if likes or retweets:
        meta["engagement"] = {"likes": likes, "retweets": retweets}

    return content, meta


def _extract_tweet_info(url: str) -> tuple[str, str]:
    """Extract (username, tweet_id) from x.com, twitter.com, or fxtwitter.com URL."""
    parts = url.rstrip("/").split("/")
    username = ""
    tweet_id = ""
    for i, p in enumerate(parts):
        if p in ("x.com", "twitter.com", "fxtwitter.com") and i + 1 < len(parts):
            username = parts[i + 1]
        if p == "status" and i + 1 < len(parts):
            tid = parts[i + 1].split("?")[0]
            if tid.isdigit():
                tweet_id = tid
    return username, tweet_id


async def _process_web(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """Web content extraction with multiple strategies.

    1. LinkedIn: cookie-based auth scraping
    2. trafilatura (primary, better extraction than readability alone)
    3. readability-lxml (fallback)
    4. archive.ph (fallback for paywalled/blocked content)
    """
    parsed = urlparse(url)
    hostname = (parsed.hostname or "").replace("www.", "")

    # t.co shortlinks: follow redirect first, then process the target URL.
    if hostname == "t.co":
        try:
            resp = await http.get(url, follow_redirects=True)
            final_url = str(resp.url)
            if final_url != url:
                log.info("t.co redirect: %s → %s", url, final_url)
                return await _process_web(http, final_url)
        except Exception:
            pass

    # LinkedIn: use li_at session cookie for authenticated scraping.
    if hostname == "linkedin.com":
        return await _process_linkedin(http, url)

    # Google Docs: use export trick.
    if hostname in ("docs.google.com",):
        return await _process_google_doc(http, url)

    # Primary: trafilatura (better article extraction).
    try:
        import trafilatura

        resp = await http.get(url, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        resp.raise_for_status()

        extracted = trafilatura.extract(
            resp.text,
            include_comments=False,
            include_tables=True,
            favor_recall=True,
            output_format="txt",
        )

        if extracted and len(extracted.strip()) > 50:
            # Also get metadata.
            meta_result = trafilatura.extract(resp.text, output_format="json")
            meta: dict = {}
            if meta_result:
                try:
                    meta_json = json.loads(meta_result) if isinstance(meta_result, str) else meta_result
                    title = meta_json.get("title", "")
                    author = meta_json.get("author", "")
                    date = meta_json.get("date", "")
                    if author:
                        meta["author"] = author
                        meta["speakers"] = [{"name": author, "role": "author", "platform": "web"}]
                    if date:
                        meta["published_at"] = date
                    if title:
                        meta["title"] = title
                        return f"# {title}\n\n{extracted}", meta
                except (json.JSONDecodeError, TypeError):
                    pass
            return extracted, meta
    except Exception:
        log.debug("trafilatura failed for %s, trying readability", url)

    # Fallback: readability-lxml.
    try:
        from readability import Document
        from bs4 import BeautifulSoup

        resp = await http.get(url, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        resp.raise_for_status()

        doc = Document(resp.text)
        title = doc.short_title() or ""
        soup = BeautifulSoup(doc.summary(), "lxml")
        text = soup.get_text(separator="\n", strip=True)

        if text and len(text.strip()) > 50:
            meta = {"title": title}
            full_soup = BeautifulSoup(resp.text, "lxml")
            for attr in [{"name": "author"}, {"property": "og:author"}, {"name": "twitter:creator"}]:
                tag = full_soup.find("meta", attrs=attr)
                if tag and tag.get("content"):
                    meta["author"] = tag["content"]
                    meta["speakers"] = [{"name": tag["content"], "role": "author", "platform": "web"}]
                    break
            content = f"# {title}\n\n{text}" if title else text
            return content, meta
    except Exception:
        log.debug("readability failed for %s, trying archive.ph", url)

    # Last resort: archive.ph for paywalled or blocked content.
    return await _process_via_archive(http, url)


async def _process_linkedin(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """LinkedIn content extraction using li_at session cookie."""
    li_at = os.environ.get("LINKEDIN_LI_AT", "")
    if not li_at:
        raise RuntimeError("No LINKEDIN_LI_AT cookie configured")

    resp = await http.get(
        url,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Cookie": f"li_at={li_at}",
        },
    )
    resp.raise_for_status()

    # LinkedIn HTML is heavy — use trafilatura for extraction.
    try:
        import trafilatura
        extracted = trafilatura.extract(resp.text, include_comments=False, favor_recall=True)
        if extracted and len(extracted.strip()) > 50:
            meta: dict = {}
            meta_result = trafilatura.extract(resp.text, output_format="json")
            if meta_result:
                try:
                    meta_json = json.loads(meta_result) if isinstance(meta_result, str) else meta_result
                    if meta_json.get("author"):
                        meta["author"] = meta_json["author"]
                        meta["speakers"] = [{"name": meta_json["author"], "role": "author", "platform": "linkedin"}]
                    if meta_json.get("title"):
                        meta["title"] = meta_json["title"]
                except (json.JSONDecodeError, TypeError):
                    pass
            return extracted, meta
    except Exception:
        log.debug("trafilatura failed for LinkedIn %s", url)

    # Fallback: readability on the authenticated HTML.
    from readability import Document
    from bs4 import BeautifulSoup

    doc = Document(resp.text)
    title = doc.short_title() or ""
    soup = BeautifulSoup(doc.summary(), "lxml")
    text = soup.get_text(separator="\n", strip=True)

    if text and len(text.strip()) > 50:
        return f"# {title}\n\n{text}" if title else text, {"title": title}

    raise RuntimeError(f"LinkedIn extraction failed for {url}")


async def _process_google_doc(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """Google Docs extraction via export-as-text URL trick."""
    # Extract document ID from URL.
    parsed = urlparse(url)
    parts = parsed.path.split("/")
    doc_id = ""
    for i, p in enumerate(parts):
        if p == "d" and i + 1 < len(parts):
            doc_id = parts[i + 1]
            break

    if not doc_id:
        raise RuntimeError(f"Could not parse Google Doc ID from {url}")

    # Try the public export endpoint.
    export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    resp = await http.get(export_url, follow_redirects=True)
    resp.raise_for_status()

    content = resp.text.strip()
    if content and len(content) > 50 and "Sign in" not in content[:200]:
        return content, {"title": f"Google Doc {doc_id[:8]}...", "source_format": "google_doc"}

    raise RuntimeError(f"Google Doc export failed for {url} (may require auth)")


async def _process_via_archive(http: httpx.AsyncClient, url: str) -> tuple[str, dict]:
    """Try to fetch content via archive.ph (paywall bypass / cached version)."""
    archive_url = f"https://archive.ph/newest/{url}"
    try:
        resp = await http.get(
            archive_url,
            follow_redirects=True,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )
        if resp.status_code == 200 and len(resp.text) > 500:
            import trafilatura
            extracted = trafilatura.extract(resp.text, include_comments=False, favor_recall=True)
            if extracted and len(extracted.strip()) > 50:
                return extracted, {"fetch_status": "archive_ph", "archive_url": str(resp.url)}
    except Exception:
        log.debug("archive.ph failed for %s", url)

    raise RuntimeError(f"All extraction methods failed for {url}")


# ── YouTube metadata ───────────────────────────────────────────────


async def _fetch_yt_metadata(http: httpx.AsyncClient, video_id: str) -> tuple[str, str, str, str]:
    """Fetch channel, date, title, description from YouTube Data API v3.

    Returns (channel_title, published_at, title, description).
    Degrades gracefully if no API key.
    """
    key = os.environ.get("YOUTUBE_API_KEY") or os.environ.get("GEMINI_API_KEY", "")
    if not key:
        return "", "", "", ""
    try:
        resp = await http.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={"part": "snippet", "id": video_id, "key": key},
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            return "", "", "", ""
        snippet = items[0]["snippet"]
        return (
            snippet.get("channelTitle", ""),
            snippet.get("publishedAt", ""),
            snippet.get("title", ""),
            snippet.get("description", ""),
        )
    except Exception:
        log.debug("YouTube Data API failed for %s, skipping metadata", video_id)
        return "", "", "", ""


# ── Helpers ────────────────────────────────────────────────────────


async def _adjudicate(anthropic: AsyncAnthropic | None, transcript: str) -> tuple[bool, str]:
    if not anthropic or not transcript or transcript.startswith("[No transcript"):
        return False, "skipped"

    from seed_storage import config

    resp = await anthropic.messages.create(
        model=config.LLM_MODEL,
        max_tokens=150,
        system=ADJUDICATOR_SYSTEM,
        messages=[{"role": "user", "content": transcript[:8000]}],
    )
    try:
        data = json.loads(resp.content[0].text)
        return data.get("visuals_important", False), data.get("reason", "")
    except (json.JSONDecodeError, IndexError):
        return False, "parse error"


async def _poll_job(http: httpx.AsyncClient, base: str, job_id: str, timeout: int = 300) -> str:
    elapsed = 0
    while elapsed < timeout:
        # Try /api/video/status/{id} (Instagram MCP backend) then /api/jobs/{id} (fallback).
        resp = await http.get(f"{base}/api/video/status/{job_id}")
        if resp.status_code == 404 or resp.status_code == 405:
            resp = await http.get(f"{base}/api/jobs/{job_id}")
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")
        if status == "completed":
            # Extract text from nested result structure.
            result = data.get("analysis_result") or data.get("result", {})
            if isinstance(result, dict):
                analysis = result.get("analysis", result)
                if isinstance(analysis, dict):
                    return analysis.get("text", "") or analysis.get("transcription", "") or str(analysis)
                return str(analysis)
            return str(result)
        if status in ("failed", "error", "FAILED"):
            raise RuntimeError(f"Job {job_id} failed: {data.get('error_message') or data.get('error')}")
        await asyncio.sleep(3)
        elapsed += 3
    raise TimeoutError(f"Job {job_id} timed out")


def _extract_yt_id(url: str) -> str:
    parsed = urlparse(url)
    if parsed.hostname in ("youtu.be",):
        return parsed.path.lstrip("/")
    if parsed.hostname in ("www.youtube.com", "youtube.com"):
        if parsed.path.startswith("/shorts/"):
            return parsed.path.split("/shorts/")[1].split("/")[0]
        return parse_qs(parsed.query).get("v", [""])[0]
    return ""


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=3)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(process_batch(limit=args.limit, concurrency=args.concurrency))
