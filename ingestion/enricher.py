"""Step 2.5: Enrich processed items with structured metadata.

Reads items with status='processed', adds:
- publish_date (extracted or LLM-inferred)
- speakers (author/creator attribution with role + platform)
- tags (2-5 from a dynamic growing set, via Haiku)
- summary (one-sentence)

Updates status to 'enriched'.

Run as: python -m ingestion.enricher
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import anthropic as anthropic_module
from anthropic import AsyncAnthropic

from ingestion import discord_touch
from seed_storage import staging
from seed_storage.config import DISCORD_OPS_ALERTS_CHANNEL, TINY_CONTENT_CHARS

log = logging.getLogger("enricher")


class _CreditExhausted(Exception):
    """Raised when Anthropic credit is exhausted — signals batch to stop."""

ENRICHER_SYSTEM = """\
You categorize knowledge content for a technology-focused knowledge graph.
Tags should identify KNOWLEDGE TOPICS for future AI agents to find and use.
Only return tags and a summary. Dates and speakers are extracted separately.

Existing tags: {existing_tags}

Rules for tags:
- Return 2-5 tags that describe the KNOWLEDGE TOPICS in the content
- Tags must be useful for future retrieval — "what domain is this about?"
- Use existing tags when they fit; create new ones only when needed
- Be specific: "agent-orchestration" not "technology"
- Good tags: "knowledge-graphs", "video-analysis", "prompt-engineering", "multi-agent-systems"
- If a notable person is mentioned, include a tag for their expertise area
- NEVER use meta-tags about message quality like "incomplete-message" or "short-text"
- Even short messages have context: "hey check spotify" → "music-streaming", "personal-productivity"
- Conversation fragments should be tagged by what they DISCUSS, not by their length
- If the content is truly too short to categorize (single emoji, "ok"), return ["uncategorized"]

Respond ONLY with valid JSON, no markdown fences:
{"tags": ["tag1", "tag2"], "summary": "one sentence describing the core knowledge"}\
"""


def _get_existing_tags() -> list[str]:
    """Fetch all existing tags from the seed_tags table."""
    import psycopg2
    from seed_storage import config

    try:
        with psycopg2.connect(config.PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tag FROM seed_tags ORDER BY count DESC LIMIT 200")
                return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def _upsert_tags(tags: list[str]):
    """Insert new tags or increment count for existing ones."""
    import psycopg2
    from seed_storage import config

    with psycopg2.connect(config.PG_DSN) as conn:
        with conn.cursor() as cur:
            for tag in tags:
                cur.execute(
                    """INSERT INTO seed_tags (tag, count)
                       VALUES (%s, 1)
                       ON CONFLICT (tag) DO UPDATE SET count = seed_tags.count + 1""",
                    (tag,),
                )
        conn.commit()


def init_tags_table():
    """Create the seed_tags table if it doesn't exist."""
    import psycopg2
    from seed_storage import config

    with psycopg2.connect(config.PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seed_tags (
                    id SERIAL PRIMARY KEY,
                    tag TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    count INT DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_tags_tag ON seed_tags(tag);
            """)
        conn.commit()


async def enrich_batch(limit: int = 50, concurrency: int = 5):
    """Enrich a batch of processed items with metadata."""
    items = staging.get_staged(status="processed", limit=limit)
    if not items:
        log.info("No processed items to enrich")
        return

    from seed_storage import config

    if not config.LLM_API_KEY:
        log.warning("No LLM_API_KEY — skipping enrichment, promoting to enriched")
        ids = [str(i["id"]) for i in items]
        staging.update_status(ids, "enriched")
        return

    init_tags_table()
    anthropic = AsyncAnthropic(api_key=config.LLM_API_KEY)
    existing_tags = _get_existing_tags()

    log.info("Enriching %d items (concurrency=%d, %d existing tags)", len(items), concurrency, len(existing_tags))

    sem = asyncio.Semaphore(concurrency)
    credit_error = asyncio.Event()

    async def _enrich_guarded(item):
        if credit_error.is_set():
            return
        async with sem:
            if credit_error.is_set():
                return
            try:
                await enrich_one(item, anthropic, existing_tags)
            except (anthropic_module.AuthenticationError, _CreditExhausted):
                log.error("Credit/auth error — stopping remaining enrichments")
                credit_error.set()

    tasks = [asyncio.create_task(_enrich_guarded(item)) for item in items]
    await asyncio.gather(*tasks, return_exceptions=True)


async def enrich_one(
    item: dict,
    anthropic: AsyncAnthropic,
    existing_tags: list[str],
) -> None:
    """Enrich a single processed item — add tags, summary, curator attribution.

    Updates the staging table directly (status → 'enriched' or 'failed').
    Can be called from enrich_batch() or express_ingest().
    """
    item_id = str(item["id"])
    content = (item.get("raw_content") or "").strip()

    # Skip API call for tiny content — auto-tag as uncategorized.
    if len(content) < TINY_CONTENT_CHARS and "http" not in content:
        log.info("Tiny content (%d chars), auto-tagging: %s", len(content), item.get("source_uri"))
        meta = item.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        meta["tags"] = ["uncategorized"]
        meta["summary"] = content[:100] or "(minimal content)"
        author = item.get("author", "unknown")
        if author and author != "unknown":
            speakers = meta.get("speakers", [])
            curator_names = {s["name"] for s in speakers if s.get("role") == "curator"}
            if author not in curator_names:
                speakers.append({"name": author, "role": "curator", "platform": "discord"})
                meta["speakers"] = speakers
        staging.update_content(item_id, item["raw_content"], metadata=meta, status="enriched")
        await discord_touch.react(item, "enriched")
        return

    try:
        enrichment = await _enrich_one(anthropic, item, existing_tags)

        # Merge enrichment into existing metadata.
        # Do NOT overwrite published_at or speakers — those come from the processor.
        meta = item.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        for key, value in enrichment.items():
            if key in ("published_at", "speakers") and key in meta:
                continue  # Processor data is authoritative
            meta[key] = value

        # Ensure curator is always recorded.
        author = item.get("author", "unknown")
        speakers = meta.get("speakers", [])
        curator_names = {s["name"] for s in speakers if s.get("role") == "curator"}
        if author and author != "unknown" and author not in curator_names:
            speakers.append({"name": author, "role": "curator", "platform": "discord"})
            meta["speakers"] = speakers

        staging.update_content(
            item_id,
            item["raw_content"],
            metadata=meta,
            status="enriched",
        )

        # Update tag table + refresh local cache.
        new_tags = enrichment.get("tags", [])
        if new_tags:
            _upsert_tags(new_tags)
            for t in new_tags:
                if t not in existing_tags:
                    existing_tags.append(t)

        log.info("Enriched [%s] %s → tags=%s", item["source_type"], item["source_uri"], new_tags)
        await discord_touch.react(item, "enriched")

    except anthropic_module.AuthenticationError as exc:
        log.error("URGENT: Auth error in enricher for %s: %s", item.get("source_uri"), exc)
        staging.update_status([item_id], "failed")
        staging.trip_breaker(f"ENRICHER_AUTH: {str(exc)[:200]}", cooldown_hours=None)
        await discord_touch.alert(
            DISCORD_OPS_ALERTS_CHANNEL,
            "URGENT: Auth Error — seed-storage enricher",
            f"Anthropic authentication failed during enrichment.\n**Item:** {item.get('source_uri')}\n**Error:** {str(exc)[:300]}",
            urgent=True,
        )
        raise  # Stop the batch

    except anthropic_module.RateLimitError as exc:
        if "credit balance" in str(exc).lower():
            log.error("URGENT: Credit exhaustion in enricher: %s", exc)
            staging.update_status([item_id], "failed")
            staging.trip_breaker(f"ENRICHER_CREDIT: {str(exc)[:200]}", cooldown_hours=None)
            await discord_touch.alert(
                DISCORD_OPS_ALERTS_CHANNEL,
                "URGENT: Credit Exhaustion — seed-storage enricher",
                f"Anthropic credit exhausted during enrichment.\n**Error:** {str(exc)[:300]}",
                urgent=True,
            )
            raise _CreditExhausted(str(exc))  # Stop the batch
        # Regular rate limit — fail this item, continue batch
        log.warning("Rate limit in enricher, marking failed: %s", item.get("source_uri"))
        staging.update_status([item_id], "failed")

    except Exception:
        log.exception("Failed to enrich [%s] %s", item["source_type"], item["source_uri"])
        staging.update_status([item_id], "failed")


async def _enrich_one(anthropic: AsyncAnthropic, item: dict, existing_tags: list[str]) -> dict:
    """Call Haiku to enrich a single item."""
    content = (item.get("raw_content") or "")[:3000]
    source_type = item.get("source_type", "unknown")
    author = item.get("author", "unknown")
    channel = item.get("channel", "")

    prompt = ENRICHER_SYSTEM.replace("{existing_tags}", ", ".join(existing_tags[:100]))

    user_msg = (
        f"Source type: {source_type}\n"
        f"Channel: {channel}\n"
        f"Posted by (curator): {author}\n"
        f"Content:\n{content}"
    )

    from seed_storage import config

    resp = await anthropic.messages.create(
        model=config.LLM_MODEL,
        max_tokens=300,
        system=prompt,
        messages=[{"role": "user", "content": user_msg}],
    )

    try:
        raw = resp.content[0].text.strip()
        # Strip markdown code fences if present.
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        data = json.loads(raw)
        # Only keep tags + summary from LLM — dates/speakers come from processor.
        return {
            "tags": data.get("tags", []),
            "summary": data.get("summary", ""),
        }
    except (json.JSONDecodeError, IndexError):
        log.warning("Enricher returned non-JSON for %s: %s", item.get("source_uri"), resp.content[0].text[:100])
        return {"tags": [], "summary": ""}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=5)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(enrich_batch(limit=args.limit, concurrency=args.concurrency))
