"""Backfill discord_msg_id metadata by scanning Discord channel history.

Matches Discord messages to existing seed_staging items by URL or discord:// URI,
then patches metadata with the Discord message context. After running this,
run backfill_reactions.py to add emoji reactions.

Usage:
    PG_DSN=... DISCORD_BOT_ANT_FARM_TOKEN=... \
    python -m ingestion.backfill_discord_metadata [--dry-run] [--channel ID] [--limit N]
"""

import asyncio
import json
import logging
import os
import sys

import httpx

from ingestion.classifier import extract_urls
from seed_storage import staging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_discord_metadata")

DISCORD_API = "https://discord.com/api/v10"

# All watched channel IDs (same as watcher manifest WATCHED_CHANNELS).
ALL_CHANNELS = [
    "1487357825087701063",   # #imessages
    "1487648814280998963",   # #instagram-inspiration
    "1489132846131052627",   # #granola-wyler
    "1489204533337788476",   # #granola-flynn
    "1488084249188765796",   # #hive-mind-announcements
    "1488083965389836369",   # #scouting-external-colonies
    "1487354063749382234",   # #seed-storage
    "1489826423643308203",   # #gh-inspirational-materials
    "1490082833627218112",   # #youtube-inspiration
    "1490083051496280145",   # #x-inspiration
    "1490083131921797220",   # #web-articles
    "1487576638269948167",   # #ant-food-router
]


def _get_token() -> str:
    token_file = os.environ.get("DISCORD_BOT_ANT_FARM_FILE_PATH", "")
    if token_file and os.path.exists(token_file):
        return open(token_file).read().strip()
    token = os.environ.get("DISCORD_BOT_ANT_FARM_TOKEN", "")
    if token.startswith("Bot "):
        token = token[4:]
    return token


async def scan_channel(http: httpx.AsyncClient, headers: dict, channel_id: str,
                       dry_run: bool, max_pages: int, stats: dict):
    """Scan one channel's history and patch metadata on matching items."""
    before_id = None
    pages = 0

    while pages < max_pages:
        params: dict = {"limit": 100}
        if before_id:
            params["before"] = before_id

        resp = await http.get(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers=headers,
            params=params,
        )

        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 2)
            log.warning("Rate limited on channel %s, waiting %.1fs", channel_id, retry_after)
            await asyncio.sleep(retry_after)
            continue

        if resp.status_code != 200:
            log.error("Failed to fetch channel %s: %d %s", channel_id, resp.status_code, resp.text[:200])
            break

        messages = resp.json()
        if not messages:
            break

        pages += 1

        for msg in messages:
            msg_id = msg["id"]
            guild_id = msg.get("guild_id", "")
            author_id = msg.get("author", {}).get("id", "")
            timestamp = msg.get("timestamp", "")
            content = msg.get("content", "")

            patch = {
                "discord_msg_id": msg_id,
                "discord_channel_id": channel_id,
                "discord_guild_id": guild_id,
                "discord_author_id": author_id,
                "discord_timestamp": timestamp,
            }

            # Try URL matching first
            urls = extract_urls(content) if content else []
            matched = False

            for url in urls:
                item = staging.get_by_uri(url)
                if item:
                    existing_meta = item.get("metadata") or {}
                    if isinstance(existing_meta, str):
                        existing_meta = json.loads(existing_meta)
                    if existing_meta.get("discord_msg_id"):
                        stats["already_has_meta"] += 1
                        matched = True
                        continue
                    if dry_run:
                        log.info("[DRY] Would patch %s → msg %s in ch %s", url[:60], msg_id, channel_id)
                    else:
                        staging.patch_metadata(str(item["id"]), patch)
                    stats["patched"] += 1
                    matched = True

            # Try discord:// URI for plain text messages (no URLs)
            if not urls and content.strip():
                discord_uri = f"discord://{guild_id}/{channel_id}/{msg_id}"
                item = staging.get_by_uri(discord_uri)
                if item:
                    existing_meta = item.get("metadata") or {}
                    if isinstance(existing_meta, str):
                        existing_meta = json.loads(existing_meta)
                    if existing_meta.get("discord_msg_id"):
                        stats["already_has_meta"] += 1
                    else:
                        if dry_run:
                            log.info("[DRY] Would patch discord:// → msg %s", msg_id)
                        else:
                            staging.patch_metadata(str(item["id"]), patch)
                        stats["patched"] += 1
                    matched = True

            if not matched:
                stats["no_match"] += 1

        before_id = messages[-1]["id"]

        if len(messages) < 100:
            break  # No more messages

        await asyncio.sleep(0.5)  # Rate limit between pages

    stats["pages"] += pages


async def backfill(dry_run: bool = False, channel_filter: str | None = None, max_pages: int = 50):
    token = _get_token()
    if not token:
        log.error("No Discord token available")
        return

    headers = {"Authorization": f"Bot {token}"}
    channels = [channel_filter] if channel_filter else ALL_CHANNELS

    stats = {"patched": 0, "no_match": 0, "already_has_meta": 0, "pages": 0}

    async with httpx.AsyncClient(timeout=30) as http:
        for ch_id in channels:
            log.info("Scanning channel %s...", ch_id)
            await scan_channel(http, headers, ch_id, dry_run, max_pages, stats)
            log.info("  Channel %s done (pages=%d, patched=%d so far)",
                     ch_id, stats["pages"], stats["patched"])

    log.info("Done: %d patched, %d already had metadata, %d no match, %d pages fetched",
             stats["patched"], stats["already_has_meta"], stats["no_match"], stats["pages"])


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    channel = None
    max_pages = 50
    for i, arg in enumerate(sys.argv):
        if arg == "--channel" and i + 1 < len(sys.argv):
            channel = sys.argv[i + 1]
        if arg == "--max-pages" and i + 1 < len(sys.argv):
            max_pages = int(sys.argv[i + 1])

    asyncio.run(backfill(dry_run=dry_run, channel_filter=channel, max_pages=max_pages))
