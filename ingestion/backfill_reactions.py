"""Backfill Discord emoji reactions for items that were processed before reactions worked.

Usage:
    PG_DSN=... python -m ingestion.backfill_reactions [--dry-run] [--limit N]

Adds the appropriate stage emoji to each Discord message based on the item's
current status. Rate-limits to ~1 reaction/second to avoid Discord 429s.
"""

import asyncio
import json
import logging
import os
import sys
from urllib.parse import quote as url_quote

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_reactions")

DISCORD_API = "https://discord.com/api/v10"

STAGE_EMOJI = {
    "staged":    "\U0001f4e5",      # 📥
    "processed": "\u2699\ufe0f",    # ⚙️
    "enriched":  "\U0001f3f7\ufe0f",# 🏷️
    "loaded":    "\U0001f9e0",      # 🧠
    "failed":    "\u274c",          # ❌
}

# Statuses that should get the "final" reaction for their highest-reached stage.
# e.g. "loaded" items get 🧠, "enriched" items get 🏷️
BACKFILL_STATUSES = ["loaded", "enriched", "processed", "staged", "failed"]


def _get_token() -> str:
    token_file = os.environ.get("DISCORD_BOT_ANT_FARM_FILE_PATH", "")
    if token_file and os.path.exists(token_file):
        return open(token_file).read().strip()
    token = os.environ.get("DISCORD_BOT_ANT_FARM_TOKEN", "")
    if token.startswith("Bot "):
        token = token[4:]
    return token


async def backfill(dry_run: bool = False, limit: int = 0):
    import psycopg2
    import psycopg2.extras

    dsn = os.environ.get("PG_DSN", "postgresql://taskman:postgres@127.0.0.1:30433/task_manager")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    token = _get_token()
    if not token:
        log.error("No Discord token available")
        return

    total = 0
    success = 0
    skipped = 0
    rate_limited = 0

    for status in BACKFILL_STATUSES:
        emoji = STAGE_EMOJI.get(status)
        if not emoji:
            continue

        query = """
            SELECT id, metadata FROM seed_staging
            WHERE status = %s
            ORDER BY id
        """
        params = [status]
        if limit:
            query += " LIMIT %s"
            params.append(limit)

        cur.execute(query, params)
        items = cur.fetchall()
        log.info("Status '%s': %d items to backfill with %s", status, len(items), emoji)

        async with httpx.AsyncClient(timeout=10) as http:
            headers = {"Authorization": f"Bot {token}"}

            for item in items:
                meta = item.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except (json.JSONDecodeError, TypeError):
                        skipped += 1
                        continue

                msg_id = meta.get("discord_msg_id")
                channel_id = meta.get("discord_channel_id")
                if not msg_id or not channel_id:
                    skipped += 1
                    continue

                total += 1
                encoded = url_quote(emoji)
                url = f"{DISCORD_API}/channels/{channel_id}/messages/{msg_id}/reactions/{encoded}/@me"

                if dry_run:
                    log.info("[DRY RUN] Would react %s on msg %s in channel %s", status, msg_id, channel_id)
                    continue

                try:
                    resp = await http.put(url, headers=headers)
                    if resp.status_code == 204:
                        success += 1
                    elif resp.status_code == 429:
                        rate_limited += 1
                        retry_after = json.loads(resp.content).get("retry_after", 1)
                        await asyncio.sleep(retry_after)
                        resp2 = await http.put(url, headers=headers)
                        if resp2.status_code == 204:
                            success += 1
                    elif resp.status_code == 404:
                        skipped += 1  # Message deleted
                    else:
                        log.warning("Unexpected %d for msg %s", resp.status_code, msg_id)
                except Exception as e:
                    log.warning("Error for msg %s: %s", msg_id, e)

                # Rate limit: ~1 reaction per second
                await asyncio.sleep(0.5)

                if total % 50 == 0:
                    log.info("Progress: %d processed, %d success, %d skipped, %d rate-limited",
                             total, success, skipped, rate_limited)

    log.info("Done: %d total, %d success, %d skipped, %d rate-limited", total, success, skipped, rate_limited)
    conn.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    limit_val = 0
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit_val = int(sys.argv[i + 1])

    asyncio.run(backfill(dry_run=dry_run, limit=limit_val))
