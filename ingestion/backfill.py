"""Backfill: walk Discord channel history and stage all messages.

Uses the Discord HTTP API (not the gateway) to fetch message history
for all watched channels. Stages everything through the same
classify → filter → stage pipeline as the live watcher.

Run as: python -m ingestion.backfill
Env: DISCORD_BOT_ANT_FARM_TOKEN, WATCHED_CHANNELS, PG_DSN
"""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

from ingestion.classifier import Platform, classify, extract_urls
from ingestion.signal_filter import is_noise
from seed_storage import staging

log = logging.getLogger("backfill")

DISCORD_API = "https://discord.com/api/v10"


async def backfill_all():
    """Backfill all watched channels."""
    token = os.environ.get("DISCORD_BOT_ANT_FARM_TOKEN", "")
    if not token:
        log.error("DISCORD_BOT_ANT_FARM_TOKEN not set")
        return

    channel_ids = [
        ch.strip()
        for ch in os.environ.get("WATCHED_CHANNELS", "").split(",")
        if ch.strip()
    ]
    if not channel_ids:
        log.error("WATCHED_CHANNELS not set")
        return

    staging.init_tables()

    headers = {"Authorization": f"Bot {token}"}
    total_staged = 0
    total_skipped = 0
    total_filtered = 0

    async with httpx.AsyncClient(timeout=30, headers=headers) as http:
        for channel_id in channel_ids:
            # Get channel name.
            try:
                ch_resp = await http.get(f"{DISCORD_API}/channels/{channel_id}")
                ch_resp.raise_for_status()
                channel_name = ch_resp.json().get("name", channel_id)
            except Exception:
                channel_name = channel_id
                log.warning("Could not fetch channel info for %s", channel_id)

            log.info("Backfilling #%s (%s)...", channel_name, channel_id)
            staged, skipped, filtered = await _backfill_channel(http, channel_id, channel_name)
            total_staged += staged
            total_skipped += skipped
            total_filtered += filtered
            log.info(
                "#%s: %d staged, %d skipped (dedup), %d filtered (noise)",
                channel_name, staged, skipped, filtered,
            )

    log.info(
        "Backfill complete: %d staged, %d skipped, %d filtered",
        total_staged, total_skipped, total_filtered,
    )


async def _backfill_channel(
    http: httpx.AsyncClient, channel_id: str, channel_name: str
) -> tuple[int, int, int]:
    """Walk a channel's full message history, oldest first."""
    staged = 0
    skipped = 0
    filtered = 0
    before = None

    while True:
        params = {"limit": 100}
        if before:
            params["before"] = before

        resp = await http.get(f"{DISCORD_API}/channels/{channel_id}/messages", params=params)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 5)
            log.warning("Rate limited, waiting %.1fs", retry_after)
            await asyncio.sleep(retry_after)
            continue
        resp.raise_for_status()

        messages = resp.json()
        if not messages:
            break

        for msg in messages:
            text = (msg.get("content") or "").strip()
            if not text:
                continue

            author_obj = msg.get("author", {})
            # Don't skip bot messages — Granola and iMessage sync bots
            # post valuable content. The signal filter handles noise.

            author = f"{author_obj.get('username', 'unknown')}#{author_obj.get('discriminator', '0')}"
            msg_id = msg["id"]
            timestamp = msg.get("timestamp")
            guild_id = msg.get("guild_id", "")

            meta = {
                "discord_msg_id": msg_id,
                "discord_channel_id": channel_id,
                "discord_guild_id": guild_id,
                "discord_author_id": author_obj.get("id", ""),
                "discord_timestamp": timestamp,
            }

            urls = extract_urls(text)

            if urls:
                for url in urls:
                    category = classify(url)
                    sid = staging.stage(
                        source_type=category.value,
                        source_uri=url,
                        raw_content=text,
                        author=author,
                        channel=channel_name,
                        created_at=timestamp,
                        metadata=meta,
                    )
                    if sid:
                        staged += 1
                    else:
                        skipped += 1
            else:
                if is_noise(text):
                    filtered += 1
                    continue

                msg_uri = f"discord://{guild_id}/{channel_id}/{msg_id}"
                sid = staging.stage(
                    source_type=Platform.PLAIN_TEXT.value,
                    source_uri=msg_uri,
                    raw_content=text,
                    author=author,
                    channel=channel_name,
                    created_at=timestamp,
                    metadata=meta,
                )
                if sid:
                    staged += 1
                else:
                    skipped += 1

        # Paginate backwards (oldest message in this batch).
        before = messages[-1]["id"]

        # Brief pause to be respectful of rate limits.
        await asyncio.sleep(0.5)

    return staged, skipped, filtered


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    asyncio.run(backfill_all())
