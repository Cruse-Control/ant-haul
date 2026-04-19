"""seed_storage/ingestion/bot.py — Discord bot real-time ingestion.

Listens on configured channels, builds raw_payload per Contract 1, and
enqueues each message via enrich_message.delay(). Also subscribes to
the seed:reactions Redis pubsub channel to add emoji reactions to messages.

Contract 1 (raw_payload shape):
    source_type:    "discord"
    source_id:      Discord snowflake (str)
    source_channel: channel name
    author:         display name
    content:        raw message text
    timestamp:      ISO 8601 with timezone
    attachments:    list of attachment URLs
    metadata:       {channel_id, author_id, guild_id}

Error contract (Contract 1):
    - content empty AND attachments empty → skip (log DEBUG)
    - author is bot → skip (log DEBUG)

Contract 4 (seed:reactions pubsub):
    Workers publish {message_id, channel_id, emoji}.
    Bot subscribes and adds reactions. Dropped silently when disconnected.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import discord

from seed_storage.config import settings

logger = logging.getLogger(__name__)

# STUB: replace with worker-agent implementation
# This stub exists only for development. At merge, worker-agent's real
# seed_storage/worker/tasks.py replaces this.
try:
    from seed_storage.worker.tasks import enrich_message as _enrich_message  # type: ignore[import]
except ImportError:

    class _EnrichMessageStub:
        @staticmethod
        def delay(payload: dict) -> None:
            logger.debug("enrich_message.delay() stub called (worker not yet available)")

    _enrich_message = _EnrichMessageStub()  # type: ignore[assignment]


REACTIONS_CHANNEL = "seed:reactions"
BOT_CONNECTED_KEY = "seed:bot:connected"


def _build_raw_payload(message: discord.Message) -> dict[str, Any]:
    """Build a raw_payload dict from a discord.Message (Contract 1)."""
    return {
        "source_type": "discord",
        "source_id": str(message.id),
        "source_channel": message.channel.name,
        "author": message.author.display_name,
        "content": message.content,
        "timestamp": message.created_at.isoformat(),
        "attachments": [a.url for a in message.attachments],
        "metadata": {
            "channel_id": str(message.channel.id),
            "author_id": str(message.author.id),
            "guild_id": str(message.guild.id) if message.guild else "",
        },
    }


class SeedBot(discord.Client):
    """Discord bot that ingests messages from configured channels."""

    def __init__(self, redis_client: Any = None, **kwargs: Any) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents, **kwargs)
        self._redis = redis_client
        self._reaction_task: asyncio.Task | None = None

    async def on_ready(self) -> None:
        logger.info("Discord bot connected as %s", self.user)
        if self._redis is not None:
            await self._redis.set(BOT_CONNECTED_KEY, "1")
            self._reaction_task = asyncio.create_task(self._reaction_listener())

    async def close(self) -> None:
        """Clear bot connection flag before disconnecting."""
        if self._redis is not None:
            try:
                await self._redis.delete(BOT_CONNECTED_KEY)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Failed to clear bot connected flag: %s", exc)
        await super().close()

    async def on_message(self, message: discord.Message) -> None:
        """Handle an incoming Discord message."""
        channel_ids = settings.discord_channel_ids

        # When channel filtering is active, ignore non-configured channels.
        if channel_ids and str(message.channel.id) not in channel_ids:
            return

        # Skip messages from bots (Contract 1 error contract).
        if message.author.bot:
            logger.debug("Skipping bot message id=%s author=%s", message.id, message.author)
            return

        # Skip messages with no content and no attachments (Contract 1 error contract).
        if not message.content and not message.attachments:
            logger.debug("Skipping empty message id=%s", message.id)
            return

        payload = _build_raw_payload(message)
        _enrich_message.delay(payload)
        logger.debug("Enqueued message id=%s channel=%s", message.id, message.channel.name)

    async def _reaction_listener(self) -> None:
        """Subscribe to seed:reactions pubsub and add emoji reactions to Discord messages.

        Contract 4: Workers publish {message_id, channel_id, emoji}.
        Events are dropped silently when the bot is disconnected.
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(REACTIONS_CHANNEL)
        logger.info("Subscribed to Redis pubsub channel %s", REACTIONS_CHANNEL)
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            try:
                event = json.loads(msg["data"])
                channel_id = int(event["channel_id"])
                message_id = int(event["message_id"])
                emoji = event["emoji"]
                channel = self.get_channel(channel_id)
                if channel is None:
                    logger.debug("Reaction target channel %s not found", channel_id)
                    continue
                discord_msg = await channel.fetch_message(message_id)
                await discord_msg.add_reaction(emoji)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Failed to add reaction: %s", exc)


async def run_bot() -> None:
    """Start the Discord bot. Blocks until the bot disconnects."""
    import redis.asyncio as aioredis

    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    bot = SeedBot(redis_client=redis_client)
    await bot.start(settings.DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    asyncio.run(run_bot())
