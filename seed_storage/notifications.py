"""seed_storage/notifications.py — Fire-and-forget Discord webhook alerts with debounce.

Contract 10: send_alert(message, debounce_key=None) — sync, never raises.
Empty DISCORD_ALERTS_WEBHOOK_URL → silently skipped (alerts disabled).
Debounce via Redis SET NX EX — skip if key exists (within window).
"""

from __future__ import annotations

import logging

import httpx
import redis as redis_lib

from seed_storage.config import settings

logger = logging.getLogger(__name__)

DEBOUNCE_WINDOW = 300  # seconds (5 minutes)
_DEBOUNCE_PREFIX = "seed:alert_debounce:"


def send_alert(message: str, debounce_key: str | None = None) -> None:
    """Fire-and-forget Discord webhook alert. Sync httpx.Client. Never raises.

    Empty DISCORD_ALERTS_WEBHOOK_URL → silently skipped (alerts disabled).
    If debounce_key is provided, the alert is suppressed when the same key
    was sent within DEBOUNCE_WINDOW seconds.
    """
    webhook_url = settings.DISCORD_ALERTS_WEBHOOK_URL
    if not webhook_url:
        return

    if debounce_key is not None:
        try:
            r = redis_lib.from_url(settings.REDIS_URL)
            redis_key = f"{_DEBOUNCE_PREFIX}{debounce_key}"
            # SET NX EX: returns None if key already exists (debounce active)
            if r.set(redis_key, "1", nx=True, ex=DEBOUNCE_WINDOW) is None:
                return
        except Exception as exc:  # noqa: BLE001
            # Redis failure — degrade gracefully and still send the alert
            logger.warning("send_alert: Redis debounce check failed: %s", exc)

    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.post(webhook_url, json={"content": message})
            resp.raise_for_status()
    except httpx.TimeoutException as exc:
        logger.warning("send_alert: request timed out: %s", exc)
    except Exception as exc:  # noqa: BLE001
        logger.warning("send_alert: request failed: %s", exc)
