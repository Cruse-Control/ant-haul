"""Unit tests for seed_storage.notifications.send_alert."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

import seed_storage.notifications as notif_mod
from seed_storage.notifications import DEBOUNCE_WINDOW, send_alert


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WEBHOOK_URL = "https://discord.com/api/webhooks/test/token"


def _make_mock_redis(set_return=True):
    """Return a mock Redis client whose SET returns set_return."""
    r = MagicMock()
    # redis SET with nx=True returns "OK"-ish truthy when key was set, None when not
    r.set.return_value = "OK" if set_return else None
    return r


# ---------------------------------------------------------------------------
# Correct POST body
# ---------------------------------------------------------------------------

def test_correct_post_body():
    """send_alert POSTs {"content": message} to the webhook URL."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.return_value = mock_response

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("hello world")

    mock_client.post.assert_called_once_with(WEBHOOK_URL, json={"content": "hello world"})


# ---------------------------------------------------------------------------
# HTTP failure → log WARNING
# ---------------------------------------------------------------------------

def test_http_failure_logs_warning(caplog):
    """HTTP error response logs a WARNING and does not raise."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.side_effect = httpx.HTTPStatusError(
        "400 Bad Request",
        request=MagicMock(),
        response=MagicMock(),
    )

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client), \
         caplog.at_level("WARNING", logger="seed_storage.notifications"):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("test message")  # must not raise

    assert any("failed" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Timeout → log WARNING
# ---------------------------------------------------------------------------

def test_timeout_logs_warning(caplog):
    """Timeout exception logs a WARNING and does not raise."""
    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.side_effect = httpx.TimeoutException("timed out")

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client), \
         caplog.at_level("WARNING", logger="seed_storage.notifications"):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("test message")  # must not raise

    assert any("timed out" in r.message.lower() or "timeout" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Debounce: skip within window
# ---------------------------------------------------------------------------

def test_debounce_skip_within_window():
    """When Redis SET NX returns None (key exists), the alert is not POSTed."""
    mock_redis = _make_mock_redis(set_return=False)  # None → key already exists

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.redis_lib.from_url", return_value=mock_redis), \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("repeated alert", debounce_key="circuit:graphiti")

    mock_client.post.assert_not_called()


# ---------------------------------------------------------------------------
# Debounce: send after expiry (key not set yet)
# ---------------------------------------------------------------------------

def test_debounce_send_after_expiry():
    """When Redis SET NX returns truthy (key was newly set), the alert IS POSTed."""
    mock_redis = _make_mock_redis(set_return=True)  # key newly set

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.return_value = mock_response

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.redis_lib.from_url", return_value=mock_redis), \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("first alert", debounce_key="circuit:graphiti")

    mock_client.post.assert_called_once()
    # Verify SET was called with correct key, NX=True, and correct TTL
    mock_redis.set.assert_called_once_with(
        "seed:alert_debounce:circuit:graphiti", "1", nx=True, ex=DEBOUNCE_WINDOW
    )


# ---------------------------------------------------------------------------
# No debounce_key → always sends
# ---------------------------------------------------------------------------

def test_no_debounce_key_always_sends():
    """When debounce_key is None, Redis is not consulted and the alert always fires."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.return_value = mock_response

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.redis_lib.from_url") as mock_from_url, \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("no debounce")

    mock_from_url.assert_not_called()
    mock_client.post.assert_called_once()


# ---------------------------------------------------------------------------
# Empty webhook URL → silent skip
# ---------------------------------------------------------------------------

def test_empty_webhook_url_skips_silently():
    """When DISCORD_ALERTS_WEBHOOK_URL is empty, nothing is sent and no exception raised."""
    mock_client = MagicMock()

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = ""
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("should be skipped")  # must not raise

    mock_client.assert_not_called()


# ---------------------------------------------------------------------------
# Redis failure during debounce → still sends
# ---------------------------------------------------------------------------

def test_redis_failure_still_sends():
    """If Redis is unreachable during debounce check, the alert is sent anyway."""
    mock_redis = MagicMock()
    mock_redis.set.side_effect = Exception("Redis connection refused")

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    mock_client = MagicMock()
    mock_client.__enter__ = MagicMock(return_value=mock_client)
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.post.return_value = mock_response

    with patch("seed_storage.notifications.settings") as mock_settings, \
         patch("seed_storage.notifications.redis_lib.from_url", return_value=mock_redis), \
         patch("seed_storage.notifications.httpx.Client", return_value=mock_client):
        mock_settings.DISCORD_ALERTS_WEBHOOK_URL = WEBHOOK_URL
        mock_settings.REDIS_URL = "redis://localhost/2"

        send_alert("urgent", debounce_key="some:key")  # must not raise

    mock_client.post.assert_called_once()
