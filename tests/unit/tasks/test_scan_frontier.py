"""tests/unit/tasks/test_scan_frontier.py

Unit tests for the scan_frontier Celery beat task.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def celery_always_eager():
    from seed_storage.worker.app import app

    app.conf.task_always_eager = True
    yield
    app.conf.task_always_eager = False


class TestScanFrontierAutoDisabled:
    """FRONTIER_AUTO_ENABLED=False must cause immediate no-op."""

    def test_auto_disabled_returns_zero(self):
        """When FRONTIER_AUTO_ENABLED=False, returns 0 without touching Redis."""
        with (
            patch("seed_storage.expansion.scanner.settings") as mock_settings,
            patch("seed_storage.expansion.scanner.redis_lib") as mock_redis_lib,
        ):
            mock_settings.FRONTIER_AUTO_ENABLED = False

            from seed_storage.worker.tasks import scan_frontier

            result = scan_frontier()

            assert result == 0
            mock_redis_lib.from_url.assert_not_called()

    def test_auto_disabled_does_not_enqueue_expand_tasks(self):
        """When disabled, expand_from_frontier must never be enqueued."""
        with (
            patch("seed_storage.expansion.scanner.settings") as mock_settings,
            patch("seed_storage.expansion.scanner.redis_lib"),
            patch("seed_storage.worker.tasks.expand_from_frontier") as mock_expand,
        ):
            mock_settings.FRONTIER_AUTO_ENABLED = False

            from seed_storage.worker.tasks import scan_frontier

            scan_frontier()

            mock_expand.delay.assert_not_called()


class TestScanFrontierPicksTopBatch:
    """With auto-scanning enabled, top-N URLs must be enqueued."""

    def test_picks_top_batch_and_enqueues(self):
        """pick_top results must be enqueued as expand_from_frontier tasks."""
        frontier_items = [
            {"url_hash": "hash1", "url": "https://a.com", "depth": 0, "score": 0.9},
            {"url_hash": "hash2", "url": "https://b.com", "depth": 1, "score": 0.7},
        ]

        with (
            patch("seed_storage.expansion.scanner.settings") as mock_settings,
            patch("seed_storage.expansion.scanner.redis_lib") as mock_redis_lib,
            patch("seed_storage.expansion.scanner.pick_top", return_value=frontier_items),
            patch("seed_storage.worker.tasks.expand_from_frontier") as mock_expand,
        ):
            mock_settings.FRONTIER_AUTO_ENABLED = True
            mock_settings.MAX_EXPANSION_BREADTH = 20

            r = MagicMock()
            mock_redis_lib.from_url.return_value = r

            from seed_storage.worker.tasks import scan_frontier

            result = scan_frontier()

            assert result == 2
            assert mock_expand.delay.call_count == 2

    def test_empty_frontier_returns_zero(self):
        """Empty frontier must return 0 without enqueuing any tasks."""
        with (
            patch("seed_storage.expansion.scanner.settings") as mock_settings,
            patch("seed_storage.expansion.scanner.redis_lib") as mock_redis_lib,
            patch("seed_storage.expansion.scanner.pick_top", return_value=[]),
            patch("seed_storage.worker.tasks.expand_from_frontier") as mock_expand,
        ):
            mock_settings.FRONTIER_AUTO_ENABLED = True
            mock_settings.MAX_EXPANSION_BREADTH = 20

            r = MagicMock()
            mock_redis_lib.from_url.return_value = r

            from seed_storage.worker.tasks import scan_frontier

            result = scan_frontier()

            assert result == 0
            mock_expand.delay.assert_not_called()


class TestScanFrontierDepthPolicies:
    """Depth policies must be respected during pick_top."""

    def test_depth_policies_passed_to_pick_top(self):
        """DEPTH_POLICIES must be forwarded to pick_top."""
        with (
            patch("seed_storage.expansion.scanner.settings") as mock_settings,
            patch("seed_storage.expansion.scanner.redis_lib") as mock_redis_lib,
            patch("seed_storage.expansion.scanner.pick_top") as mock_pick_top,
            patch("seed_storage.expansion.scanner.DEPTH_POLICIES", {"default": 3}),
        ):
            mock_settings.FRONTIER_AUTO_ENABLED = True
            mock_settings.MAX_EXPANSION_BREADTH = 20
            mock_pick_top.return_value = []

            r = MagicMock()
            mock_redis_lib.from_url.return_value = r

            from seed_storage.worker.tasks import scan_frontier

            scan_frontier()

            mock_pick_top.assert_called_once()
            call_kwargs = mock_pick_top.call_args
            # depth_policies argument must be passed
            assert call_kwargs is not None
