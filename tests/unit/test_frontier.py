"""tests/unit/test_frontier.py — Unit tests for frontier operations and priority scoring.

All Redis calls are mocked. No real infrastructure required.
~15 tests covering: add_to_frontier, get_frontier_meta, remove_from_frontier,
pick_top, compute_priority, DEPTH_POLICIES, and boundary conditions.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from seed_storage.expansion.frontier import (
    FRONTIER_KEY,
    META_KEY_PREFIX,
    add_to_frontier,
    get_frontier_meta,
    pick_top,
    remove_from_frontier,
)
from seed_storage.expansion.policies import (
    BASE_PRIORITY,
    DEPTH_PENALTY_PER_LEVEL,
    DEPTH_POLICIES,
    DOMAIN_BONUS,
    RESOLVER_BONUS,
    compute_priority,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_redis(zadd_return=1, hgetall_return=None, zrevrangebyscore_return=None):
    """Return a MagicMock shaped like a Redis client."""
    r = MagicMock()
    r.zadd.return_value = zadd_return
    r.hgetall.return_value = hgetall_return or {}
    r.zrevrangebyscore.return_value = zrevrangebyscore_return or []
    return r


def _make_meta(url="https://example.com", depth=0, resolver_hint="webpage"):
    return {
        "url": url,
        "discovered_from_url": "https://origin.com",
        "discovered_from_source_id": "src-123",
        "source_channel": "general",
        "depth": depth,
        "resolver_hint": resolver_hint,
        "discovered_at": "2026-04-18T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# Test: add_to_frontier
# ---------------------------------------------------------------------------


class TestAddToFrontier:
    def test_zadd_nx_called(self):
        r = _make_redis()
        add_to_frontier(r, "abc123", 0.7, _make_meta())
        r.zadd.assert_called_once_with(FRONTIER_KEY, {"abc123": 0.7}, nx=True)

    def test_hset_stores_meta(self):
        r = _make_redis()
        meta = _make_meta(url="https://example.com", depth=1, resolver_hint="github")
        add_to_frontier(r, "abc123", 0.5, meta)
        r.hset.assert_called_once()
        call_kwargs = r.hset.call_args
        assert call_kwargs[0][0] == f"{META_KEY_PREFIX}abc123"
        stored = call_kwargs[1]["mapping"]
        assert stored["url"] == "https://example.com"
        assert stored["depth"] == "1"
        assert stored["resolver_hint"] == "github"

    def test_all_meta_values_stored_as_strings(self):
        """Redis HSET requires string values; int depth must be converted."""
        r = _make_redis()
        meta = _make_meta(depth=3)
        add_to_frontier(r, "deadbeef", 0.6, meta)
        stored = r.hset.call_args[1]["mapping"]
        for v in stored.values():
            assert isinstance(v, str), f"Expected str, got {type(v)}: {v!r}"

    def test_nx_semantics_does_not_overwrite_score(self):
        """When URL already exists (zadd returns 0), meta is still refreshed."""
        r = _make_redis(zadd_return=0)  # 0 means already existed
        meta = _make_meta()
        add_to_frontier(r, "existing", 0.9, meta)
        # ZADD NX should still be called
        r.zadd.assert_called_once_with(FRONTIER_KEY, {"existing": 0.9}, nx=True)
        # But metadata should still be written
        r.hset.assert_called_once()


# ---------------------------------------------------------------------------
# Test: get_frontier_meta
# ---------------------------------------------------------------------------


class TestGetFrontierMeta:
    def test_returns_none_for_missing_hash(self):
        r = _make_redis(hgetall_return={})
        assert get_frontier_meta(r, "nonexistent") is None

    def test_metadata_round_trip(self):
        """Stored bytes are decoded and depth is returned as int."""
        raw = {
            b"url": b"https://github.com/owner/repo",
            b"depth": b"2",
            b"resolver_hint": b"github",
            b"discovered_from_url": b"https://discord.com",
            b"discovered_from_source_id": b"src-456",
            b"source_channel": b"general",
            b"discovered_at": b"2026-04-18T00:00:00+00:00",
        }
        r = _make_redis(hgetall_return=raw)
        meta = get_frontier_meta(r, "somehash")
        assert meta is not None
        assert meta["url"] == "https://github.com/owner/repo"
        assert meta["depth"] == 2  # int, not string
        assert meta["resolver_hint"] == "github"

    def test_depth_coercion_to_int(self):
        r = _make_redis(hgetall_return={b"depth": b"5", b"url": b"https://x.com"})
        meta = get_frontier_meta(r, "h")
        assert isinstance(meta["depth"], int)
        assert meta["depth"] == 5

    def test_correct_redis_key_queried(self):
        r = _make_redis(hgetall_return={})
        get_frontier_meta(r, "myhash")
        r.hgetall.assert_called_once_with(f"{META_KEY_PREFIX}myhash")


# ---------------------------------------------------------------------------
# Test: remove_from_frontier
# ---------------------------------------------------------------------------


class TestRemoveFromFrontier:
    def test_zrem_and_delete_called(self):
        r = _make_redis()
        remove_from_frontier(r, "abc123")
        r.zrem.assert_called_once_with(FRONTIER_KEY, "abc123")
        r.delete.assert_called_once_with(f"{META_KEY_PREFIX}abc123")

    def test_idempotent_on_missing_url(self):
        """remove_from_frontier should not raise when hash is not present."""
        r = _make_redis()
        r.zrem.return_value = 0  # nothing removed
        r.delete.return_value = 0
        remove_from_frontier(r, "gone")  # must not raise


# ---------------------------------------------------------------------------
# Test: pick_top
# ---------------------------------------------------------------------------


class TestPickTop:
    def _raw_entry(self, url_hash: str, score: float):
        return (url_hash.encode(), score)

    def _meta_bytes(self, depth=0, resolver_hint="webpage"):
        return {
            b"url": b"https://example.com",
            b"depth": str(depth).encode(),
            b"resolver_hint": resolver_hint.encode(),
            b"source_channel": b"general",
            b"discovered_from_url": b"",
            b"discovered_from_source_id": b"",
            b"discovered_at": b"2026-04-18T00:00:00+00:00",
        }

    def test_empty_frontier_returns_empty_list(self):
        r = _make_redis(zrevrangebyscore_return=[])
        result = pick_top(r, batch_size=10, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        assert result == []

    def test_returns_at_most_batch_size(self):
        entries = [self._raw_entry(f"hash{i}", 0.9 - i * 0.01) for i in range(10)]
        r = _make_redis(zrevrangebyscore_return=entries)
        r.hgetall.return_value = self._meta_bytes(depth=0, resolver_hint="webpage")
        result = pick_top(r, batch_size=3, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        assert len(result) <= 3

    def test_entries_sorted_by_score_descending(self):
        entries = [
            self._raw_entry("hashA", 0.9),
            self._raw_entry("hashB", 0.7),
            self._raw_entry("hashC", 0.5),
        ]
        r = _make_redis(zrevrangebyscore_return=entries)
        r.hgetall.return_value = self._meta_bytes(depth=0, resolver_hint="webpage")
        result = pick_top(r, batch_size=10, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        scores = [item["score"] for item in result]
        assert scores == sorted(scores, reverse=True)

    def test_depth_policy_filters_too_deep(self):
        """URLs whose depth exceeds the policy should be excluded."""
        # youtube max depth = 3 per DEPTH_POLICIES
        entries = [
            self._raw_entry("tooDeep", 0.9),
        ]
        r = _make_redis(zrevrangebyscore_return=entries)
        r.hgetall.return_value = self._meta_bytes(depth=4, resolver_hint="youtube")
        result = pick_top(r, batch_size=10, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        assert all(item["url_hash"] != "tooDeep" for item in result)

    def test_depth_within_policy_included(self):
        entries = [self._raw_entry("ok", 0.8)]
        r = _make_redis(zrevrangebyscore_return=entries)
        r.hgetall.return_value = self._meta_bytes(depth=2, resolver_hint="youtube")
        result = pick_top(r, batch_size=10, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        assert any(item["url_hash"] == "ok" for item in result)

    def test_missing_meta_skips_entry(self):
        """If metadata hash is missing, the entry is skipped silently."""
        entries = [self._raw_entry("nometahash", 0.9)]
        r = _make_redis(zrevrangebyscore_return=entries, hgetall_return={})
        result = pick_top(r, batch_size=10, min_threshold=0.0, depth_policies=DEPTH_POLICIES)
        assert result == []


# ---------------------------------------------------------------------------
# Test: compute_priority (policies.py)
# ---------------------------------------------------------------------------


class TestComputePriority:
    def test_base_priority_default(self):
        score = compute_priority()
        assert score == pytest.approx(BASE_PRIORITY, abs=1e-6)

    def test_depth_penalty_applied(self):
        score_d0 = compute_priority(depth=0)
        score_d1 = compute_priority(depth=1)
        assert score_d1 == pytest.approx(score_d0 - DEPTH_PENALTY_PER_LEVEL, abs=1e-6)

    def test_resolver_bonus_applied(self):
        score_webpage = compute_priority(resolver_hint="webpage")
        score_youtube = compute_priority(resolver_hint="youtube")
        diff = RESOLVER_BONUS["youtube"] - RESOLVER_BONUS["webpage"]
        assert score_youtube == pytest.approx(score_webpage + diff, abs=1e-6)

    def test_domain_bonus_applied(self):
        score_no_domain = compute_priority(domain="")
        score_github = compute_priority(domain="github.com")
        assert score_github == pytest.approx(score_no_domain + DOMAIN_BONUS["github.com"], abs=1e-6)

    def test_channel_bonus_applied(self):
        bonuses = {"special-channel": 0.3}
        score_no_channel = compute_priority(source_channel="other", channel_bonuses=bonuses)
        score_with_channel = compute_priority(
            source_channel="special-channel", channel_bonuses=bonuses
        )
        assert score_with_channel == pytest.approx(score_no_channel + 0.3, abs=1e-6)

    def test_floor_at_zero(self):
        """Very deep URLs with negative-bonus resolvers should floor at 0."""
        score = compute_priority(
            depth=100,
            resolver_hint="twitter",  # -1.0 resolver bonus
            base_priority=0.5,
        )
        assert score == 0.0

    def test_no_channel_bonuses_dict(self):
        """channel_bonuses=None should not raise and should not add bonus."""
        score = compute_priority(source_channel="anything", channel_bonuses=None)
        expected = compute_priority(source_channel="anything")
        assert score == pytest.approx(expected, abs=1e-6)


# ---------------------------------------------------------------------------
# Test: DEPTH_POLICIES dict
# ---------------------------------------------------------------------------


class TestDepthPolicies:
    def test_all_resolver_types_present(self):
        for hint in ("youtube", "github", "pdf", "image", "video", "webpage", "unknown"):
            assert hint in DEPTH_POLICIES, f"Missing resolver hint: {hint}"

    def test_default_key_present(self):
        assert "default" in DEPTH_POLICIES

    def test_twitter_depth_zero(self):
        """Twitter/tweet should have depth 0 — we do not expand them."""
        assert DEPTH_POLICIES.get("twitter", -1) == 0
        assert DEPTH_POLICIES.get("tweet", -1) == 0
