"""seed_storage/smoke_test.py — 10-step post-deploy verification.

Run as: python -m seed_storage.smoke_test
Exit 0 on all steps passing, exit 1 on any failure.
"""

from __future__ import annotations

import logging
import sys
from datetime import date

logger = logging.getLogger(__name__)

_PASS = "\u2713"
_FAIL = "\u2717"


def _ok(step: int, msg: str) -> None:
    print(f"  [{_PASS}] step {step:02d}: {msg}")


def _err(step: int, msg: str) -> None:
    print(f"  [{_FAIL}] step {step:02d}: {msg}", file=sys.stderr)


def run_smoke_test() -> bool:
    """Run all 10 verification steps. Returns True if all pass."""
    failures: list[str] = []

    print("Seed Storage — post-deploy smoke test")
    print("=" * 50)

    # ── Step 1: Config loads ───────────────────────────────────────────────
    try:
        from seed_storage.config import settings

        assert settings.HEALTH_PORT > 0
        assert settings.API_PORT > 0
        _ok(1, f"Config loaded (HEALTH_PORT={settings.HEALTH_PORT}, API_PORT={settings.API_PORT})")
    except Exception as exc:
        _err(1, f"Config error: {exc}")
        failures.append("config")
        return _finish(failures)  # can't continue without config

    # ── Step 2: Redis connectivity ─────────────────────────────────────────
    r = None
    try:
        import redis as redis_lib

        r = redis_lib.from_url(settings.REDIS_URL, socket_timeout=5.0)
        r.ping()
        _ok(2, f"Redis reachable at {settings.REDIS_URL}")
    except Exception as exc:
        _err(2, f"Redis unreachable: {exc}")
        failures.append("redis")

    # ── Step 3: Neo4j connectivity ─────────────────────────────────────────
    try:
        from seed_storage.health import check_neo4j

        status = check_neo4j()
        if status == "ok":
            _ok(3, f"Neo4j reachable at {settings.NEO4J_URI}")
        else:
            _err(3, f"Neo4j unreachable at {settings.NEO4J_URI}")
            failures.append("neo4j")
    except Exception as exc:
        _err(3, f"Neo4j check error: {exc}")
        failures.append("neo4j")

    # ── Step 4: Frontier key accessible ───────────────────────────────────
    if r is not None:
        try:
            from seed_storage.health import FRONTIER_KEY

            size = r.zcard(FRONTIER_KEY)
            _ok(4, f"Frontier accessible (size={size})")
        except Exception as exc:
            _err(4, f"Frontier error: {exc}")
            failures.append("frontier")
    else:
        _err(4, "Frontier skipped (Redis unavailable)")
        failures.append("frontier")

    # ── Step 5: Dead letter queue accessible ──────────────────────────────
    if r is not None:
        try:
            from seed_storage.health import DEAD_LETTERS_KEY

            count = r.llen(DEAD_LETTERS_KEY)
            _ok(5, f"Dead letter queue accessible (count={count})")
        except Exception as exc:
            _err(5, f"Dead letter queue error: {exc}")
            failures.append("dead_letters")
    else:
        _err(5, "Dead letters skipped (Redis unavailable)")
        failures.append("dead_letters")

    # ── Step 6: Cost tracking key accessible ──────────────────────────────
    if r is not None:
        try:
            cost_key = f"seed:cost:daily:{date.today().isoformat()}"
            val = r.get(cost_key)
            cost = float(val) if val is not None else 0.0
            _ok(6, f"Cost tracking accessible (today={cost:.4f} USD, budget={settings.DAILY_LLM_BUDGET} USD)")
        except Exception as exc:
            _err(6, f"Cost tracking error: {exc}")
            failures.append("cost")
    else:
        _err(6, "Cost tracking skipped (Redis unavailable)")
        failures.append("cost")

    # ── Step 7: Seen messages counter accessible ───────────────────────────
    if r is not None:
        try:
            from seed_storage.health import SEEN_MESSAGES_KEY

            count = r.scard(SEEN_MESSAGES_KEY)
            _ok(7, f"Seen messages accessible (count={count})")
        except Exception as exc:
            _err(7, f"Seen messages error: {exc}")
            failures.append("seen_messages")
    else:
        _err(7, "Seen messages skipped (Redis unavailable)")
        failures.append("seen_messages")

    # ── Step 8: Seen URLs counter accessible ──────────────────────────────
    if r is not None:
        try:
            from seed_storage.health import SEEN_URLS_KEY

            count = r.scard(SEEN_URLS_KEY)
            _ok(8, f"Seen URLs accessible (count={count})")
        except Exception as exc:
            _err(8, f"Seen URLs error: {exc}")
            failures.append("seen_urls")
    else:
        _err(8, "Seen URLs skipped (Redis unavailable)")
        failures.append("seen_urls")

    # ── Step 9: Circuit breaker keys scannable ────────────────────────────
    if r is not None:
        try:
            open_cbs = [k for k in r.scan_iter("seed:circuit:*:opened_at")]
            _ok(9, f"Circuit breaker scan OK (open={len(open_cbs)})")
        except Exception as exc:
            _err(9, f"Circuit breaker scan error: {exc}")
            failures.append("circuit_breakers")
    else:
        _err(9, "Circuit breakers skipped (Redis unavailable)")
        failures.append("circuit_breakers")

    # ── Step 10: Health module public API imports cleanly ─────────────────
    try:
        from seed_storage.health import (  # noqa: F401
            CHECK_TIMEOUT,
            app,
            check_bot,
            check_celery,
            check_neo4j,
            check_redis,
            get_details,
        )

        assert CHECK_TIMEOUT == 5.0
        _ok(10, "Health module public API imports OK")
    except Exception as exc:
        _err(10, f"Health module import error: {exc}")
        failures.append("health_module")

    return _finish(failures)


def _finish(failures: list[str]) -> bool:
    print("=" * 50)
    if failures:
        print(f"FAILED ({len(failures)} step(s)): {', '.join(failures)}", file=sys.stderr)
        return False
    print("ALL STEPS PASSED")
    return True


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    success = run_smoke_test()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
