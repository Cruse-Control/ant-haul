"""Run multiple loader batches back-to-back with auto-breaker-reset.

Usage: python3 scripts/load_batches.py [--batches N] [--concurrency N]
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

# Load .env
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("load_batches")


async def run_batches(n: int = 12, concurrency: int = 3):
    import psycopg2
    from ingestion.loader import load_batch

    pg_dsn = os.environ.get("PG_DSN", "postgresql://taskman:postgres@127.0.0.1:30433/task_manager")

    for i in range(n):
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        cur.execute("UPDATE seed_circuit_breaker SET resolved_at = NOW() WHERE resolved_at IS NULL")
        cur.execute(
            "UPDATE seed_staging SET status = 'enriched', batch_id = NULL "
            "WHERE status IN ('failed', 'loading') AND length(raw_content) > 20"
        )
        conn.commit()
        cur.execute("SELECT count(*) FROM seed_staging WHERE status = 'enriched'")
        remaining = cur.fetchone()[0]
        conn.close()

        if remaining == 0:
            log.info("Batch %d: No enriched items left. Done!", i + 1)
            break

        log.info("=== Batch %d/%d — %d enriched items remaining ===", i + 1, n, remaining)
        await load_batch(concurrency=concurrency, limit=200)

    log.info("All batches complete.")


if __name__ == "__main__":
    batches = 12
    concurrency = 3
    for arg in sys.argv[1:]:
        if arg.startswith("--batches="):
            batches = int(arg.split("=")[1])
        elif arg.startswith("--concurrency="):
            concurrency = int(arg.split("=")[1])

    asyncio.run(run_batches(n=batches, concurrency=concurrency))
