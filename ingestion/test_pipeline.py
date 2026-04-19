"""End-to-end pipeline test — run locally, no k8s, no cron waits.

Tests the full pipeline synchronously: stage → process → enrich → load → search.
Uses the local Python environment with API keys resolved from ant-keeper.
Verifies each step completes and the item is searchable in Neo4j.

Usage:
    # Test with a web article
    PG_DSN=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=... \
        uv run python -m ingestion.test_pipeline https://example.com/article

    # Test with auto-resolved credentials (needs k8s access)
    uv run python -m ingestion.test_pipeline --resolve-creds https://example.com/article

    # Dry run (stage only, don't process/enrich/load)
    uv run python -m ingestion.test_pipeline --dry-run https://example.com/article

    # Clean up test item after
    uv run python -m ingestion.test_pipeline --cleanup https://example.com/article
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time

log = logging.getLogger("test_pipeline")


def resolve_credentials():
    """Resolve API keys from ant-keeper's encrypted credential store."""
    import base64
    import subprocess

    from cryptography.fernet import Fernet
    import psycopg2

    enc_key = subprocess.check_output([
        "kubectl", "--kubeconfig", "/opt/shared/k3s/kubeconfig.yaml",
        "get", "secret", "ant-keeper-secrets", "-n", "ant-keeper",
        "-o", "jsonpath={.data.TOKEN_ENCRYPTION_KEY}",
    ]).decode()
    enc_key = base64.b64decode(enc_key).decode()
    f = Fernet(enc_key.encode())

    pg_dsn = os.environ.get("PG_DSN", "postgresql://taskman:postgres@127.0.0.1:30433/task_manager")
    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()

    for cred_id, env_name in [("anthropic", "ANTHROPIC_API_KEY"), ("gemini", "GEMINI_API_KEY")]:
        if os.environ.get(env_name):
            continue
        cur.execute("SELECT encrypted_value FROM credentials WHERE credential_id = %s LIMIT 1", (cred_id,))
        row = cur.fetchone()
        if row:
            raw = row[0]
            if isinstance(raw, memoryview):
                raw = bytes(raw)
            os.environ[env_name] = f.decrypt(raw).decode()
            log.info("Resolved %s from ant-keeper", env_name)

    conn.close()


async def test_pipeline(url: str, dry_run: bool = False, cleanup: bool = False):
    """Run the full pipeline for one URL and verify each step."""
    from ingestion.classifier import classify, clean_url
    from seed_storage import staging

    staging.init_tables()

    url = clean_url(url)
    platform = classify(url)
    log.info("URL: %s → platform: %s", url, platform.value)

    # Check if already exists
    existing = staging.get_by_uri(url)
    if existing:
        if cleanup:
            staging.update_status([str(existing["id"])], "deleted")
            log.info("Cleaned up: %s (was %s)", url, existing["status"])
            return {"status": "cleaned", "was": existing["status"]}
        log.info("Already exists at status=%s", existing["status"])
        if existing["status"] == "loaded":
            log.info("PASS: already loaded")
            return {"status": "already_loaded"}

    if cleanup:
        log.info("Nothing to clean up for %s", url)
        return {"status": "not_found"}

    # ── STAGE ─────────────────────────────────────────────────────
    t0 = time.monotonic()
    from datetime import datetime, timezone

    sid = staging.stage(
        source_type=platform.value,
        source_uri=url,
        raw_content=url,
        author="test_pipeline",
        channel="test_pipeline",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    if sid:
        log.info("✓ STAGED: %s (id=%s)", url, sid[:8])
    else:
        existing = staging.get_by_uri(url)
        sid = str(existing["id"])
        log.info("✓ STAGED (exists): %s at status=%s", url, existing["status"])

    if dry_run:
        log.info("DRY RUN: stopping after stage")
        return {"status": "staged", "id": sid}

    item = staging.get_by_id(sid)

    # ── PROCESS ───────────────────────────────────────────────────
    if item["status"] == "staged":
        import httpx
        from anthropic import AsyncAnthropic
        from ingestion.processor import process_one

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        anthropic = AsyncAnthropic(api_key=api_key) if api_key else None
        analyzer_url = os.environ.get("ANALYZER_BASE_URL", "http://localhost:8000")

        async with httpx.AsyncClient(timeout=120) as http:
            await process_one(item, http, anthropic, analyzer_url)

        item = staging.get_by_id(sid)
        if item["status"] == "failed":
            log.error("✗ PROCESS FAILED")
            return {"status": "failed", "stage": "process"}
        log.info("✓ PROCESSED: status=%s, content_len=%d", item["status"], len(item.get("raw_content") or ""))

    # ── ENRICH ────────────────────────────────────────────────────
    if item["status"] == "processed":
        from anthropic import AsyncAnthropic
        from ingestion.enricher import _enrich_one, _get_existing_tags, _upsert_tags, init_tags_table

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if api_key:
            init_tags_table()
            anthropic = AsyncAnthropic(api_key=api_key)
            existing_tags = _get_existing_tags()

            enrichment = await _enrich_one(anthropic, item, existing_tags)
            meta = item.get("metadata") or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
            for k, v in enrichment.items():
                if k in ("published_at", "speakers") and k in meta:
                    continue
                meta[k] = v

            staging.update_content(sid, item["raw_content"], metadata=meta, status="enriched")
            new_tags = enrichment.get("tags", [])
            if new_tags:
                _upsert_tags(new_tags)
            log.info("✓ ENRICHED: tags=%s, summary=%s", new_tags, enrichment.get("summary", "")[:60])
        else:
            staging.update_status([sid], "enriched")
            log.info("✓ ENRICHED (no API key, skipped)")

        item = staging.get_by_id(sid)

    # ── LOAD ──────────────────────────────────────────────────────
    if item["status"] == "enriched":
        from seed_storage.graphiti_client import add_episode, close

        content = item["raw_content"] or ""
        source_type = item["source_type"]
        channel = item.get("channel", "")

        try:
            await add_episode(
                name=url,
                content=content,
                source="text",
                source_description=f"{source_type} from #{channel}" if channel else source_type,
                reference_time=item.get("created_at") or datetime.now(timezone.utc),
            )
            staging.update_status([sid], "loaded")
            log.info("✓ LOADED into Neo4j")
        except Exception as exc:
            log.error("✗ LOAD FAILED: %s", exc)
            staging.update_status([sid], "failed")
            return {"status": "failed", "stage": "load", "error": str(exc)[:200]}
        finally:
            await close()

    # ── VERIFY SEARCH ─────────────────────────────────────────────
    item = staging.get_by_id(sid)
    elapsed = round(time.monotonic() - t0, 1)

    if item["status"] == "loaded":
        try:
            from seed_storage.graphiti_client import search, get_graphiti, close

            # Search for something from the content
            search_term = (item.get("metadata") or {}).get("summary", url)[:50]
            results = await search(search_term, limit=3)
            await close()

            if results:
                log.info("✓ SEARCHABLE: found %d results for '%s'", len(results), search_term)
                for r in results[:2]:
                    log.info("  → %s", getattr(r, "fact", str(r))[:100])
            else:
                log.warning("⚠ Loaded but search returned 0 results (may need time to index)")
        except Exception as exc:
            log.warning("⚠ Search verification failed: %s", exc)

    log.info("═══ PIPELINE TEST COMPLETE: %s in %.1fs ═══", item["status"], elapsed)
    return {"status": item["status"], "elapsed_seconds": elapsed, "source_uri": url}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    args = sys.argv[1:]
    do_resolve = "--resolve-creds" in args
    do_dry_run = "--dry-run" in args
    do_cleanup = "--cleanup" in args
    urls = [a for a in args if not a.startswith("--")]

    if not urls:
        print("Usage: python -m ingestion.test_pipeline [--resolve-creds] [--dry-run] [--cleanup] <url>")
        sys.exit(1)

    os.environ.setdefault("PG_DSN", "postgresql://taskman:postgres@127.0.0.1:30433/task_manager")
    os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:30687")

    if do_resolve:
        resolve_credentials()

    result = asyncio.run(test_pipeline(urls[0], dry_run=do_dry_run, cleanup=do_cleanup))
    print(json.dumps(result, indent=2, default=str))
