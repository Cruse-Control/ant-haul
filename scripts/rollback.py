#!/usr/bin/env python3
"""scripts/rollback.py — Remove Graphiti episodes ingested after a given timestamp.

Usage:
    python scripts/rollback.py --after 2026-04-01T00:00:00Z
    python scripts/rollback.py --after 2026-04-01T00:00:00Z --dry-run
    python scripts/rollback.py --after 2026-04-01T00:00:00Z --yes
    python scripts/rollback.py --after 2026-04-01T00:00:00Z --group-id seed-storage

The script deletes Episodic nodes (and their edges) whose ``created_at`` is
after the given timestamp. It uses direct Neo4j Cypher — NOT Graphiti — because
Graphiti does not provide a bulk-delete API. This is the one intentional bypass
of Graphiti: rollback acts on the raw graph data.

After rollback, flush the dedup sets if you want to re-ingest the removed
episodes:
    redis-cli -n 2 DEL seed:seen_messages seed:seen_urls seed:ingested_content

WARNING: This operation is irreversible. Entity nodes shared with other episodes
are NOT deleted. Only Episodic nodes (and their MENTIONS/RELATES_TO edges) are
removed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime


def _parse_timestamp(ts: str) -> datetime:
    """Parse ISO 8601 timestamp, attaching UTC if no timezone is given."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError as exc:
        print(f"Error: invalid timestamp {ts!r}: {exc}", file=sys.stderr)
        sys.exit(1)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _connect():
    """Return a Neo4j driver from settings."""
    try:
        from neo4j import GraphDatabase  # type: ignore[import]
    except ImportError:
        print("Error: neo4j package not installed. Run: uv pip install neo4j", file=sys.stderr)
        sys.exit(1)

    try:
        from seed_storage.config import settings
    except ImportError:
        print(
            "Error: seed_storage.config not importable. Run from the project root.",
            file=sys.stderr,
        )
        sys.exit(1)

    return GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
    )


def _list_episodes(driver, after_dt: datetime, group_id: str) -> list[dict]:
    """List Episodic nodes created after *after_dt* for *group_id*, ordered newest first."""
    query = """
    MATCH (e:Episodic {group_id: $group_id})
    WHERE e.created_at > $after
    RETURN e.uuid AS uuid, e.source_description AS source_description, e.created_at AS created_at
    ORDER BY e.created_at DESC
    """
    with driver.session() as session:
        result = session.run(query, group_id=group_id, after=after_dt.isoformat())
        return [dict(record) for record in result]


def _count_episodes(driver, after_dt: datetime, group_id: str) -> int:
    """Count Episodic nodes created after *after_dt* for *group_id*."""
    query = """
    MATCH (e:Episodic {group_id: $group_id})
    WHERE e.created_at > $after
    RETURN count(e) AS n
    """
    with driver.session() as session:
        result = session.run(query, group_id=group_id, after=after_dt.isoformat())
        record = result.single()
        return record["n"] if record else 0


def _delete_episodes(driver, after_dt: datetime, group_id: str) -> int:
    """Delete Episodic nodes (and their edges) created after *after_dt* for *group_id*.

    Returns the number of nodes deleted.
    Uses batched deletes to avoid large transactions.
    """
    deleted = 0
    batch_size = 500

    query = """
    MATCH (e:Episodic {group_id: $group_id})
    WHERE e.created_at > $after
    WITH e LIMIT $batch
    DETACH DELETE e
    RETURN count(e) AS n
    """

    while True:
        with driver.session() as session:
            result = session.run(
                query, group_id=group_id, after=after_dt.isoformat(), batch=batch_size
            )
            record = result.single()
            batch_deleted = record["n"] if record else 0

        deleted += batch_deleted
        if batch_deleted < batch_size:
            break

        print(f"  Deleted {deleted} episodes so far...", flush=True)

    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Roll back seed-storage graph episodes ingested after a timestamp.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--after",
        required=True,
        metavar="TIMESTAMP",
        help="Delete episodes created after this ISO 8601 timestamp (e.g. 2026-04-01T00:00:00Z)",
    )
    parser.add_argument(
        "--group-id",
        default="seed-storage",
        metavar="GROUP_ID",
        help='Graphiti group_id to scope the rollback (default: "seed-storage")',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching episodes and count without deleting",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )

    args = parser.parse_args()
    after_dt = _parse_timestamp(args.after)
    group_id: str = args.group_id

    print(f"Rollback: episodes in group_id={group_id!r} created after {after_dt.isoformat()}")
    print()

    driver = _connect()

    try:
        if args.dry_run:
            episodes = _list_episodes(driver, after_dt, group_id)
            count = len(episodes)
            print(f"Found {count} Episodic node(s) — dry run, no changes will be made.")
            if episodes:
                print()
                print(f"{'UUID':<38}  {'created_at':<28}  source_description")
                print("-" * 100)
                for ep in episodes:
                    uuid = str(ep.get("uuid", ""))[:36]
                    created_at = str(ep.get("created_at", ""))
                    source_desc = str(ep.get("source_description", ""))
                    print(f"{uuid:<38}  {created_at:<28}  {source_desc}")
            print()
            print("Dry run — no changes made.")
            return

        count = _count_episodes(driver, after_dt, group_id)
        print(f"Found {count} Episodic node(s) to remove.")

        if count == 0:
            print("Nothing to do.")
            return

        if not args.yes:
            try:
                answer = (
                    input(f"\nDelete {count} episode(s) and their edges? [y/N] ").strip().lower()
                )
            except (EOFError, KeyboardInterrupt):
                print("\nAborted.")
                sys.exit(1)

            if answer != "y":
                print("Aborted.")
                sys.exit(0)

        print(f"\nDeleting {count} episode(s)...")
        deleted = _delete_episodes(driver, after_dt, group_id)
        print(f"Done. Deleted {deleted} Episodic node(s) and their edges.")
        print()
        print("To re-ingest removed content, flush the dedup sets:")
        print("  redis-cli -n 2 DEL seed:seen_messages seed:seen_urls seed:ingested_content")

    finally:
        driver.close()


if __name__ == "__main__":
    main()
