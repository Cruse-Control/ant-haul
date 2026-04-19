#!/usr/bin/env python3
"""CLI query interface for the seed-storage knowledge graph.

Usage:
    python scripts/query.py "your query here"
    python scripts/query.py "your query here" --limit 20
    python scripts/query.py "your query here" --json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys


async def _run_query(query: str, limit: int, output_json: bool) -> None:
    """Execute a search query and print results."""
    from seed_storage.query.search import search

    results = await search(query=query, num_results=limit)

    if output_json:
        output = []
        for edge in results:
            output.append(
                {
                    "uuid": str(edge.uuid),
                    "name": edge.name,
                    "fact": edge.fact,
                    "group_id": edge.group_id,
                    "source_node_uuid": str(edge.source_node_uuid),
                    "target_node_uuid": str(edge.target_node_uuid),
                    "created_at": edge.created_at.isoformat() if edge.created_at else None,
                    "valid_at": edge.valid_at.isoformat() if edge.valid_at else None,
                    "invalid_at": edge.invalid_at.isoformat() if edge.invalid_at else None,
                }
            )
        print(json.dumps(output, indent=2))
    else:
        if not results:
            print("No results found.")
            return

        print(f"Found {len(results)} result(s) for: {query!r}\n")
        for i, edge in enumerate(results, 1):
            print(f"[{i}] {edge.name}")
            print(f"    Fact: {edge.fact}")
            print(f"    UUID: {edge.uuid}")
            if edge.valid_at:
                print(f"    Valid at: {edge.valid_at.isoformat()}")
            print()


def main() -> None:
    """Entry point for the query CLI."""
    parser = argparse.ArgumentParser(
        description="Query the seed-storage knowledge graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("query", help="Search query string")
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=10,
        help="Maximum number of results to return (default: 10)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output results as JSON",
    )

    args = parser.parse_args()

    if not args.query.strip():
        print("Error: query cannot be empty", file=sys.stderr)
        sys.exit(1)

    if args.limit < 1:
        print("Error: --limit must be >= 1", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(_run_query(args.query, args.limit, args.output_json))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
