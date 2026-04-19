"""seed_storage/expansion/cli.py — Manual expansion CLI.

Usage:
    python -m seed_storage.expansion.cli expand <url> [--priority FLOAT] [--depth INT]
    python -m seed_storage.expansion.cli list [--limit INT]
    python -m seed_storage.expansion.cli remove <url_hash>
    python -m seed_storage.expansion.cli scan

Examples:
    python -m seed_storage.expansion.cli expand https://github.com/owner/repo
    python -m seed_storage.expansion.cli expand https://youtu.be/dQw4w9WgXcQ --priority 0.9
    python -m seed_storage.expansion.cli list --limit 20
    python -m seed_storage.expansion.cli scan
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from urllib.parse import urlparse

import redis as redis_lib

from seed_storage.config import settings
from seed_storage.dedup import url_hash
from seed_storage.expansion.frontier import (
    add_to_frontier,
    pick_top,
    remove_from_frontier,
)
from seed_storage.expansion.policies import (
    DEPTH_POLICIES,
    compute_priority,
)

logger = logging.getLogger(__name__)


def _infer_resolver_hint(url: str) -> str:
    """Infer the likely resolver type from a URL."""
    host = urlparse(url).netloc.lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    if "github.com" in host:
        return "github"
    if "twitter.com" in host or "x.com" in host:
        return "twitter"
    path = urlparse(url).path.lower()
    if path.endswith(".pdf"):
        return "pdf"
    if any(path.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
        return "image"
    if any(path.endswith(ext) for ext in (".mp4", ".mkv", ".webm", ".mov")):
        return "video"
    return "webpage"


def _get_domain(url: str) -> str:
    """Extract registered domain from URL for bonus lookup."""
    netloc = urlparse(url).netloc.lower()
    # Strip port
    netloc = netloc.split(":")[0]
    # Strip www prefix
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def cmd_expand(args: argparse.Namespace, redis_client) -> int:
    """Add a URL to the frontier for expansion."""
    url = args.url
    h = url_hash(url)
    resolver_hint = _infer_resolver_hint(url)
    domain = _get_domain(url)

    priority = (
        args.priority
        if args.priority is not None
        else compute_priority(
            depth=args.depth,
            resolver_hint=resolver_hint,
            domain=domain,
        )
    )

    meta = {
        "url": url,
        "discovered_from_url": "",
        "discovered_from_source_id": "",
        "source_channel": args.channel,
        "depth": args.depth,
        "resolver_hint": resolver_hint,
        "discovered_at": datetime.now(tz=UTC).isoformat(),
    }

    add_to_frontier(redis_client, h, priority, meta)
    print(json.dumps({"url_hash": h, "priority": priority, "resolver_hint": resolver_hint}))
    return 0


def cmd_list(args: argparse.Namespace, redis_client) -> int:
    """List the top entries in the frontier."""
    limit = args.limit
    batch = pick_top(
        redis_client,
        batch_size=limit,
        min_threshold=0.0,
        depth_policies=DEPTH_POLICIES,
    )
    if not batch:
        print("Frontier is empty.")
        return 0
    for item in batch:
        print(json.dumps(item))
    return 0


def cmd_remove(args: argparse.Namespace, redis_client) -> int:
    """Remove a URL hash from the frontier."""
    remove_from_frontier(redis_client, args.url_hash)
    print(f"Removed {args.url_hash}")
    return 0


def cmd_scan(args: argparse.Namespace, redis_client) -> int:
    """Trigger a frontier scan (enqueues expansion tasks)."""
    from seed_storage.expansion.scanner import scan_frontier  # noqa: PLC0415

    count = scan_frontier(redis_client)
    print(f"Enqueued {count} expansion tasks.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m seed_storage.expansion.cli",
        description="Manual frontier management for seed-storage URL expansion.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # expand
    p_expand = sub.add_parser("expand", help="Add a URL to the frontier.")
    p_expand.add_argument("url", help="URL to add.")
    p_expand.add_argument(
        "--priority",
        type=float,
        default=None,
        help="Override computed priority score (0.0–1.0+).",
    )
    p_expand.add_argument(
        "--depth",
        type=int,
        default=0,
        help="Expansion depth (0 = direct link from message).",
    )
    p_expand.add_argument(
        "--channel",
        default="manual",
        help="Source channel label (default: manual).",
    )

    # list
    p_list = sub.add_parser("list", help="List top frontier entries.")
    p_list.add_argument("--limit", type=int, default=20, help="Maximum rows to show.")

    # remove
    p_remove = sub.add_parser("remove", help="Remove a URL hash from the frontier.")
    p_remove.add_argument("url_hash", help="SHA256 hex hash of the URL to remove.")

    # scan
    sub.add_parser("scan", help="Trigger frontier scan (enqueue expansion tasks).")

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING)
    parser = build_parser()
    args = parser.parse_args(argv)

    redis_client = redis_lib.from_url(settings.REDIS_URL)

    commands = {
        "expand": cmd_expand,
        "list": cmd_list,
        "remove": cmd_remove,
        "scan": cmd_scan,
    }
    handler = commands.get(args.command)
    if handler is None:
        parser.print_help()
        return 1
    return handler(args, redis_client)


if __name__ == "__main__":
    sys.exit(main())
