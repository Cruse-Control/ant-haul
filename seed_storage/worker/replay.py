"""seed_storage/worker/replay.py — CLI for dead-letter queue replay.

Usage:
    python -m seed_storage.worker.replay --list   # list without consuming
    python -m seed_storage.worker.replay --one    # pop and display oldest entry
    python -m seed_storage.worker.replay --all    # pop and display all entries
"""

from __future__ import annotations

import argparse
import json
import sys

import redis as redis_lib

from seed_storage.config import settings
from seed_storage.worker.dead_letters import list_dead_letters, replay_all, replay_one


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m seed_storage.worker.replay",
        description="Dead-letter queue management for seed-storage workers",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="List dead letters without consuming")
    group.add_argument("--one", action="store_true", help="Pop and replay the oldest dead letter")
    group.add_argument("--all", action="store_true", help="Pop and replay all dead letters")
    args = parser.parse_args(argv)

    r = redis_lib.from_url(settings.REDIS_URL)

    if args.list:
        count, entries = list_dead_letters(r)
        print(f"Dead letters in queue: {count}")
        for entry in entries:
            print(json.dumps(entry, indent=2, default=str))
        return 0

    if args.one:
        result = replay_one(r)
        if result is None:
            print("Queue is empty — no dead letters to replay.")
            return 0
        task_name, payload = result
        print(f"task_name: {task_name}")
        print(json.dumps(payload, indent=2, default=str))
        return 0

    if args.all:
        results = replay_all(r)
        if not results:
            print("Queue is empty — no dead letters to replay.")
            return 0
        print(f"Replaying {len(results)} dead letter(s):")
        for task_name, payload in results:
            print(f"  task_name={task_name!r}  payload={json.dumps(payload, default=str)}")
        return 0

    return 1  # unreachable (argparse enforces required group)


if __name__ == "__main__":
    sys.exit(main())
