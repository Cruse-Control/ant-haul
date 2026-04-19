You are the **frontier-agent** for a forge build (iteration 5, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/expansion/__init__.py`
- `seed_storage/expansion/frontier.py`
- `seed_storage/expansion/policies.py`
- `seed_storage/expansion/scanner.py`
- `seed_storage/expansion/cli.py`

### Test files you OWN:

- `tests/unit/test_frontier.py`

### Expected test count: ~15

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Contract 9: Frontier interface

```python
# seed_storage/expansion/frontier.py

def add_to_frontier(redis_client, url_hash: str, priority: float, meta: dict) -> None:
    """ZADD NX to seed:frontier + HSET seed:frontier:meta:{url_hash}."""
    ...

def pick_top(redis_client, batch_size: int, min_threshold: float,
             depth_policies: dict) -> list[dict]:
    """Top N URLs from frontier where score >= threshold and depth within policy."""
    ...

def remove_from_frontier(redis_client, url_hash: str) -> None:
    """ZREM + DEL metadata hash."""
    ...

def get_frontier_meta(redis_client, url_hash: str) -> dict | None:
    """HGETALL seed:frontier:meta:{url_hash}."""
    ...
```

Frontier metadata shape:
```python
frontier_meta: dict = {
    "url": str,
    "discovered_from_url": str,
    "discovered_from_source_id": str,
    "source_channel": str,
    "depth": int,                   # 0 = direct link from message
    "resolver_hint": str,           # expected resolver type
    "discovered_at": str,           # ISO 8601
}
```

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Dockerfile rule:** If you own a Dockerfile, you MUST `COPY` all source code directories BEFORE running `pip install .` or any install command. `pip install .` reads pyproject.toml which references the package — the source must already be in the image. Correct order: COPY pyproject.toml → COPY source dirs → RUN pip install.
5b. **supervisord.conf rule:** If you own supervisord.conf, NEVER use `%(ENV_*)s` interpolation for optional env vars — supervisord crashes if the env var is not set. Hardcode default values directly (e.g., `--concurrency=2`).
6. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6b. **Celery async/sync rule:** Celery tasks run synchronously. If your task must call async code (e.g., graphiti client), bridge it with `asyncio.run()` at the task boundary. Never `await` inside a sync task, and never pass a coroutine wrapper as if it were a coroutine — call it first to get the coroutine object:
  ```python
  # CORRECT
  @app.task
  def enrich_message(source_id: str):
      asyncio.run(_do_async_work(source_id))

  async def _do_async_work(source_id: str):
      client = GraphitiClient()
      await client.add_episode(...)

  # WRONG — asyncio.run() needs a coroutine object, not a function
  # asyncio.run(_do_async_work)        ← TypeError: a coroutine was expected
  # asyncio.run(some_wrapper)          ← same error if wrapper isn't called
  ```
7. **Commit your work** when done: `git add -A && git commit -m "forge: frontier-agent iteration 5"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
