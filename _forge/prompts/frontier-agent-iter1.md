You are the **frontier-agent** for a forge build (iteration 1, tier 0).

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
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: frontier-agent iteration 1"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Constraints from previous iteration

- FIX TEST: FAILED tests/unit/test_graphiti_client.py::TestLLMProviderBranching::test_anthropic_provider_returns_anthropic_client
- FIX CONVENTION: HARDCODED_KEYS: seed_storage/worker/dead_letters.py:29:    re.compile(r"sk-ant-[A-Za-z0-9\-_]{20,}"),
seed_storage/config.py:230:    re.compile(r"sk-a

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
