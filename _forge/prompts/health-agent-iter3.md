You are the **health-agent** for a forge build (iteration 3, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/health.py`
- `seed_storage/smoke_test.py`

### Test files you OWN:

- `tests/unit/test_health.py`

### Expected test count: ~8

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Contract 13: Health endpoint

```python
# GET /health → 200 (healthy) or 503 (unhealthy)
# Response body:
{
    "status": "healthy" | "unhealthy",
    "checks": {
        "redis": "ok" | "error",
        "neo4j": "ok" | "error",
        "celery": "ok" | "error",
        "bot": "connected" | "disconnected"
    },
    "details": {
        "raw_messages_queue_depth": int,
        "graph_ingest_queue_depth": int,
        "frontier_size": int,
        "dead_letter_count": int,
        "daily_cost_usd": float,
        "daily_budget_usd": float,
        "messages_seen_total": int,
        "urls_seen_total": int,
        "open_circuit_breakers": list[str]
    }
}
```

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: health-agent iteration 3"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Constraints from previous iteration

- FIX DEPLOY: Failed to trigger: None

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
