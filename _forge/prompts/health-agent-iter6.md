You are the **health-agent** for a forge build (iteration 6, tier 0).

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
7. **Commit your work** when done: `git add -A && git commit -m "forge: health-agent iteration 6"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Constraints from previous iteration

- FIX DEPLOY: Deploy failed: Credential 'openai' missing proxy_target — all env-mode credentials require proxy_target for iron-proxy. Set it via: ./infra/scripts/proxy-enable.sh openai <upstream_url>
- FIX INTEGRATION: ERROR    graphiti_core.driver.neo4j_driver:neo4j_driver.py:174 Error executing Neo4j query: Task <Task pending name='Task-155' coro=<_add_episode() running at /home/wyler-zahm/Desktop/cruse-control/se
- FIX INTEGRATION: FAILED tests/integration/test_graphiti.py::test_add_episode_creates_nodes - R...
- FIX E2E: FAILED tests/e2e/test_message_to_graph.py::test_plain_text_to_graph - Asserti...

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
