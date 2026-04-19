You are the **redis-utils-agent** for a forge build (iteration 8, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/dedup.py`
- `seed_storage/circuit_breaker.py`
- `seed_storage/cost_tracking.py`
- `seed_storage/rate_limiting.py`

### Test files you OWN:

- `tests/unit/test_dedup.py`
- `tests/unit/test_url_canonicalization.py`
- `tests/unit/test_circuit_breaker.py`
- `tests/unit/test_cost_tracking.py`
- `tests/unit/test_rate_limiting.py`

### Expected test count: ~62

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Contract 5: DedupStore interface

```python
class DedupStore:
    def __init__(self, redis_client: redis.Redis, set_key: str): ...
    def is_seen(self, key: str) -> bool: ...
    def mark_seen(self, key: str) -> None: ...
    def seen_or_mark(self, key: str) -> bool:
        """Atomic SADD. Returns True if already seen."""
        ...
```

Three separate Redis SETs:
- `seed:seen_messages` — member = `{source_type}:{source_id}`
- `seed:seen_urls` — member = SHA256 hex of canonical URL
- `seed:ingested_content` — member = URL hash (tracks graph ingestion, not just resolution)

---

### Contract 6: CircuitBreaker interface

```python
class CircuitBreaker:
    def __init__(self, redis_client: redis.Redis, service_name: str,
                 failure_threshold: int = 5, cooldown_seconds: int = 300): ...

    def record_success(self) -> None: ...
    def record_failure(self) -> None: ...
    def is_open(self) -> bool: ...
    @property
    def state(self) -> Literal["closed", "open", "half-open"]: ...
```

Redis key: `seed:circuit:{service_name}`. State shared across all workers.

When circuit opens → call `send_alert(...)`. When circuit closes → call `send_alert(...)`.

---

### Contract 7: CostTracker interface

```python
class CostTracker:
    def __init__(self, redis_client: redis.Redis, daily_budget: float,
                 cost_per_call: float, warning_threshold: float = 0.8): ...

    def increment(self) -> None:
        """Increment daily counter by cost_per_call. Key: seed:cost:daily:YYYY-MM-DD, TTL 48h."""
        ...
    def is_budget_exceeded(self) -> bool: ...
    def is_warning_threshold(self) -> bool: ...
    def get_current_spend(self) -> float: ...
```

---

### Contract 8: RateLimiter interface

```python
class RateLimiter:
    def __init__(self, redis_client: redis.Redis, key: str, max_per_minute: int): ...
    def allow(self) -> bool:
        """Sliding window check. Returns True if under limit."""
        ...
```

Redis key: `seed:ratelimit:graphiti`.

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
7. **Commit your work** when done: `git add -A && git commit -m "forge: redis-utils-agent iteration 8"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Constraints from previous iteration

- FIX DEPLOY: Deploy failed: Credential 'openai' missing proxy_target — all env-mode credentials require proxy_target for iron-proxy. Set it via: ./infra/scripts/proxy-enable.sh openai <upstream_url>
- FIX INTEGRATION: tests/integration/test_graphiti.py::test_add_episode_creates_nodes FAILED [ 74%]
- FIX INTEGRATION: ERROR    graphiti_core.driver.neo4j_driver:neo4j_driver.py:174 Error executing Neo4j query: neo4j._async.driver.AsyncDriver.execute_query() got multiple values for keyword argument 'parameters_'
- FIX INTEGRATION: FAILED tests/integration/test_graphiti.py::test_add_episode_creates_nodes - T...

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
