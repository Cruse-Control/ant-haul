You are the **resolvers-agent** for a forge build (iteration 4, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/enrichment/resolvers/__init__.py`
- `seed_storage/enrichment/resolvers/base.py`
- `seed_storage/enrichment/resolvers/webpage.py`
- `seed_storage/enrichment/resolvers/youtube.py`
- `seed_storage/enrichment/resolvers/image.py`
- `seed_storage/enrichment/resolvers/pdf.py`
- `seed_storage/enrichment/resolvers/github.py`
- `seed_storage/enrichment/resolvers/video.py`
- `seed_storage/enrichment/resolvers/twitter.py`
- `seed_storage/enrichment/resolvers/fallback.py`
- `seed_storage/enrichment/dispatcher.py`

### Test files you OWN:

- `tests/unit/resolvers/__init__.py`
- `tests/unit/resolvers/test_webpage.py`
- `tests/unit/resolvers/test_youtube.py`
- `tests/unit/resolvers/test_image.py`
- `tests/unit/resolvers/test_pdf.py`
- `tests/unit/resolvers/test_github.py`
- `tests/unit/resolvers/test_video.py`
- `tests/unit/resolvers/test_twitter.py`
- `tests/unit/resolvers/test_fallback.py`
- `tests/unit/test_dispatcher.py`

### Expected test count: ~58

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Async/sync boundary

- Resolvers: `async def resolve()` (non-blocking HTTP via httpx).
- Celery tasks: synchronous. Bridge with `asyncio.run()` per-task invocation.
- Do NOT use `--pool=gevent`. Keep default prefork pool.
- `send_alert()`: synchronous `httpx.Client` — no `asyncio.run()`.

---

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: resolvers-agent iteration 4"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Constraints from previous iteration

- FIX DEPLOY: Deployed but health endpoint never responded
- FIX INTEGRATION: tests/integration/test_celery_tasks.py::test_enrich_end_to_end FAILED    [  3%]
- FIX INTEGRATION: ERROR    seed_storage.worker.tasks:tasks.py:262 enrich_message: failed source_id=msg-7c47ebe7: a coroutine was expected, got <function _async_return.<locals>._inner at 0x77f71ea0e660>
- FIX INTEGRATION: FAILED tests/integration/test_celery_tasks.py::test_enrich_end_to_end - Asser...

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
