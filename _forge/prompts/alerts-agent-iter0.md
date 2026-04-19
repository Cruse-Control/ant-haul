You are the **alerts-agent** for a forge build (iteration 0, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/notifications.py`
- `seed_storage/worker/__init__.py`
- `seed_storage/worker/dead_letters.py`
- `seed_storage/worker/replay.py`

### Test files you OWN:

- `tests/unit/test_notifications.py`
- `tests/unit/test_dead_letters.py`

### Expected test count: ~16

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Contract 10: Notifications interface

```python
def send_alert(message: str, debounce_key: str | None = None) -> None:
    """Fire-and-forget Discord webhook alert. Sync httpx.Client. Never raises.
    Empty DISCORD_ALERTS_WEBHOOK_URL → silently skipped (alerts disabled)."""
    ...
```

---

### Contract 11: Dead letter interface

```python
def dead_letter(task_name: str, payload: dict, exc: Exception, retries: int) -> None:
    """RPUSH to seed:dead_letters. Sanitize traceback (strip credential paths, mask API keys)."""
    ...

def list_dead_letters(redis_client) -> tuple[int, list[dict]]:
    """LRANGE — count + preview without consuming."""
    ...

def replay_one(redis_client) -> tuple[str, dict] | None:
    """LPOP oldest entry. Returns (task_name, payload) or None."""
    ...

def replay_all(redis_client) -> list[tuple[str, dict]]:
    """Pop all entries. Returns list of (task_name, payload)."""
    ...
```

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: alerts-agent iteration 0"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
