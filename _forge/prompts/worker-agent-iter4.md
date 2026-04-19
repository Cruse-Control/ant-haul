You are the **worker-agent** for a forge build (iteration 4, tier 1).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/worker/app.py`
- `seed_storage/worker/tasks.py`

### Test files you OWN:

- `tests/unit/tasks/__init__.py`
- `tests/unit/tasks/test_enrich_message.py`
- `tests/unit/tasks/test_ingest_episode.py`
- `tests/unit/tasks/test_expand_frontier.py`
- `tests/unit/tasks/test_scan_frontier.py`

### Expected test count: ~35

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

# Seed Storage — Parallel Implementation Spec

> Derived from [seed-storage-spec-v2.md](seed-storage-spec-v2.md) using the [Parallel Agent Implementation Guide](personas/parallel-impl-guide-merged-created-2026-04-12.md).
> Created: 2026-04-14.
> Source spec: seed-storage-spec-v2.md (v2 revision 7, 2026-04-12).
> Goal: decompose the seed-storage replacement build into parallel agent work — preventing drift, incompatible code, and coordination failures.

---

## Section 1: Module Decomposition

Every module listed here has a one-sentence responsibility, typed Python interfaces, and explicit file ownership. Agents reading only this document should implement their module without reading any other agent's source.

### Shared Types — `seed_storage/enrichment/models.py`

**Responsibility:** Canonical location for all shared data types used across modules.

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

ContentType = Literal["webpage", "youtube", "video", "image", "pdf", "github", "tweet", "unknown"]

@dataclass
class ResolvedContent:
    source_url: str
    content_type: ContentType
    title: str | None
    text: str                       # clean extracted text; empty string on failure
    transcript: str | None          # for video/audio content
    summary: str | None             # populated by vision LLM for images
    expansion_urls: list[str]       # secondary URLs found within this content
    metadata: dict[str, Any]        # source-specific extras
    extraction_error: str | None    # None on success, error message on failure
    resolved_at: datetime           # UTC, set by dispatcher after resolution completes

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict. datetime → ISO 8601 string."""
        ...

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResolvedContent":
        """Deserialize from dict. Ignores unknown keys (forward compatibility)."""
        ...

    @classmethod
    def error_result(cls, url: str, error: str) -> "ResolvedContent":
        """Factory for failed resolutions. text='', extraction_error=error, resolved_at=utcnow()."""
        ...
```

**URL canonicalization** lives in `seed_storage/dedup.py` as `canonicalize_url(url: str) -> str`. Used for URL dedup key generation.

```python
def canonicalize_url(url: str) -> str:
    """Normalize URL for dedup. Strips utm_*, fbclid, ref, si, t, s params.
    Lowercases scheme+host. Preserves path case. Sorts remaining query params.
    Removes trailing slash and fragment. Returns original on malformed input."""
    ...

def url_hash(url: str) -> str:
    """SHA256 hex digest of canonicalize_url(url)."""
    ...
```

### Module List

| Module | Responsibility | Key Interface |
|--------|---------------|---------------|
| `enrichment/models.py` | Shared types: `ResolvedContent`, `ContentType` | Dataclass with `to_dict()`, `from_dict()`, `error_result()` |
| `config.py`

... (could not match sections, showing first 3000 chars)

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: worker-agent iteration 4"`

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
