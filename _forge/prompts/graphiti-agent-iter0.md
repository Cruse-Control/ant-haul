You are the **graphiti-agent** for a forge build (iteration 0, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `seed_storage/graphiti_client.py`
- `seed_storage/query/__init__.py`
- `seed_storage/query/search.py`
- `scripts/query.py`

### Test files you OWN:

- `tests/unit/test_graphiti_client.py`
- `tests/unit/test_query.py`

### Expected test count: ~14

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Contract 12: Graphiti client interface

```python
def get_graphiti() -> Graphiti:
    """Singleton. Calls build_indices_and_constraints() on first init.
    Provider branching: openai→OpenAIClient, anthropic→AnthropicClient, groq→GroqClient.
    Embedder: always OpenAIEmbedder (requires OPENAI_API_KEY regardless of LLM_PROVIDER)."""
    ...

def get_vision_client():
    """Returns SDK client for VISION_PROVIDER (defaults to LLM_PROVIDER).
    Used by image resolver. Separate from Graphiti LLM client."""
    ...
```

All `add_episode()` calls MUST use `group_id="seed-storage"`. Never per-channel.

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: graphiti-agent iteration 0"`

## What to implement

Implement every function, class, and constant from the spec excerpt above. Write tests that cover the expected behaviors. If the spec excerpt is insufficient, read the full spec at `docs/PARALLEL-SPEC-v2.md` — but only the sections relevant to your agent.

For unit tests:
- Mock all external dependencies (Redis, Neo4j, HTTP, Discord, etc.)
- No real infrastructure required
- Test edge cases: empty input, error paths, boundary conditions

## Done

When all your tests pass and all files are created, commit and stop. Do not implement files owned by other agents.
