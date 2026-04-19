You are the **integration-test-agent** for a forge build (iteration 5, tier 2).

## Your assignment

### Files you OWN (create or modify these):

- `tests/__init__.py`
- `tests/conftest.py`
- `tests/unit/__init__.py`
- `tests/unit/resolvers/__init__.py`
- `tests/unit/tasks/__init__.py`
- `tests/integration/__init__.py`
- `tests/integration/conftest.py`
- `tests/e2e/__init__.py`
- `tests/e2e/conftest.py`
- `tests/security/__init__.py`

### Test files you OWN:

- `tests/integration/test_dedup_redis.py`
- `tests/integration/test_circuit_breaker_redis.py`
- `tests/integration/test_graphiti.py`
- `tests/integration/test_celery_tasks.py`
- `tests/e2e/test_message_to_graph.py`
- `tests/security/test_injection.py`
- `tests/security/test_credential_isolation.py`

### Expected test count: ~40

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Test hierarchy — 4 levels

| Level | Dir | Infrastructure | Gate |
|-------|-----|---------------|------|
| Unit | `tests/unit/` | None — zero external dependencies | Tier 0 merge gate |
| Integration | `tests/integration/` | Real Redis + Neo4j (docker-compose) | Tier 1 merge gate |
| E2E | `tests/e2e/` | Full stack (all processes) | Tier 2 gate |
| Security | `tests/security/` | Mixed (some need infra) | Tier 2 gate |

---

### Integration tests (~69 total) — integration-test-agent (Tier 2)

Require real Redis + Neo4j. Marker: `pytest.mark.integration`.

- `test_dedup_redis.py` (~6) — real SADD/SISMEMBER, concurrent access, atomicity, large set, persistence, isolation
- `test_circuit_breaker_redis.py` (~5) — cross-worker state, concurrent failures, cooldown timing, reconnect recovery, KEYS listing
- `test_cost_tracking_redis.py` (~4) — concurrent workers, TTL, parseable float, midnight boundary
- `test_rate_limiting_redis.py` (~4) — real timing, concurrent requests, window expiry, accuracy
- `test_frontier_redis.py` (~6) — ZADD NX, ZRANGEBYSCORE, metadata hash, cleanup, large frontier, score update
- `test_content_cache_redis.py` (~4) — SET+TTL, round-trip, expired → None, miss → None
- `test_reaction_pubsub.py` (~3) — publish→receive, disconnected → dropped, multiple subscribers
- `test_graphiti.py` (~8) — add_episode creates nodes, entity merging (3 episodes → 1 Entity), MENTIONS edges, idempotency, source_description persisted, group_id scoping, build_indices idempotent, search returns results
- `test_celery_tasks.py` (~8) — enrich end-to-end, ingest writes to Neo4j, retry on transient error, dead-letter after max, reject_on_worker_lost, expand task, beat fires, queue routing
- `test_enrichment_pipeline.py` (~6) — full dispatch, multiple URLs, mixed success/failure, cache hit, cache populated, truncation
- `test_notifications_integration.py` (~4) — real POST to mock server, debounce in Redis, debounce expired, connection refused
- `test_dead_letters_redis.py` (~4) — RPUSH+LLEN+LPOP FIFO, concurrent, LRANGE listing, replay round-trip
- `test_health_endpoint.py` (~4) — real HTTP 200, queue depth reflects actual, cost reflects actual, circuit breaker reflects actual
- `test_config_loading.py` (~3) — real env vars, real file credential, .env fallback

---

### E2E tests (~38 total) — integration-test-agent (Tier 2)

Full stack. All clean up after themselves (test-specific `source_description` prefix + yield teardown).

- `test_message_to_graph.py` (~6) — YouTube, GitHub, image, PDF, multi-URL, plain text
- `test_batch_import.py` (~4) — fixture file, --offset, 5000 cap, mixed types
- `test_query.py` (~3) — search→results, no matches→empty, source_description filtering
- `test_dedup.py` (~4) — same message twice, same URL in two messages, bot+batch overlap, canonical URL matching
- `test_graceful_degradation.py` (~3) — dead URL, all URLs fail, resolver timeout
- `test_source_tracking.py` (~3) — multi-channel source_description, Cypher filter, cross-channel entity merge
- `test_reactions.py` (~3) — pubsub event order, platform emoji, dedup emoji
- `test_frontier_expansion.py` (~4) — expansion_urls appear, auto-scanner processes, manual expansion, depth limit
- `test_circuit_breaker_e2e.py` (~3) — trip→skip→alert, recover→alert, open→error_result+message still ingests
- `test_cost_ceiling.py` (~3) — budget exceeded→pause, retry after delay, 80% warning
- `test_pipeline_restart.py` (~2) — restart→tasks re-queued, dedup survives

---

### Security tests (~20 total) — integration-test-agent (Tier 2)

- `test_injection.py` (~5) — SQL, XSS, SSTI, oversized payload, unicode edge cases
- `test_credential_isolation.py` (~4) — no keys in startup logs, no keys in task logs, masking format, bot token absent
- `test_dedup_key_isolation.py` (~3) — separate SETs, no message↔URL collision, no URL↔ingested collision
- `test_egress_boundary.py` (~3) — allowlisted domain succeeds, non-allowlisted blocked, internal services accessible
- `test_input_validation.py` (~5) — missing source_type, wrong timestamp type, null content, non-URL attachments, deep metadata

---

### Verify before Tier 2 (integration tests)

| Precondition | Verify command | Expected |
|-------------|---------------|----------|
| docker-compose Redis + Neo4j running | `docker compose -p seed-storage-dev ps` | Both running |
| Neo4j accessible | `curl -s http://localhost:7474` | 200 |
| Redis accessible | `redis-cli PING` | `PONG` |

## Rules

1. **Implement exactly what the spec says.** The test hierarchy, markers, and infrastructure requirements are defined in the spec.
2. **Only create files in your assignment.** Do not modify implementation code — only test files and conftest.py.
3. **Write all files FIRST, then run tests.** Start writing code immediately based on the spec excerpt above.
4. **Commit your work** when done: `git add -A && git commit -m "forge: integration-test-agent iteration 5"`

## CRITICAL: Test infrastructure requirements

The spec defines 4 test levels with different infrastructure requirements:

### Unit tests (`tests/unit/`) — mock everything
- Already written by Tier-0 agents. You create conftest.py and __init__.py files only.

### Integration tests (`tests/integration/`) — REAL Redis + Neo4j
- **These tests MUST connect to real Redis and Neo4j.** Do NOT mock Redis or Neo4j in integration tests.
- Use env vars for connection: `os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/2")`, `os.environ.get("NEO4J_URI", "bolt://127.0.0.1:7687")`
- Mark all integration tests with `@pytest.mark.integration`
- Use `conftest.py` fixtures that create real Redis/Neo4j connections and clean up after each test
- Integration test fixtures should flush test keys on teardown (use a test-specific key prefix like `test:`)
- For Neo4j: create a test-specific constraint prefix or clean up created nodes in teardown

### E2E tests (`tests/e2e/`) — REAL full stack
- **These tests MUST exercise the actual pipeline end-to-end.** Do NOT mock anything.
- Use real Redis, real Neo4j, real Celery task execution (use `task.apply()` for synchronous in-process execution instead of `.delay()`)
- Each test should: create input → process through pipeline → verify output exists in Neo4j/Redis
- Mark with `@pytest.mark.e2e`
- Clean up all created data in teardown (use test-specific `source_description` prefix)

### Security tests (`tests/security/`) — mixed
- Some need real infra (credential isolation checks), some are pure logic (injection tests)
- Injection tests can mock the pipeline but must verify the actual sanitization code paths
- Credential tests must verify real log output format

## conftest.py fixtures

Create `tests/integration/conftest.py` with:
```python
import os
import pytest
import redis

@pytest.fixture
def redis_client():
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/2")
    client = redis.from_url(url)
    yield client
    # Clean up test keys
    for key in client.keys("test:*"):
        client.delete(key)
    client.close()
```

Similar pattern for Neo4j driver fixture.

Create `tests/e2e/conftest.py` with fixtures that set up the full pipeline (Redis + Neo4j + Celery app) and tear down test data.

## Done

When all files are created, commit and stop. Integration and E2E tests will be run by the coordinator against real infrastructure — they are expected to fail if mocked.
