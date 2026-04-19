# Seed Storage — Agent Instructions

## What this project is

A Discord-first knowledge graph for CruseControl. Ingests Discord messages and linked content into Neo4j via Graphiti for entity extraction and semantic search. Runs as an ant-keeper daemon (K8s pod, `ant-keeper` namespace).

## DEPLOY PREREQUISITES (read before registering the daemon)

**Env-mode credentials require `proxy_target` before daemon registration.** `openai` and `github-pat` are env-mode, proxy-enabled credentials. They **must** have a `proxy_target` URL configured in ant-keeper before the daemon is registered. Registering the daemon first causes an immediate deploy failure:

```
Credential 'openai' missing proxy_target
```

Run these commands **before** `POST /api/tasks` (daemon registration):

```bash
./infra/scripts/proxy-enable.sh openai https://api.openai.com
./infra/scripts/proxy-enable.sh github-pat https://api.github.com  # if using github-pat
```

File-mode credentials (`discord-bot-seed-storage`, `neo4j-seed-storage`, `discord-alerts-webhook`) do NOT need proxy targets.

## Architecture decisions

### Graph: Graphiti, not raw Cypher

All graph writes go through `graphiti.add_episode()`. **Never bypass Graphiti with direct Cypher.** Graphiti's entity resolution is the core value — it merges entities across episodes and builds the `RELATES_TO`/`MENTIONS` edge graph. Bypassing it produces a graph without cross-episode entity linking.

`group_id` is always `"seed-storage"`. Never per-channel — the entire knowledge base is one unified graph.

Schema is managed by `build_indices_and_constraints()` called on startup (idempotent). No migration chain.

### Credentials: file-mode via ant-keeper

Sensitive credentials (`DISCORD_BOT_TOKEN`, `NEO4J_PASSWORD`, `DISCORD_ALERTS_WEBHOOK_URL`) are stored in ant-keeper and injected as file paths (`*_PATH` env vars). `config.py` reads the file at startup. **Never hardcode credentials. Never bypass iron-proxy.**

### Redis: DB 2

All seed-storage Redis keys are on **DB 2** (`redis://redis.ant-keeper.svc:6379/2`). Ant-keeper uses DB 0. Without the `/2` suffix, keys collide with ant-keeper's task state.

Key namespaces:
- `seed:seen_messages` — message dedup SET
- `seed:seen_urls` — URL dedup SET (SHA256 hashes)
- `seed:ingested_content` — graph-ingested URL SET
- `seed:frontier` — expansion frontier ZSET (score = priority)
- `seed:frontier:meta:{hash}` — frontier metadata HASH
- `seed:dead_letters` — failed task LIST
- `seed:circuit:{service}:*` — circuit breaker state
- `seed:cost:daily:YYYY-MM-DD` — daily LLM spend counter
- `seed:ratelimit:graphiti` — rate limiter ZSET
- `seed:reactions` — Discord reaction pubsub channel
- `seed:bot:connected` — bot liveness flag

### Async/sync boundary

- **Resolvers:** `async def resolve()` — non-blocking HTTP via httpx
- **Celery tasks:** synchronous — bridge with `asyncio.run()` per-task invocation
- **`send_alert()`:** synchronous `httpx.Client` — never use `asyncio.run()`
- **Worker pool:** default prefork. Never `--pool=gevent`

**Mocking async functions in integration tests:** When a Celery task calls `asyncio.run(some_async_fn(...))`, the mock must be an `AsyncMock` so calling it returns a coroutine. Use:

```python
from unittest.mock import AsyncMock, patch

with patch("seed_storage.worker.tasks._resolve_urls",
           new=AsyncMock(return_value=[resolved_rc])):
    ...
```

Do NOT use `MagicMock` or a plain function for async targets — `asyncio.run()` will raise `ValueError: a coroutine was expected` because `asyncio.iscoroutine()` returns False for non-coroutines.

### Embeddings: always OpenAI

`OPENAI_API_KEY` is required regardless of `LLM_PROVIDER`. Graphiti uses `OpenAIEmbedder` for all embeddings. The `LLM_PROVIDER` setting controls the entity-extraction LLM only.

## Key paths

| Path | Purpose |
|------|---------|
| `seed_storage/config.py` | All configuration (pydantic-settings `Settings` singleton) |
| `seed_storage/worker/tasks.py` | All 4 Celery tasks |
| `seed_storage/worker/app.py` | Celery app + queue routing + beat schedule |
| `seed_storage/enrichment/dispatcher.py` | Routes URLs to resolvers |
| `seed_storage/enrichment/models.py` | Shared types: `ResolvedContent`, `ContentType` |
| `seed_storage/graphiti_client.py` | Graphiti singleton + vision client |
| `seed_storage/ingestion/bot.py` | Discord bot real-time ingestion |
| `seed_storage/ingestion/batch.py` | DiscordChatExporter JSON batch import |
| `seed_storage/health.py` | Health endpoint on :8080 |
| `seed_storage/expansion/frontier.py` | Redis frontier operations |
| `seed_storage/dedup.py` | Dedup store + URL canonicalization |
| `scripts/query.py` | CLI query interface |
| `scripts/rollback.py` | Graph rollback by timestamp |
| `infra/k8s/neo4j.yaml` | Neo4j K8s StatefulSet |
| `infra/scripts/proxy-enable.sh` | Configure proxy_target for env-mode credentials |

## Resolver quirks

**Twitter/X:** Stub only. Returns `error_result()`. Real extraction is out of scope for Phase A.

**YouTube:** Uses yt-dlp for metadata and transcript. Manual captions preferred over auto-generated. Falls back to Whisper transcription if no captions available.

**Video:** Downloads to temp file → ffmpeg → Whisper. Temp file cleaned up in `finally` block regardless of outcome.

**PDF:** docling primary, unstructured fallback. Both are heavy imports; test with mocks.

**Image:** Calls vision LLM (configured via `VISION_PROVIDER`). Returns description in `summary` field and copies it to `text`.

**Webpage:** trafilatura primary, readability-lxml fallback. Both-fail returns `error_result()`.

**Dispatcher priority order (highest to lowest):** Twitter → YouTube → GitHub → Image → PDF → Video → Webpage → Fallback

The dispatcher catches all resolver exceptions and converts them to `error_result()`. Resolvers themselves should also catch internally — the dispatcher catch is a last resort.

## Celery configuration

**Two queues:**
- `raw_messages` — `enrich_message` tasks (concurrency: `WORKER_CONCURRENCY_RAW`, default 2)
- `graph_ingest` — `ingest_episode`, `expand_from_frontier`, `scan_frontier` tasks (concurrency: `WORKER_CONCURRENCY_GRAPH`, default 4)

**Retry policy:**
- `enrich_message`: 3 retries, 60s delay
- `ingest_episode`: 5 retries, 30s delay
- `expand_from_frontier`: 3 retries, 60s delay
- `scan_frontier`: 1 retry, 10s delay

All tasks use `acks_late=True` and `reject_on_worker_lost=True` to prevent message loss.

**Beat:** `scan_frontier` runs every 60s. No-op when `FRONTIER_AUTO_ENABLED=false`.

## Ingestion contracts

### Contract 1 — `raw_payload` shape

```python
{
    "source_type": str,       # "discord", "expansion", ...
    "source_id": str,         # Discord snowflake or frontier hash
    "source_channel": str,    # channel name
    "author": str,            # display name
    "content": str,           # raw text including URLs
    "timestamp": str,         # ISO 8601
    "attachments": list[str], # direct URLs
    "metadata": dict,         # source-specific
}
```

Skip conditions: `content` empty AND `attachments` empty → skip. Bot author → skip.

### Contract 2 — `enriched_payload` shape

```python
{
    "message": raw_payload,
    "resolved_contents": [rc.to_dict(), ...],
}
```

### source_description format (on Episodic nodes)

- Message episodes: `"{source_type.title()} #{source_channel}"` → `"Discord #general"`
- Content episodes: `"content_from_{source_type.title()}_{source_channel}:{content_type}"` → `"content_from_Discord_general:youtube"`

Note: content episodes use `_` not `#` before channel name. The `#` format is message-only. This is intentional and differs from the parallel spec — the code is correct.

## Expansion frontier

Priority score components (see `expansion/policies.py`):
1. Base: 0.5
2. Depth penalty: −0.15 per hop
3. Resolver bonus: +0.2 for youtube/github, −1.0 for twitter
4. Domain bonus: +0.3 for arxiv.org, +0.2 for github.com/youtube.com
5. Floor at 0.0

ZADD uses NX — existing entries keep their priority (not overwritten).

**Depth policies** (max hops by resolver type):
- youtube, github: 3
- pdf, image, video, unknown: 2
- webpage: 4
- twitter/tweet: 0 (never expand)
- fallback: 1

## Dead letters

Stored as JSON in `seed:dead_letters` LIST (RPUSH). Each entry includes `task_name`, `payload`, `traceback` (sanitized — credentials masked), `retries`, `timestamp`.

Replay via `python -m seed_storage.worker.replay`.

## Known limitations

- **Twitter/X:** Stub only — returns error for all twitter.com and x.com URLs.
- **Frontier auto-expansion:** Disabled by default (`FRONTIER_AUTO_ENABLED=false`). Enable only after verifying budget limits.
- **No API server:** CLI-only (`scripts/query.py`). Web UI and MCP server are Phase B.
- **No v1 migration:** Clean-room build. Existing v1 Neo4j data is not migrated.
- **Single graph:** All sources share `group_id="seed-storage"`. No per-channel graph isolation.
- **Reactions require bot + Redis pubsub:** If the bot is disconnected, reaction events published to `seed:reactions` are dropped silently.
- **Ruff baseline:** Run `ruff check . && ruff format .` early when modifying code.
- **`asyncio.run()` inside Celery tasks:** Each task that calls async code uses `asyncio.run()` — this creates a new event loop per invocation. This is intentional (prefork workers). Integration tests must use `AsyncMock` for any async function patched inside these tasks (see "Mocking async functions" note above).
- **Health endpoint startup:** The `health` supervisord process starts immediately; if Redis or Neo4j are unavailable at startup, health checks will return `"error"` until they come up. The health process itself does not crash — it serves 503 until dependencies are healthy.
- **Env-mode credentials require proxy_target before deploy:** `openai` and `github-pat` are env-mode, proxy-enabled credentials. They **must** have a `proxy_target` URL configured in ant-keeper (via `./infra/scripts/proxy-enable.sh`) before the daemon is registered. Registering the daemon first causes an immediate deploy failure: `Credential 'openai' missing proxy_target`. File-mode credentials (`discord-bot-seed-storage`, `neo4j-seed-storage`, `discord-alerts-webhook`) are unaffected.
- **Neo4j `execute_query` parameter syntax:** The Neo4j Python driver v5 `execute_query` method does NOT accept a `params=` keyword argument. Use `parameters_={"key": value}` (note the trailing underscore) or pass parameters directly as `**kwargs` (e.g., `gid=test_group_id`). Using `params={"gid": value}` silently passes a query parameter named `params` — the `$gid` reference in Cypher is left unbound and evaluates to `null`, causing `WHERE n.group_id = $gid` to match nothing and producing `AssertionError: assert 0 > 0` in integration tests. This is the root cause of `test_add_episode_creates_nodes` failures. Correct usage:
  ```python
  # CORRECT — explicit parameters_ dict
  result, _, _ = await g.driver.execute_query(
      "MATCH (n) WHERE n.group_id = $gid RETURN count(n) AS cnt",
      parameters_={"gid": group_id},
  )
  # CORRECT — kwargs directly
  result, _, _ = await g.driver.execute_query(
      "MATCH (n) WHERE n.group_id = $gid RETURN count(n) AS cnt",
      gid=group_id,
  )
  # WRONG — params kwarg is not unpacked; $gid stays unbound
  result, _, _ = await g.driver.execute_query(
      "MATCH (n) WHERE n.group_id = $gid RETURN count(n) AS cnt",
      params={"gid": group_id},  # ← $gid will be null
  )
  ```
- **Single asyncio.run() per task invocation:** `ingest_episode` calls `asyncio.run(_write_all_episodes(...))` once, writing all episodes (message + content) in a single event loop. Do NOT call `asyncio.run()` multiple times within one task — the Graphiti singleton holds Neo4j driver connections bound to an event loop. Calling `asyncio.run()` a second time creates a new loop, invalidating the driver's connections and raising `Task pending in closed loop`.
- **`source_description` format discrepancy with spec:** The parallel spec (Section 4) describes content episode `source_description` as `"content_from_{source_type.title()}_#{source_channel}:{content_type}"` (with `#`). The actual code and all tests use `"content_from_{source_type.title()}_{source_channel}:{content_type}"` (without `#`). The code is authoritative. Do not introduce `#` in content episode `source_description`.
- **Graphiti `execute_query` "got multiple values" error:** If you see `neo4j._async.driver.AsyncDriver.execute_query() got multiple values for keyword argument 'parameters_'` logged from `graphiti_core.driver.neo4j_driver`, this is a graphiti-core internal issue — distinct from the `params=` silent-null trap above. It means graphiti is passing both a positional/keyword `parameters_` dict AND additional `**kwargs` in the same call. This can surface on certain graphiti-core versions when internal query methods mix the two calling conventions. In your own direct calls to `g.driver.execute_query()`, **never mix the two patterns**: use EITHER `parameters_={"gid": value}` OR `gid=value` as a bare kwarg — never both in the same call. If you see this error originating from inside graphiti itself (not from test code), it indicates a graphiti-core version incompatibility with the installed Neo4j Python driver; pin `graphiti-core` to the last known-good version.

## Adding a new resolver

See `docs/resolvers.md` for the full step-by-step guide.

## Running tests

```bash
# Unit tests (no infrastructure required)
uv run pytest tests/unit/ -v

# Integration tests (requires docker compose up)
docker compose -p seed-storage-dev up -d
uv run pytest tests/integration/ -m integration -v

# E2E tests (requires full stack)
uv run pytest tests/e2e/ -v

# Security tests
uv run pytest tests/security/ -v
```

Expected counts: ~390 unit, ~27 integration, ~6 e2e, ~9 security.
