# Seed Storage

A Discord-first knowledge graph for CruseControl. Ingests Discord messages, URLs (webpages, YouTube, GitHub, PDFs, images, video), and DiscordChatExporter JSON exports into a Neo4j graph via [Graphiti](https://github.com/getzep/graphiti) for entity extraction and semantic search.

## Architecture overview

```
Discord bot  ──┐
Batch import ──┼──► raw_messages queue ──► enrich_message task
               │                              │
               │                    URL extraction + dedup
               │                    ContentDispatcher (8 resolvers)
               │                              │
               │                    graph_ingest queue ──► ingest_episode task
               │                              │
               │                        Graphiti add_episode()
               │                        Neo4j (bolt :30687)
               │
scan_frontier (beat, 60s) ──► expand_from_frontier task
                                    └── resolves frontier URLs ──► ingest_episode
```

**Components:**

| Component | Technology | Port |
|-----------|-----------|------|
| Celery workers | Prefork, 2 queues | — |
| Celery beat | Frontier scanner | — |
| Discord bot | discord.py | — |
| Health endpoint | aiohttp | 8080 |
| Redis (broker + dedup + state) | Redis DB 2 | 30679 (K8s NodePort) |
| Graph database | Neo4j 5 (K8s) | Bolt 30687, HTTP 30474 |

## Prerequisites

- Python 3.12+
- `uv` package manager
- Redis (K8s NodePort 30679 or local)
- Neo4j 5 (K8s StatefulSet or `docker compose`)

## Local development setup

```bash
# 1. Clone
git clone https://github.com/Cruse-Control/seed-storage.git
cd seed-storage

# 2. Install dependencies
uv sync

# 3. Start local Redis + Neo4j
docker compose -p seed-storage-dev up -d

# 4. Copy and fill in credentials
cp .env.example .env
$EDITOR .env

# 5. Start workers
uv run celery -A seed_storage.worker.app worker \
  -Q raw_messages --concurrency 2 --loglevel INFO &

uv run celery -A seed_storage.worker.app worker \
  -Q graph_ingest --concurrency 4 --loglevel INFO &

# 6. Start health endpoint
uv run python -m seed_storage.health &

# 7. Start Discord bot (requires DISCORD_BOT_TOKEN)
uv run python -m seed_storage.ingestion.bot
```

## Configuration

All configuration is via environment variables (or `.env` file). Copy `.env.example` and fill in the required fields.

**Required:**

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | Required for embeddings regardless of `LLM_PROVIDER` |
| `DISCORD_BOT_TOKEN` or `DISCORD_BOT_TOKEN_PATH` | Discord bot token |
| `DISCORD_CHANNEL_IDS` | Comma-separated channel snowflakes to monitor |
| `NEO4J_PASSWORD` or `NEO4J_PASSWORD_PATH` | Neo4j password |

**LLM provider** (default: `openai`):

```bash
LLM_PROVIDER=openai      # openai | anthropic | groq
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...    # always required for embeddings
```

**Optional:**

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis.ant-keeper.svc:6379/2` | Redis connection (DB 2) |
| `NEO4J_URI` | `bolt://neo4j.ant-keeper.svc:7687` | Neo4j Bolt URI |
| `DAILY_LLM_BUDGET` | `5.00` | Daily LLM spend cap (USD) |
| `FRONTIER_AUTO_ENABLED` | `false` | Auto-expand discovered URLs |
| `HARD_DEPTH_CEILING` | `5` | Max expansion hop depth |
| `MAX_EXPANSION_BREADTH` | `20` | Max child URLs per resolution |
| `GITHUB_TOKEN` | — | GitHub PAT for private repo access |
| `TRANSCRIPTION_BACKEND` | `whisper` | `whisper` or `assemblyai` |
| `DISCORD_ALERTS_WEBHOOK_URL` | — | Discord webhook for alerts |
| `VISION_PROVIDER` | (= `LLM_PROVIDER`) | LLM provider for image description |

## Batch import (DiscordChatExporter)

Export a channel using [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter) in JSON format, then:

```bash
# Import all messages
uv run python -m seed_storage.ingestion.batch channel-export.json

# Skip the first N messages (for resuming)
uv run python -m seed_storage.ingestion.batch channel-export.json --offset 1000
```

- Caps at 5,000 messages per run
- Bot-authored messages are automatically skipped
- Malformed entries are skipped with a warning; processing continues

## Querying the graph

```bash
# Text search
python scripts/query.py "CruseControl brand strategy"

# Increase result count
python scripts/query.py "content calendar" --limit 20

# JSON output (for scripting)
python scripts/query.py "sponsorship rates" --json
```

The query CLI uses Graphiti's hybrid search (vector + fulltext + temporal graph traversal).

## Content resolvers

The dispatcher routes URLs to the appropriate resolver:

| Resolver | Handles | Truncation |
|----------|---------|-----------|
| YouTube | youtube.com, youtu.be | 12,000 tokens |
| GitHub | github.com repos | README + metadata |
| Image | jpg/png/gif/webp/etc. | Vision LLM description |
| PDF | .pdf | 10,000 tokens |
| Video | mp4/mov/avi/etc. | ffmpeg + Whisper transcription |
| Twitter | twitter.com, x.com | Stub (returns error) |
| Webpage | generic HTTP(S) | 8,000 tokens (trafilatura + readability fallback) |
| Fallback | all HTTP(S) | Best-effort BeautifulSoup |

To add a new resolver, see `docs/resolvers.md`.

## Expansion frontier

When content is resolved, `expansion_urls` (links found within the content) are added to a Redis sorted set (`seed:frontier`). The beat task `scan_frontier` runs every 60 seconds and enqueues `expand_from_frontier` tasks for the highest-priority URLs.

**Enable auto-expansion:**
```bash
FRONTIER_AUTO_ENABLED=true
```

**Manual expansion:**
```bash
python -m seed_storage.expansion.cli expand https://example.com
```

## Cost management

Daily LLM spend is tracked in Redis (`seed:cost:daily:YYYY-MM-DD`). When the budget is exceeded, `graph_ingest` tasks pause and retry after 5 minutes. An alert is sent to the Discord webhook.

**Check current spend:**
```bash
redis-cli -n 2 GET seed:cost:daily:$(date +%Y-%m-%d)
```

**Default budget:** $5.00/day (configurable via `DAILY_LLM_BUDGET`).

## Dead letters

Tasks that exhaust all retries are stored in `seed:dead_letters` for manual inspection and replay.

```bash
# List dead letters
python -m seed_storage.worker.replay --list

# Replay oldest entry
python -m seed_storage.worker.replay --one

# Replay all entries
python -m seed_storage.worker.replay --all
```

## Health check

```bash
curl http://localhost:8080/health
```

Returns JSON with Redis, Neo4j, Celery, and bot status plus operational metrics (queue depths, frontier size, dead letter count, daily cost, open circuit breakers).

## Deployment (ant-keeper)

> **IMPORTANT:** Steps 4 and 5 must happen in this exact order. Registering the daemon (step 5) before enabling proxy targets (step 4) causes an immediate deploy failure: `Credential 'openai' missing proxy_target`.

1. Apply Neo4j K8s manifest and wait for ready:
   ```bash
   kubectl apply -f infra/k8s/neo4j.yaml
   kubectl wait --for=condition=Ready pod -l app=neo4j -n ant-keeper --timeout=120s
   ```

2. Change Neo4j default password and store as file-mode credential in ant-keeper.

3. Store all credentials in ant-keeper:
   - `openai` → `OPENAI_API_KEY`
   - `discord-bot-seed-storage` → `DISCORD_BOT_TOKEN_PATH`
   - `neo4j-seed-storage` → `NEO4J_PASSWORD_PATH`
   - `github-pat` → `GITHUB_TOKEN` (optional)
   - `discord-alerts-webhook` → `DISCORD_ALERTS_WEBHOOK_PATH` (optional)

4. **Enable proxy targets for env-mode credentials (required before daemon registration):**
   ```bash
   ./infra/scripts/proxy-enable.sh openai https://api.openai.com
   ./infra/scripts/proxy-enable.sh github-pat https://api.github.com  # if using GitHub PAT
   ```
   Env-mode credentials (`openai`, `github-pat`) route outbound HTTP through iron-proxy and **must** have a `proxy_target` set before daemon registration. File-mode credentials (`discord-bot-seed-storage`, `neo4j-seed-storage`, `discord-alerts-webhook`) do not need proxy targets.

5. Register the daemon:
   ```bash
   curl -X POST http://127.0.0.1:7070/api/tasks \
     -H "Authorization: Bearer $ANT_KEEPER_TOKEN" \
     -H "Content-Type: application/json" \
     -d @manifest.json
   ```

6. Verify deployment:
   ```bash
   curl http://localhost:8080/health
   uv run python -m seed_storage.smoke_test
   ```

## Rollback

```bash
# Disable daemon
curl -X POST http://127.0.0.1:7070/api/tasks/seed-storage/disable \
  -H "Authorization: Bearer $ANT_KEEPER_TOKEN"

# Preview episodes ingested after a timestamp (dry run)
python scripts/rollback.py --after 2026-04-01T00:00:00Z --dry-run

# Remove episodes ingested after a timestamp (optional)
python scripts/rollback.py --after 2026-04-01T00:00:00Z

# Remove without confirmation prompt
python scripts/rollback.py --after 2026-04-01T00:00:00Z --yes

# Scope rollback to a specific group_id (default: "seed-storage")
python scripts/rollback.py --after 2026-04-01T00:00:00Z --group-id seed-storage

# Flush dedup sets to allow re-ingestion (optional)
redis-cli -n 2 DEL seed:seen_messages seed:seen_urls seed:ingested_content
```

## Running tests

```bash
# Unit tests — no infrastructure required
uv run pytest tests/unit/ -v

# Integration tests — requires Redis + Neo4j (docker compose up first)
docker compose -p seed-storage-dev up -d
uv run pytest tests/integration/ -m integration -v

# E2E tests — requires full stack
uv run pytest tests/e2e/ -v

# Security tests
uv run pytest tests/security/ -v

# Lint
uv run ruff check . && uv run ruff format --check .
```

Expected counts: ~390 unit, ~27 integration, ~6 e2e, ~9 security.

## Troubleshooting

**Bot not connecting:** Check `DISCORD_BOT_TOKEN` and that `Message Content Intent` is enabled in the Discord Developer Portal.

**Graph ingest paused:** Check `redis-cli -n 2 GET seed:cost:daily:$(date +%Y-%m-%d)` — budget may be exceeded. Check open circuit breakers via `/health`.

**Dead letters accumulating:** Run `python -m seed_storage.worker.replay --list` and investigate. Common causes: Neo4j down, Graphiti API errors, rate limits.

**Frontier not expanding:** Verify `FRONTIER_AUTO_ENABLED=true` and that the beat worker is running. Check `redis-cli -n 2 ZCARD seed:frontier`.

**Deploy failure: `Credential 'openai' missing proxy_target`:** Run `./infra/scripts/proxy-enable.sh openai https://api.openai.com` before registering the daemon. See step 4 in the deployment section above.
