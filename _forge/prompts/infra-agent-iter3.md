You are the **infra-agent** for a forge build (iteration 3, tier 0).

## Your assignment

### Files you OWN (create or modify these):

- `Dockerfile`
- `supervisord.conf`
- `manifest.json`
- `docker-compose.yml`
- `infra/k8s/neo4j.yaml`
- `.gitignore`

### Test files you OWN:



### Expected test count: ~0

## Spec (relevant sections extracted from docs/PARALLEL-SPEC-v2.md)

### Additional files (no Python logic)

| File | Purpose |
|------|---------|
| `Dockerfile` | Python 3.12 + ffmpeg + supervisord + whisper model pre-download |
| `supervisord.conf` | 5 processes: bot + worker-raw + worker-graph + beat + health |
| `manifest.json` | Ant-keeper daemon task manifest |
| `docker-compose.yml` | Local dev: Redis + Neo4j |
| `infra/k8s/neo4j.yaml` | Neo4j StatefulSet + Service for ant-keeper namespace |
| `pyproject.toml` | Dependencies + dev extras + pytest config |
| `.env.example` | All config vars with descriptions and defaults |
| `.gitignore` | Standard Python + .env |

---

---

### Dockerfile

```dockerfile
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .
RUN python -c "import whisper; whisper.load_model('base')"

COPY seed_storage/ seed_storage/
COPY scripts/ scripts/
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8080

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
```

---

### supervisord.conf

5 processes — bot + worker-raw + worker-graph + beat + health. All log to stdout/stderr. Concurrency read from env vars: `%(ENV_WORKER_CONCURRENCY_RAW)s`, `%(ENV_WORKER_CONCURRENCY_GRAPH)s`.

---

### manifest.json (ant-keeper daemon)

Task ID: `seed-storage`. Type: `daemon`. Owner: `wyler-zahm`. Health check: `:8080/health`.

Credentials:
- `openai` → `OPENAI_API_KEY` (env-mode, proxy-enabled)
- `discord-bot-seed-storage` → `DISCORD_BOT_TOKEN_PATH` (file-mode)
- `neo4j-seed-storage` → `NEO4J_PASSWORD_PATH` (file-mode)
- `github-pat` → `GITHUB_TOKEN` (env-mode, optional)
- `discord-alerts-webhook` → `DISCORD_ALERTS_WEBHOOK_PATH` (file-mode)

Resources: 1 CPU request / 2 CPU limit, 3Gi memory request / 6Gi limit.

---

### docker-compose.yml (local dev only)

Redis 7 (port 6379, appendonly) + Neo4j 5 (ports 7474, 7687, auth neo4j/localdev, APOC plugin).

## Rules

1. **Implement exactly what the spec says.** Every interface, type, and contract above is authoritative. Do not invent new interfaces.
2. **Only create files in your assignment.** Do not create or modify files owned by other agents.
3. **Import shared types from their canonical locations** (e.g., `from seed_storage.enrichment.models import ResolvedContent`). If the module doesn't exist yet, use a minimal inline stub with a comment `# STUB: provided by {other-agent}`.
4. **Write all files FIRST, then run tests.** Do not read the spec for 20 turns — start writing code immediately based on the spec excerpt above.
5. **Run your tests before finishing.** Execute `uv run pytest {your test files} -v --tb=short` and fix any failures.
6. **Commit your work** when done: `git add -A && git commit -m "forge: infra-agent iteration 3"`

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
