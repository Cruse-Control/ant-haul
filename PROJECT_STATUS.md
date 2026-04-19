# Seed Storage — Project Status

**Last updated**: 2026-04-04
**Branch**: `ch1-implementation` (merged to `main` at `50f1385`)
**Tests**: 107 passing (`uv run pytest tests/test_*.py`)

---

## What's Running

| Service | Type | Schedule | Status |
|---------|------|----------|--------|
| `ant-food-watcher` | daemon | real-time | 9 Discord channels, AntFarm#7792 |
| `seed-storage-processor` | script | */5 * * * * | Extracts content by medium |
| `seed-storage-enricher` | script | */5 * * * * | Haiku tags + summaries |
| `seed-storage-loader` | script | 06:00 daily | Neo4j via Graphiti, concurrency=3 |
| `seed-storage-digest` | script | 06:15 daily | Summary to #announcements |
| `instagram-video-analyzer` | daemon | always-on | NodePort 32207 |

## Knowledge Graph State

| Metric | Count |
|--------|-------|
| Items in Neo4j | 358 |
| Queued for loading | ~1,727 (loading ~200/day) |
| Content-deduped | 128 |
| Conversation threads | 265 fragments → ~31 threads |
| Discord channels | 9 |
| Dynamic tags | ~150 unique |

## Pipeline Architecture

```
Discord → [FILTER] → [CAPTURE] → [PROCESS] → [ENRICH] → [LOAD] → Neo4j
           noise      classify     extract     tags,       Graphiti
           reject     + stage      by medium   speakers    add_episode
           (free)     (free)       (some $)    ($0.001)    ($0.005)
```

Status flow: `staged → processed → enriched → loading → loaded`
Also: `threaded` (fragments grouped), `deduped` (content hash match), `failed`

## Infrastructure

- **Neo4j 5.26**: bolt:30687, http:30474 (K8s, ant-keeper namespace)
- **PostgreSQL**: port 30433 — `seed_staging`, `seed_costs`, `seed_tags` tables
- **Instagram Video Analyzer**: NodePort 32207, Gemini-powered, C0mput3rGuy auth
- **Credentials**: gemini (id:17), anthropic (id:18), discord-bot-ant-farm (id:11) — all shared

### Neo4j Access (for Wyler or any user on this machine)
```
Neo4j Browser: http://127.0.0.1:30474
Bolt URI:      bolt://127.0.0.1:30687
Username:      neo4j
Password:      seedstorage2026
```

## Discord Channels (9)

| Channel | ID |
|---------|-----|
| #imessages | 1487357825087701063 |
| #direct-manual | 1487577509204594810 |
| #instagram-inspiration | 1487648814280998963 |
| #granola-wyler | 1489132846131052627 |
| #granola-flynn | 1489204533337788476 |
| #hive-mind-announcements | 1488084249188765796 |
| #scouting-external-colonies | 1488083965389836369 |
| #seed-storage | 1487354063749382234 |
| #github-repos | 1489826423643308203 |

## Next Steps (priority order)

1. **MCP Server** — FastMCP with search/context/explore tools. Makes graph queryable from Claude Code.
2. **Ingest CruseControl repos** — run file_scanner on ant-keeper, seed-storage, scratch, GTM.
3. **Rate limit resilience** — exponential backoff in loader for Haiku/Gemini 429s.
4. **GitHub submodule automation** (#24) — auto-add repos from #github-repos as submodules.
5. **In-article outbound links** (#22) — capture URLs within articles as metadata.
6. **Knowledge Base Agent Skill** (#23) — teach agents to use the MCP server.

## Key Paths

| Resource | Path |
|----------|------|
| This project | `/home/flynn-cruse/Code/CruseControl/seed-storage/` |
| Inspirational materials | `/home/flynn-cruse/Code/CruseControl/inspirational-materials/` |
| Ant-keeper | `/opt/shared/ant-keeper/` |
| Plan file | `~/.claude/plans/steady-zooming-fox.md` |
| Memory | `~/.claude/projects/-home-flynn-cruse-Code-CruseControl-inspirational-materials/memory/` |

## How to Resume

```bash
cd /home/flynn-cruse/Code/CruseControl/seed-storage
uv run pytest tests/test_*.py -v  # 107 tests

# Check infrastructure
curl -s http://127.0.0.1:32207/health  # Instagram analyzer
kubectl --kubeconfig /opt/shared/k3s/kubeconfig.yaml get pods -n ant-keeper

# Check pipeline status
PG_DSN="postgresql://taskman:postgres@127.0.0.1:30433/task_manager" \
uv run python3 -c "from seed_storage import staging; print(staging.count_by_status())"

# Query the knowledge graph
NEO4J_URI="bolt://127.0.0.1:30687" GEMINI_API_KEY="..." ANTHROPIC_API_KEY="..." \
uv run python3 -c "
import asyncio
from seed_storage.graphiti_client import get_graphiti, close
async def q():
    g = await get_graphiti()
    r = await g.search('your query here', num_results=5)
    for x in r: print(getattr(x, 'fact', str(x))[:120])
    await close()
asyncio.run(q())
"

# Manual loader trigger
TOKEN="0d6c3ead9b39811a1d90b17e93c1afc367d08626be0d474b1cb3a03c4bde9ca8"
curl -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7070/api/tasks/seed-storage-loader/trigger
```
