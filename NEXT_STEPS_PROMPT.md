# Seed Storage — Next Steps Execution Prompt

Copy this entire block into a new Claude Code session to execute the next steps unmonitored.

---

## Prompt

You are continuing work on the seed-storage knowledge graph system at `/home/flynn-cruse/Code/CruseControl/seed-storage/`. 

**Read these files first to get full context:**
1. `seed-storage/PROJECT_STATUS.md` — infrastructure state, what's running, how to verify
2. `~/.claude/projects/-home-flynn-cruse-Code-CruseControl-inspirational-materials/memory/project_seed_storage_full.md` — complete system architecture, all channel IDs, credentials, design decisions
3. `~/.claude/projects/-home-flynn-cruse-Code-CruseControl-inspirational-materials/memory/project_next_steps.md` — detailed specs for each task below

**Work on the `ch1-implementation` branch**, merge to `main` after each task. Run `uv run pytest tests/test_*.py` after each change (121+ tests should pass).

Execute these tasks in order:

### Task 1: Rate Limit Resilience in Loader (~30 min)

The daily loader at 06:00 hits Haiku/Gemini API rate limits after ~100 items. 1,727 items are queued.

**File**: `ingestion/loader.py` — modify the `_load_one()` async function inside `load_batch()`.

Add exponential backoff:
- Wrap the `add_episode()` call in a retry loop (max 3 retries)
- On exception: wait 30s × 2^attempt (30s, 60s, 120s) before retrying
- Log the rate limit event
- After 3 retries, mark as failed and continue to next item (don't fail the batch)

Write a test in `tests/test_loader.py` for the backoff logic. Commit and push.

### Task 2: MCP Server for Seed-Storage (~1 session)

Build a FastMCP server that exposes the knowledge graph to Claude Code sessions.

**File**: `seed_storage/mcp_server.py` (new)

Tools to implement:
- `search(query: str, tags: list[str] = None) -> list[dict]` — uses `graphiti_client.search()` for hybrid vector+fulltext search. If tags provided, filter results.
- `get_context(entity: str) -> dict` — search for an entity, return all connected facts/sources/relationships
- `explore(concept: str) -> dict` — search + expand to related themes/domains via graph traversal
- `recent(limit: int = 10) -> list[dict]` — query `seed_staging` table for most recently loaded items

Use FastMCP (`from mcp.server.fastmcp import FastMCP`). The server needs env vars: `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY`, `PG_DSN`.

After building, register in `~/.claude/settings.json`:
```json
{
  "mcpServers": {
    "seed-storage": {
      "command": "uv",
      "args": ["--directory", "/home/flynn-cruse/Code/CruseControl/seed-storage", "run", "python", "-m", "seed_storage.mcp_server"],
      "env": {
        "NEO4J_URI": "bolt://127.0.0.1:30687",
        "GEMINI_API_KEY": "...",
        "ANTHROPIC_API_KEY": "...",
        "PG_DSN": "postgresql://taskman:postgres@127.0.0.1:30433/task_manager"
      }
    }
  }
}
```

Write tests. Commit and push.

### Task 3: Ingest CruseControl Repos (~10 min)

Run the existing `file_scanner.py` on our own repos:

```bash
cd /home/flynn-cruse/Code/CruseControl/seed-storage
for REPO in ant-keeper seed-storage scratch; do
  PG_DSN="postgresql://taskman:postgres@127.0.0.1:30433/task_manager" \
  uv run python -m ingestion.file_scanner /home/flynn-cruse/Code/CruseControl/$REPO
done
```

Then run the processor + enricher to process the new docs:
```bash
ANTHROPIC_API_KEY="..." PG_DSN="..." uv run python -m ingestion.processor
ANTHROPIC_API_KEY="..." PG_DSN="..." uv run python -m ingestion.enricher
```

The daily loader will pick them up at 06:00, or trigger manually via ant-keeper API.

### Task 4: In-Article Outbound Links (~30 min)

**File**: `ingestion/processor.py` — modify `_process_web()`.

After the readability extraction (which parses `full_soup`), extract all outbound links:
```python
outbound = []
for a in full_soup.find_all("a", href=True):
    href = a["href"]
    if href.startswith("http") and len(href) < 500:
        outbound.append(href)
if outbound:
    meta["outbound_links"] = outbound[:50]  # cap at 50
```

Write a test. Commit and push.

### Task 5: Knowledge Base Agent Skill (~1 session)

Create a Claude Code skill at `~/.claude/skills/knowledge-base/SKILL.md` that teaches agents:
- How to search the knowledge graph via the MCP server tools
- When to follow outbound links vs use existing data
- How to file exploration results back as `source_type="exploration"`
- Best practices for querying (use tags for filtering, entity names for precision)

### Task 6: PRD Personas (skip — separate workstream)

Tracked in `/home/flynn-cruse/Code/CruseControl/inspirational-materials/.claude/agents/FUTURE_AGENT_PLANS.md`. Do not execute in this session.

---

## Verification After All Tasks

```bash
# Tests pass
cd /home/flynn-cruse/Code/CruseControl/seed-storage && uv run pytest tests/test_*.py -v

# Pipeline status
PG_DSN="postgresql://taskman:postgres@127.0.0.1:30433/task_manager" \
uv run python3 -c "from seed_storage import staging; print(staging.count_by_status())"

# MCP server responds
uv run python -m seed_storage.mcp_server  # should start without errors

# Knowledge graph queryable
NEO4J_URI="bolt://127.0.0.1:30687" GEMINI_API_KEY="..." ANTHROPIC_API_KEY="..." \
uv run python3 -c "
import asyncio
from seed_storage.graphiti_client import get_graphiti, close
async def q():
    g = await get_graphiti()
    r = await g.search('knowledge graph', num_results=3)
    for x in r: print(getattr(x, 'fact', str(x))[:120])
    await close()
asyncio.run(q())
"
```

Update `PROJECT_STATUS.md` and commit when done.
