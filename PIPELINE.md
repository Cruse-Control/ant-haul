# Seed-Storage Pipeline Architecture

## Pipeline Flow

```
                              ┌─────────────────────┐
                              │    Discord Server    │
                              │   (12 channels)      │
                              └──────────┬──────────┘
                                         │ WebSocket (real-time)
                              ┌──────────▼──────────┐
                              │   🐜 ANT-FOOD-WATCHER │ daemon
                              │   (Discord Bot)      │
                              │                      │
                              │ • Classify URLs       │
                              │ • Filter noise        │
                              │ • Route #ant-food →   │
                              │   typed channels      │
                              │ • Stage to Postgres   │
                              │ • React 📥            │
                              └──────────┬──────────┘
                                         │ status: staged
                                         ▼
┌─────────────────┐          ┌──────────────────────┐
│  📁 FILE-SCANNER │ manual   │     PostgreSQL        │
│  (local docs)   ├─────────▶│   seed_staging table  │
└─────────────────┘          │                      │
┌─────────────────┐          │  staged → processed  │
│  📜 BACKFILL    │ manual   │  → enriched → loading │
│  (history)      ├─────────▶│  → loaded / failed   │
└─────────────────┘          └──────────┬──────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
         ┌──────────▼──────┐ ┌─────────▼────────┐ ┌───────▼─────────┐
         │ ⚙️ PROCESSOR    │ │ 🏷️ ENRICHER      │ │ 🧠 LOADER       │
         │ cron */5        │ │ cron */5         │ │ cron hourly     │
         │                 │ │                  │ │                 │
         │ Reads: staged   │ │ Reads: processed │ │ Reads: enriched │
         │ Writes: processed│ │ Writes: enriched│ │ Writes: loaded  │
         │                 │ │                  │ │                 │
         │ • YouTube:      │ │ • Haiku LLM call │ │ • Graphiti      │
         │   transcript    │ │   → tags, summary│ │   add_episode() │
         │ • Instagram:    │ │ • Curator attrib │ │ • Haiku extract │
         │   analyzer+adj  │ │ • Tag table sync │ │ • Gemini embed  │
         │ • GitHub: API   │ │                  │ │ • Neo4j store   │
         │ • Web: readabil │ │ Skip if <50 char │ │                 │
         │ • X: readabil   │ │ (no URL)         │ │ Circuit breaker │
         │ • Plain: pass   │ │                  │ │ Cost ceiling $2 │
         │                 │ │ Circuit breaker  │ │ Error classify  │
         │ Fallback:       │ │ on credit/auth   │ │                 │
         │ Discord context │ │                  │ │ Concurrency: 3  │
         └────────┬────────┘ └────────┬─────────┘ └────────┬────────┘
                  │                   │                     │
                  │    React ⚙️       │    React 🏷️         │    React 🧠
                  ▼                   ▼                     ▼
         ┌─────────────────────────────────────────────────────────┐
         │                    Neo4j Knowledge Graph                │
         │              (Graphiti temporal graph)                  │
         │                                                        │
         │  Entity ──RELATES_TO──▶ Entity                         │
         │    │                      │                            │
         │    └── facts, validity windows, episodes               │
         └──────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
         ┌──────────▼──────┐     ┌──────────▼──────┐
         │ 🔍 MCP SERVER   │     │ 📊 DIGEST        │
         │ (Claude Code)   │     │ cron 06:15       │
         │                 │     │                  │
         │ • search_graph  │     │ Posts daily      │
         │ • get_context   │     │ summary to       │
         │ • explore       │     │ #seed-storage    │
         │ • recent        │     │                  │
         │ • status        │     └──────────────────┘
         │ • express_ingest│
         │ • rush_item     │
         └─────────────────┘
```

## Express Ingest (Fast Path)

```
  URL ──▶ stage ──▶ process ──▶ enrich ──▶ load ──▶ searchable
          │          │           │          │
          └──────────┴───────────┴──────────┘
                 5-15 seconds (synchronous)
```

Bypasses all cron waits. Available via MCP tool or CLI.

## Status Flow

```
staged ──▶ processed ──▶ enriched ──▶ loading ──▶ loaded ✓
  │            │             │            │
  └────────────┴─────────────┴────────────┴──▶ failed ✗
                                               (retryable → back to enriched)

  staged ──▶ threaded (conversation fragments grouped)
  staged ──▶ deduped (content hash match)
  any    ──▶ deleted (manual cleanup)
```

## Cost per Item

| Stage | Model | Cost/item |
|-------|-------|-----------|
| Process (video only) | Haiku | ~$0.001 |
| Enrich | Haiku | ~$0.0004 |
| Load (Graphiti) | Haiku + Gemini | ~$0.02 |
| **Total** | | **~$0.02** |

Batch ceiling: $2.00/batch. Circuit breaker on credit/auth errors.

## Schedules

| Task | Schedule | What |
|------|----------|------|
| ant-food-watcher | always-on daemon | Discord → staged |
| seed-storage-processor | `*/5 * * * *` | staged → processed |
| seed-storage-enricher | `*/5 * * * *` | processed → enriched |
| seed-storage-loader | `0 * * * *` | enriched → loaded |
| seed-storage-digest | `06:15` daily | summary → Discord |

## Discord Reactions (Pipeline Status)

📥 staged → ⚙️ processed → 🏷️ enriched → 🧠 loaded (or ❌ failed, 🔁 deduped)
