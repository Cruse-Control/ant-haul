#!/usr/bin/env python3
"""Generate graph index: write to Neo4j __Meta__ node + Discord summary.

Called by Celery beat task `generate_graph_index` (daily 12:30 UTC).
Also runnable manually:

    cd /home/wyler-zahm/Desktop/cruse-control/AntHaul
    NEO4J_URI=bolt://127.0.0.1:30687 NEO4J_PASSWORD=anthaul2026 \\
      source .venv/bin/activate && python -m scripts.generate_index

Outputs:
  Neo4j __Meta__ node (key='graph_index') -- queryable by any agent
  Discord message to #seed-storage          -- operational visibility
  /opt/shared/ant-keeper/graph-index.md    -- local only, skipped if not writable
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("generate_index")

_LOCAL_OUTPUT = "/opt/shared/ant-keeper/graph-index.md"
_LOCAL_LOG = "/opt/shared/ant-keeper/graph-log.md"
_DIGEST_CHANNEL = "1487354063749382234"  # #seed-storage
_DISCORD_API = "https://discord.com/api/v10"


async def _build_content(driver, stats: dict, now: str) -> tuple[str, dict]:
    """Build full markdown index. Returns (content, counts) where counts has synth+community."""
    async with driver.session() as session:
        top_entities = []
        async for r in await session.run("""
            MATCH (e:__Entity__)
            WITH e, size([(e)-[r]-() | r]) AS degree
            ORDER BY degree DESC LIMIT 20
            RETURN e.name AS name, e.entity_type AS type,
                   left(coalesce(e.description,''), 120) AS desc, degree,
                   e.synthesis IS NOT NULL AS has_synthesis
        """):
            top_entities.append(dict(r))

        top_tags = []
        async for r in await session.run("""
            MATCH (t:Tag)<-[:HAS_TAG]-()
            RETURN t.name AS tag, count(*) AS n ORDER BY n DESC LIMIT 20
        """):
            top_tags.append(dict(r))

        recent_sources = []
        async for r in await session.run("""
            MATCH (s:Source)
            RETURN s.source_uri AS uri, s.author AS author, s.channel AS channel,
                   left(coalesce(s.ingested_at,''), 19) AS ingested
            ORDER BY s.ingested_at DESC LIMIT 10
        """):
            recent_sources.append(dict(r))

        saved_queries = []
        async for r in await session.run("""
            MATCH (q:Query)
            RETURN q.question AS question, left(q.created_at, 10) AS date
            ORDER BY q.created_at DESC LIMIT 10
        """):
            saved_queries.append(dict(r))

        synth_record = await (await session.run("""
            MATCH (e:__Entity__)
            WITH count(e) AS total,
                 sum(CASE WHEN e.synthesis IS NOT NULL THEN 1 ELSE 0 END) AS has_synth
            RETURN total, has_synth
        """)).single()
        total_entities = synth_record["total"]
        has_synth = synth_record["has_synth"]

        comm_record = await (await session.run(
            "MATCH (c:__Community__) RETURN count(c) AS n"
        )).single()
        community_count = comm_record["n"]

    synth_pct = round(has_synth / max(total_entities, 1) * 100, 1)

    lines = [
        "# AntHaul Graph Index",
        f"> Updated {now}. Query: `MATCH (m:__Meta__ {{key:'graph_index'}}) RETURN m.content`",
        "",
        "## Node Counts",
        "| Label | Count |",
        "|---|---|",
    ]
    for k, v in sorted(stats["nodes"].items(), key=lambda x: -x[1]):
        lines.append(f"| {k} | {v} |")

    lines += ["", "## Relationship Types", "| Type | Count |", "|---|---|"]
    for k, v in sorted(stats["relationships"].items(), key=lambda x: -x[1]):
        lines.append(f"| {k} | {v} |")

    lines += [
        "",
        f"## Synthesis Coverage: {has_synth}/{total_entities} ({synth_pct}%)",
        f"## Communities: {community_count}",
    ]

    lines += ["", "## Top Entities (by degree)"]
    for e in top_entities:
        synth_mark = " (synthesized)" if e["has_synthesis"] else ""
        lines.append(f"- **{e['name']}** ({e['type']}, {e['degree']} rels{synth_mark}) - {e['desc']}")

    lines += ["", "## Top Tags"]
    lines.append(", ".join(f"`{t['tag']}` ({t['n']})" for t in top_tags))

    lines += ["", "## Recent Sources (last 10)"]
    for s in recent_sources:
        lines.append(f"- `{s['uri']}` by {s['author']} in #{s['channel']} at {s['ingested']}")

    if saved_queries:
        lines += ["", "## Saved Queries"]
        for q in saved_queries:
            lines.append(f"- [{q['date']}] {q['question']}")

    counts = {"has_synth": has_synth, "total_entities": total_entities, "community_count": community_count}
    return "\n".join(lines), counts


def _post_discord(node_count: int, src_count: int, counts: dict, now: str) -> None:
    """Post brief summary to Discord. Never raises."""
    import httpx
    from seed_storage.config import settings
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        log.warning("DISCORD_BOT_TOKEN not set - skipping Discord post")
        return
    msg = (
        f"\U0001f4ca **Graph Index Updated** - {now}\n"
        f"Entities: **{node_count}** | Sources: **{src_count}** | "
        f"Synthesis: **{counts['has_synth']}/{node_count}** | "
        f"Communities: **{counts['community_count']}**\n"
        f"Read: `MATCH (m:__Meta__ {{key:'graph_index'}}) RETURN m.content`"
    )
    try:
        with httpx.Client(timeout=5.0) as client:
            r = client.post(
                f"{_DISCORD_API}/channels/{_DIGEST_CHANNEL}/messages",
                headers={"Authorization": f"Bot {token}"},
                json={"content": msg[:1950]},
            )
            r.raise_for_status()
    except Exception as exc:
        log.warning("Discord post failed: %s", exc)


async def generate() -> None:
    """Main entry point. Called by Celery task and __main__."""
    from seed_storage.graph import get_driver, get_stats, upsert_meta
    driver = await get_driver()
    stats = await get_stats()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    content, counts = await _build_content(driver, stats, now)

    # Primary: Neo4j __Meta__ node
    await upsert_meta(key="graph_index", content=content, content_type="markdown")
    log.info("Written to Neo4j __Meta__ (key='graph_index')")

    node_count = stats["nodes"].get("__Entity__", 0)
    src_count = stats["nodes"].get("Source", 0)

    # Secondary: Discord
    _post_discord(node_count, src_count, counts, now)

    # Tertiary: local file (skip if not writable in K8s)
    try:
        os.makedirs(os.path.dirname(_LOCAL_OUTPUT), exist_ok=True)
        with open(_LOCAL_OUTPUT, "w") as f:
            f.write(content)
        log_entry = f"\n## [{now}] generate-index | {node_count} entities, {src_count} sources\n"
        with open(_LOCAL_LOG, "a") as f:
            f.write(log_entry)
        log.info("Also written to %s", _LOCAL_OUTPUT)
    except OSError:
        log.debug("Skipping local file (not writable - expected in K8s)")

    print(f"Index generated: {node_count} entities, {src_count} sources, "
          f"{counts['has_synth']} synthesized, {counts['community_count']} communities")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(generate())
