#!/usr/bin/env python3
"""Generate /opt/shared/ant-keeper/graph-index.md and append to graph-log.md.

Usage:
    cd /home/wyler-zahm/Desktop/cruse-control/AntHaul
    source .venv/bin/activate
    python -m scripts.generate_index
"""
from __future__ import annotations
import asyncio
import os
from datetime import datetime, timezone

OUTPUT = "/opt/shared/ant-keeper/graph-index.md"
LOG    = "/opt/shared/ant-keeper/graph-log.md"


async def generate() -> None:
    from seed_storage.graph import get_driver, get_stats

    driver = await get_driver()
    stats  = await get_stats()
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

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

    synth_pct = round(has_synth / max(total_entities, 1) * 100, 1)

    lines = [
        "# AntHaul Graph Index",
        f"> Auto-generated {now}. **Read this before any query or write operation.**",
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
        f"## Synthesis Coverage: {has_synth}/{total_entities} entities ({synth_pct}%)",
    ]

    lines += ["", "## Top Entities (by degree - most connected)"]
    for e in top_entities:
        synth_mark = " (synthesized)" if e["has_synthesis"] else ""
        lines.append(f"- **{e['name']}** ({e['type']}, {e['degree']} rels{synth_mark}) - {e['desc']}")

    lines += ["", "## Top Tags"]
    tag_parts = [f"`{t['tag']}` ({t['n']})" for t in top_tags]
    lines.append(", ".join(tag_parts))

    lines += ["", "## Recent Sources (last 10)"]
    for s in recent_sources:
        lines.append(f"- `{s['uri']}` by {s['author']} in #{s['channel']} at {s['ingested']}")

    if saved_queries:
        lines += ["", "## Saved Queries"]
        for q in saved_queries:
            lines.append(f"- [{q['date']}] {q['question']}")

    content = "\n".join(lines)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        f.write(content)

    node_count = stats["nodes"].get("__Entity__", 0)
    src_count  = stats["nodes"].get("Source", 0)
    log_entry  = f"\n## [{now}] generate-index | {node_count} entities, {src_count} sources\n"
    with open(LOG, "a") as f:
        f.write(log_entry)

    print(f"Written: {OUTPUT}")
    print(f"Entities: {node_count}, Sources: {src_count}, Synthesis: {has_synth}/{total_entities}")


if __name__ == "__main__":
    asyncio.run(generate())
