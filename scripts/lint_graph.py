#!/usr/bin/env python3
"""AntHaul graph lint - health check with PART_OF overuse detection.

Checks: PART_OF overload on Person nodes, orphan entities, low-signal entities,
high-degree entities missing synthesis, tag sprawl, missing descriptions.

Usage:
    cd /home/wyler-zahm/Desktop/cruse-control/AntHaul
    source .venv/bin/activate
    python -m scripts.lint_graph
"""
from __future__ import annotations
import asyncio
import os
from datetime import datetime, timezone

OUTPUT = "/opt/shared/ant-keeper/lint-report.md"


async def lint() -> None:
    from seed_storage.graph import get_driver

    driver = await get_driver()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report = [f"# Graph Lint Report\n> Generated {now}\n"]
    summary: dict[str, int] = {}

    async with driver.session() as session:
        part_of_overload = []
        async for r in await session.run("""
            MATCH (e:__Entity__:Person)
            WITH e,
              size([(e)-[r:PART_OF]->() | r]) AS part_of_count,
              size([(e)-[r]-() | r]) AS total_count
            WHERE total_count > 0
              AND toFloat(part_of_count) / toFloat(total_count) > 0.5
            RETURN e.name AS name, e.id AS id,
                   part_of_count, total_count,
                   round(100.0 * part_of_count / total_count) AS pct
            ORDER BY pct DESC LIMIT 20
        """):
            part_of_overload.append(dict(r))

        summary["part_of_overload"] = len(part_of_overload)
        report.append(f"## PART_OF Overload - Person nodes with >50% PART_OF rels ({len(part_of_overload)})\n")
        report.append("Correct types to use instead: CREATED, USES, WORKS_FOR, CITES, FOUNDED\n")
        for e in part_of_overload:
            report.append(f"- **{e['name']}**: {e['part_of_count']}/{e['total_count']} PART_OF ({int(e['pct'])}%) - id: {e['id']}")
        if not part_of_overload:
            report.append("_None found - clean!_")

        orphans = []
        async for r in await session.run("""
            MATCH (e:__Entity__)
            WHERE NOT (e)-[]-()
            RETURN e.name AS name, e.entity_type AS type, e.id AS id
            ORDER BY e.created_at DESC LIMIT 50
        """):
            orphans.append(dict(r))

        summary["orphans"] = len(orphans)
        report.append(f"\n## Orphan entities - zero relationships ({len(orphans)})\n")
        for o in orphans:
            report.append(f"- [{o['type']}] {o['name']} (id: {o['id']})")
        if not orphans:
            report.append("_None found - clean!_")

        low_signal = []
        async for r in await session.run("""
            MATCH (e:__Entity__)
            WITH e, size([(e)-[r]-() | r]) AS degree WHERE degree = 1
            RETURN e.name AS name, e.entity_type AS type
            ORDER BY e.name LIMIT 30
        """):
            low_signal.append(dict(r))

        summary["low_signal"] = len(low_signal)
        report.append(f"\n## Low-signal entities - exactly 1 relationship ({len(low_signal)})\n")
        for e in low_signal:
            report.append(f"- [{e['type']}] {e['name']}")
        if not low_signal:
            report.append("_None found - clean!_")

        no_synth = []
        async for r in await session.run("""
            MATCH (e:__Entity__)
            WHERE e.synthesis IS NULL
            WITH e, size([(e)-[r]-() | r]) AS degree WHERE degree >= 5
            RETURN e.name AS name, e.entity_type AS type, e.id AS id, degree
            ORDER BY degree DESC LIMIT 25
        """):
            no_synth.append(dict(r))

        summary["synthesis_candidates"] = len(no_synth)
        report.append(f"\n## Synthesis candidates - high-degree (>=5 rels), no synthesis ({len(no_synth)})\n")
        for e in no_synth:
            report.append(f"- **{e['name']}** ({e['type']}, {e['degree']} rels) - id: {e['id']}")
        if not no_synth:
            report.append("_All high-degree entities have synthesis - great!_")

        sprawl_tags: list[str] = []
        async for r in await session.run("""
            MATCH (t:Tag)<-[:HAS_TAG]-(s)
            WITH t, count(s) AS uses WHERE uses = 1
            RETURN t.name AS tag ORDER BY tag LIMIT 60
        """):
            sprawl_tags.append(r["tag"])

        summary["tag_sprawl"] = len(sprawl_tags)
        report.append(f"\n## Tag sprawl - single-use tags ({len(sprawl_tags)})\n")
        if sprawl_tags:
            report.append(", ".join(f"`{t}`" for t in sprawl_tags))
        else:
            report.append("_No single-use tags - clean!_")

        no_desc_record = await (await session.run("""
            MATCH (e:__Entity__)
            WHERE e.description IS NULL OR e.description = ''
            RETURN count(e) AS n
        """)).single()
        no_desc = no_desc_record["n"] if no_desc_record else 0

        summary["missing_descriptions"] = no_desc
        report.append(f"\n## Entities missing descriptions: {no_desc}\n")

    summary_lines = [
        "\n## Summary",
        "| Check | Count |",
        "|---|---|",
        f"| PART_OF overload | {summary['part_of_overload']} |",
        f"| Orphan entities | {summary['orphans']} |",
        f"| Low-signal (1 rel) | {summary['low_signal']} |",
        f"| Synthesis candidates | {summary['synthesis_candidates']} |",
        f"| Single-use tags | {summary['tag_sprawl']} |",
        f"| Missing descriptions | {summary['missing_descriptions']} |",
        "",
    ]
    report = report[:2] + summary_lines + report[2:]

    content = "\n".join(report)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        f.write(content)

    print(f"Lint report: {OUTPUT}")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(lint())
