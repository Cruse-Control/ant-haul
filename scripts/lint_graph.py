#!/usr/bin/env python3
"""Graph lint health check: write to Neo4j __Meta__ node + Discord post.

Called by Celery beat task `run_graph_lint` (weekly Monday 13:00 UTC).
Also runnable manually:

    cd /home/wyler-zahm/Desktop/cruse-control/AntHaul
    NEO4J_URI=bolt://127.0.0.1:30687 NEO4J_PASSWORD=anthaul2026 \\
      source .venv/bin/activate && python -m scripts.lint_graph

Outputs:
  Neo4j __Meta__ node (key='lint_report') -- queryable by any agent
  Discord message to #seed-storage         -- operational visibility
  /opt/shared/ant-keeper/lint-report.md   -- local only, skipped if not writable
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("lint_graph")

_LOCAL_OUTPUT = "/opt/shared/ant-keeper/lint-report.md"
_DIGEST_CHANNEL = "1487354063749382234"  # #seed-storage
_DISCORD_API = "https://discord.com/api/v10"


async def _run_checks(session) -> tuple[list[str], dict[str, int]]:
    report: list[str] = []
    summary: dict[str, int] = {}

    # 1. PART_OF overload on Person nodes
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
    report.append(f"## PART_OF Overload - Person nodes >50% PART_OF ({len(part_of_overload)})")
    report.append("Correct types: CREATED, USES, WORKS_FOR, CITES, FOUNDED")
    for e in part_of_overload:
        report.append(f"- **{e['name']}**: {e['part_of_count']}/{e['total_count']} ({int(e['pct'])}%) id:{e['id']}")
    if not part_of_overload:
        report.append("_None - clean!_")

    # 2. Orphan entities
    orphans = []
    async for r in await session.run("""
        MATCH (e:__Entity__) WHERE NOT (e)--()
        RETURN e.name AS name, e.entity_type AS type, e.id AS id
        ORDER BY e.created_at DESC LIMIT 50
    """):
        orphans.append(dict(r))
    summary["orphans"] = len(orphans)
    report.append(f"\n## Orphan entities - zero relationships ({len(orphans)})")
    for o in orphans:
        report.append(f"- [{o['type']}] {o['name']} id:{o['id']}")
    if not orphans:
        report.append("_None - clean!_")

    # 3. Low-signal (1 relationship)
    low_signal = []
    async for r in await session.run("""
        MATCH (e:__Entity__)
        WITH e, size([(e)-[r]-() | r]) AS degree WHERE degree = 1
        RETURN e.name AS name, e.entity_type AS type ORDER BY e.name LIMIT 30
    """):
        low_signal.append(dict(r))
    summary["low_signal"] = len(low_signal)
    report.append(f"\n## Low-signal entities - 1 relationship ({len(low_signal)})")
    for e in low_signal:
        report.append(f"- [{e['type']}] {e['name']}")
    if not low_signal:
        report.append("_None - clean!_")

    # 4. Synthesis candidates
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
    report.append(f"\n## Synthesis candidates - >=5 rels, no synthesis ({len(no_synth)})")
    for e in no_synth:
        report.append(f"- **{e['name']}** ({e['type']}, {e['degree']} rels) id:{e['id']}")
    if not no_synth:
        report.append("_All high-degree entities synthesized!_")

    # 5. Tag sprawl
    sprawl_tags: list[str] = []
    async for r in await session.run("""
        MATCH (t:Tag)<-[:HAS_TAG]-(s) WITH t, count(s) AS uses
        WHERE uses = 1 RETURN t.name AS tag ORDER BY tag LIMIT 60
    """):
        sprawl_tags.append(r["tag"])
    summary["tag_sprawl"] = len(sprawl_tags)
    report.append(f"\n## Single-use tags ({len(sprawl_tags)})")
    report.append(", ".join(f"`{t}`" for t in sprawl_tags) or "_None - clean!_")

    # 6. Missing descriptions
    no_desc_record = await (await session.run("""
        MATCH (e:__Entity__) WHERE e.description IS NULL OR e.description = ''
        RETURN count(e) AS n
    """)).single()
    no_desc = no_desc_record["n"] if no_desc_record else 0
    summary["missing_descriptions"] = no_desc
    report.append(f"\n## Entities missing descriptions: {no_desc}")

    return report, summary


def _post_discord(summary: dict, now: str) -> None:
    """Post lint summary table to Discord. Never raises."""
    import httpx
    from seed_storage.config import settings
    token = settings.DISCORD_BOT_TOKEN
    if not token:
        log.warning("DISCORD_BOT_TOKEN not set - skipping Discord post")
        return
    rows = "\n".join(f"| {k.replace('_',' ').title()} | {v} |" for k, v in summary.items())
    msg = (
        f"\U0001f50d **Graph Lint Report** - {now}\n"
        f"| Check | Count |\n|---|---|\n{rows}\n\n"
        f"Full report: `MATCH (m:__Meta__ {{key:'lint_report'}}) RETURN m.content`"
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


async def lint() -> None:
    """Main entry point. Called by Celery task and __main__."""
    from seed_storage.graph import get_driver, upsert_meta
    driver = await get_driver()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    async with driver.session() as session:
        report_lines, summary = await _run_checks(session)

    summary_table = [
        "## Summary", "| Check | Count |", "|---|---|",
    ] + [f"| {k.replace('_',' ').title()} | {v} |" for k, v in summary.items()]

    full_content = "\n".join(
        [f"# Graph Lint Report", f"> Generated {now}", ""] +
        summary_table + [""] + report_lines
    )

    # Primary: Neo4j __Meta__ node
    await upsert_meta(key="lint_report", content=full_content, content_type="markdown")
    log.info("Written to Neo4j __Meta__ (key='lint_report')")

    # Secondary: Discord
    _post_discord(summary, now)

    # Tertiary: local file (skip if not writable in K8s)
    try:
        os.makedirs(os.path.dirname(_LOCAL_OUTPUT), exist_ok=True)
        with open(_LOCAL_OUTPUT, "w") as f:
            f.write(full_content)
        log.info("Also written to %s", _LOCAL_OUTPUT)
    except OSError:
        log.debug("Skipping local file (not writable - expected in K8s)")

    print("Lint complete:")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(lint())
