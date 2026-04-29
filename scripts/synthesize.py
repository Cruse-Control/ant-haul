#!/usr/bin/env python3
"""Synthesis worker - generate entity articles using Missions-pattern.

Orchestrator selects candidates -> worker generates synthesis -> validator scores.
Resumable via /tmp/synthesis_run_YYYYMMDD.json

Usage:
    cd /home/wyler-zahm/Desktop/cruse-control/AntHaul
    source .venv/bin/activate
    python -m scripts.synthesize --dry-run --min-degree 5 --limit 10
    python -m scripts.synthesize --min-degree 10 --limit 5
    python -m scripts.synthesize --entity-id <id>
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
from datetime import date

SYNTHESIS_PROMPT = """\
You are a knowledge base curator for CruseControl HQ - an AI-focused engineering community.
Write a 3-5 paragraph article about this entity in markdown format, based on its graph context.

Include:
- What this entity is / does (be specific, not generic)
- Key relationships: who created it, what it uses, what it's part of
- Why it matters in the context of AI agents, knowledge systems, and engineering
- Any notable facts or connections

**Entity:** {name} (type: {entity_type})
**Description:** {description}

**Outgoing relationships:**
{outgoing}

**Incoming relationships:**
{incoming}

Write the article now starting with `# {name}`. Reference related entities by name. Be factual and concise."""

VALIDATOR_PROMPT = """\
Rate this entity article on a scale of 1-10.
Deduct points for:
- Generic/vague claims not grounded in the relationships (-2 each)
- Under 150 words (-3)
- Missing major connected entities that were provided (-1 each)

Article:
{synthesis}

Entity name: {name}
Connected entities: {entity_names}

Reply with JSON only: {{"score": N, "issues": ["issue1"]}}"""


async def synthesize_one(entity: dict, client, dry_run: bool = False) -> dict:
    from seed_storage.graph import get_entity_context, write_synthesis

    ctx = await get_entity_context(entity["id"])
    if not ctx["found"]:
        return {"status": "not_found", "id": entity["id"]}

    out_lines = [
        f"  [{r['type']}] -> {r['target'].get('name', '?')}: {str(r.get('description', ''))[:80]}"
        for r in ctx.get("outgoing", [])[:15]
    ]
    in_lines = [
        f"  [{r['type']}] <- {r['source'].get('name', '?')}"
        for r in ctx.get("incoming", [])[:10]
    ]
    entity_names = [
        r["target"].get("name") for r in ctx.get("outgoing", []) if r.get("target")
    ]

    prompt = SYNTHESIS_PROMPT.format(
        name=entity.get("name", ""),
        entity_type=entity.get("entity_type", ""),
        description=entity.get("description", ""),
        outgoing="\n".join(out_lines) or "  (none)",
        incoming="\n".join(in_lines) or "  (none)",
    )

    if dry_run:
        print(f"  [dry-run] would synthesize: {entity.get('name')} ({len(out_lines)} out-rels)")
        return {"status": "dry_run", "id": entity["id"]}

    # Generate synthesis
    resp = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=800,
    )
    synthesis = resp.choices[0].message.content.strip()

    # Validate
    val_resp = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": VALIDATOR_PROMPT.format(
            synthesis=synthesis,
            name=entity.get("name", ""),
            entity_names=", ".join(str(x) for x in entity_names[:10]),
        )}],
        max_tokens=150,
        response_format={"type": "json_object"},
    )
    try:
        verdict = json.loads(val_resp.choices[0].message.content)
        score = verdict.get("score", 0)
    except Exception:
        score = 5

    if score >= 6:
        await write_synthesis(entity_id=entity["id"], synthesis=synthesis)
        print(f"  OK {entity.get('name')} (score: {score}, {len(synthesis)} chars)")
        return {"status": "written", "id": entity["id"], "score": score}
    else:
        issues = verdict.get("issues", []) if isinstance(verdict, dict) else []
        print(f"  SKIP {entity.get('name')} (score: {score} < 6, issues: {issues})")
        return {"status": "low_score", "id": entity["id"], "score": score}


async def run(
    min_degree: int = 5,
    limit: int = 50,
    dry_run: bool = False,
    entity_id: str = "",
) -> None:
    from seed_storage.graph import get_driver, get_entity_context  # noqa
    from seed_storage.config import settings
    from openai import AsyncOpenAI

    driver = await get_driver()
    state_file = f"/tmp/synthesis_run_{date.today()}.json"

    # Load existing run state (resumable)
    done_ids: set[str] = set()
    state: dict = {"results": []}
    if os.path.exists(state_file):
        with open(state_file) as f:
            state = json.load(f)
            done_ids = {r["id"] for r in state.get("results", []) if r.get("status") == "written"}
        print(f"Resuming: {len(done_ids)} already synthesized today")

    async with driver.session() as session:
        if entity_id:
            rows = [r async for r in await session.run(
                "MATCH (e:__Entity__ {id: $id}) RETURN e {.*} AS e", {"id": entity_id}
            )]
            candidates = [rows[0]["e"]] if rows else []
        else:
            candidates = [
                r["e"] async for r in await session.run(
                    """
                    MATCH (e:__Entity__)
                    WHERE e.synthesis IS NULL AND NOT e.id IN $done
                    WITH e, size([(e)-[r]-() | r]) AS degree WHERE degree >= $min
                    RETURN e {.*} AS e ORDER BY degree DESC LIMIT $limit
                    """,
                    {"done": list(done_ids), "min": min_degree, "limit": limit},
                )
            ]

    print(f"Candidates: {len(candidates)} entities (min_degree={min_degree})")
    if not candidates:
        print("Nothing to synthesize.")
        return

    client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    results: list[dict] = []

    for entity in candidates:
        result = await synthesize_one(entity, client, dry_run=dry_run)
        results.append(result)
        if not dry_run:
            state["results"] = state.get("results", []) + [result]
            with open(state_file, "w") as f:
                json.dump(state, f)

    written = sum(1 for r in results if r.get("status") == "written")
    print(f"\nDone: {written}/{len(results)} entities synthesized.")
    if not dry_run:
        print(f"State saved: {state_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthesize entity articles")
    parser.add_argument("--min-degree", type=int, default=5)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--entity-id", default="")
    args = parser.parse_args()
    asyncio.run(run(args.min_degree, args.limit, args.dry_run, args.entity_id))
