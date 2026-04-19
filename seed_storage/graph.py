"""Neo4j client wrapper for seed-storage."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver

from . import config


_driver: AsyncDriver | None = None


async def get_driver() -> AsyncDriver:
    global _driver
    if _driver is None:
        _driver = AsyncGraphDatabase.driver(
            config.NEO4J_URI,
            auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
        )
    return _driver


async def close():
    global _driver
    if _driver:
        await _driver.close()
        _driver = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uuid() -> str:
    return str(uuid.uuid4())


# ── Schema initialization ──────────────────────────────────────────


async def init_schema():
    """Create indexes and constraints. Idempotent."""
    driver = await get_driver()

    constraints = [
        # Layer 1: Epistemic (existing)
        "CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:Source) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT fact_id IF NOT EXISTS FOR (f:Fact) REQUIRE f.id IS UNIQUE",
        "CREATE CONSTRAINT concept_id IF NOT EXISTS FOR (c:Concept) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT theme_id IF NOT EXISTS FOR (t:Theme) REQUIRE t.id IS UNIQUE",
        "CREATE CONSTRAINT domain_id IF NOT EXISTS FOR (d:Domain) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
        "CREATE CONSTRAINT question_id IF NOT EXISTS FOR (q:Question) REQUIRE q.id IS UNIQUE",
        "CREATE CONSTRAINT gap_id IF NOT EXISTS FOR (g:Gap) REQUIRE g.id IS UNIQUE",
        "CREATE CONSTRAINT community_id IF NOT EXISTS FOR (c:Community) REQUIRE c.id IS UNIQUE",
        # Layer 2: Reasoning (PRD — Person→Decision→Context→Outcome, MentalModel)
        "CREATE CONSTRAINT decision_id IF NOT EXISTS FOR (d:Decision) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT context_id IF NOT EXISTS FOR (c:Context) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT outcome_id IF NOT EXISTS FOR (o:Outcome) REQUIRE o.id IS UNIQUE",
        "CREATE CONSTRAINT mentalmodel_id IF NOT EXISTS FOR (m:MentalModel) REQUIRE m.id IS UNIQUE",
        # Layer 3: Operational (PRD — Agent→Skill→Output→Review)
        "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
        # Enrichment: dynamic tags
        "CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE",
    ]

    vector_indexes = [
        """CREATE VECTOR INDEX source_embedding IF NOT EXISTS
           FOR (s:Source) ON (s.embedding)
           OPTIONS {indexConfig: {
             `vector.dimensions`: 3072,
             `vector.similarity_function`: 'cosine'
           }}""",
        """CREATE VECTOR INDEX fact_embedding IF NOT EXISTS
           FOR (f:Fact) ON (f.embedding)
           OPTIONS {indexConfig: {
             `vector.dimensions`: 3072,
             `vector.similarity_function`: 'cosine'
           }}""",
        """CREATE VECTOR INDEX concept_embedding IF NOT EXISTS
           FOR (c:Concept) ON (c.embedding)
           OPTIONS {indexConfig: {
             `vector.dimensions`: 3072,
             `vector.similarity_function`: 'cosine'
           }}""",
        """CREATE VECTOR INDEX question_embedding IF NOT EXISTS
           FOR (q:Question) ON (q.embedding)
           OPTIONS {indexConfig: {
             `vector.dimensions`: 3072,
             `vector.similarity_function`: 'cosine'
           }}""",
        """CREATE VECTOR INDEX entity_embedding IF NOT EXISTS
           FOR (e:Entity) ON (e.embedding)
           OPTIONS {indexConfig: {
             `vector.dimensions`: 3072,
             `vector.similarity_function`: 'cosine'
           }}""",
    ]

    lookup_indexes = [
        "CREATE INDEX source_uri IF NOT EXISTS FOR (s:Source) ON (s.source_uri)",
        "CREATE INDEX source_type IF NOT EXISTS FOR (s:Source) ON (s.type)",
        "CREATE INDEX fact_realm IF NOT EXISTS FOR (f:Fact) ON (f.realm)",
        "CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)",
        "CREATE INDEX question_status IF NOT EXISTS FOR (q:Question) ON (q.status)",
        "CREATE INDEX concept_name IF NOT EXISTS FOR (c:Concept) ON (c.name)",
        # Reasoning layer indexes
        "CREATE INDEX decision_title IF NOT EXISTS FOR (d:Decision) ON (d.title)",
        "CREATE INDEX mentalmodel_name IF NOT EXISTS FOR (m:MentalModel) ON (m.name)",
        "CREATE INDEX tag_name_idx IF NOT EXISTS FOR (t:Tag) ON (t.name)",
    ]

    fulltext_indexes = [
        """CREATE FULLTEXT INDEX source_content IF NOT EXISTS
           FOR (s:Source) ON EACH [s.raw_content]""",
        """CREATE FULLTEXT INDEX fact_statement IF NOT EXISTS
           FOR (f:Fact) ON EACH [f.statement]""",
    ]

    async with driver.session() as session:
        for stmt in constraints + vector_indexes + lookup_indexes + fulltext_indexes:
            await session.run(stmt)


# ── Write operations ───────────────────────────────────────────────


async def create_source(
    *,
    source_type: str,
    source_uri: str,
    raw_content: str,
    media_type: str = "text",
    embedding: list[float],
    author: str,
    created_at: str | None = None,
    channel: str = "",
    confidence: float = 1.0,
    metadata: dict | None = None,
) -> str:
    """Create a Source node. Returns the node id."""
    driver = await get_driver()
    node_id = _uuid()
    now = _now()

    async with driver.session() as session:
        await session.run(
            """
            CREATE (s:Source {
                id: $id,
                type: $type,
                source_uri: $source_uri,
                raw_content: $raw_content,
                media_type: $media_type,
                embedding: $embedding,
                author: $author,
                created_at: $created_at,
                ingested_at: $ingested_at,
                channel: $channel,
                confidence: $confidence,
                metadata: $metadata
            })
            """,
            id=node_id,
            type=source_type,
            source_uri=source_uri,
            raw_content=raw_content,
            media_type=media_type,
            embedding=embedding,
            author=author,
            created_at=created_at or now,
            ingested_at=now,
            channel=channel,
            confidence=confidence,
            metadata=str(metadata or {}),
        )
    return node_id


async def create_fact(
    *,
    statement: str,
    embedding: list[float],
    confidence: float = 0.8,
    realm: str = "known_known",
    source_id: str,
    extraction_method: str = "claude",
) -> str:
    """Create a Fact node linked to its Source. Returns the fact id."""
    driver = await get_driver()
    fact_id = _uuid()
    now = _now()

    async with driver.session() as session:
        await session.run(
            """
            MATCH (s:Source {id: $source_id})
            CREATE (f:Fact {
                id: $id,
                statement: $statement,
                embedding: $embedding,
                confidence: $confidence,
                verified: false,
                extracted_at: $now,
                last_validated: $now,
                realm: $realm
            })
            CREATE (f)-[:EXTRACTED_FROM {method: $method}]->(s)
            """,
            id=fact_id,
            statement=statement,
            embedding=embedding,
            confidence=confidence,
            realm=realm,
            source_id=source_id,
            now=now,
            method=extraction_method,
        )
    return fact_id


async def create_entity(
    *,
    name: str,
    entity_type: str,
    description: str = "",
    embedding: list[float],
) -> str:
    """Create or merge an Entity node. Returns the entity id."""
    driver = await get_driver()
    entity_id = _uuid()

    async with driver.session() as session:
        result = await session.run(
            """
            MERGE (e:Entity {name: $name})
            ON CREATE SET
                e.id = $id,
                e.type = $type,
                e.description = $description,
                e.embedding = $embedding
            ON MATCH SET
                e.description = CASE WHEN size(e.description) < size($description)
                                     THEN $description ELSE e.description END
            RETURN e.id AS id
            """,
            id=entity_id,
            name=name,
            type=entity_type,
            description=description,
            embedding=embedding,
        )
        record = await result.single()
        return record["id"]


async def link_fact_entity(fact_id: str, entity_id: str):
    """Create a MENTIONS relationship between a Fact and Entity."""
    driver = await get_driver()
    async with driver.session() as session:
        await session.run(
            """
            MATCH (f:Fact {id: $fact_id})
            MATCH (e:Entity {id: $entity_id})
            MERGE (f)-[:MENTIONS]->(e)
            """,
            fact_id=fact_id,
            entity_id=entity_id,
        )


# ── Read operations ────────────────────────────────────────────────


async def vector_search(
    embedding: list[float],
    label: str = "Fact",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search nodes by vector similarity."""
    driver = await get_driver()
    index_name = f"{label.lower()}_embedding"

    async with driver.session() as session:
        result = await session.run(
            f"""
            CALL db.index.vector.queryNodes($index, $k, $embedding)
            YIELD node, score
            RETURN node {{.*, embedding: null}} AS node, score
            ORDER BY score DESC
            """,
            index=index_name,
            k=limit,
            embedding=embedding,
        )
        return [{"node": r["node"], "score": r["score"]} async for r in result]


async def vector_search_with_expansion(
    embedding: list[float],
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Vector search on Facts with graph expansion (provenance + concepts + entities)."""
    driver = await get_driver()

    async with driver.session() as session:
        result = await session.run(
            """
            CALL db.index.vector.queryNodes('fact_embedding', $k, $embedding)
            YIELD node AS fact, score

            OPTIONAL MATCH (fact)-[:EXTRACTED_FROM]->(source:Source)
            OPTIONAL MATCH (fact)-[:SUPPORTS]->(concept:Concept)
            OPTIONAL MATCH (fact)-[:MENTIONS]->(entity:Entity)

            RETURN
                fact {.*, embedding: null} AS fact,
                score,
                source {.id, .type, .source_uri, .author, .created_at, .channel} AS source,
                concept {.id, .name, .description, .maturity} AS concept,
                collect(entity {.id, .name, .type}) AS entities
            ORDER BY score DESC
            """,
            k=limit,
            embedding=embedding,
        )
        return [
            {
                "fact": r["fact"],
                "score": r["score"],
                "source": r["source"],
                "concept": r["concept"],
                "entities": r["entities"],
            }
            async for r in result
        ]


async def fulltext_search(
    query: str,
    label: str = "Fact",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Full-text search on node content."""
    driver = await get_driver()
    index_name = f"{label.lower()}_{'statement' if label == 'Fact' else 'content'}"

    async with driver.session() as session:
        result = await session.run(
            f"""
            CALL db.index.fulltext.queryNodes($index, $search_query)
            YIELD node, score
            RETURN node {{.*, embedding: null}} AS node, score
            ORDER BY score DESC
            LIMIT $limit
            """,
            index=index_name,
            search_query=query,
            limit=limit,
        )
        return [{"node": r["node"], "score": r["score"]} async for r in result]


async def get_stats() -> dict:
    """Return graph statistics."""
    driver = await get_driver()
    async with driver.session() as session:
        result = await session.run(
            """
            MATCH (n)
            WITH labels(n) AS lbls, count(*) AS cnt
            UNWIND lbls AS label
            RETURN label, sum(cnt) AS count
            ORDER BY count DESC
            """
        )
        counts = {r["label"]: r["count"] async for r in result}

        rel_result = await session.run(
            """
            MATCH ()-[r]->()
            RETURN type(r) AS type, count(*) AS count
            ORDER BY count DESC
            """
        )
        rels = {r["type"]: r["count"] async for r in rel_result}

        return {"nodes": counts, "relationships": rels}
