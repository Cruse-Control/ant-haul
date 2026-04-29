"""Unit/integration tests for new CRUD operations.
Requires live Neo4j at bolt://127.0.0.1:30687.
Run: pytest tests/unit/test_graph_crud.py -v -m integration
"""
from __future__ import annotations
import asyncio
import pytest
from seed_storage import graph

pytestmark = pytest.mark.integration

@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)

def run(coro, loop):
    return loop.run_until_complete(coro)

def test_delete_entity_removes_node(event_loop):
    async def _body():
        eid = await graph.upsert_entity(
            canonical_name="test-crud-delete-target",
            name="TestCRUDDeleteTarget",
            entity_type="Concept",
            description="temporary test entity for delete",
            embedding=[0.0] * 1536,
        )
        deleted = await graph.delete_entity(entity_id=eid)
        assert deleted is True
        ctx = await graph.get_entity_context(eid)
        assert ctx["found"] is False
    run(_body(), event_loop)

def test_delete_entity_missing_returns_false(event_loop):
    async def _body():
        result = await graph.delete_entity(entity_id="nonexistent-id-xyz-123")
        assert result is False
    run(_body(), event_loop)

def test_merge_entities_consolidates(event_loop):
    async def _body():
        eid_a = await graph.upsert_entity(
            canonical_name="test-crud-merge-keep", name="MergeCRUDKeep",
            entity_type="Concept", description="keep this entity",
            embedding=[0.0] * 1536,
        )
        eid_b = await graph.upsert_entity(
            canonical_name="test-crud-merge-remove", name="MergeCRUDRemove",
            entity_type="Concept", description="remove this entity",
            embedding=[0.0] * 1536,
        )
        result = await graph.merge_entities(keep_id=eid_a, remove_id=eid_b)
        assert result["keep_id"] == eid_a
        ctx_b = await graph.get_entity_context(eid_b)
        assert ctx_b["found"] is False
        await graph.delete_entity(entity_id=eid_a)
    run(_body(), event_loop)

def test_delete_relationship(event_loop):
    async def _body():
        eid_a = await graph.upsert_entity(
            canonical_name="test-crud-rel-a", name="CRUDRelA",
            entity_type="Concept", description="rel test a", embedding=[0.0] * 1536,
        )
        eid_b = await graph.upsert_entity(
            canonical_name="test-crud-rel-b", name="CRUDRelB",
            entity_type="Concept", description="rel test b", embedding=[0.0] * 1536,
        )
        await graph.create_relationship(
            source_entity_id=eid_a, target_entity_id=eid_b,
            relationship_type="RELATED_TO", description="test relationship",
        )
        deleted = await graph.delete_relationship(
            source_entity_id=eid_a, target_entity_id=eid_b,
            relationship_type="RELATED_TO",
        )
        assert deleted is True
        await graph.delete_entity(entity_id=eid_a)
        await graph.delete_entity(entity_id=eid_b)
    run(_body(), event_loop)

def test_write_synthesis(event_loop):
    async def _body():
        eid = await graph.upsert_entity(
            canonical_name="test-crud-synthesis", name="CRUDSynthesisTest",
            entity_type="Concept", description="synthesis test entity",
            embedding=[0.0] * 1536,
        )
        updated = await graph.write_synthesis(
            entity_id=eid,
            synthesis="# CRUDSynthesisTest\n\nThis is a test synthesis article.",
        )
        assert updated is True
        await graph.delete_entity(entity_id=eid)
    run(_body(), event_loop)

def test_persist_query(event_loop):
    async def _body():
        eid = await graph.upsert_entity(
            canonical_name="test-crud-query-ref", name="CRUDQueryRef",
            entity_type="Concept", description="query ref entity",
            embedding=[0.0] * 1536,
        )
        qid = await graph.persist_query(
            question="What is CRUDQueryRef?",
            answer_md="It is a test entity for query persistence validation.",
            entity_ids=[eid],
        )
        assert qid and len(qid) > 0
        await graph.delete_entity(entity_id=eid)
    run(_body(), event_loop)


def test_upsert_and_get_meta(event_loop):
    async def _body():
        await graph.upsert_meta(
            key="test-meta-key",
            content="# Test\n\nThis is a test.",
            content_type="markdown",
        )
        result = await graph.get_meta("test-meta-key")
        assert result is not None
        assert result["content"] == "# Test\n\nThis is a test."
        assert result["content_type"] == "markdown"
        assert result["key"] == "test-meta-key"
        # cleanup
        driver = await graph.get_driver()
        async with driver.session() as session:
            await session.run("MATCH (m:__Meta__ {key: 'test-meta-key'}) DELETE m")
    run(_body(), event_loop)
