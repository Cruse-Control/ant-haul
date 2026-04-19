"""Shared test fixtures (root conftest)."""

from __future__ import annotations

import uuid

import pytest


@pytest.fixture
def unique_uri():
    """Generate a unique source_uri for dedup-safe testing."""
    return f"https://test.example.com/{uuid.uuid4()}"


@pytest.fixture
def staged_item(unique_uri):
    """Stage a test item and return its ID. Cleaned up after test.

    Requires PostgreSQL (seed_storage.staging). Skipped when unavailable.
    """
    try:
        from seed_storage import staging
    except Exception as exc:
        pytest.skip(f"seed_storage.staging unavailable: {exc}")
        return

    try:
        sid = staging.stage(
            source_type="web",
            source_uri=unique_uri,
            raw_content="Test content for staging integration test.",
            author="test-bot",
            channel="test-channel",
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL staging unavailable: {exc}")
        return

    yield sid

    if sid:
        try:
            staging.update_status([sid], "deleted")
        except Exception:
            pass
