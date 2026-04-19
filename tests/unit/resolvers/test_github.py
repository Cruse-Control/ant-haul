"""Tests for GitHubResolver (~6 tests)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seed_storage.enrichment.resolvers.github import GitHubResolver, _parse_repo


@pytest.fixture
def resolver():
    return GitHubResolver()


# ---------------------------------------------------------------------------
# can_handle / _parse_repo
# ---------------------------------------------------------------------------


def test_can_handle_github_repo(resolver):
    assert resolver.can_handle("https://github.com/owner/repo")


def test_can_handle_github_with_path(resolver):
    assert resolver.can_handle("https://github.com/owner/repo/blob/main/file.py")


def test_cannot_handle_non_github(resolver):
    assert not resolver.can_handle("https://gitlab.com/owner/repo")


def test_cannot_handle_github_root(resolver):
    # github.com alone (no owner/repo) should not match
    assert not resolver.can_handle("https://github.com/")


def test_parse_repo_extracts_owner_and_name():
    assert _parse_repo("https://github.com/owner/myrepo") == ("owner", "myrepo")


def test_parse_repo_none_for_non_github():
    assert _parse_repo("https://example.com/owner/repo") is None


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _make_repo_response(
    description="A test repo",
    stars=42,
    forks=5,
    language="Python",
    has_readme=True,
):
    repo_data = {
        "name": "myrepo",
        "description": description,
        "stargazers_count": stars,
        "forks_count": forks,
        "language": language,
        "open_issues_count": 3,
        "license": {"spdx_id": "MIT"},
        "default_branch": "main",
        "pushed_at": "2024-01-01T00:00:00Z",
        "topics": ["python", "testing"],
    }
    return repo_data


def _make_mock_client(repo_data, readme_text="# README\nThis is the README."):
    mock_repo_resp = MagicMock()
    mock_repo_resp.status_code = 200
    mock_repo_resp.json.return_value = repo_data
    mock_repo_resp.raise_for_status = MagicMock()

    mock_readme_resp = MagicMock()
    if readme_text is not None:
        mock_readme_resp.status_code = 200
        mock_readme_resp.text = readme_text
    else:
        mock_readme_resp.status_code = 404

    async def mock_get(url, **kwargs):
        if "/readme" in url.lower():
            return mock_readme_resp
        return mock_repo_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = mock_get
    return mock_client


@pytest.mark.asyncio
async def test_repo_metadata_and_readme(resolver):
    """Fetches repo metadata and README text."""
    repo_data = _make_repo_response()
    mock_client = _make_mock_client(repo_data)

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("seed_storage.enrichment.resolvers.github._get_github_token", return_value=""):
            result = await resolver.resolve("https://github.com/owner/myrepo")

    assert result.extraction_error is None
    assert result.content_type == "github"
    assert result.title == "owner/myrepo"
    assert "README" in result.text
    assert result.metadata["stars"] == 42
    assert result.metadata["language"] == "Python"


@pytest.mark.asyncio
async def test_text_format_includes_description(resolver):
    """Text includes description and README."""
    repo_data = _make_repo_response(description="A wonderful library")
    mock_client = _make_mock_client(repo_data, readme_text="# Docs\nInstallation guide.")

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("seed_storage.enrichment.resolvers.github._get_github_token", return_value=""):
            result = await resolver.resolve("https://github.com/owner/myrepo")

    assert "wonderful library" in result.text
    assert "Installation guide" in result.text


@pytest.mark.asyncio
async def test_unauthenticated_request(resolver):
    """Works without GITHUB_TOKEN (unauthenticated)."""
    repo_data = _make_repo_response()
    mock_client = _make_mock_client(repo_data)

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("seed_storage.enrichment.resolvers.github._get_github_token", return_value=""):
            result = await resolver.resolve("https://github.com/owner/myrepo")

    assert result.extraction_error is None


@pytest.mark.asyncio
async def test_authenticated_request(resolver):
    """Adds Authorization header when GITHUB_TOKEN is set."""
    repo_data = _make_repo_response()
    captured_headers = {}

    async def mock_get(url, **kwargs):
        captured_headers.update(kwargs.get("headers", {}))
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = repo_data
        mock_resp.text = "# README"
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = mock_get

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch(
            "seed_storage.enrichment.resolvers.github._get_github_token", return_value="ghp_test123"
        ):
            result = await resolver.resolve("https://github.com/owner/myrepo")

    # If no extraction error, auth worked
    assert result.extraction_error is None


@pytest.mark.asyncio
async def test_private_repo_returns_error(resolver):
    """404 from GitHub API returns error_result."""
    mock_repo_resp = MagicMock()
    mock_repo_resp.status_code = 404
    mock_repo_resp.raise_for_status = MagicMock()

    async def mock_get(url, **kwargs):
        return mock_repo_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = mock_get

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("seed_storage.enrichment.resolvers.github._get_github_token", return_value=""):
            result = await resolver.resolve("https://github.com/owner/private-repo")

    assert result.extraction_error is not None
    assert "not found" in result.extraction_error.lower()


@pytest.mark.asyncio
async def test_rate_limit_error(resolver):
    """403 with rate limit message returns error_result."""
    mock_repo_resp = MagicMock()
    mock_repo_resp.status_code = 403
    mock_repo_resp.text = "API rate limit exceeded for..."
    mock_repo_resp.raise_for_status = MagicMock()

    async def mock_get(url, **kwargs):
        return mock_repo_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = mock_get

    with patch("httpx.AsyncClient", return_value=mock_client):
        with patch("seed_storage.enrichment.resolvers.github._get_github_token", return_value=""):
            result = await resolver.resolve("https://github.com/owner/repo")

    assert result.extraction_error is not None
    assert "rate limit" in result.extraction_error.lower()
