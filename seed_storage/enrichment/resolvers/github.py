"""GitHub resolver — REST API metadata + README extraction.

Supports repository URLs (github.com/<owner>/<repo>). Fetches repo metadata
and README text via the GitHub REST API. Uses GITHUB_TOKEN when available.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx

from seed_storage.enrichment.models import ResolvedContent
from seed_storage.enrichment.resolvers.base import BaseResolver

logger = logging.getLogger(__name__)

_GITHUB_HOST = "github.com"
_API_BASE = "https://api.github.com"
_TIMEOUT = 15.0


def _parse_repo(url: str) -> tuple[str, str] | None:
    """Extract (owner, repo) from a github.com URL. Returns None if not a repo URL."""
    parsed = urlparse(url)
    if parsed.hostname not in (_GITHUB_HOST, f"www.{_GITHUB_HOST}"):
        return None
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def _get_github_token() -> str:
    try:
        from seed_storage.config import settings

        return settings.GITHUB_TOKEN
    except Exception:  # noqa: BLE001
        return ""


class GitHubResolver(BaseResolver):
    """Fetches GitHub repository metadata and README text."""

    def can_handle(self, url: str) -> bool:
        return _parse_repo(url) is not None

    async def resolve(self, url: str) -> ResolvedContent:
        try:
            return await self._resolve_internal(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("GitHubResolver failed for %s: %s", url, exc)
            return ResolvedContent.error_result(url, str(exc))

    async def _resolve_internal(self, url: str) -> ResolvedContent:
        repo_parts = _parse_repo(url)
        if not repo_parts:
            raise ValueError(f"Cannot parse GitHub repo from URL: {url}")
        owner, repo = repo_parts

        token = _get_github_token()
        headers: dict[str, str] = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "seed-storage/2.0",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=_TIMEOUT, headers=headers) as client:
            # Fetch repo metadata
            repo_resp = await client.get(f"{_API_BASE}/repos/{owner}/{repo}")
            if repo_resp.status_code == 401:
                raise ValueError("GitHub authentication failed — check GITHUB_TOKEN")
            if repo_resp.status_code == 403:
                # Check for rate limit
                if "rate limit" in repo_resp.text.lower():
                    raise ValueError("GitHub API rate limit exceeded")
                raise ValueError(f"GitHub API returned 403: {repo_resp.text[:200]}")
            if repo_resp.status_code == 404:
                raise ValueError(f"GitHub repository not found: {owner}/{repo}")
            repo_resp.raise_for_status()
            repo_data = repo_resp.json()

            # Fetch README
            readme_text: str | None = None
            readme_resp = await client.get(
                f"{_API_BASE}/repos/{owner}/{repo}/readme",
                headers={**headers, "Accept": "application/vnd.github.v3.raw"},
            )
            if readme_resp.status_code == 200:
                readme_text = readme_resp.text

        # Build text
        text_parts = []
        description = repo_data.get("description") or ""
        if description:
            text_parts.append(f"Description: {description}")
        topics = repo_data.get("topics") or []
        if topics:
            text_parts.append(f"Topics: {', '.join(topics)}")
        if readme_text:
            text_parts.append(f"README:\n{readme_text}")

        text = "\n\n".join(text_parts)
        title = f"{owner}/{repo}"

        metadata = {
            "owner": owner,
            "repo": repo,
            "stars": repo_data.get("stargazers_count"),
            "forks": repo_data.get("forks_count"),
            "language": repo_data.get("language"),
            "open_issues": repo_data.get("open_issues_count"),
            "license": (repo_data.get("license") or {}).get("spdx_id"),
            "default_branch": repo_data.get("default_branch"),
            "pushed_at": repo_data.get("pushed_at"),
            "has_readme": readme_text is not None,
        }

        return ResolvedContent(
            source_url=url,
            content_type="github",
            title=title,
            text=text,
            transcript=None,
            summary=None,
            expansion_urls=[],
            metadata=metadata,
            extraction_error=None,
            resolved_at=datetime.now(tz=UTC),
        )
