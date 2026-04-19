"""GitHub repo auto-ingestion — add as submodule to inspirational-materials.

When a GitHub URL is processed, this module:
1. Creates a feature branch (add/<repo-name>)
2. Adds the repo as a git submodule
3. Updates index-of-inspiration.md with a description
4. Scans the new submodule's docs for the knowledge graph
5. Commits, pushes, and optionally creates a PR

Failures here do NOT block the main pipeline.

Run standalone: python -m ingestion.submodule_adder <github-url>
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

from ingestion.file_scanner import scan_directory
from seed_storage import staging

log = logging.getLogger("submodule_adder")

INSPIRATIONAL_ROOT = "/home/flynn-cruse/Code/CruseControl/inspirational-materials"


def _parse_github_url(url: str) -> tuple[str, str]:
    """Extract (owner, repo) from a GitHub URL."""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) >= 2:
        repo = parts[1].removesuffix(".git")
        return parts[0], repo
    return "", ""


def _repo_already_exists(root: str, owner: str, repo: str) -> bool:
    """Check if this repo is already a submodule."""
    gitmodules = Path(root) / ".gitmodules"
    if not gitmodules.exists():
        return False
    text = gitmodules.read_text()
    # Check for repo URL in .gitmodules (with or without .git suffix).
    repo_url_patterns = [
        f"github.com/{owner}/{repo}.git",
        f"github.com/{owner}/{repo}\n",
        f"github.com/{owner}/{repo}\"",
    ]
    text_lower = text.lower()
    return any(p.lower() in text_lower for p in repo_url_patterns)


def _run_git(args: list[str], cwd: str, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run a git command with optional env overrides."""
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["git"] + args,
        cwd=cwd,
        capture_output=True,
        text=True,
        env=full_env,
        timeout=120,
    )


def _determine_path(owner: str, repo: str) -> str:
    """Decide where to place the submodule — top-level or under a category.

    Uses owner name for known companies/authors, otherwise top-level.
    """
    # Known category mappings (owner → directory prefix).
    categories = {
        "karpathy": "Karpathy",
        "openai": "OpenAI",
        "nvidia": "NVIDIA",
        "google": "Google-DeepMind",
        "googledeepmind": "Google-DeepMind",
        "algorithmicsuperintelligence": "Google-DeepMind",
        "langchain-ai": "langchain",
        "vercel-labs": "Vercels-Materials",
        "strongdm": "Ai-Factory-Strong-DM",
        "anthropics": "claude-code-tips",
        "remotion-dev": "remotion-skills",
    }
    owner_lower = owner.lower()
    if owner_lower in categories:
        return f"{categories[owner_lower]}/{repo}"
    return repo


def _build_index_entry(owner: str, repo: str, submodule_path: str, description: str) -> str:
    """Build a markdown entry for index-of-inspiration.md."""
    header = f"### {submodule_path}/"
    if not description:
        description = f"GitHub repository [{owner}/{repo}](https://github.com/{owner}/{repo})."
    # Truncate very long descriptions.
    if len(description) > 1000:
        description = description[:1000] + "..."
    return f"\n{header}\n{description}\n"


def _find_index_section(index_text: str, owner: str, repo: str) -> str:
    """Find the best section in the index to append to. Returns section header."""
    # Map known owners to sections.
    section_map = {
        "karpathy": "LLM Training & Research",
        "openai": "Multi-Agent Orchestration",
        "nvidia": "Agent Architectures & Frameworks",
        "langchain-ai": "LangChain",
        "anthropics": "Claude Code Tips & Skills",
    }
    owner_lower = owner.lower()
    if owner_lower in section_map:
        return section_map[owner_lower]
    # Default: append to "Platform & Tooling References" as a catch-all.
    return "Platform & Tooling References"


def add_submodule(
    github_url: str,
    description: str = "",
    root: str = INSPIRATIONAL_ROOT,
    push: bool = True,
    create_pr: bool = True,
) -> dict:
    """Add a GitHub repo as a submodule and update the index.

    Returns a dict with status and details:
    - {"status": "added", "path": "...", "branch": "..."}
    - {"status": "exists", "reason": "..."}
    - {"status": "failed", "error": "..."}
    """
    owner, repo = _parse_github_url(github_url)
    if not owner or not repo:
        return {"status": "failed", "error": f"Could not parse owner/repo from {github_url}"}

    # Check if already exists.
    if _repo_already_exists(root, owner, repo):
        log.info("Submodule %s/%s already exists — skipping", owner, repo)
        return {"status": "exists", "reason": f"{owner}/{repo} already in .gitmodules"}

    submodule_path = _determine_path(owner, repo)
    branch_name = f"add/{repo}"
    clone_url = f"https://github.com/{owner}/{repo}.git"

    log.info("Adding submodule %s/%s at %s", owner, repo, submodule_path)

    try:
        # Ensure we're on a clean main branch first.
        _run_git(["checkout", "main"], cwd=root)
        _run_git(["pull", "--ff-only"], cwd=root)

        # Check if branch already exists.
        result = _run_git(["branch", "--list", branch_name], cwd=root)
        if branch_name in result.stdout:
            log.info("Branch %s already exists — skipping", branch_name)
            return {"status": "exists", "reason": f"Branch {branch_name} already exists"}

        # Create feature branch.
        _run_git(["checkout", "-b", branch_name], cwd=root)

        # Add submodule.
        result = _run_git(["submodule", "add", clone_url, submodule_path], cwd=root)
        if result.returncode != 0:
            _run_git(["checkout", "main"], cwd=root)
            _run_git(["branch", "-D", branch_name], cwd=root)
            return {"status": "failed", "error": f"git submodule add failed: {result.stderr}"}

        # Update index-of-inspiration.md.
        index_path = Path(root) / "index-of-inspiration.md"
        if index_path.exists():
            index_text = index_path.read_text()
            section = _find_index_section(index_text, owner, repo)
            entry = _build_index_entry(owner, repo, submodule_path, description)

            # Find the section and append after it.
            section_pattern = f"## {section}"
            if section_pattern in index_text:
                # Find the next section (## header) after our target section.
                section_start = index_text.index(section_pattern)
                rest = index_text[section_start + len(section_pattern):]
                next_section = rest.find("\n## ")
                if next_section != -1:
                    # Insert before the next section's separator (---).
                    insert_point = section_start + len(section_pattern) + next_section
                    # Look for --- before the next ## header.
                    pre = index_text[:insert_point]
                    last_separator = pre.rfind("\n---\n")
                    if last_separator > section_start:
                        insert_point = last_separator
                    index_text = index_text[:insert_point] + entry + index_text[insert_point:]
                else:
                    # Last section — append at end.
                    index_text += entry
            else:
                # Section not found — append at end.
                index_text += f"\n---\n\n## Uncategorized\n{entry}"

            index_path.write_text(index_text)

        # Stage and commit.
        _run_git(["add", ".gitmodules", submodule_path, "index-of-inspiration.md"], cwd=root)
        commit_msg = f"Add {owner}/{repo} submodule and update index"
        _run_git(["commit", "-m", commit_msg], cwd=root)

        # Scan the new submodule's docs for the knowledge graph.
        submodule_full_path = str(Path(root) / submodule_path)
        if Path(submodule_full_path).is_dir():
            staging.init_tables()
            scanned, _, _ = scan_directory(submodule_full_path)
            log.info("Scanned %d docs from new submodule %s", scanned, submodule_path)

        # Push and optionally create PR.
        if push:
            result = _run_git(["push", "-u", "origin", branch_name], cwd=root)
            if result.returncode != 0:
                log.warning("Push failed: %s", result.stderr)
            elif create_pr:
                try:
                    pr_result = subprocess.run(
                        ["gh", "pr", "create",
                         "--title", f"Add {owner}/{repo} submodule",
                         "--body", f"Auto-added from #github-repos Discord channel.\n\n{description[:500]}"],
                        cwd=root,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    if pr_result.returncode == 0:
                        log.info("PR created: %s", pr_result.stdout.strip())
                except Exception:
                    log.warning("PR creation failed — branch pushed, create PR manually")

        # Return to main.
        _run_git(["checkout", "main"], cwd=root)

        return {
            "status": "added",
            "path": submodule_path,
            "branch": branch_name,
            "owner": owner,
            "repo": repo,
        }

    except Exception as e:
        # Ensure we return to main on any failure.
        try:
            _run_git(["checkout", "main"], cwd=root)
        except Exception:
            pass
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python -m ingestion.submodule_adder <github-url>")
        sys.exit(1)

    url = sys.argv[1]
    result = add_submodule(url, push="--no-push" not in sys.argv, create_pr="--no-pr" not in sys.argv)
    print(result)
