"""Tests for file scanner — pattern matching and exclusion logic."""

from pathlib import Path

from ingestion.file_scanner import _should_include, _get_repo_name, EXCLUDE_DIRS


class TestShouldInclude:
    def test_markdown(self):
        assert _should_include(Path("README.md")) is True

    def test_txt(self):
        assert _should_include(Path("notes.txt")) is True

    def test_claude_md(self):
        assert _should_include(Path("CLAUDE.md")) is True

    def test_agents_md(self):
        assert _should_include(Path("AGENTS.md")) is True

    def test_python_excluded(self):
        assert _should_include(Path("main.py")) is False

    def test_javascript_excluded(self):
        assert _should_include(Path("index.js")) is False

    def test_typescript_excluded(self):
        assert _should_include(Path("app.tsx")) is False

    def test_json_excluded(self):
        assert _should_include(Path("package.json")) is False

    def test_pdf_included(self):
        assert _should_include(Path("guide.pdf")) is True


class TestGetRepoName:
    def test_top_level_file(self):
        root = Path("/repo")
        assert _get_repo_name(Path("/repo/README.md"), root) == "README.md"

    def test_submodule_file(self):
        root = Path("/repo")
        assert _get_repo_name(Path("/repo/manim/README.md"), root) == "manim"

    def test_deep_file(self):
        root = Path("/repo")
        assert _get_repo_name(Path("/repo/claude-code-tips/skills/SKILL.md"), root) == "claude-code-tips"


class TestExcludeDirs:
    def test_git_excluded(self):
        assert ".git" in EXCLUDE_DIRS

    def test_node_modules_excluded(self):
        assert "node_modules" in EXCLUDE_DIRS

    def test_venv_excluded(self):
        assert ".venv" in EXCLUDE_DIRS

    def test_pycache_excluded(self):
        assert "__pycache__" in EXCLUDE_DIRS
