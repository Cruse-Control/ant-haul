"""Tests for submodule_adder — URL parsing, path logic, duplicate detection."""

from ingestion.submodule_adder import (
    _parse_github_url,
    _repo_already_exists,
    _determine_path,
    _build_index_entry,
    _find_index_section,
    INSPIRATIONAL_ROOT,
)


class TestParseGithubUrl:
    def test_standard(self):
        assert _parse_github_url("https://github.com/anthropics/claude-code") == ("anthropics", "claude-code")

    def test_with_git_suffix(self):
        assert _parse_github_url("https://github.com/foo/bar.git") == ("foo", "bar")

    def test_with_path(self):
        assert _parse_github_url("https://github.com/foo/bar/tree/main/src") == ("foo", "bar")

    def test_invalid(self):
        assert _parse_github_url("https://example.com") == ("", "")

    def test_trailing_slash(self):
        assert _parse_github_url("https://github.com/foo/bar/") == ("foo", "bar")


class TestDeterminePath:
    def test_known_owner_karpathy(self):
        assert _determine_path("karpathy", "autoresearch") == "Karpathy/autoresearch"

    def test_known_owner_openai(self):
        assert _determine_path("openai", "symphony") == "OpenAI/symphony"

    def test_known_owner_nvidia(self):
        assert _determine_path("NVIDIA", "NemoClaw") == "NVIDIA/NemoClaw"

    def test_unknown_owner_top_level(self):
        assert _determine_path("random-user", "cool-project") == "cool-project"

    def test_langchain(self):
        assert _determine_path("langchain-ai", "deepagents") == "langchain/deepagents"


class TestRepoAlreadyExists:
    def test_existing_repo(self):
        # manim is already a submodule.
        assert _repo_already_exists(INSPIRATIONAL_ROOT, "3b1b", "manim") is True

    def test_nonexistent_repo(self):
        assert _repo_already_exists(INSPIRATIONAL_ROOT, "nonexistent", "fake-repo-xyz") is False

    def test_case_insensitive(self):
        # GitHub URLs are case-insensitive.
        assert _repo_already_exists(INSPIRATIONAL_ROOT, "3B1B", "manim") is True


class TestBuildIndexEntry:
    def test_with_description(self):
        entry = _build_index_entry("foo", "bar", "bar", "A cool project.")
        assert "### bar/" in entry
        assert "A cool project." in entry

    def test_without_description(self):
        entry = _build_index_entry("foo", "bar", "bar", "")
        assert "### bar/" in entry
        assert "github.com/foo/bar" in entry

    def test_long_description_truncated(self):
        long_desc = "x" * 2000
        entry = _build_index_entry("foo", "bar", "bar", long_desc)
        assert len(entry) < 1200


class TestFindIndexSection:
    def test_known_owner(self):
        assert _find_index_section("", "karpathy", "test") == "LLM Training & Research"

    def test_unknown_defaults(self):
        assert _find_index_section("", "random", "test") == "Platform & Tooling References"
