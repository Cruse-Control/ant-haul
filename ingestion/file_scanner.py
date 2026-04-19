"""File scanner — stage documentation files from local directories.

Walks a directory tree, stages markdown/text/PDF files as documents,
and creates repo_index entries for each top-level directory/submodule.

Skips code files, node_modules, .git, etc. The knowledge graph is for
discovery — agents read code on demand once relevance is determined.

Run as: python -m ingestion.file_scanner [--dry-run] [path]
Default path: /home/flynn-cruse/Code/CruseControl/inspirational-materials
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from seed_storage import staging

log = logging.getLogger("file_scanner")

DEFAULT_ROOT = "/home/flynn-cruse/Code/CruseControl/inspirational-materials"

INCLUDE_EXTENSIONS = {".md", ".txt"}
INCLUDE_NAMES = {"README.md", "CLAUDE.md", "AGENTS.md", "SKILL.md", "STATUS.md"}

EXCLUDE_DIRS = {
    ".git", "node_modules", "__pycache__", ".venv", "venv",
    "dist", "build", ".next", ".cache", "target", ".mypy_cache",
    ".pytest_cache", ".tox", "egg-info", ".eggs", "site-packages",
}

MAX_FILE_SIZE = 100_000  # 100KB — skip generated docs / data dumps


def _should_include(path: Path) -> bool:
    """Check if a file should be staged."""
    if path.suffix.lower() in INCLUDE_EXTENSIONS:
        return True
    if path.name in INCLUDE_NAMES:
        return True
    if path.suffix.lower() == ".pdf":
        return True
    return False


def _get_repo_name(file_path: Path, root: Path) -> str:
    """Extract the top-level submodule/directory name."""
    rel = file_path.relative_to(root)
    parts = rel.parts
    return parts[0] if parts else ""


def scan_directory(root: str = DEFAULT_ROOT, dry_run: bool = False) -> tuple[int, int, int]:
    """Scan a directory tree and stage documentation files.

    Returns (staged, skipped_dedup, skipped_other).
    """
    root_path = Path(root).resolve()
    if not root_path.is_dir():
        log.error("Not a directory: %s", root)
        return 0, 0, 0

    staging.init_tables()
    staged = 0
    skipped_dedup = 0
    skipped_other = 0

    for dirpath, dirnames, filenames in os.walk(root_path):
        # Prune excluded directories.
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]

        for filename in filenames:
            file_path = Path(dirpath) / filename

            if not _should_include(file_path):
                continue

            # Size check.
            try:
                size = file_path.stat().st_size
            except OSError:
                continue
            if size > MAX_FILE_SIZE or size == 0:
                skipped_other += 1
                continue

            # Read content.
            try:
                if file_path.suffix.lower() == ".pdf":
                    text = _read_pdf(file_path)
                else:
                    text = file_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                log.debug("Could not read %s", file_path)
                skipped_other += 1
                continue

            if not text.strip():
                skipped_other += 1
                continue

            rel_path = str(file_path.relative_to(root_path))
            repo_name = _get_repo_name(file_path, root_path)
            source_uri = f"file://{file_path}"

            if dry_run:
                log.info("[dry-run] %s (%d words, repo=%s)", rel_path, len(text.split()), repo_name)
                staged += 1
                continue

            sid = staging.stage(
                source_type="document",
                source_uri=source_uri,
                raw_content=text,
                author="",
                channel="inspirational-materials",
                metadata={
                    "file_path": rel_path,
                    "repo": repo_name,
                    "file_type": file_path.suffix.lower(),
                },
            )
            if sid:
                staged += 1
                log.debug("Staged: %s", rel_path)
            else:
                skipped_dedup += 1

    log.info("Scan complete: %d staged, %d dedup, %d skipped", staged, skipped_dedup, skipped_other)
    return staged, skipped_dedup, skipped_other


def scan_repo_index(root: str = DEFAULT_ROOT, dry_run: bool = False) -> int:
    """Stage repo_index entries from index-of-inspiration.md."""
    index_path = Path(root) / "index-of-inspiration.md"
    if not index_path.exists():
        log.warning("No index-of-inspiration.md found at %s", root)
        return 0

    text = index_path.read_text(encoding="utf-8", errors="replace")

    # Parse sections: ### dir_name/ followed by description.
    import re
    sections = re.split(r"^### ", text, flags=re.MULTILINE)
    count = 0

    for section in sections[1:]:  # Skip header.
        lines = section.strip().split("\n")
        if not lines:
            continue

        header = lines[0].strip()
        # Extract directory name from header like "openclaw-tips/" or "manim/"
        dir_match = re.match(r"([\w\-./]+/?)", header)
        if not dir_match:
            continue

        dir_name = dir_match.group(1).rstrip("/")
        description = "\n".join(lines[1:]).strip()
        if not description:
            continue

        dir_path = Path(root) / dir_name
        source_uri = f"repo://{dir_name}"

        if dry_run:
            log.info("[dry-run repo] %s (%d words)", dir_name, len(description.split()))
            count += 1
            continue

        sid = staging.stage(
            source_type="repo_index",
            source_uri=source_uri,
            raw_content=f"# {dir_name}\n\n{description}",
            author="",
            channel="inspirational-materials",
            metadata={
                "repo_path": str(dir_path) if dir_path.exists() else "",
                "repo": dir_name.split("/")[0],
            },
        )
        if sid:
            count += 1

    log.info("Repo index: %d entries staged", count)
    return count


def _read_pdf(path: Path) -> str:
    """Extract text from a PDF file."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(path))
        text = "\n\n".join(page.get_text() for page in doc)
        doc.close()
        return text
    except ImportError:
        log.debug("PyMuPDF not installed, skipping PDF: %s", path)
        return ""
    except Exception:
        log.debug("Could not read PDF: %s", path)
        return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    dry_run = "--dry-run" in sys.argv
    root = sys.argv[-1] if len(sys.argv) > 1 and not sys.argv[-1].startswith("-") else DEFAULT_ROOT

    scan_repo_index(root, dry_run=dry_run)
    scan_directory(root, dry_run=dry_run)
