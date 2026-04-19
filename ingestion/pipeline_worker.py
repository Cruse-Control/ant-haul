"""Event-driven pipeline worker — processes URLs end-to-end as they arrive.

Runs inside the watcher daemon. Uses an asyncio.Queue with a pool of worker
coroutines that call express_ingest() for each item. Posts batch summaries
to Discord after a quiet period.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

from ingestion import discord_touch
from seed_storage import staging
from seed_storage.config import DISCORD_OPS_ALERTS_CHANNEL

log = logging.getLogger("pipeline_worker")

SUMMARY_QUIET_SECONDS = 60  # Post summary after this many seconds of no completions
MAX_SUMMARY_ITEMS = 10      # Show at most this many individual items in summary


@dataclass
class WorkItem:
    """An item to process through the pipeline."""
    item_id: str
    source_uri: str
    source_type: str
    author: str
    channel: str


@dataclass
class WorkResult:
    """Result of processing one item."""
    source_uri: str
    source_type: str
    status: str         # loaded, failed, already_loaded, error
    elapsed: float = 0
    error: str = ""


class PipelineWorker:
    """Async queue + worker pool for real-time URL processing."""

    def __init__(self, concurrency: int = 3):
        self._queue: asyncio.Queue[WorkItem] = asyncio.Queue(maxsize=500)
        self._concurrency = concurrency
        self._workers: list[asyncio.Task] = []
        self._summary_task: asyncio.Task | None = None
        self._running = False
        self._results: list[WorkResult] = []
        self._results_lock = asyncio.Lock()
        self._last_completion = 0.0
        self._batch_start = 0.0

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    async def start(self):
        """Start worker coroutines and summary reporter."""
        self._running = True
        for i in range(self._concurrency):
            task = asyncio.create_task(self._worker(i), name=f"pipeline-worker-{i}")
            self._workers.append(task)
        self._summary_task = asyncio.create_task(self._summary_reporter(), name="pipeline-summary")
        log.info("Pipeline worker started (concurrency=%d)", self._concurrency)

    async def stop(self):
        """Drain queue and shut down workers."""
        self._running = False
        # Drain remaining items
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break
        for task in self._workers:
            task.cancel()
        if self._summary_task:
            self._summary_task.cancel()
        # Post final summary if any results pending
        await self._post_summary()
        log.info("Pipeline worker stopped")

    def enqueue(self, item: dict) -> bool:
        """Add a staged item to the processing queue. Returns False if queue is full."""
        try:
            work = WorkItem(
                item_id=str(item["id"]),
                source_uri=item["source_uri"],
                source_type=item["source_type"],
                author=item.get("author", "unknown"),
                channel=item.get("channel", "unknown"),
            )
            self._queue.put_nowait(work)
            log.info("Queued [%s] %s (depth=%d)", work.source_type, work.source_uri[:60], self.queue_depth)
            return True
        except asyncio.QueueFull:
            log.warning("Queue full — item will be picked up by cron: %s", item.get("source_uri", "?")[:60])
            return False

    async def _worker(self, worker_id: int):
        """Worker coroutine — pulls from queue, runs express_ingest."""
        while self._running:
            try:
                work = await asyncio.wait_for(self._queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return

            # Check circuit breaker
            breaker = staging.is_breaker_tripped()
            if breaker:
                log.warning("Worker %d: circuit breaker tripped (%s) — skipping %s",
                            worker_id, breaker["reason"][:40], work.source_uri[:40])
                self._queue.task_done()
                continue

            t0 = time.monotonic()
            if not self._batch_start:
                self._batch_start = t0

            try:
                from ingestion.express import express_ingest
                result = await express_ingest(
                    url=work.source_uri,
                    author=work.author,
                    channel=work.channel,
                )
                elapsed = round(time.monotonic() - t0, 1)
                wr = WorkResult(
                    source_uri=work.source_uri,
                    source_type=work.source_type,
                    status=result.get("status", "unknown"),
                    elapsed=elapsed,
                )
                log.info("Worker %d: [%s] %s → %s (%.1fs)",
                         worker_id, work.source_type, work.source_uri[:50], wr.status, elapsed)

            except Exception as exc:
                elapsed = round(time.monotonic() - t0, 1)
                wr = WorkResult(
                    source_uri=work.source_uri,
                    source_type=work.source_type,
                    status="error",
                    elapsed=elapsed,
                    error=str(exc)[:200],
                )
                log.exception("Worker %d failed for %s", worker_id, work.source_uri[:50])

            async with self._results_lock:
                self._results.append(wr)
                self._last_completion = time.monotonic()

            self._queue.task_done()

    async def _summary_reporter(self):
        """Posts a summary to Discord after a quiet period."""
        while self._running:
            try:
                await asyncio.sleep(10)

                async with self._results_lock:
                    if not self._results:
                        continue
                    if time.monotonic() - self._last_completion < SUMMARY_QUIET_SECONDS:
                        continue
                    # Quiet period elapsed — post summary
                    results = self._results.copy()
                    self._results.clear()
                    batch_start = self._batch_start
                    self._batch_start = 0.0

                await self._post_summary_for(results, batch_start)

            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("Summary reporter error")

    async def _post_summary(self):
        """Post whatever results are pending (used during shutdown)."""
        async with self._results_lock:
            if not self._results:
                return
            results = self._results.copy()
            self._results.clear()
            batch_start = self._batch_start
        await self._post_summary_for(results, batch_start)

    async def _post_summary_for(self, results: list[WorkResult], batch_start: float):
        """Format and post a summary to Discord."""
        if not results:
            return

        loaded = [r for r in results if r.status == "loaded"]
        failed = [r for r in results if r.status in ("failed", "error")]
        skipped = [r for r in results if r.status in ("already_loaded", "deduped")]
        total_time = round(time.monotonic() - batch_start, 1) if batch_start else 0

        header = f"**{len(loaded)}** loaded, **{len(failed)}** failed, **{len(skipped)}** skipped ({total_time}s)"
        lines = [header, ""]

        shown = 0
        for r in results[:MAX_SUMMARY_ITEMS]:
            lines.append(_format_item(r))
            shown += 1

        remaining = len(results) - shown
        if remaining > 0:
            lines.append(f"  ...and {remaining} more")

        color = 0x00FF00 if not failed else 0xFF8C00
        title = "Pipeline summary" if not failed else "Pipeline summary (with failures)"

        await discord_touch.alert(
            DISCORD_OPS_ALERTS_CHANNEL,
            title,
            "\n".join(lines),
            color=color,
        )
        log.info("Posted pipeline summary: %d loaded, %d failed, %d skipped", len(loaded), len(failed), len(skipped))


def _format_item(r: WorkResult) -> str:
    """Format a single WorkResult as a compact, readable line."""
    icon = {"loaded": "🧠", "failed": "❌", "error": "❌", "already_loaded": "🔁", "deduped": "🔁"}.get(r.status, "❓")

    if r.source_uri.startswith("discord://"):
        # Plain text — show content preview
        item = staging.get_by_uri(r.source_uri)
        preview = ""
        if item and item.get("raw_content"):
            preview = item["raw_content"][:50].replace("\n", " ")
            if len(item["raw_content"]) > 50:
                preview += "…"
            preview = f'"{preview}"'
        else:
            preview = r.source_uri.split("/")[-1]  # msg ID fallback
        line = f"{icon} text | {preview}"
    else:
        # URL — show domain + path
        parsed = urlparse(r.source_uri)
        domain = parsed.netloc.replace("www.", "")
        path = parsed.path.rstrip("/")
        display = f"{domain}{path}"
        if len(display) > 60:
            display = display[:57] + "…"
        line = f"{icon} {r.source_type} | {display}"

    if r.error:
        line += f" — {r.error[:30]}"
    return line
