"""Entry point for the Discord watcher daemon.

Run as: python -m ingestion.run_watcher
"""

import asyncio
import logging
import os
import signal

import uvicorn
from fastapi import FastAPI

from ingestion.watcher import start_watcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("ant-food-watcher")

health_app = FastAPI()


@health_app.get("/health")
async def health():
    from ingestion.watcher import _pipeline
    return {
        "status": "healthy",
        "pipeline_queue_depth": _pipeline.queue_depth if _pipeline else 0,
        "pipeline_enabled": _pipeline is not None,
    }


async def _run_health_server():
    config = uvicorn.Config(health_app, host="0.0.0.0", port=8081, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    log.info("Starting ant-food-watcher (seed-storage ingestion)")
    loop = asyncio.get_event_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    health_task = asyncio.create_task(_run_health_server())
    watcher_task = asyncio.create_task(start_watcher())

    await stop.wait()
    log.info("Shutting down")

    # Gracefully stop pipeline worker (drain queue, post final summary)
    from ingestion.watcher import _pipeline
    if _pipeline:
        await _pipeline.stop()

    health_task.cancel()
    watcher_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
