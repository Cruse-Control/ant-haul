"""seed_storage/worker/app.py — Celery app + queue routing + beat schedule.

Two queues:
  - raw_messages  — inbound Discord messages waiting to be enriched
  - graph_ingest  — enriched payloads waiting to be written to Graphiti

Beat schedule:
  - scan_frontier — runs every 60 s when FRONTIER_AUTO_ENABLED=True
"""

from __future__ import annotations

from celery import Celery
from celery.schedules import crontab  # noqa: F401 — available for callers
from kombu import Exchange, Queue

from seed_storage.config import settings

# ---------------------------------------------------------------------------
# Celery application
# ---------------------------------------------------------------------------

app = Celery("seed_storage")

app.config_from_object(
    {
        # Broker + backend
        "broker_url": settings.REDIS_URL,
        "result_backend": settings.REDIS_URL,
        # Serialization
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        # Timezone
        "timezone": "UTC",
        "enable_utc": True,
        # Task routing
        "task_routes": {
            "seed_storage.worker.tasks.enrich_message": {"queue": "raw_messages"},
            "seed_storage.worker.tasks.ingest_episode": {"queue": "graph_ingest"},
            "seed_storage.worker.tasks.expand_from_frontier": {"queue": "graph_ingest"},
            "seed_storage.worker.tasks.scan_frontier": {"queue": "graph_ingest"},
        },
        # Queue definitions
        "task_queues": (
            Queue("raw_messages", Exchange("raw_messages"), routing_key="raw_messages"),
            Queue("graph_ingest", Exchange("graph_ingest"), routing_key="graph_ingest"),
        ),
        "task_default_queue": "raw_messages",
        # Retry behaviour
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
        # Beat schedule — scan_frontier every 60 s
        "beat_schedule": {
            "scan-frontier-every-60s": {
                "task": "seed_storage.worker.tasks.scan_frontier",
                "schedule": 60.0,
                "options": {"queue": "graph_ingest"},
            },
        },
        # Worker concurrency is read from settings but the *actual* Celery workers
        # are launched by supervisord with --concurrency flags.  The values here
        # are used as defaults when launching workers programmatically.
        "worker_concurrency": settings.WORKER_CONCURRENCY_GRAPH,
    }
)

# Autodiscover tasks in the worker package
app.autodiscover_tasks(["seed_storage.worker"])
