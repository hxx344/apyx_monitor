from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import get_settings
from .monitoring import MonitoringService


def build_scheduler(service: MonitoringService) -> AsyncIOScheduler:
    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        service.poll_once,
        "interval",
        seconds=settings.collection_interval_seconds,
        id="apyx-monitor-poll",
        max_instances=1,
        coalesce=True,
    )
    return scheduler
