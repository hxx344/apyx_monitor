from __future__ import annotations

from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import get_settings
from .monitoring import MonitoringService


def build_scheduler(service: MonitoringService) -> AsyncIOScheduler:
    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone="UTC")
    nav_curve_offset_seconds = max(1, min(settings.nav_curve_interval_seconds // 2, 10))
    scheduler.add_job(
        service.poll_once,
        "interval",
        seconds=settings.collection_interval_seconds,
        id="apyx-monitor-poll",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        service.poll_nav_curve_once,
        "interval",
        seconds=settings.nav_curve_interval_seconds,
        id="apyx-monitor-nav-curve-poll",
        start_date=datetime.now(timezone.utc) + timedelta(seconds=nav_curve_offset_seconds),
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        service.poll_arbitrage_once,
        "interval",
        seconds=settings.arbitrage_interval_seconds,
        id="apyx-monitor-arbitrage-poll",
        start_date=datetime.now(timezone.utc) + timedelta(seconds=45),
        max_instances=1,
        coalesce=True,
    )
    return scheduler
