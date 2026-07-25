from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from .app_logging import configure_logging, uvicorn_log_config
from .db import init_db
from .routers import alerts_router, dashboard_router, health_router, jobs_router, metrics_router
from .services import MonitoringService, build_scheduler


configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    service = MonitoringService()
    scheduler = build_scheduler(service)
    app.state.monitoring_service = service
    app.state.scheduler = scheduler
    scheduler.start()
    asyncio.create_task(service.poll_once())
    asyncio.create_task(service.poll_pool_arbitrage_once())
    pool_arbitrage_watch_task = (
        asyncio.create_task(service.watch_pool_arbitrage_events())
        if service.pool_arbitrage_ws_url
        else None
    )
    try:
        yield
    finally:
        if pool_arbitrage_watch_task is not None:
            pool_arbitrage_watch_task.cancel()
            await asyncio.gather(pool_arbitrage_watch_task, return_exceptions=True)
        scheduler.shutdown(wait=False)


app = FastAPI(title="APYX Monitor MVP", version="0.1.0", lifespan=lifespan)
app.include_router(dashboard_router)
app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(alerts_router)
app.include_router(jobs_router)


def run() -> None:
    uvicorn.run(
        "apyx_monitor.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_config=uvicorn_log_config(),
    )


if __name__ == "__main__":
    run()
