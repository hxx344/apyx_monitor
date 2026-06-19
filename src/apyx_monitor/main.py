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
    try:
        yield
    finally:
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
