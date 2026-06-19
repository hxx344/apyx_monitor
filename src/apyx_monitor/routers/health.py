from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(tags=["health"])


@router.get("/healthz")
async def healthz(request: Request) -> dict:
    service = request.app.state.monitoring_service
    return {
        "status": service.last_run_status,
        "last_run_at": service.last_run_at.isoformat() if service.last_run_at else None,
        "collector_errors": service.last_errors,
        "nav_curve_status": service.last_nav_curve_status,
        "nav_curve_last_run_at": (
            service.last_nav_curve_run_at.isoformat() if service.last_nav_curve_run_at else None
        ),
        "nav_curve_errors": service.last_nav_curve_errors,
        "arbitrage_status": service.last_arbitrage_status,
        "arbitrage_last_run_at": (
            service.last_arbitrage_run_at.isoformat()
            if service.last_arbitrage_run_at
            else None
        ),
        "arbitrage_errors": service.last_arbitrage_errors,
    }
