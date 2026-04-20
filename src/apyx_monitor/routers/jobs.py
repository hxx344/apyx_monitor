from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


@router.post("/poll")
async def run_poll(request: Request) -> dict:
    service = request.app.state.monitoring_service
    return await service.poll_once()
