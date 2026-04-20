from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, select

from ..db import get_session
from ..models import AlertEvent

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("")
def list_alerts(
    status: str | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
    session: Session = Depends(get_session),
) -> list[dict]:
    statement = select(AlertEvent)
    if status:
        statement = statement.where(AlertEvent.status == status)
    rows = sorted(
        session.exec(statement).all(),
        key=lambda row: row.last_triggered_at,
        reverse=True,
    )[:limit]
    return [
        {
            "id": row.id,
            "rule_id": row.rule_id,
            "entity_id": row.entity_id,
            "metric_name": row.metric_name,
            "severity": row.severity,
            "status": row.status,
            "summary": row.summary,
            "current_value": row.current_value,
            "threshold": row.threshold,
            "occurrences": row.occurrences,
            "first_triggered_at": row.first_triggered_at,
            "last_triggered_at": row.last_triggered_at,
            "resolved_at": row.resolved_at,
            "details": json.loads(row.details_json) if row.details_json else {},
        }
        for row in rows
    ]
