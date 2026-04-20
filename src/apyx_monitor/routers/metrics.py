from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, select

from ..db import get_session
from ..models import MetricSnapshot

router = APIRouter(prefix="/api/v1/metrics", tags=["metrics"])


def _serialize_metric(row: MetricSnapshot) -> dict:
    return {
        "entity_id": row.entity_id,
        "entity_type": row.entity_type,
        "metric_name": row.metric_name,
        "value": row.value,
        "unit": row.unit,
        "source": row.source,
        "recorded_at": row.recorded_at,
        "details": json.loads(row.details_json) if row.details_json else {},
    }


@router.get("/latest")
def latest_metrics(
    entity_id: str | None = Query(default=None),
    metric_name: str | None = Query(default=None),
    limit: int = Query(default=200, le=1000),
    session: Session = Depends(get_session),
) -> list[dict]:
    statement = select(MetricSnapshot)
    if entity_id:
        statement = statement.where(MetricSnapshot.entity_id == entity_id)
    if metric_name:
        statement = statement.where(MetricSnapshot.metric_name == metric_name)
    rows = sorted(session.exec(statement).all(), key=lambda row: row.recorded_at, reverse=True)

    latest: dict[tuple[str, str], MetricSnapshot] = {}
    for row in rows:
        key = (row.entity_id, row.metric_name)
        if key not in latest:
            latest[key] = row
        if len(latest) >= limit:
            break

    return [_serialize_metric(row) for row in latest.values()]


@router.get("/history")
def metric_history(
    entity_id: str = Query(...),
    metric_name: str = Query(...),
    limit: int = Query(default=100, le=1000),
    session: Session = Depends(get_session),
) -> list[dict]:
    statement = (
        select(MetricSnapshot)
        .where(MetricSnapshot.entity_id == entity_id, MetricSnapshot.metric_name == metric_name)
        .limit(limit)
    )
    rows = sorted(session.exec(statement).all(), key=lambda row: row.recorded_at, reverse=True)[:limit]
    return [_serialize_metric(row) for row in rows]


@router.get("/trends")
def metric_trends(
    entity_id: str = Query(...),
    metric_name: str = Query(...),
    hours: int = Query(default=24, ge=1, le=24 * 30),
    bucket_minutes: int = Query(default=15, ge=1, le=24 * 60),
    session: Session = Depends(get_session),
) -> dict:
    since_at = datetime.now(timezone.utc) - timedelta(hours=hours)
    statement = select(MetricSnapshot).where(
        MetricSnapshot.entity_id == entity_id,
        MetricSnapshot.metric_name == metric_name,
        MetricSnapshot.recorded_at >= since_at,
    )
    rows = sorted(session.exec(statement).all(), key=lambda row: row.recorded_at)
    if not rows:
        return {
            "entity_id": entity_id,
            "metric_name": metric_name,
            "unit": None,
            "bucket_minutes": bucket_minutes,
            "hours": hours,
            "points": [],
        }

    buckets: dict[datetime, list[MetricSnapshot]] = defaultdict(list)
    interval_seconds = bucket_minutes * 60
    for row in rows:
        timestamp = row.recorded_at
        bucket_ts = int(timestamp.timestamp() // interval_seconds * interval_seconds)
        bucket_at = datetime.fromtimestamp(bucket_ts, tz=timezone.utc)
        buckets[bucket_at].append(row)

    points: list[dict] = []
    for bucket_at in sorted(buckets):
        bucket_rows = buckets[bucket_at]
        values = [row.value for row in bucket_rows]
        points.append(
            {
                "timestamp": bucket_at,
                "value": values[-1],
                "avg": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "count": len(values),
            }
        )

    latest_row = rows[-1]
    return {
        "entity_id": entity_id,
        "entity_type": latest_row.entity_type,
        "metric_name": metric_name,
        "unit": latest_row.unit,
        "source": latest_row.source,
        "bucket_minutes": bucket_minutes,
        "hours": hours,
        "points": points,
    }


@router.get("/catalog")
def metrics_catalog(session: Session = Depends(get_session)) -> dict:
    rows = session.exec(select(MetricSnapshot)).all()
    latest: dict[tuple[str, str], MetricSnapshot] = {}
    for row in sorted(rows, key=lambda item: item.recorded_at, reverse=True):
        key = (row.entity_id, row.metric_name)
        if key not in latest:
            latest[key] = row

    catalog: dict[str, dict] = {}
    for row in latest.values():
        entity = catalog.setdefault(
            row.entity_id,
            {
                "entity_type": row.entity_type,
                "metrics": [],
            },
        )
        entity["metrics"].append(
            {
                "metric_name": row.metric_name,
                "unit": row.unit,
                "source": row.source,
                "recorded_at": row.recorded_at,
            }
        )

    return {"entities": catalog}
