from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, Text
from sqlmodel import Field, SQLModel


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MetricSnapshot(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    entity_id: str = Field(index=True)
    entity_type: str = Field(index=True)
    metric_name: str = Field(index=True)
    value: float
    unit: str = Field(default="")
    source: str = Field(index=True)
    recorded_at: datetime = Field(index=True)
    details_json: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    created_at: datetime = Field(default_factory=utc_now, index=True)


class AlertEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    rule_id: str = Field(index=True)
    entity_id: str = Field(index=True)
    metric_name: str = Field(index=True)
    fingerprint: str = Field(index=True)
    severity: str = Field(index=True)
    comparator: str
    threshold: float
    current_value: float
    summary: str
    status: str = Field(default="firing", index=True)
    occurrences: int = Field(default=1)
    first_triggered_at: datetime = Field(default_factory=utc_now, index=True)
    last_triggered_at: datetime = Field(default_factory=utc_now, index=True)
    resolved_at: Optional[datetime] = Field(default=None, index=True)
    notified_at: Optional[datetime] = Field(default=None)
    details_json: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
