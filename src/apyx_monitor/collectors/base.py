from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class MetricPoint:
    entity_id: str
    entity_type: str
    metric_name: str
    value: float
    unit: str
    source: str
    recorded_at: datetime = field(default_factory=utc_now)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["recorded_at"] = self.recorded_at.isoformat()
        return payload


class BaseCollector(ABC):
    name: str

    @abstractmethod
    async def collect(self) -> list[MetricPoint]:
        raise NotImplementedError
