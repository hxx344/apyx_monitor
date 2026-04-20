from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlmodel import Session

from ..collectors import MorphoCollector, OnChainCollector, PendleCollector
from ..collectors.base import MetricPoint
from ..config import get_asset_catalog, get_rule_catalog, get_settings
from ..db import engine
from ..models import MetricSnapshot
from .alerting import FeishuNotifier
from .rule_engine import RuleEngine


logger = logging.getLogger(__name__)


class MonitoringService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.asset_catalog = get_asset_catalog()
        self.rule_catalog = get_rule_catalog()
        self.collectors = [
            OnChainCollector(self.settings, self.asset_catalog),
            PendleCollector(self.settings, self.asset_catalog),
            MorphoCollector(self.settings, self.asset_catalog),
        ]
        self.rule_engine = RuleEngine(self.rule_catalog, FeishuNotifier(self.settings))
        self._lock = asyncio.Lock()
        self.last_run_at: datetime | None = None
        self.last_run_status: str = "never"
        self.last_errors: dict[str, str] = {}

    async def poll_once(self) -> dict[str, object]:
        if self._lock.locked():
            return {"status": "skipped", "reason": "poll already in progress"}

        async with self._lock:
            all_points: list[MetricPoint] = []
            self.last_errors = {}
            for collector in self.collectors:
                try:
                    points = await collector.collect()
                    all_points.extend(points)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("collector %s failed", collector.name)
                    self.last_errors[collector.name] = str(exc)

            with Session(engine) as session:
                for point in all_points:
                    session.add(
                        MetricSnapshot(
                            entity_id=point.entity_id,
                            entity_type=point.entity_type,
                            metric_name=point.metric_name,
                            value=point.value,
                            unit=point.unit,
                            source=point.source,
                            recorded_at=point.recorded_at,
                            details_json=json.dumps(point.details, ensure_ascii=False),
                        )
                    )
                latest_metrics = self._latest_metric_map(all_points)
                alerts = await self.rule_engine.evaluate(session, latest_metrics)
                session.commit()

            self.last_run_at = datetime.now(timezone.utc)
            self.last_run_status = "partial_failure" if self.last_errors else "ok"
            return {
                "status": self.last_run_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(alerts),
                "errors": self.last_errors,
                "last_run_at": self.last_run_at.isoformat(),
            }

    @staticmethod
    def _latest_metric_map(points: list[MetricPoint]) -> dict[tuple[str, str], dict]:
        latest: dict[tuple[str, str], dict] = {}
        for point in points:
            key = (point.entity_id, point.metric_name)
            existing = latest.get(key)
            if existing is None or point.recorded_at >= existing["recorded_at"]:
                latest[key] = {
                    "value": point.value,
                    "unit": point.unit,
                    "source": point.source,
                    "recorded_at": point.recorded_at,
                    "details": point.details,
                }
        return latest
