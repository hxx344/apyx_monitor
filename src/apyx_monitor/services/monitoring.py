from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlmodel import Session

from ..collectors import ArbitrageCollector, MorphoCollector, OnChainCollector
from ..collectors.base import BaseCollector, MetricPoint
from ..config import get_asset_catalog, get_rule_catalog, get_settings
from ..db import engine
from ..models import MetricSnapshot
from .alerting import FeishuNotifier
from .rule_engine import NotificationMessage, RuleEngine, RuleEvaluationResult


logger = logging.getLogger(__name__)


class MonitoringService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.asset_catalog = get_asset_catalog()
        self.rule_catalog = get_rule_catalog()
        self.onchain_collector = OnChainCollector(self.settings, self.asset_catalog)
        self.collectors = [
            self.onchain_collector,
            MorphoCollector(self.settings, self.asset_catalog),
            ArbitrageCollector(self.settings, self.asset_catalog),
        ]
        self.rule_engine = RuleEngine(self.rule_catalog, FeishuNotifier(self.settings))
        self._lock = asyncio.Lock()
        self.last_run_at: datetime | None = None
        self.last_run_status: str = "never"
        self.last_errors: dict[str, str] = {}
        self.last_nav_curve_run_at: datetime | None = None
        self.last_nav_curve_status: str = "never"
        self.last_nav_curve_errors: dict[str, str] = {}

    async def poll_once(self) -> dict[str, object]:
        if self._lock.locked():
            return {"status": "skipped", "reason": "poll already in progress"}

        async with self._lock:
            self.last_errors = {}
            all_points, collect_errors = await self._collect_all()
            self.last_errors.update(collect_errors)

            evaluation = await asyncio.to_thread(self._persist_and_evaluate, all_points)
            await self._send_notifications(evaluation.notifications, self.last_errors)

            self.last_run_at = datetime.now(timezone.utc)
            self.last_run_status = "partial_failure" if self.last_errors else "ok"
            return {
                "status": self.last_run_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(evaluation.events),
                "errors": self.last_errors,
                "last_run_at": self.last_run_at.isoformat(),
            }

    async def poll_nav_curve_once(self) -> dict[str, object]:
        if self._lock.locked():
            return {"status": "skipped", "reason": "poll already in progress"}

        async with self._lock:
            self.last_nav_curve_errors = {}
            all_points: list[MetricPoint] = []
            try:
                all_points = await self.onchain_collector.collect_nav_curve()
            except Exception as exc:  # noqa: BLE001
                logger.exception("nav/curve collector failed")
                self.last_nav_curve_errors["nav_curve"] = str(exc)

            evaluation = await asyncio.to_thread(self._persist_and_evaluate, all_points)
            await self._send_notifications(evaluation.notifications, self.last_nav_curve_errors)

            self.last_nav_curve_run_at = datetime.now(timezone.utc)
            self.last_nav_curve_status = "partial_failure" if self.last_nav_curve_errors else "ok"
            return {
                "status": self.last_nav_curve_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(evaluation.events),
                "errors": self.last_nav_curve_errors,
                "last_run_at": self.last_nav_curve_run_at.isoformat(),
            }

    async def _collect_all(self) -> tuple[list[MetricPoint], dict[str, str]]:
        results = []
        for collector in self.collectors:
            results.append(await self._collect_one(collector))
        all_points: list[MetricPoint] = []
        errors: dict[str, str] = {}
        for collector_name, points, error in results:
            all_points.extend(points)
            if error is not None:
                errors[collector_name] = error
        return all_points, errors

    @staticmethod
    async def _collect_one(
        collector: BaseCollector,
    ) -> tuple[str, list[MetricPoint], str | None]:
        try:
            return collector.name, await collector.collect(), None
        except Exception as exc:  # noqa: BLE001
            logger.exception("collector %s failed", collector.name)
            return collector.name, [], str(exc)

    def _persist_and_evaluate(self, all_points: list[MetricPoint]) -> RuleEvaluationResult:
        if not all_points:
            return RuleEvaluationResult(events=[], notifications=[])

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
            evaluation = self.rule_engine.evaluate(session, latest_metrics)
            session.commit()
        return evaluation

    async def _send_notifications(
        self,
        notifications: list[NotificationMessage],
        errors: dict[str, str],
    ) -> None:
        for notification in notifications:
            try:
                await self.rule_engine.notifier.notify(notification.title, notification.body)
            except Exception as exc:  # noqa: BLE001
                logger.exception("alert notification failed")
                errors[f"notification:{notification.title}"] = str(exc)

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
