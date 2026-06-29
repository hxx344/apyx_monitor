from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from sqlmodel import Session, desc, select

from ..collectors import (
    AccountableCollector,
    ArbitrageCollector,
    FinnhubStockCollector,
    MorphoCollector,
    OnChainCollector,
)
from ..collectors.accountable import APXUSD_REDEMPTION_ENTITY_ID, REDEMPTION_VALUE_METRIC
from ..collectors.arbitrage import APXUSD_MARKET_ENTITY_ID, APXUSD_PRICE_METRIC
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
        self.arbitrage_collector = ArbitrageCollector(self.settings, self.asset_catalog)
        self.finnhub_stock_collector = FinnhubStockCollector(self.settings, self.asset_catalog)
        self.collectors = [
            self.onchain_collector,
            MorphoCollector(self.settings, self.asset_catalog),
            self.arbitrage_collector,
            AccountableCollector(self.settings),
        ]
        self.rule_engine = RuleEngine(self.rule_catalog, FeishuNotifier(self.settings))
        self._lock = asyncio.Lock()
        self._finnhub_stock_lock = asyncio.Lock()
        self.last_run_at: datetime | None = None
        self.last_run_status: str = "never"
        self.last_errors: dict[str, str] = {}
        self.last_nav_curve_run_at: datetime | None = None
        self.last_nav_curve_status: str = "never"
        self.last_nav_curve_errors: dict[str, str] = {}
        self.last_arbitrage_run_at: datetime | None = None
        self.last_arbitrage_status: str = "never"
        self.last_arbitrage_errors: dict[str, str] = {}
        self.last_finnhub_stock_run_at: datetime | None = None
        self.last_finnhub_stock_status: str = "never"
        self.last_finnhub_stock_errors: dict[str, str] = {}

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
                logger.exception("NAV/Curve 快扫失败")
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

    async def poll_arbitrage_once(
        self,
        wait_for_lock_seconds: float = 120.0,
        force_new_cycle: bool = False,
    ) -> dict[str, object]:
        if self._lock.locked():
            logger.info(
                "闭环套利刷新等待中 │ 原因=已有采集任务正在运行 │ 最长等待=%.0f秒",
                wait_for_lock_seconds,
            )

        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=wait_for_lock_seconds)
        except asyncio.TimeoutError:
            logger.warning(
                "跳过闭环套利刷新 │ 原因=等待采集锁超时 │ 等待=%.0f秒",
                wait_for_lock_seconds,
            )
            return {"status": "skipped", "reason": "poll already in progress"}

        try:
            self.last_arbitrage_errors = {}
            all_points: list[MetricPoint] = []
            try:
                all_points = await self.arbitrage_collector.collect(
                    force=True,
                    reset_refresh_cycle=force_new_cycle,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("闭环套利采集失败")
                self.last_arbitrage_errors["arbitrage"] = str(exc)

            evaluation = await asyncio.to_thread(self._persist_and_evaluate, all_points)
            await self._send_notifications(evaluation.notifications, self.last_arbitrage_errors)

            self.last_arbitrage_run_at = datetime.now(timezone.utc)
            self.last_arbitrage_status = "partial_failure" if self.last_arbitrage_errors else "ok"
            return {
                "status": self.last_arbitrage_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(evaluation.events),
                "errors": self.last_arbitrage_errors,
                "last_run_at": self.last_arbitrage_run_at.isoformat(),
            }
        finally:
            self._lock.release()

    async def poll_finnhub_stock_once(self) -> dict[str, object]:
        if self._finnhub_stock_lock.locked():
            return {"status": "skipped", "reason": "finnhub stock poll already in progress"}

        async with self._finnhub_stock_lock:
            self.last_finnhub_stock_errors = {}
            all_points: list[MetricPoint] = []
            try:
                all_points = await self.finnhub_stock_collector.collect()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Finnhub stock polling failed")
                self.last_finnhub_stock_errors["finnhub_stock"] = str(exc)

            evaluation = await asyncio.to_thread(self._persist_and_evaluate, all_points)
            await self._send_notifications(
                evaluation.notifications,
                self.last_finnhub_stock_errors,
            )

            self.last_finnhub_stock_run_at = datetime.now(timezone.utc)
            self.last_finnhub_stock_status = "partial_failure" if self.last_finnhub_stock_errors else "ok"
            return {
                "status": self.last_finnhub_stock_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(evaluation.events),
                "errors": self.last_finnhub_stock_errors,
                "last_run_at": self.last_finnhub_stock_run_at.isoformat(),
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
            logger.exception("采集器失败 │ 名称=%s", collector.name)
            return collector.name, [], str(exc)

    def _persist_and_evaluate(self, all_points: list[MetricPoint]) -> RuleEvaluationResult:
        if not all_points:
            return RuleEvaluationResult(events=[], notifications=[])

        with Session(engine) as session:
            all_points = self._with_apxusd_redemption_spread(session, all_points)
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
            event_points = [point for point in all_points if point.details.get("alert_fingerprint")]
            latest_metrics = self._latest_metric_map(
                [point for point in all_points if not point.details.get("alert_fingerprint")]
            )
            evaluation = self.rule_engine.evaluate(session, latest_metrics)
            for point in event_points:
                event_evaluation = self.rule_engine.evaluate(
                    session,
                    {(point.entity_id, point.metric_name): self._metric_payload(point)},
                )
                evaluation.events.extend(event_evaluation.events)
                evaluation.notifications.extend(event_evaluation.notifications)
            session.commit()
        return evaluation

    @staticmethod
    def _with_apxusd_redemption_spread(
        session: Session,
        points: list[MetricPoint],
    ) -> list[MetricPoint]:
        price_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_MARKET_ENTITY_ID,
            APXUSD_PRICE_METRIC,
        )
        redemption_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_REDEMPTION_ENTITY_ID,
            REDEMPTION_VALUE_METRIC,
        )
        if price_point is None:
            price_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_MARKET_ENTITY_ID,
                APXUSD_PRICE_METRIC,
            )
            if price_snapshot is not None:
                price_point = MonitoringService._point_from_snapshot(price_snapshot)
        if redemption_point is None:
            redemption_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_REDEMPTION_ENTITY_ID,
                REDEMPTION_VALUE_METRIC,
            )
            if redemption_snapshot is not None:
                redemption_point = MonitoringService._point_from_snapshot(redemption_snapshot)
        if price_point is None or redemption_point is None:
            return points

        price_recorded_at = MonitoringService._as_utc(price_point.recorded_at)
        redemption_recorded_at = MonitoringService._as_utc(redemption_point.recorded_at)
        recorded_at = max(price_recorded_at, redemption_recorded_at)
        spread = price_point.value - redemption_point.value
        return [
            *points,
            MetricPoint(
                entity_id=APXUSD_MARKET_ENTITY_ID,
                entity_type="market_price",
                metric_name="price_vs_redemption_spread_usd",
                value=spread,
                unit="usd",
                source="derived",
                recorded_at=recorded_at,
                details={
                    "price_usd": price_point.value,
                    "redemption_value_usd": redemption_point.value,
                    "price_recorded_at": price_recorded_at.isoformat(),
                    "redemption_recorded_at": redemption_recorded_at.isoformat(),
                },
            ),
        ]

    @staticmethod
    def _latest_point_for_metric(
        points: list[MetricPoint],
        entity_id: str,
        metric_name: str,
    ) -> MetricPoint | None:
        candidates = [
            point
            for point in points
            if point.entity_id == entity_id and point.metric_name == metric_name
        ]
        return max(candidates, key=lambda point: point.recorded_at, default=None)

    @staticmethod
    def _latest_snapshot(
        session: Session,
        entity_id: str,
        metric_name: str,
    ) -> MetricSnapshot | None:
        return session.exec(
            select(MetricSnapshot)
            .where(MetricSnapshot.entity_id == entity_id)
            .where(MetricSnapshot.metric_name == metric_name)
            .order_by(desc(MetricSnapshot.recorded_at), desc(MetricSnapshot.id))
            .limit(1)
        ).first()

    @staticmethod
    def _point_from_snapshot(snapshot: MetricSnapshot) -> MetricPoint:
        details = {}
        if snapshot.details_json:
            try:
                payload = json.loads(snapshot.details_json)
                if isinstance(payload, dict):
                    details = payload
            except json.JSONDecodeError:
                pass
        return MetricPoint(
            entity_id=snapshot.entity_id,
            entity_type=snapshot.entity_type,
            metric_name=snapshot.metric_name,
            value=snapshot.value,
            unit=snapshot.unit,
            source=snapshot.source,
            recorded_at=MonitoringService._as_utc(snapshot.recorded_at),
            details=details,
        )

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    async def _send_notifications(
        self,
        notifications: list[NotificationMessage],
        errors: dict[str, str],
    ) -> None:
        for notification in notifications:
            try:
                await self.rule_engine.notifier.notify(notification.title, notification.body)
            except Exception as exc:  # noqa: BLE001
                logger.exception("告警通知发送失败")
                errors[f"notification:{notification.title}"] = str(exc)

    @staticmethod
    def _latest_metric_map(points: list[MetricPoint]) -> dict[tuple[str, str], dict]:
        latest: dict[tuple[str, str], dict] = {}
        for point in points:
            key = (point.entity_id, point.metric_name)
            existing = latest.get(key)
            if existing is None or point.recorded_at >= existing["recorded_at"]:
                latest[key] = MonitoringService._metric_payload(point)
        return latest

    @staticmethod
    def _metric_payload(point: MetricPoint) -> dict:
        return {
            "value": point.value,
            "unit": point.unit,
            "source": point.source,
            "recorded_at": point.recorded_at,
            "details": point.details,
        }
