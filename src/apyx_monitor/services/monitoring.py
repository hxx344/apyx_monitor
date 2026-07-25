from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import delete
from sqlmodel import Session, desc, select
from websockets.asyncio.client import connect

from ..collectors import (
    AccountableCollector,
    ArbitrageCollector,
    MorphoCollector,
    OnChainCollector,
    PoolArbitrageCollector,
)
from ..collectors.accountable import APXUSD_REDEMPTION_ENTITY_ID, REDEMPTION_VALUE_METRIC
from ..collectors.arbitrage import (
    APXUSD_BUY_PRICE_METRIC,
    APXUSD_MARKET_ENTITY_ID,
    APXUSD_PRICE_METRIC,
    APXUSD_SELL_PRICE_METRIC,
)
from ..collectors.base import BaseCollector, MetricPoint
from ..config import get_asset_catalog, get_rule_catalog, get_settings
from ..db import engine
from ..models import MetricSnapshot, MonitorControl
from .alerting import FeishuNotifier
from .rule_engine import NotificationMessage, RuleEngine, RuleEvaluationResult

logger = logging.getLogger(__name__)

REDEMPTION_SPREAD_PCT_METRIC = "price_vs_redemption_spread_pct"
REDEMPTION_BUY_SPREAD_PCT_METRIC = "buy_price_vs_redemption_spread_pct"
REDEMPTION_SELL_SPREAD_PCT_METRIC = "sell_price_vs_redemption_spread_pct"
REDEMPTION_MA_WINDOWS = {
    "price_vs_redemption_spread_pct_ma_12h": 12,
    "price_vs_redemption_spread_pct_ma_24h": 24,
    "price_vs_redemption_spread_pct_ma_72h": 72,
    "price_vs_redemption_spread_pct_ma_7d": 24 * 7,
}
CROSSCHAIN_ARBITRAGE_MONITOR_ID = "crosschain_arbitrage"


class MonitoringService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.asset_catalog = get_asset_catalog()
        self.rule_catalog = get_rule_catalog()
        self.onchain_collector = OnChainCollector(self.settings, self.asset_catalog)
        self.arbitrage_collector = ArbitrageCollector(self.settings, self.asset_catalog)
        self.pool_arbitrage_collector = PoolArbitrageCollector(self.settings, self.asset_catalog)
        self.collectors = [
            self.onchain_collector,
            MorphoCollector(self.settings, self.asset_catalog),
            AccountableCollector(self.settings),
        ]
        self.rule_engine = RuleEngine(self.rule_catalog, FeishuNotifier(self.settings))
        self._lock = asyncio.Lock()
        self._pool_arbitrage_lock = asyncio.Lock()
        self._last_pool_arbitrage_block: tuple[int, str] | None = None
        self.last_run_at: datetime | None = None
        self.last_run_status: str = "never"
        self.last_errors: dict[str, str] = {}
        self.last_nav_curve_run_at: datetime | None = None
        self.last_nav_curve_status: str = "never"
        self.last_nav_curve_errors: dict[str, str] = {}
        self.last_arbitrage_run_at: datetime | None = None
        self.last_arbitrage_status: str = "never"
        self.last_arbitrage_errors: dict[str, str] = {}
        self.last_pool_arbitrage_run_at: datetime | None = None
        self.last_pool_arbitrage_status: str = "never"
        self.last_pool_arbitrage_errors: dict[str, str] = {}
        self._last_metric_retention_cleanup_at: datetime | None = None

    def is_crosschain_arbitrage_enabled(self) -> bool:
        with Session(engine) as session:
            control = session.exec(
                select(MonitorControl).where(
                    MonitorControl.monitor_id == CROSSCHAIN_ARBITRAGE_MONITOR_ID
                )
            ).first()
        if control is None:
            return self.settings.crosschain_arbitrage_enabled
        return control.enabled

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
        if not self.is_crosschain_arbitrage_enabled():
            self.last_arbitrage_status = "disabled"
            return {"status": "disabled", "reason": "cross-chain arbitrage monitoring is off"}

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

    @property
    def pool_arbitrage_ws_url(self) -> str | None:
        configured = (self.settings.ethereum_ws_url or "").strip()
        if configured:
            return configured
        ethereum = self.asset_catalog.chain_map().get("ethereum")
        if ethereum is None:
            return None
        rpc_url = ethereum.resolve_rpc_url()
        parsed = urlsplit(rpc_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return None
        if not parsed.hostname.endswith(".alchemy.com"):
            return None
        return urlunsplit(("wss", parsed.netloc, parsed.path, parsed.query, parsed.fragment))

    async def watch_pool_arbitrage_blocks(self) -> None:
        ws_url = self.pool_arbitrage_ws_url
        if not ws_url:
            return
        retry_seconds = 1
        while True:
            try:
                async with connect(
                    ws_url,
                    open_timeout=self.settings.http_timeout_seconds,
                    ping_interval=20,
                    ping_timeout=20,
                ) as socket:
                    await socket.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 1,
                                "method": "eth_subscribe",
                                "params": ["newHeads"],
                            }
                        )
                    )
                    subscription = json.loads(await socket.recv())
                    if subscription.get("error"):
                        raise RuntimeError(str(subscription["error"]))
                    retry_seconds = 1
                    logger.info("Curve/v4 套利监控已订阅 Ethereum 新区块")
                    async for raw_message in socket:
                        payload = json.loads(raw_message)
                        header = payload.get("params", {}).get("result", {})
                        number_hex = header.get("number")
                        block_hash = str(header.get("hash") or "")
                        if not number_hex:
                            continue
                        block_number = int(number_hex, 16)
                        block_key = (block_number, block_hash)
                        if block_key == self._last_pool_arbitrage_block:
                            continue
                        self._last_pool_arbitrage_block = block_key
                        await self.poll_pool_arbitrage_once(block_number=block_number)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Ethereum 新区块订阅中断，%s 秒后重连 │ 错误=%s",
                    retry_seconds,
                    exc,
                )
                await asyncio.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2, 30)

    async def poll_pool_arbitrage_once(
        self,
        block_number: int | None = None,
    ) -> dict[str, object]:
        if self._pool_arbitrage_lock.locked():
            return {"status": "skipped", "reason": "pool arbitrage poll already in progress"}

        async with self._pool_arbitrage_lock:
            self.last_pool_arbitrage_errors = {}
            all_points: list[MetricPoint] = []
            try:
                all_points = await self.pool_arbitrage_collector.collect(block_number=block_number)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Curve/v4 池间套利采集失败")
                self.last_pool_arbitrage_errors["pool_arbitrage"] = str(exc)

            evaluation = await asyncio.to_thread(self._persist_and_evaluate, all_points)
            await self._send_notifications(
                evaluation.notifications,
                self.last_pool_arbitrage_errors,
            )
            self.last_pool_arbitrage_run_at = datetime.now(timezone.utc)
            self.last_pool_arbitrage_status = (
                "partial_failure" if self.last_pool_arbitrage_errors else "ok"
            )
            return {
                "status": self.last_pool_arbitrage_status,
                "collected_metrics": len(all_points),
                "alerts_touched": len(evaluation.events),
                "errors": self.last_pool_arbitrage_errors,
                "last_run_at": self.last_pool_arbitrage_run_at.isoformat(),
            }

    async def _collect_all(self) -> tuple[list[MetricPoint], dict[str, str]]:
        collectors = list(self.collectors)
        if self.is_crosschain_arbitrage_enabled():
            collectors.append(self.arbitrage_collector)
        results = []
        for collector in collectors:
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
        with Session(engine) as session:
            self._cleanup_old_metric_snapshots(session)
            if not all_points:
                session.commit()
                return RuleEvaluationResult(events=[], notifications=[])

            all_points = self._with_apxusd_redemption_spread(session, all_points)
            all_points = self._with_apxusd_redemption_moving_averages(session, all_points)
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

    def _cleanup_old_metric_snapshots(self, session: Session) -> None:
        retention_days = self.settings.metric_retention_days
        if retention_days <= 0:
            return

        now = datetime.now(timezone.utc)
        interval_seconds = self.settings.metric_retention_cleanup_interval_seconds
        if (
            self._last_metric_retention_cleanup_at is not None
            and interval_seconds > 0
            and (now - self._last_metric_retention_cleanup_at).total_seconds() < interval_seconds
        ):
            return

        cutoff_at = now - timedelta(days=retention_days)
        result = session.exec(
            delete(MetricSnapshot).where(MetricSnapshot.recorded_at < cutoff_at)
        )
        deleted_count = result.rowcount or 0
        if deleted_count:
            logger.info(
                "Deleted old metric snapshots for retention policy: count=%s cutoff=%s",
                deleted_count,
                cutoff_at.isoformat(),
            )
        self._last_metric_retention_cleanup_at = now

    @staticmethod
    def _with_apxusd_redemption_spread(
        session: Session,
        points: list[MetricPoint],
    ) -> list[MetricPoint]:
        legacy_price_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_MARKET_ENTITY_ID,
            APXUSD_PRICE_METRIC,
        )
        buy_price_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_MARKET_ENTITY_ID,
            APXUSD_BUY_PRICE_METRIC,
        ) or legacy_price_point
        sell_price_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_MARKET_ENTITY_ID,
            APXUSD_SELL_PRICE_METRIC,
        )
        redemption_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_REDEMPTION_ENTITY_ID,
            REDEMPTION_VALUE_METRIC,
        )
        if buy_price_point is None and session is not None:
            buy_price_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_MARKET_ENTITY_ID,
                APXUSD_BUY_PRICE_METRIC,
            )
            if buy_price_snapshot is not None:
                buy_price_point = MonitoringService._point_from_snapshot(buy_price_snapshot)
        if buy_price_point is None and session is not None:
            legacy_price_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_MARKET_ENTITY_ID,
                APXUSD_PRICE_METRIC,
            )
            if legacy_price_snapshot is not None:
                buy_price_point = MonitoringService._point_from_snapshot(legacy_price_snapshot)
        if sell_price_point is None and session is not None:
            sell_price_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_MARKET_ENTITY_ID,
                APXUSD_SELL_PRICE_METRIC,
            )
            if sell_price_snapshot is not None:
                sell_price_point = MonitoringService._point_from_snapshot(sell_price_snapshot)
        if redemption_point is None:
            redemption_snapshot = MonitoringService._latest_snapshot(
                session,
                APXUSD_REDEMPTION_ENTITY_ID,
                REDEMPTION_VALUE_METRIC,
            )
            if redemption_snapshot is not None:
                redemption_point = MonitoringService._point_from_snapshot(redemption_snapshot)
        if buy_price_point is None or redemption_point is None:
            return points

        buy_price_recorded_at = MonitoringService._as_utc(buy_price_point.recorded_at)
        redemption_recorded_at = MonitoringService._as_utc(redemption_point.recorded_at)
        recorded_at = max(buy_price_recorded_at, redemption_recorded_at)
        spread = buy_price_point.value - redemption_point.value
        derived_points = [
            MetricPoint(
                entity_id=APXUSD_MARKET_ENTITY_ID,
                entity_type="market_price",
                metric_name="price_vs_redemption_spread_usd",
                value=spread,
                unit="usd",
                source="derived",
                recorded_at=recorded_at,
                details={
                    "price_usd": buy_price_point.value,
                    "price_side": "buy",
                    "redemption_value_usd": redemption_point.value,
                    "price_recorded_at": buy_price_recorded_at.isoformat(),
                    "redemption_recorded_at": redemption_recorded_at.isoformat(),
                },
            ),
            MetricPoint(
                entity_id=APXUSD_MARKET_ENTITY_ID,
                entity_type="market_price",
                metric_name="buy_price_vs_redemption_spread_usd",
                value=spread,
                unit="usd",
                source="derived",
                recorded_at=recorded_at,
                details={
                    "buy_price_usd": buy_price_point.value,
                    "redemption_value_usd": redemption_point.value,
                    "buy_price_recorded_at": buy_price_recorded_at.isoformat(),
                    "redemption_recorded_at": redemption_recorded_at.isoformat(),
                },
            ),
        ]
        if redemption_point.value:
            derived_points.append(
                MetricPoint(
                    entity_id=APXUSD_MARKET_ENTITY_ID,
                    entity_type="market_price",
                    metric_name=REDEMPTION_SPREAD_PCT_METRIC,
                    value=spread / redemption_point.value * 100,
                    unit="pct",
                    source="derived",
                    recorded_at=recorded_at,
                    details={
                        "price_usd": buy_price_point.value,
                        "price_side": "buy",
                        "redemption_value_usd": redemption_point.value,
                        "spread_usd": spread,
                        "price_recorded_at": buy_price_recorded_at.isoformat(),
                        "redemption_recorded_at": redemption_recorded_at.isoformat(),
                    },
                )
            )
            derived_points.append(
                MetricPoint(
                    entity_id=APXUSD_MARKET_ENTITY_ID,
                    entity_type="market_price",
                    metric_name=REDEMPTION_BUY_SPREAD_PCT_METRIC,
                    value=spread / redemption_point.value * 100,
                    unit="pct",
                    source="derived",
                    recorded_at=recorded_at,
                    details={
                        "buy_price_usd": buy_price_point.value,
                        "redemption_value_usd": redemption_point.value,
                        "spread_usd": spread,
                        "buy_price_recorded_at": buy_price_recorded_at.isoformat(),
                        "redemption_recorded_at": redemption_recorded_at.isoformat(),
                    },
                )
            )
        if sell_price_point is not None:
            sell_price_recorded_at = MonitoringService._as_utc(sell_price_point.recorded_at)
            sell_recorded_at = max(sell_price_recorded_at, redemption_recorded_at)
            sell_spread = sell_price_point.value - redemption_point.value
            derived_points.append(
                MetricPoint(
                    entity_id=APXUSD_MARKET_ENTITY_ID,
                    entity_type="market_price",
                    metric_name="sell_price_vs_redemption_spread_usd",
                    value=sell_spread,
                    unit="usd",
                    source="derived",
                    recorded_at=sell_recorded_at,
                    details={
                        "sell_price_usd": sell_price_point.value,
                        "redemption_value_usd": redemption_point.value,
                        "sell_price_recorded_at": sell_price_recorded_at.isoformat(),
                        "redemption_recorded_at": redemption_recorded_at.isoformat(),
                    },
                )
            )
            if redemption_point.value:
                derived_points.append(
                    MetricPoint(
                        entity_id=APXUSD_MARKET_ENTITY_ID,
                        entity_type="market_price",
                        metric_name=REDEMPTION_SELL_SPREAD_PCT_METRIC,
                        value=sell_spread / redemption_point.value * 100,
                        unit="pct",
                        source="derived",
                        recorded_at=sell_recorded_at,
                        details={
                            "sell_price_usd": sell_price_point.value,
                            "redemption_value_usd": redemption_point.value,
                            "spread_usd": sell_spread,
                            "sell_price_recorded_at": sell_price_recorded_at.isoformat(),
                            "redemption_recorded_at": redemption_recorded_at.isoformat(),
                        },
                    )
                )
        return [
            *points,
            *derived_points,
        ]

    @staticmethod
    def _with_apxusd_redemption_moving_averages(
        session: Session,
        points: list[MetricPoint],
    ) -> list[MetricPoint]:
        spread_pct_point = MonitoringService._latest_point_for_metric(
            points,
            APXUSD_MARKET_ENTITY_ID,
            REDEMPTION_SPREAD_PCT_METRIC,
        )
        if spread_pct_point is None:
            return points

        recorded_at = MonitoringService._as_utc(spread_pct_point.recorded_at)
        max_window_hours = max(REDEMPTION_MA_WINDOWS.values())
        cutoff_at = recorded_at - timedelta(hours=max_window_hours)
        historical_rows = session.exec(
            select(MetricSnapshot)
            .where(MetricSnapshot.entity_id == APXUSD_MARKET_ENTITY_ID)
            .where(MetricSnapshot.metric_name == REDEMPTION_SPREAD_PCT_METRIC)
            .where(MetricSnapshot.recorded_at >= cutoff_at)
            .order_by(MetricSnapshot.recorded_at.asc(), MetricSnapshot.id.asc())
        ).all()
        samples = [
            (MonitoringService._as_utc(row.recorded_at), row.value)
            for row in historical_rows
        ]
        samples.append((recorded_at, spread_pct_point.value))

        derived_points: list[MetricPoint] = []
        for metric_name, window_hours in REDEMPTION_MA_WINDOWS.items():
            window_cutoff_at = recorded_at - timedelta(hours=window_hours)
            window_values = [
                value
                for sample_at, value in samples
                if window_cutoff_at <= sample_at <= recorded_at
            ]
            if not window_values:
                continue
            derived_points.append(
                MetricPoint(
                    entity_id=APXUSD_MARKET_ENTITY_ID,
                    entity_type="market_price",
                    metric_name=metric_name,
                    value=sum(window_values) / len(window_values),
                    unit="pct",
                    source="derived",
                    recorded_at=recorded_at,
                    details={
                        "base_metric_name": REDEMPTION_SPREAD_PCT_METRIC,
                        "window_hours": window_hours,
                        "sample_count": len(window_values),
                    },
                )
            )

        return [*points, *derived_points]

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
