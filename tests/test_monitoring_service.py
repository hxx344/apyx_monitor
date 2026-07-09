from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from apyx_monitor.collectors.accountable import APXUSD_REDEMPTION_ENTITY_ID, REDEMPTION_VALUE_METRIC
from apyx_monitor.collectors.arbitrage import (
    APXUSD_BUY_PRICE_METRIC,
    APXUSD_MARKET_ENTITY_ID,
    APXUSD_PRICE_METRIC,
    APXUSD_SELL_PRICE_METRIC,
)
from apyx_monitor.collectors.base import MetricPoint
from apyx_monitor.services.monitoring import MonitoringService
from apyx_monitor.services.rule_engine import RuleEvaluationResult


def test_arbitrage_poll_waits_for_active_collection_lock():
    asyncio.run(_run_arbitrage_poll_waits_for_active_collection_lock_test())


async def _run_arbitrage_poll_waits_for_active_collection_lock_test():
    class FakeArbitrageCollector:
        def __init__(self) -> None:
            self.called = False

        async def collect(self, force: bool = False, reset_refresh_cycle: bool = False):
            self.called = force
            return []

    service = MonitoringService()
    fake_collector = FakeArbitrageCollector()
    service.arbitrage_collector = fake_collector
    service._persist_and_evaluate = lambda points: RuleEvaluationResult(events=[], notifications=[])

    await service._lock.acquire()
    task = asyncio.create_task(service.poll_arbitrage_once(wait_for_lock_seconds=1))
    await asyncio.sleep(0.05)

    assert not task.done()

    service._lock.release()
    result = await task

    assert result["status"] == "ok"
    assert fake_collector.called is True


def test_apxusd_redemption_spread_is_derived_from_collected_points():
    recorded_at = datetime.now(timezone.utc)
    points = [
        MetricPoint(
            entity_id=APXUSD_MARKET_ENTITY_ID,
            entity_type="market_price",
            metric_name=APXUSD_PRICE_METRIC,
            value=0.82,
            unit="usd",
            source="test",
            recorded_at=recorded_at,
        ),
        MetricPoint(
            entity_id=APXUSD_REDEMPTION_ENTITY_ID,
            entity_type="proof_of_solvency",
            metric_name=REDEMPTION_VALUE_METRIC,
            value=0.79,
            unit="usd",
            source="test",
            recorded_at=recorded_at,
        ),
    ]

    enriched = MonitoringService._with_apxusd_redemption_spread(None, points)
    spread = next(
        point for point in enriched if point.metric_name == "price_vs_redemption_spread_usd"
    )
    spread_pct = next(
        point for point in enriched if point.metric_name == "price_vs_redemption_spread_pct"
    )

    assert spread.entity_id == APXUSD_MARKET_ENTITY_ID
    assert spread.metric_name == "price_vs_redemption_spread_usd"
    assert round(spread.value, 10) == 0.03
    assert spread_pct.entity_id == APXUSD_MARKET_ENTITY_ID
    assert round(spread_pct.value, 10) == round(0.03 / 0.79 * 100, 10)


def test_apxusd_redemption_spread_uses_directional_buy_and_sell_prices():
    recorded_at = datetime.now(timezone.utc)
    points = [
        MetricPoint(
            entity_id=APXUSD_MARKET_ENTITY_ID,
            entity_type="market_price",
            metric_name=APXUSD_BUY_PRICE_METRIC,
            value=0.78,
            unit="usd",
            source="test",
            recorded_at=recorded_at,
        ),
        MetricPoint(
            entity_id=APXUSD_MARKET_ENTITY_ID,
            entity_type="market_price",
            metric_name=APXUSD_SELL_PRICE_METRIC,
            value=0.82,
            unit="usd",
            source="test",
            recorded_at=recorded_at,
        ),
        MetricPoint(
            entity_id=APXUSD_REDEMPTION_ENTITY_ID,
            entity_type="proof_of_solvency",
            metric_name=REDEMPTION_VALUE_METRIC,
            value=0.80,
            unit="usd",
            source="test",
            recorded_at=recorded_at,
        ),
    ]

    enriched = MonitoringService._with_apxusd_redemption_spread(None, points)
    buy_spread_pct = next(
        point for point in enriched if point.metric_name == "buy_price_vs_redemption_spread_pct"
    )
    sell_spread_pct = next(
        point for point in enriched if point.metric_name == "sell_price_vs_redemption_spread_pct"
    )

    assert round(buy_spread_pct.value, 10) == -2.5
    assert round(sell_spread_pct.value, 10) == 2.5


def test_apxusd_redemption_spread_moving_averages_are_derived():
    recorded_at = datetime.now(timezone.utc)

    class FakeResult:
        def all(self):
            return [
                SimpleNamespace(
                    recorded_at=recorded_at - timedelta(hours=6),
                    value=1.0,
                ),
                SimpleNamespace(
                    recorded_at=recorded_at - timedelta(hours=13),
                    value=3.0,
                ),
            ]

    class FakeSession:
        def exec(self, statement):
            return FakeResult()

    points = [
        MetricPoint(
            entity_id=APXUSD_MARKET_ENTITY_ID,
            entity_type="market_price",
            metric_name="price_vs_redemption_spread_pct",
            value=5.0,
            unit="pct",
            source="test",
            recorded_at=recorded_at,
        ),
    ]

    enriched = MonitoringService._with_apxusd_redemption_moving_averages(FakeSession(), points)
    ma_12h = next(
        point for point in enriched if point.metric_name == "price_vs_redemption_spread_pct_ma_12h"
    )
    ma_24h = next(
        point for point in enriched if point.metric_name == "price_vs_redemption_spread_pct_ma_24h"
    )

    assert ma_12h.value == 3.0
    assert ma_24h.value == 3.0
    assert ma_12h.unit == "pct"


def test_metric_retention_cleanup_deletes_old_snapshots():
    class FakeDeleteResult:
        rowcount = 2

    class FakeSession:
        def __init__(self) -> None:
            self.statement = None

        def exec(self, statement):
            self.statement = statement
            return FakeDeleteResult()

    service = object.__new__(MonitoringService)
    service.settings = SimpleNamespace(
        metric_retention_days=7,
        metric_retention_cleanup_interval_seconds=0,
    )
    service._last_metric_retention_cleanup_at = None
    session = FakeSession()

    service._cleanup_old_metric_snapshots(session)

    assert session.statement is not None
    assert service._last_metric_retention_cleanup_at is not None


def test_metric_retention_cleanup_respects_cleanup_interval():
    class FakeSession:
        def __init__(self) -> None:
            self.called = False

        def exec(self, statement):
            self.called = True

    service = object.__new__(MonitoringService)
    service.settings = SimpleNamespace(
        metric_retention_days=30,
        metric_retention_cleanup_interval_seconds=3600,
    )
    service._last_metric_retention_cleanup_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    session = FakeSession()

    service._cleanup_old_metric_snapshots(session)

    assert session.called is False


def test_metric_retention_cleanup_runs_every_time_when_interval_is_zero():
    class FakeDeleteResult:
        rowcount = 0

    class FakeSession:
        def __init__(self) -> None:
            self.calls = 0

        def exec(self, statement):
            self.calls += 1
            return FakeDeleteResult()

    service = object.__new__(MonitoringService)
    service.settings = SimpleNamespace(
        metric_retention_days=7,
        metric_retention_cleanup_interval_seconds=0,
    )
    service._last_metric_retention_cleanup_at = datetime.now(timezone.utc)
    session = FakeSession()

    service._cleanup_old_metric_snapshots(session)

    assert session.calls == 1
