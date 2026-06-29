from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from apyx_monitor.collectors.accountable import APXUSD_REDEMPTION_ENTITY_ID, REDEMPTION_VALUE_METRIC
from apyx_monitor.collectors.arbitrage import APXUSD_MARKET_ENTITY_ID, APXUSD_PRICE_METRIC
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
