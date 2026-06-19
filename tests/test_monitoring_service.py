from __future__ import annotations

import asyncio

from apyx_monitor.services.monitoring import MonitoringService
from apyx_monitor.services.rule_engine import RuleEvaluationResult


def test_arbitrage_poll_waits_for_active_collection_lock():
    asyncio.run(_run_arbitrage_poll_waits_for_active_collection_lock_test())


async def _run_arbitrage_poll_waits_for_active_collection_lock_test():
    class FakeArbitrageCollector:
        def __init__(self) -> None:
            self.called = False

        async def collect(self, force: bool = False):
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
