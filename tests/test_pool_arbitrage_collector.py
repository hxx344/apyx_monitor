from __future__ import annotations

from datetime import datetime, timezone

from apyx_monitor.collectors.pool_arbitrage import (
    CURVE_BUY_V4_SELL,
    V4_BUY_CURVE_SELL,
    PoolArbitrageCollector,
    PoolArbitrageQuote,
)
from apyx_monitor.config import PoolArbitrageMonitorDefinition


def _monitor() -> PoolArbitrageMonitorDefinition:
    return PoolArbitrageMonitorDefinition(
        monitor_id="arb-apxusd-usdc-v4-curve",
        label="Curve/v4",
        curve_pool_address="0x0000000000000000000000000000000000000001",
        v4_pool_id="0x" + "12" * 32,
        v4_quoter_address="0x0000000000000000000000000000000000000002",
        currency0_asset_id="apxusd-ethereum",
        currency1_asset_id="usdc-ethereum",
        fee=500,
        tick_spacing=10,
        notionals_usdc=[100000],
    )


def _quote(direction: str, notional: float, final_usdc: float) -> PoolArbitrageQuote:
    return PoolArbitrageQuote(
        monitor=_monitor(),
        direction=direction,
        direction_label=direction,
        notional_usdc=notional,
        intermediate_apxusd=notional * 1.15,
        final_usdc=final_usdc,
        net_profit_usd=final_usdc - notional,
        net_edge_pct=(final_usdc - notional) / notional * 100,
        v4_quote_gas=150000,
        block_number=123,
        recorded_at=datetime.now(timezone.utc),
    )


def test_quote_metrics_select_best_executable_loop() -> None:
    quotes = [
        _quote(CURVE_BUY_V4_SELL, 100000.0, 100002.0),
        _quote(V4_BUY_CURVE_SELL, 100000.0, 100005.0),
    ]

    metrics = PoolArbitrageCollector._quotes_to_metrics(quotes)
    best = next(metric for metric in metrics if metric.metric_name == "best_net_profit_usd")

    assert best.value == 5.0
    assert best.details["strategy_id"] == V4_BUY_CURVE_SELL
    assert best.details["notional_usd"] == 100000.0
    assert best.details["v4_pool_id"] == _monitor().v4_pool_id


def test_quote_metrics_keep_negative_best_for_no_opportunity_state() -> None:
    quote = _quote(CURVE_BUY_V4_SELL, 100000.0, 99999.0)

    metrics = PoolArbitrageCollector._quotes_to_metrics([quote])
    best = next(metric for metric in metrics if metric.metric_name == "best_net_profit_usd")

    assert best.value == -1.0
