from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from apyx_monitor.collectors import arbitrage as arbitrage_module
from apyx_monitor.collectors import pendle_rate_limit
from apyx_monitor.collectors.arbitrage import (
    BUY_SOURCE_SELL_TARGET,
    BUY_TARGET_SELL_SOURCE,
    ArbitrageCollector,
    PENDLESWAP_PROVIDER,
    PendleSwapRouteUnavailableError,
    SwapQuote,
)
from apyx_monitor.config import (
    ArbitrageMonitorDefinition,
    AssetCatalog,
    AssetDefinition,
    ChainDefinition,
    Settings,
)
from apyx_monitor.models import MetricSnapshot


def _asset(
    asset_id: str,
    group_id: str,
    symbol: str,
    chain: str,
    address: str,
    decimals: int = 18,
    enabled: bool = True,
) -> AssetDefinition:
    return AssetDefinition(
        asset_id=asset_id,
        group_id=group_id,
        symbol=symbol,
        kind="base" if symbol != "apyUSD" else "yield",
        chain=chain,
        contract_address=address,
        decimals=decimals,
        standard="erc20" if symbol != "apyUSD" else "erc4626",
        price_hint_usd=1.0,
        enabled=enabled,
    )


class MockArbitrageCollector(ArbitrageCollector):
    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        super().__init__(settings, catalog)
        self.quote_calls: list[tuple[int, str, str, int]] = []

    def _should_calculate_arbitrage_paths(self, now):
        return True

    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_provider: str = PENDLESWAP_PROVIDER,
    ) -> SwapQuote:
        amounts = {
            ("usdc", "eth-apx"): amount_in_raw * 10**12,
            ("usdc", "eth-apy"): amount_in_raw * 2 * 10**12,
            ("eth-apx", "eth-apy"): amount_in_raw * 2,
            ("base-apy", "base-apx"): amount_in_raw * 103 // 200,
            ("base-apx", "base-apy"): amount_in_raw * 2,
            ("bsc-apy", "bsc-apx"): amount_in_raw * 104 // 200,
            ("bsc-apx", "bsc-apy"): amount_in_raw * 2,
            ("eth-apy", "eth-apx"): amount_in_raw * 51 // 100,
            ("eth-apx", "usdc"): amount_in_raw // 10**12,
            ("eth-apy", "usdc"): amount_in_raw * 51 // 100 // 10**12,
        }
        self.quote_calls.append((chain_id, token_in, token_out, amount_in_raw))
        amount_out_raw = amounts[(token_in, token_out)]
        return SwapQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=amount_out_raw,
            min_out_raw=None,
            token_in=token_in,
            token_out=token_out,
            method="mock",
        )


class FailingRemoteBuyCollector(MockArbitrageCollector):
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_provider: str = PENDLESWAP_PROVIDER,
    ) -> SwapQuote:
        if token_in == "base-apx" and token_out == "base-apy":
            request = httpx.Request("GET", "https://example.test/convert")
            response = httpx.Response(501, request=request, json={"message": "not implemented"})
            raise httpx.HTTPStatusError("501 Not Implemented", request=request, response=response)
        return await super()._quote(
            client,
            chain_id,
            monitor,
            token_in,
            token_out,
            amount_in_raw,
            quote_provider=quote_provider,
        )


class UnprocessableRemoteBuyCollector(MockArbitrageCollector):
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_provider: str = PENDLESWAP_PROVIDER,
    ) -> SwapQuote:
        if token_in == "base-apx" and token_out == "base-apy":
            request = httpx.Request(
                "POST", "https://api-v2.pendle.finance/core/v3/sdk/8453/convert"
            )
            response = httpx.Response(422, request=request, json={"error": "no route"})
            raise httpx.HTTPStatusError(
                "422 Unprocessable Entity", request=request, response=response
            )
        return await super()._quote(
            client,
            chain_id,
            monitor,
            token_in,
            token_out,
            amount_in_raw,
            quote_provider=quote_provider,
        )


class UnavailableRemotePairCollector(MockArbitrageCollector):
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_provider: str = PENDLESWAP_PROVIDER,
    ) -> SwapQuote:
        if {token_in, token_out} == {"base-apx", "base-apy"}:
            raise PendleSwapRouteUnavailableError("PendleSwap route unavailable")
        return await super()._quote(
            client,
            chain_id,
            monitor,
            token_in,
            token_out,
            amount_in_raw,
            quote_provider=quote_provider,
        )


class FailingSettlementSellCollector(MockArbitrageCollector):
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_provider: str = PENDLESWAP_PROVIDER,
    ) -> SwapQuote:
        if token_in == "eth-apy" and token_out == "usdc":
            request = httpx.Request("GET", "https://example.test/convert")
            response = httpx.Response(500, request=request, json={"message": "server error"})
            raise httpx.HTTPStatusError(
                "500 Internal Server Error", request=request, response=response
            )
        return await super()._quote(
            client,
            chain_id,
            monitor,
            token_in,
            token_out,
            amount_in_raw,
            quote_provider=quote_provider,
        )


def test_arbitrage_profit_is_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_SOURCE_SELL_TARGET, 10300, 300, 3))


def test_reverse_arbitrage_path_is_also_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_TARGET_SELL_SOURCE, 10200, 200, 2))


def test_collect_rotates_base_and_bsc_routes():
    asyncio.run(_run_collect_quote_count_test())


def test_remote_buy_uses_reverse_quote_fallback_when_pendle_returns_501():
    asyncio.run(_run_remote_buy_fallback_test())


def test_remote_buy_uses_reverse_quote_fallback_when_pendleswap_returns_422():
    asyncio.run(_run_remote_buy_422_fallback_test())


def test_collect_skips_strategy_when_pendleswap_route_is_unavailable():
    asyncio.run(_run_route_unavailable_skip_test())


def test_settlement_sell_uses_reverse_quote_fallback_when_pendle_returns_500():
    asyncio.run(_run_settlement_sell_fallback_test())


def test_quote_uses_pendleswap_sdk_convert_api(monkeypatch):
    asyncio.run(_run_quote_uses_pendleswap_sdk_convert_api_test(monkeypatch))


def test_quote_falls_back_to_jumper_when_pendleswap_is_rate_limited(monkeypatch):
    asyncio.run(_run_quote_falls_back_to_jumper_when_pendleswap_is_rate_limited_test(monkeypatch))


def test_quote_falls_back_to_velora_when_pendleswap_and_jumper_are_rate_limited(
    monkeypatch,
):
    asyncio.run(
        _run_quote_falls_back_to_velora_when_pendleswap_and_jumper_are_rate_limited_test(
            monkeypatch
        )
    )


def test_curve_nav_gate_skips_path_calculation_when_deviation_is_quiet(monkeypatch):
    now = datetime.now(timezone.utc)
    collector = ArbitrageCollector(Settings(), _minimal_catalog())
    monkeypatch.setattr(
        collector,
        "_latest_curve_nav_deviation_snapshots",
        lambda limit: [
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.12,
                unit="pct",
                source="test",
                recorded_at=now,
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.10,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=20),
            ),
        ],
    )

    assert collector._should_calculate_arbitrage_paths(now) is False


def test_curve_nav_gate_skips_path_calculation_when_only_deviation_is_large(monkeypatch):
    now = datetime.now(timezone.utc)
    collector = ArbitrageCollector(Settings(), _minimal_catalog())
    monkeypatch.setattr(
        collector,
        "_latest_curve_nav_deviation_snapshots",
        lambda limit: [
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.75,
                unit="pct",
                source="test",
                recorded_at=now,
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.75,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=20),
            )
        ],
    )

    assert collector._should_calculate_arbitrage_paths(now) is False


def test_curve_nav_gate_allows_path_calculation_when_deviation_changes_quickly(monkeypatch):
    now = datetime.now(timezone.utc)
    collector = ArbitrageCollector(Settings(), _minimal_catalog())
    monkeypatch.setattr(
        collector,
        "_latest_curve_nav_deviation_snapshots",
        lambda limit: [
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.30,
                unit="pct",
                source="test",
                recorded_at=now,
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.05,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=20),
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=1.20,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=80),
            ),
        ],
    )

    assert collector._should_calculate_arbitrage_paths(now) is True


def test_curve_nav_gate_ignores_change_outside_one_minute(monkeypatch):
    now = datetime.now(timezone.utc)
    collector = ArbitrageCollector(Settings(), _minimal_catalog())
    monkeypatch.setattr(
        collector,
        "_latest_curve_nav_deviation_snapshots",
        lambda limit: [
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.12,
                unit="pct",
                source="test",
                recorded_at=now,
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.11,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=20),
            ),
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=0.45,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=80),
            ),
        ],
    )

    assert collector._should_calculate_arbitrage_paths(now) is False


def test_curve_nav_gate_skips_path_calculation_when_deviation_is_stale(monkeypatch):
    now = datetime.now(timezone.utc)
    collector = ArbitrageCollector(Settings(), _minimal_catalog())
    monkeypatch.setattr(
        collector,
        "_latest_curve_nav_deviation_snapshots",
        lambda limit: [
            MetricSnapshot(
                entity_id="curve-apyusd-apxusd",
                entity_type="curve_pool",
                metric_name="curve_rate_vs_nav_deviation_pct",
                value=1.25,
                unit="pct",
                source="test",
                recorded_at=now - timedelta(seconds=181),
            )
        ],
    )

    assert collector._should_calculate_arbitrage_paths(now) is False


async def _run_arbitrage_profit_test(
    strategy_id: str, final_usdc: float, profit: float, edge_pct: float
):
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = MockArbitrageCollector(Settings(), catalog)

    async with httpx.AsyncClient() as client:
        sample = await collector._sample_monitor(
            client,
            monitor,
            {"ethereum": 1, "base": 8453},
            catalog.assets[0],
            catalog.assets[1],
            catalog.assets[2],
            catalog.assets[3],
            catalog.assets[4],
            strategy_id,
            10000,
            {},
        )

    assert sample.start_asset.symbol == "USDC"
    assert sample.final_asset.symbol == "USDC"
    assert sample.start_amount == 10000
    if strategy_id == BUY_SOURCE_SELL_TARGET:
        assert sample.entry_apxusd_amount == 0
        assert sample.route_steps[0]["to_symbol"] == "apyUSD"
    else:
        assert sample.entry_apxusd_amount == 10000
        assert sample.route_steps[0]["to_symbol"] == "apxUSD"
    if strategy_id == BUY_SOURCE_SELL_TARGET:
        assert sample.final_apxusd_amount == final_usdc
    else:
        assert sample.final_apxusd_amount == 10000
    assert sample.final_amount == final_usdc
    assert sample.net_profit_usd == profit
    assert sample.net_edge_pct == edge_pct
    assert sample.route_steps[0]["from_symbol"] == "USDC"
    assert sample.route_steps[-1]["to_symbol"] == "USDC"
    assert sample.exit_leg.method == "mock"


async def _run_collect_quote_count_test():
    base_monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    bsc_monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-bsc",
        label="Ethereum <-> BSC",
        source_chain="ethereum",
        target_chain="bsc",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-bsc",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
            ChainDefinition(chain="bsc", chain_id=56, rpc_url_env="BSC_RPC", default_rpc_url=""),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
            _asset("apxusd-bsc", "apxusd", "apxUSD", "bsc", "bsc-apx"),
            _asset("apyusd-bsc", "apyusd", "apyUSD", "bsc", "bsc-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[base_monitor, bsc_monitor],
    )
    collector = MockArbitrageCollector(Settings(), catalog)

    first_metrics = await collector.collect()

    assert first_metrics
    assert len(collector.quote_calls) == 6
    assert (
        len(
            [
                call
                for call in collector.quote_calls
                if call[1:] == ("usdc", "eth-apy", 10_000_000_000)
            ]
        )
        == 1
    )
    assert (
        len(
            [
                call
                for call in collector.quote_calls
                if call[1] == "eth-apx" and call[2] == "usdc"
            ]
        )
        == 1
    )
    assert (
        len(
            [
                call
                for call in collector.quote_calls
                if call[1] == "eth-apy" and call[2] == "usdc"
            ]
        )
        == 1
    )
    assert [call for call in collector.quote_calls if call[1].startswith("base-")]
    assert not [call for call in collector.quote_calls if call[1].startswith("bsc-")]
    assert [call for call in collector.quote_calls if call[1] == "eth-apx" and call[2] == "usdc"]

    first_best = _best_profit_metric(first_metrics)
    assert (
        first_best.details["sample_entity_id"] == "arb-ethereum-base-buy-source-sell-target-10000"
    )

    second_metrics = await collector.collect()

    assert second_metrics
    assert len(collector.quote_calls) == 12
    second_collect_calls = collector.quote_calls[6:]
    assert (
        len(
            [
                call
                for call in second_collect_calls
                if call[1:] == ("usdc", "eth-apy", 10_000_000_000)
            ]
        )
        == 1
    )
    assert [call for call in second_collect_calls if call[1].startswith("bsc-")]
    assert not [call for call in second_collect_calls if call[1].startswith("base-")]

    second_best = _best_profit_metric(second_metrics)
    assert (
        second_best.details["sample_entity_id"] == "arb-ethereum-bsc-buy-source-sell-target-10000"
    )


async def _run_remote_buy_fallback_test():
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = FailingRemoteBuyCollector(Settings(), catalog)

    async with httpx.AsyncClient() as client:
        sample = await collector._sample_monitor(
            client,
            monitor,
            {"ethereum": 1, "base": 8453},
            catalog.assets[0],
            catalog.assets[1],
            catalog.assets[2],
            catalog.assets[3],
            catalog.assets[4],
            BUY_TARGET_SELL_SOURCE,
            10000,
            {},
        )

    assert sample.first_leg.method == "derived_reverse_http_501"
    assert round(sample.bought_apyusd_amount, 6) == 19417.475728
    assert round(sample.final_amount, 6) == 9902.912621
    assert any(call[1] == "base-apy" and call[2] == "base-apx" for call in collector.quote_calls)


async def _run_remote_buy_422_fallback_test():
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = UnprocessableRemoteBuyCollector(Settings(), catalog)

    async with httpx.AsyncClient() as client:
        sample = await collector._sample_monitor(
            client,
            monitor,
            {"ethereum": 1, "base": 8453},
            catalog.assets[0],
            catalog.assets[1],
            catalog.assets[2],
            catalog.assets[3],
            catalog.assets[4],
            BUY_TARGET_SELL_SOURCE,
            10000,
            {},
        )

    assert sample.first_leg.method == "derived_reverse_http_422"
    assert round(sample.bought_apyusd_amount, 6) == 19417.475728
    assert any(call[1] == "base-apy" and call[2] == "base-apx" for call in collector.quote_calls)


async def _run_route_unavailable_skip_test():
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = UnavailableRemotePairCollector(Settings(), catalog)

    metrics = await collector.collect()

    assert not any(
        metric.entity_id == "arb-ethereum-base-buy-target-sell-source-10000"
        for metric in metrics
    )


async def _run_settlement_sell_fallback_test():
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
        notionals_usd=[10000],
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = FailingSettlementSellCollector(Settings(), catalog)

    async with httpx.AsyncClient() as client:
        sample = await collector._sample_monitor(
            client,
            monitor,
            {"ethereum": 1, "base": 8453},
            catalog.assets[0],
            catalog.assets[1],
            catalog.assets[2],
            catalog.assets[3],
            catalog.assets[4],
            BUY_TARGET_SELL_SOURCE,
            10000,
            {},
        )

    assert sample.exit_leg.method == "derived_reverse_http_500"
    assert round(sample.final_amount, 6) == 10000
    assert any(call[1] == "usdc" and call[2] == "eth-apy" for call in collector.quote_calls)


async def _run_quote_uses_pendleswap_sdk_convert_api_test(monkeypatch):
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("apyx_monitor.collectors.arbitrage.asyncio.sleep", fake_sleep)

    class FakeClient:
        def __init__(self) -> None:
            self.url: str | None = None
            self.payload: dict | None = None

        async def post(self, url: str, json: dict) -> httpx.Response:
            self.url = url
            self.payload = json
            request = httpx.Request("POST", url)
            return httpx.Response(
                200,
                request=request,
                json={
                    "outputs": [
                        {
                            "token": "eth-apy",
                            "amount": "2000000000000000000",
                        }
                    ],
                    "route": {"provider": "PendleSwap", "steps": ["curve"]},
                    "tx": {"to": "0xpendle", "data": "0x1234"},
                    "gasUsd": "0.01",
                },
            )

    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url="")
        ],
        assets=[
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    collector = ArbitrageCollector(Settings(), catalog)
    client = FakeClient()

    quote = await collector._quote(client, 1, monitor, "eth-apx", "eth-apy", 10**18)

    assert client.url == "https://api-v2.pendle.finance/core/v3/sdk/1/convert"
    assert client.payload["receiver"] == monitor.receiver_address
    assert client.payload["slippage"] == 0.005
    assert client.payload["enableAggregator"] is True
    assert client.payload["aggregators"] == ["kyberswap", "odos"]
    assert client.payload["inputs"] == [{"token": "eth-apx", "amount": str(10**18)}]
    assert client.payload["outputs"] == ["eth-apy"]
    assert client.payload["redeemRewards"] is False
    assert client.payload["needScale"] is False
    assert client.payload["useLimitOrder"] is True
    assert quote.amount_out_raw == 2 * 10**18
    assert quote.method == "pendleswap_sdk"
    assert quote.routing["provider"] == "pendleswap"
    assert quote.routing["mode"] == "hosted_sdk_convert"
    assert quote.routing["route"] == {"provider": "PendleSwap", "steps": ["curve"]}
    assert quote.routing["tx"] == {"to": "0xpendle", "data": "0x1234"}
    assert sleep_calls


async def _run_quote_falls_back_to_jumper_when_pendleswap_is_rate_limited_test(monkeypatch):
    pendle_rate_limit.clear_rate_limit()
    arbitrage_module._quote_provider_cooldowns.clear()
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("apyx_monitor.collectors.arbitrage.asyncio.sleep", fake_sleep)

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, dict]] = []

        async def post(self, url: str, json: dict) -> httpx.Response:
            self.calls.append(("POST", url, json))
            request = httpx.Request("POST", url)
            return httpx.Response(429, request=request, headers={"retry-after": "1"})

        async def get(self, url: str, params: dict) -> httpx.Response:
            self.calls.append(("GET", url, params))
            request = httpx.Request("GET", url)
            return httpx.Response(
                200,
                request=request,
                json={
                    "estimate": {
                        "toAmount": _fallback_quote_amount(params["toToken"]),
                        "toAmountMin": _fallback_quote_amount(params["toToken"]),
                        "approvalAddress": "0xapproval",
                    },
                    "tool": "jumper",
                },
            )

    monitor, catalog, chain_id_map = _fallback_path_catalog()
    collector = ArbitrageCollector(Settings(), catalog)
    client = FakeClient()

    sample = await collector._sample_monitor(
        client,
        monitor,
        chain_id_map,
        catalog.assets[0],
        catalog.assets[1],
        catalog.assets[2],
        catalog.assets[3],
        catalog.assets[4],
        BUY_SOURCE_SELL_TARGET,
        10000,
        {},
    )

    assert [call[0] for call in client.calls] == ["POST", "GET", "GET", "GET"]
    assert client.calls[1][1] == "https://li.quest/v1/quote"
    assert client.calls[1][2]["fromChain"] == "1"
    assert client.calls[1][2]["toChain"] == "1"
    assert client.calls[1][2]["fromToken"] == "usdc"
    assert client.calls[1][2]["toToken"] == "eth-apy"
    assert all(call[1] == "https://li.quest/v1/quote" for call in client.calls[1:])
    assert [step["routing"]["provider"] for step in sample.route_steps if step["type"] == "swap"] == [
        "jumper",
        "jumper",
        "jumper",
    ]
    assert sleep_calls == [4.0, 4.0, 4.0, 4.0]
    pendle_rate_limit.clear_rate_limit()
    arbitrage_module._quote_provider_cooldowns.clear()


async def _run_quote_falls_back_to_velora_when_pendleswap_and_jumper_are_rate_limited_test(
    monkeypatch,
):
    pendle_rate_limit.clear_rate_limit()
    arbitrage_module._quote_provider_cooldowns.clear()
    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("apyx_monitor.collectors.arbitrage.asyncio.sleep", fake_sleep)

    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, dict]] = []

        async def post(self, url: str, json: dict) -> httpx.Response:
            self.calls.append(("POST", url, json))
            request = httpx.Request("POST", url)
            return httpx.Response(429, request=request, headers={"retry-after": "1"})

        async def get(self, url: str, params: dict) -> httpx.Response:
            self.calls.append(("GET", url, params))
            request = httpx.Request("GET", url)
            if url == "https://li.quest/v1/quote":
                return httpx.Response(429, request=request, headers={"retry-after": "1"})
            return httpx.Response(
                200,
                request=request,
                json={
                    "priceRoute": {
                        "destAmount": _fallback_quote_amount(params["destToken"]),
                        "version": "6.2",
                        "bestRoute": [{"percent": 100}],
                    }
                },
            )

    monitor, catalog, chain_id_map = _fallback_path_catalog()
    collector = ArbitrageCollector(Settings(), catalog)
    client = FakeClient()

    sample = await collector._sample_monitor(
        client,
        monitor,
        chain_id_map,
        catalog.assets[0],
        catalog.assets[1],
        catalog.assets[2],
        catalog.assets[3],
        catalog.assets[4],
        BUY_SOURCE_SELL_TARGET,
        10000,
        {},
    )

    assert [call[0] for call in client.calls] == ["POST", "GET", "GET", "GET", "GET"]
    assert client.calls[1][1] == "https://li.quest/v1/quote"
    assert client.calls[2][1] == "https://api.paraswap.io/prices"
    assert client.calls[2][2]["srcToken"] == "usdc"
    assert client.calls[2][2]["destToken"] == "eth-apy"
    assert client.calls[2][2]["side"] == "SELL"
    assert client.calls[2][2]["network"] == "1"
    assert all(call[1] == "https://api.paraswap.io/prices" for call in client.calls[2:])
    assert [step["routing"]["provider"] for step in sample.route_steps if step["type"] == "swap"] == [
        "velora",
        "velora",
        "velora",
    ]
    assert sleep_calls == [4.0, 4.0, 4.0, 4.0, 4.0]
    pendle_rate_limit.clear_rate_limit()
    arbitrage_module._quote_provider_cooldowns.clear()


def _best_profit_metric(metrics):
    return next(
        metric
        for metric in metrics
        if metric.entity_id == "arb-apyusd-apxusd-crosschain"
        and metric.metric_name == "best_net_profit_usd"
    )


def _minimal_catalog() -> AssetCatalog:
    return AssetCatalog(
        chains=[],
        assets=[],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[],
    )


def _quote_catalog(monitor: ArbitrageMonitorDefinition) -> AssetCatalog:
    return AssetCatalog(
        chains=[
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url="")
        ],
        assets=[
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )


def _fallback_path_catalog() -> tuple[
    ArbitrageMonitorDefinition,
    AssetCatalog,
    dict[str, int],
]:
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum <-> Base",
        source_chain="ethereum",
        target_chain="base",
        funding_asset_id="usdc-ethereum",
        start_asset_id="apxusd-ethereum",
        intermediate_asset_id="apyusd-ethereum",
        final_asset_id="apxusd-base",
    )
    catalog = AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""
            ),
            ChainDefinition(
                chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""
            ),
        ],
        assets=[
            _asset("usdc-ethereum", "usdc", "USDC", "ethereum", "usdc", 6, enabled=False),
            _asset("apxusd-ethereum", "apxusd", "apxUSD", "ethereum", "eth-apx"),
            _asset("apyusd-ethereum", "apyusd", "apyUSD", "ethereum", "eth-apy"),
            _asset("apxusd-base", "apxusd", "apxUSD", "base", "base-apx"),
            _asset("apyusd-base", "apyusd", "apyUSD", "base", "base-apy"),
        ],
        pendle_markets=[],
        morpho_markets=[],
        arbitrage_monitors=[monitor],
    )
    return monitor, catalog, {"ethereum": 1, "base": 8453}


def _fallback_quote_amount(token_out: str) -> str:
    amounts = {
        "eth-apy": str(20_000 * 10**18),
        "base-apx": str(10_300 * 10**18),
        "usdc": str(10_300 * 10**6),
    }
    return amounts[token_out]
