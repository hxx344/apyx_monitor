from __future__ import annotations

import asyncio

import httpx

from apyx_monitor.collectors.arbitrage import (
    BUY_SOURCE_SELL_TARGET,
    BUY_TARGET_SELL_SOURCE,
    ArbitrageCollector,
    PendleQuote,
)
from apyx_monitor.config import (
    ArbitrageMonitorDefinition,
    AssetCatalog,
    AssetDefinition,
    ChainDefinition,
    Settings,
)


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

    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> PendleQuote:
        amounts = {
            ("usdc", "eth-apx"): amount_in_raw * 10**12,
            ("eth-apx", "eth-apy"): amount_in_raw * 2,
            ("base-apy", "base-apx"): amount_in_raw * 103 // 200,
            ("base-apx", "base-apy"): amount_in_raw * 2,
            ("bsc-apy", "bsc-apx"): amount_in_raw * 104 // 200,
            ("bsc-apx", "bsc-apy"): amount_in_raw * 2,
            ("eth-apy", "eth-apx"): amount_in_raw * 51 // 100,
            ("eth-apx", "usdc"): amount_in_raw // 10**12,
        }
        self.quote_calls.append((chain_id, token_in, token_out, amount_in_raw))
        amount_out_raw = amounts[(token_in, token_out)]
        return PendleQuote(
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
    ) -> PendleQuote:
        if token_in == "base-apx" and token_out == "base-apy":
            request = httpx.Request("GET", "https://example.test/convert")
            response = httpx.Response(501, request=request, json={"message": "not implemented"})
            raise httpx.HTTPStatusError("501 Not Implemented", request=request, response=response)
        return await super()._quote(client, chain_id, monitor, token_in, token_out, amount_in_raw)


class FailingSettlementSellCollector(MockArbitrageCollector):
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> PendleQuote:
        if token_in == "eth-apy" and token_out == "eth-apx":
            request = httpx.Request("GET", "https://example.test/convert")
            response = httpx.Response(500, request=request, json={"message": "server error"})
            raise httpx.HTTPStatusError("500 Internal Server Error", request=request, response=response)
        return await super()._quote(client, chain_id, monitor, token_in, token_out, amount_in_raw)


def test_arbitrage_profit_is_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_SOURCE_SELL_TARGET, 10300, 300, 3))


def test_reverse_arbitrage_path_is_also_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_TARGET_SELL_SOURCE, 10200, 200, 2))


def test_collect_rotates_base_and_bsc_routes():
    asyncio.run(_run_collect_quote_count_test())


def test_remote_buy_uses_reverse_quote_fallback_when_pendle_returns_501():
    asyncio.run(_run_remote_buy_fallback_test())


def test_settlement_sell_uses_reverse_quote_fallback_when_pendle_returns_500():
    asyncio.run(_run_settlement_sell_fallback_test())


async def _run_arbitrage_profit_test(strategy_id: str, final_usdc: float, profit: float, edge_pct: float):
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
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""),
            ChainDefinition(chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""),
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
    assert sample.entry_apxusd_amount == 10000
    assert sample.final_apxusd_amount == final_usdc
    assert sample.final_amount == final_usdc
    assert sample.net_profit_usd == profit
    assert sample.net_edge_pct == edge_pct
    assert sample.route_steps[0]["from_symbol"] == "USDC"
    assert sample.route_steps[-1]["to_symbol"] == "USDC"
    assert sample.exit_leg.method == "derived_reverse_entry"


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
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""),
            ChainDefinition(chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""),
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
    assert len(collector.quote_calls) == 5
    assert len([call for call in collector.quote_calls if call[1:] == ("usdc", "eth-apx", 10_000_000_000)]) == 1
    assert len([call for call in collector.quote_calls if call[1] == "eth-apx" and call[2] == "eth-apy"]) == 1
    assert len([call for call in collector.quote_calls if call[1] == "eth-apy" and call[2] == "eth-apx"]) == 1
    assert [call for call in collector.quote_calls if call[1].startswith("base-")]
    assert not [call for call in collector.quote_calls if call[1].startswith("bsc-")]
    assert not [call for call in collector.quote_calls if call[1] == "eth-apx" and call[2] == "usdc"]

    first_best = _best_profit_metric(first_metrics)
    assert first_best.details["sample_entity_id"] == "arb-ethereum-base-buy-source-sell-target-10000"

    second_metrics = await collector.collect()

    assert second_metrics
    assert len(collector.quote_calls) == 10
    second_collect_calls = collector.quote_calls[5:]
    assert len([call for call in second_collect_calls if call[1:] == ("usdc", "eth-apx", 10_000_000_000)]) == 1
    assert [call for call in second_collect_calls if call[1].startswith("bsc-")]
    assert not [call for call in second_collect_calls if call[1].startswith("base-")]

    second_best = _best_profit_metric(second_metrics)
    assert second_best.details["sample_entity_id"] == "arb-ethereum-bsc-buy-source-sell-target-10000"


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
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""),
            ChainDefinition(chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""),
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
            ChainDefinition(chain="ethereum", chain_id=1, rpc_url_env="ETH_RPC", default_rpc_url=""),
            ChainDefinition(chain="base", chain_id=8453, rpc_url_env="BASE_RPC", default_rpc_url=""),
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

    assert sample.second_leg.method == "derived_reverse_http_500"
    assert round(sample.final_amount, 6) == 10000
    assert any(call[1] == "eth-apx" and call[2] == "eth-apy" for call in collector.quote_calls)


def _best_profit_metric(metrics):
    return next(
        metric
        for metric in metrics
        if metric.entity_id == "arb-apyusd-apxusd-crosschain" and metric.metric_name == "best_net_profit_usd"
    )
