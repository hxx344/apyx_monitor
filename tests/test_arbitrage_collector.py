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
    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> PendleQuote:
        decimals = {
            "usdc": 6,
            "eth-apx": 18,
            "eth-apy": 18,
            "base-apx": 18,
            "base-apy": 18,
        }
        amounts = {
            ("usdc", "eth-apx"): amount_in_raw * 10**12,
            ("eth-apx", "eth-apy"): amount_in_raw * 2,
            ("base-apy", "base-apx"): amount_in_raw * 103 // 200,
            ("base-apx", "base-apy"): amount_in_raw * 2,
            ("eth-apy", "eth-apx"): amount_in_raw * 51 // 100,
            ("eth-apx", "usdc"): amount_in_raw // 10**12,
        }
        amount_out_raw = amounts[(token_in, token_out)]
        return PendleQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=amount_out_raw,
            min_out_raw=None,
            token_in=token_in,
            token_out=token_out,
            method="mock",
        )


def test_arbitrage_profit_is_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_SOURCE_SELL_TARGET, 10300, 300, 3))


def test_reverse_arbitrage_path_is_also_measured_from_ethereum_usdc():
    asyncio.run(_run_arbitrage_profit_test(BUY_TARGET_SELL_SOURCE, 10200, 200, 2))


async def _run_arbitrage_profit_test(strategy_id: str, final_usdc: float, profit: float, edge_pct: float):
    monitor = ArbitrageMonitorDefinition(
        monitor_id="arb-ethereum-base",
        label="Ethereum ↔ Base",
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
