from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone

from web3 import Web3

from ..config import AssetCatalog, AssetDefinition, PoolArbitrageMonitorDefinition, Settings
from .base import BaseCollector, MetricPoint
from .onchain import CURVE_POOL_ABI

logger = logging.getLogger(__name__)

POOL_ARBITRAGE_ENTITY_ID = "arb-apxusd-usdc-v4-curve"
CURVE_BUY_V4_SELL = "curve-buy-v4-sell"
V4_BUY_CURVE_SELL = "v4-buy-curve-sell"
POOL_ARBITRAGE_DIRECTIONS = (CURVE_BUY_V4_SELL, V4_BUY_CURVE_SELL)

V4_QUOTER_ABI = [
    {
        "type": "function",
        "name": "quoteExactInputSingle",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "name": "params",
                "type": "tuple",
                "components": [
                    {
                        "name": "poolKey",
                        "type": "tuple",
                        "components": [
                            {"name": "currency0", "type": "address"},
                            {"name": "currency1", "type": "address"},
                            {"name": "fee", "type": "uint24"},
                            {"name": "tickSpacing", "type": "int24"},
                            {"name": "hooks", "type": "address"},
                        ],
                    },
                    {"name": "zeroForOne", "type": "bool"},
                    {"name": "exactAmount", "type": "uint128"},
                    {"name": "hookData", "type": "bytes"},
                ],
            }
        ],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "gasEstimate", "type": "uint256"},
        ],
    }
]


@dataclass(frozen=True, slots=True)
class PoolArbitrageQuote:
    monitor: PoolArbitrageMonitorDefinition
    direction: str
    direction_label: str
    notional_usdc: float
    intermediate_apxusd: float
    final_usdc: float
    net_profit_usd: float
    net_edge_pct: float
    v4_quote_gas: int
    block_number: int
    recorded_at: datetime

    @property
    def entity_id(self) -> str:
        notional = (
            int(self.notional_usdc) if self.notional_usdc.is_integer() else self.notional_usdc
        )
        return f"{self.monitor.monitor_id}-{self.direction}-{notional}"


class PoolArbitrageCollector(BaseCollector):
    """Quotes executable Curve/Uniswap v4 loops without inspecting transactions."""

    name = "pool_arbitrage"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog
        self._curve_coin_indices: dict[tuple[str, str, str], tuple[int, int]] = {}

    async def collect(self, block_number: int | None = None) -> list[MetricPoint]:
        return await asyncio.to_thread(self._collect, block_number)

    def _collect(self, block_number: int | None = None) -> list[MetricPoint]:
        asset_map = {asset.asset_id: asset for asset in self.catalog.assets}
        chain_map = self.catalog.chain_map()
        all_quotes: list[PoolArbitrageQuote] = []
        enabled_monitors = 0

        for monitor in self.catalog.pool_arbitrage_monitors:
            if not monitor.enabled:
                continue
            enabled_monitors += 1
            currency0 = asset_map.get(monitor.currency0_asset_id)
            currency1 = asset_map.get(monitor.currency1_asset_id)
            if currency0 is None or currency1 is None:
                raise ValueError(f"Pool arbitrage assets are missing for {monitor.monitor_id}")

            rpc_url = chain_map[monitor.chain].resolve_rpc_url()
            web3 = Web3(
                Web3.HTTPProvider(
                    rpc_url, request_kwargs={"timeout": self.settings.http_timeout_seconds}
                )
            )
            quote_block = block_number if block_number is not None else web3.eth.block_number
            apxusd_index, usdc_index = self._curve_indices(
                web3,
                monitor,
                currency0,
                currency1,
                quote_block,
            )
            recorded_at = datetime.now(timezone.utc)

            jobs = [
                (direction, float(notional))
                for notional in monitor.notionals_usdc
                for direction in POOL_ARBITRAGE_DIRECTIONS
            ]
            with ThreadPoolExecutor(max_workers=min(8, len(jobs))) as executor:
                futures = {
                    executor.submit(
                        self._quote_cycle,
                        rpc_url,
                        monitor,
                        currency0,
                        currency1,
                        direction,
                        notional,
                        quote_block,
                        apxusd_index,
                        usdc_index,
                        recorded_at,
                    ): (direction, notional)
                    for direction, notional in jobs
                }
                for future in as_completed(futures):
                    direction, notional = futures[future]
                    try:
                        all_quotes.append(future.result())
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "池间套利报价失败 │ 监控=%s │ 方向=%s │ 本金=%s │ 错误=%s",
                            monitor.monitor_id,
                            direction,
                            notional,
                            exc,
                        )

        if enabled_monitors and not all_quotes:
            raise RuntimeError("All Curve/v4 pool arbitrage quotes failed")
        return self._quotes_to_metrics(all_quotes)

    def _quote_cycle(
        self,
        rpc_url: str,
        monitor: PoolArbitrageMonitorDefinition,
        apxusd: AssetDefinition,
        usdc: AssetDefinition,
        direction: str,
        notional_usdc: float,
        block_number: int,
        apxusd_index: int,
        usdc_index: int,
        recorded_at: datetime,
    ) -> PoolArbitrageQuote:
        web3 = Web3(
            Web3.HTTPProvider(
                rpc_url, request_kwargs={"timeout": self.settings.http_timeout_seconds}
            )
        )
        curve = web3.eth.contract(
            address=Web3.to_checksum_address(monitor.curve_pool_address),
            abi=CURVE_POOL_ABI,
        )
        quoter = web3.eth.contract(
            address=Web3.to_checksum_address(monitor.v4_quoter_address),
            abi=V4_QUOTER_ABI,
        )
        usdc_in_raw = int(round(notional_usdc * 10**usdc.decimals))

        if direction == CURVE_BUY_V4_SELL:
            apxusd_raw = int(
                curve.functions.get_dy(usdc_index, apxusd_index, usdc_in_raw).call(
                    block_identifier=block_number
                )
            )
            final_usdc_raw, quote_gas = self._quote_v4(
                quoter, monitor, apxusd, usdc, True, apxusd_raw, block_number
            )
            direction_label = "Curve 买 apxUSD → v4 卖出"
        elif direction == V4_BUY_CURVE_SELL:
            apxusd_raw, quote_gas = self._quote_v4(
                quoter, monitor, apxusd, usdc, False, usdc_in_raw, block_number
            )
            final_usdc_raw = int(
                curve.functions.get_dy(apxusd_index, usdc_index, apxusd_raw).call(
                    block_identifier=block_number
                )
            )
            direction_label = "v4 买 apxUSD → Curve 卖出"
        else:
            raise ValueError(f"Unsupported pool arbitrage direction: {direction}")

        intermediate_apxusd = apxusd_raw / 10**apxusd.decimals
        final_usdc = final_usdc_raw / 10**usdc.decimals
        net_profit_usd = final_usdc - notional_usdc
        net_edge_pct = net_profit_usd / notional_usdc * 100 if notional_usdc else 0.0
        return PoolArbitrageQuote(
            monitor=monitor,
            direction=direction,
            direction_label=direction_label,
            notional_usdc=notional_usdc,
            intermediate_apxusd=intermediate_apxusd,
            final_usdc=final_usdc,
            net_profit_usd=net_profit_usd,
            net_edge_pct=net_edge_pct,
            v4_quote_gas=quote_gas,
            block_number=block_number,
            recorded_at=recorded_at,
        )

    def _curve_indices(
        self,
        web3: Web3,
        monitor: PoolArbitrageMonitorDefinition,
        apxusd: AssetDefinition,
        usdc: AssetDefinition,
        block_number: int,
    ) -> tuple[int, int]:
        cache_key = (
            monitor.curve_pool_address.lower(),
            apxusd.contract_address.lower(),
            usdc.contract_address.lower(),
        )
        cached = self._curve_coin_indices.get(cache_key)
        if cached is not None:
            return cached
        curve = web3.eth.contract(
            address=Web3.to_checksum_address(monitor.curve_pool_address),
            abi=CURVE_POOL_ABI,
        )
        coin_addresses = [
            Web3.to_checksum_address(
                curve.functions.coins(index).call(block_identifier=block_number)
            )
            for index in range(2)
        ]
        indices = (
            coin_addresses.index(Web3.to_checksum_address(apxusd.contract_address)),
            coin_addresses.index(Web3.to_checksum_address(usdc.contract_address)),
        )
        self._curve_coin_indices[cache_key] = indices
        return indices

    @staticmethod
    def _quote_v4(
        quoter,
        monitor: PoolArbitrageMonitorDefinition,
        currency0: AssetDefinition,
        currency1: AssetDefinition,
        zero_for_one: bool,
        amount_in_raw: int,
        block_number: int,
    ) -> tuple[int, int]:
        pool_key = (
            Web3.to_checksum_address(currency0.contract_address),
            Web3.to_checksum_address(currency1.contract_address),
            monitor.fee,
            monitor.tick_spacing,
            Web3.to_checksum_address(monitor.hooks_address),
        )
        amount_out, gas_estimate = quoter.functions.quoteExactInputSingle(
            (pool_key, zero_for_one, amount_in_raw, b"")
        ).call(block_identifier=block_number)
        return int(amount_out), int(gas_estimate)

    @staticmethod
    def _quotes_to_metrics(quotes: list[PoolArbitrageQuote]) -> list[MetricPoint]:
        if not quotes:
            return []
        metrics: list[MetricPoint] = []
        for quote in quotes:
            details = PoolArbitrageCollector._quote_details(quote)
            values = {
                "intermediate_apxusd": (quote.intermediate_apxusd, "apxUSD"),
                "final_usdc": (quote.final_usdc, "USDC"),
                "net_profit_usd": (quote.net_profit_usd, "usd"),
                "net_edge_pct": (quote.net_edge_pct, "pct"),
            }
            metrics.extend(
                MetricPoint(
                    entity_id=quote.entity_id,
                    entity_type="pool_arbitrage_quote",
                    metric_name=name,
                    value=value,
                    unit=unit,
                    source="rpc:ethereum:curve-v4-quoter",
                    recorded_at=quote.recorded_at,
                    details=details,
                )
                for name, (value, unit) in values.items()
            )

        best = max(quotes, key=lambda quote: quote.net_profit_usd)
        best_details = PoolArbitrageCollector._quote_details(best)
        for name, value, unit in (
            ("best_net_profit_usd", best.net_profit_usd, "usd"),
            ("best_net_edge_pct", best.net_edge_pct, "pct"),
            ("best_notional_usdc", best.notional_usdc, "USDC"),
        ):
            metrics.append(
                MetricPoint(
                    entity_id=best.monitor.monitor_id,
                    entity_type="pool_arbitrage",
                    metric_name=name,
                    value=value,
                    unit=unit,
                    source="derived:curve-v4-arbitrage",
                    recorded_at=best.recorded_at,
                    details=best_details,
                )
            )
        return metrics

    @staticmethod
    def _quote_details(quote: PoolArbitrageQuote) -> dict:
        first_venue, second_venue = (
            ("Curve", "Uniswap v4")
            if quote.direction == CURVE_BUY_V4_SELL
            else ("Uniswap v4", "Curve")
        )
        return {
            "monitor_id": quote.monitor.monitor_id,
            "label": quote.monitor.label,
            "sample_entity_id": quote.entity_id,
            "strategy_id": quote.direction,
            "strategy_label": quote.direction_label,
            "notional_usd": quote.notional_usdc,
            "start_symbol": "USDC",
            "final_amount": quote.final_usdc,
            "final_symbol": "USDC",
            "intermediate_apxusd": quote.intermediate_apxusd,
            "net_profit_usd": quote.net_profit_usd,
            "net_edge_pct": quote.net_edge_pct,
            "block_number": quote.block_number,
            "v4_quote_gas": quote.v4_quote_gas,
            "curve_pool_address": quote.monitor.curve_pool_address,
            "v4_pool_id": quote.monitor.v4_pool_id,
            "route_steps": [
                {
                    "type": "swap",
                    "chain": "ethereum",
                    "venue": first_venue,
                    "from_symbol": "USDC",
                    "to_symbol": "apxUSD",
                    "amount_in": quote.notional_usdc,
                    "amount_out": quote.intermediate_apxusd,
                },
                {
                    "type": "swap",
                    "chain": "ethereum",
                    "venue": second_venue,
                    "from_symbol": "apxUSD",
                    "to_symbol": "USDC",
                    "amount_in": quote.intermediate_apxusd,
                    "amount_out": quote.final_usdc,
                },
            ],
        }
