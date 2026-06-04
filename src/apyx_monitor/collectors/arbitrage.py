from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
from typing import Any

import httpx

from ..config import ArbitrageMonitorDefinition, AssetCatalog, AssetDefinition, Settings
from .base import BaseCollector, MetricPoint


PENDLE_SDK_BASE_URL = "https://api-v2.pendle.finance/core/v2/sdk"
ARBITRAGE_ENTITY_ID = "arb-apyusd-apxusd-crosschain"
BUY_SOURCE_SELL_TARGET = "buy-source-sell-target"
BUY_TARGET_SELL_SOURCE = "buy-target-sell-source"
QUOTE_RETRY_ATTEMPTS = 5
QUOTE_RETRY_DELAY_SECONDS = 1.5
QUOTE_THROTTLE_SECONDS = 1.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PendleQuote:
    amount_in_raw: int
    amount_out_raw: int
    min_out_raw: int | None
    token_in: str
    token_out: str
    method: str | None

    @property
    def rate_raw(self) -> float:
        return self.amount_out_raw / self.amount_in_raw if self.amount_in_raw else 0.0


@dataclass(frozen=True)
class ArbitrageSample:
    monitor: ArbitrageMonitorDefinition
    strategy_id: str
    strategy_label: str
    settlement_chain: str
    remote_chain: str
    buy_chain: str
    sell_chain: str
    start_asset: AssetDefinition
    final_asset: AssetDefinition
    notional_usd: float
    start_amount: float
    first_leg: PendleQuote
    second_leg: PendleQuote
    bought_apyusd_amount: float
    sold_apxusd_amount: float
    final_amount: float
    first_bridge_cost_usd: float
    second_bridge_cost_usd: float
    gross_profit_usd: float
    net_profit_usd: float
    gross_edge_pct: float
    net_edge_pct: float
    total_cost_usd: float
    route_steps: tuple[dict[str, Any], ...]
    recorded_at: datetime

    @property
    def entity_id(self) -> str:
        notional_label = f"{int(self.notional_usd)}" if self.notional_usd.is_integer() else str(self.notional_usd)
        return f"{self.monitor.monitor_id}-{self.strategy_id}-{notional_label}"


class ArbitrageCollector(BaseCollector):
    name = "arbitrage"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog

    async def collect(self) -> list[MetricPoint]:
        asset_map = {asset.asset_id: asset for asset in self.catalog.assets}
        chain_id_map = {chain.chain: chain.chain_id for chain in self.catalog.chains}
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        samples: list[ArbitrageSample] = []

        async with httpx.AsyncClient(timeout=timeout) as client:
            for monitor in self.catalog.arbitrage_monitors:
                if not monitor.enabled:
                    continue
                settlement_apxusd = asset_map.get(monitor.start_asset_id)
                settlement_apyusd = asset_map.get(monitor.intermediate_asset_id)
                remote_apxusd = asset_map.get(monitor.final_asset_id)
                remote_apyusd = self._matching_asset(asset_map, settlement_apyusd, monitor.target_chain)
                if not all([settlement_apxusd, settlement_apyusd, remote_apxusd, remote_apyusd]):
                    continue

                for notional_usd in monitor.notionals_usd:
                    for strategy_id in (BUY_SOURCE_SELL_TARGET, BUY_TARGET_SELL_SOURCE):
                        try:
                            sample = await self._sample_monitor(
                                client,
                                monitor,
                                chain_id_map,
                                settlement_apxusd,
                                settlement_apyusd,
                                remote_apxusd,
                                remote_apyusd,
                                strategy_id,
                                float(notional_usd),
                            )
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "arbitrage sample failed: monitor=%s strategy=%s notional=%s",
                                monitor.monitor_id,
                                strategy_id,
                                notional_usd,
                            )
                            continue
                        samples.append(sample)

        return self._samples_to_metrics(samples)

    async def _sample_monitor(
        self,
        client: httpx.AsyncClient,
        monitor: ArbitrageMonitorDefinition,
        chain_id_map: dict[str, int],
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        strategy_id: str,
        notional_usd: float,
    ) -> ArbitrageSample:
        if strategy_id == BUY_SOURCE_SELL_TARGET:
            return await self._sample_buy_source_sell_target(
                client,
                monitor,
                chain_id_map,
                settlement_apxusd,
                settlement_apyusd,
                remote_apxusd,
                remote_apyusd,
                notional_usd,
            )
        if strategy_id == BUY_TARGET_SELL_SOURCE:
            return await self._sample_buy_target_sell_source(
                client,
                monitor,
                chain_id_map,
                settlement_apxusd,
                settlement_apyusd,
                remote_apxusd,
                remote_apyusd,
                notional_usd,
            )
        raise ValueError(f"Unsupported arbitrage strategy: {strategy_id}")

    async def _sample_buy_source_sell_target(
        self,
        client: httpx.AsyncClient,
        monitor: ArbitrageMonitorDefinition,
        chain_id_map: dict[str, int],
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        notional_usd: float,
    ) -> ArbitrageSample:
        recorded_at = datetime.now(timezone.utc)
        settlement_chain_id = chain_id_map[monitor.source_chain]
        remote_chain_id = chain_id_map[monitor.target_chain]
        amount_in_raw = self._to_raw_amount(notional_usd / settlement_apxusd.price_hint_usd, settlement_apxusd.decimals)
        start_amount = amount_in_raw / 10 ** settlement_apxusd.decimals

        first_leg = await self._quote(
            client,
            settlement_chain_id,
            monitor,
            settlement_apxusd.contract_address,
            settlement_apyusd.contract_address,
            amount_in_raw,
        )
        bought_apyusd_amount = first_leg.amount_out_raw / 10 ** settlement_apyusd.decimals
        remote_apyusd_raw = self._scale_raw_amount(
            first_leg.amount_out_raw,
            settlement_apyusd.decimals,
            remote_apyusd.decimals,
        )
        remote_apyusd_amount = remote_apyusd_raw / 10 ** remote_apyusd.decimals
        second_leg = await self._quote(
            client,
            remote_chain_id,
            monitor,
            remote_apyusd.contract_address,
            remote_apxusd.contract_address,
            remote_apyusd_raw,
        )
        sold_apxusd_amount = second_leg.amount_out_raw / 10 ** remote_apxusd.decimals
        final_raw = self._scale_raw_amount(
            second_leg.amount_out_raw,
            remote_apxusd.decimals,
            settlement_apxusd.decimals,
        )
        final_amount = final_raw / 10 ** settlement_apxusd.decimals
        first_bridge_cost_usd = self._bridge_cost_usd(bought_apyusd_amount, settlement_apyusd, monitor)
        second_bridge_cost_usd = self._bridge_cost_usd(sold_apxusd_amount, remote_apxusd, monitor)
        route_steps = (
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "buy_apyusd_on_settlement_chain",
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": settlement_apyusd.asset_id,
                "to_symbol": settlement_apyusd.symbol,
                "amount_in": start_amount,
                "amount_out": bought_apyusd_amount,
            },
            {
                "type": "bridge",
                "action": "bridge_apyusd_to_remote",
                "from_chain": monitor.source_chain,
                "to_chain": monitor.target_chain,
                "from_asset": settlement_apyusd.asset_id,
                "from_symbol": settlement_apyusd.symbol,
                "to_asset": remote_apyusd.asset_id,
                "to_symbol": remote_apyusd.symbol,
                "amount_in": bought_apyusd_amount,
                "amount_out": remote_apyusd_amount,
                "cost_usd": first_bridge_cost_usd,
            },
            {
                "type": "swap",
                "chain": monitor.target_chain,
                "action": "sell_apyusd_for_apxusd_on_remote_chain",
                "from_asset": remote_apyusd.asset_id,
                "from_symbol": remote_apyusd.symbol,
                "to_asset": remote_apxusd.asset_id,
                "to_symbol": remote_apxusd.symbol,
                "amount_in": remote_apyusd_amount,
                "amount_out": sold_apxusd_amount,
            },
            {
                "type": "bridge",
                "action": "bridge_apxusd_back_to_settlement",
                "from_chain": monitor.target_chain,
                "to_chain": monitor.source_chain,
                "from_asset": remote_apxusd.asset_id,
                "from_symbol": remote_apxusd.symbol,
                "to_asset": settlement_apxusd.asset_id,
                "to_symbol": settlement_apxusd.symbol,
                "amount_in": sold_apxusd_amount,
                "amount_out": final_amount,
                "cost_usd": second_bridge_cost_usd,
            },
                        )

        return self._build_sample(
            monitor=monitor,
            strategy_id=BUY_SOURCE_SELL_TARGET,
            strategy_label=f"{self._display_chain(monitor.source_chain)} 买 apyUSD → {self._display_chain(monitor.target_chain)} 卖 apyUSD",
            settlement_chain=monitor.source_chain,
            remote_chain=monitor.target_chain,
            buy_chain=monitor.source_chain,
            sell_chain=monitor.target_chain,
            start_asset=settlement_apxusd,
            final_asset=settlement_apxusd,
            notional_usd=notional_usd,
            start_amount=start_amount,
            first_leg=first_leg,
            second_leg=second_leg,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_amount=final_amount,
            first_bridge_cost_usd=first_bridge_cost_usd,
            second_bridge_cost_usd=second_bridge_cost_usd,
            route_steps=route_steps,
            recorded_at=recorded_at,
        )

    async def _sample_buy_target_sell_source(
        self,
        client: httpx.AsyncClient,
        monitor: ArbitrageMonitorDefinition,
        chain_id_map: dict[str, int],
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        notional_usd: float,
    ) -> ArbitrageSample:
        recorded_at = datetime.now(timezone.utc)
        settlement_chain_id = chain_id_map[monitor.source_chain]
        remote_chain_id = chain_id_map[monitor.target_chain]
        settlement_apxusd_raw = self._to_raw_amount(
            notional_usd / settlement_apxusd.price_hint_usd,
            settlement_apxusd.decimals,
        )
        start_amount = settlement_apxusd_raw / 10 ** settlement_apxusd.decimals
        remote_apxusd_raw = self._scale_raw_amount(
            settlement_apxusd_raw,
            settlement_apxusd.decimals,
            remote_apxusd.decimals,
        )
        remote_start_amount = remote_apxusd_raw / 10 ** remote_apxusd.decimals

        first_leg = await self._quote(
            client,
            remote_chain_id,
            monitor,
            remote_apxusd.contract_address,
            remote_apyusd.contract_address,
            remote_apxusd_raw,
        )
        bought_apyusd_amount = first_leg.amount_out_raw / 10 ** remote_apyusd.decimals
        settlement_apyusd_raw = self._scale_raw_amount(
            first_leg.amount_out_raw,
            remote_apyusd.decimals,
            settlement_apyusd.decimals,
        )
        settlement_apyusd_amount = settlement_apyusd_raw / 10 ** settlement_apyusd.decimals
        second_leg = await self._quote(
            client,
            settlement_chain_id,
            monitor,
            settlement_apyusd.contract_address,
            settlement_apxusd.contract_address,
            settlement_apyusd_raw,
        )
        sold_apxusd_amount = second_leg.amount_out_raw / 10 ** settlement_apxusd.decimals
        final_amount = sold_apxusd_amount
        first_bridge_cost_usd = self._bridge_cost_usd(start_amount, settlement_apxusd, monitor)
        second_bridge_cost_usd = self._bridge_cost_usd(bought_apyusd_amount, remote_apyusd, monitor)
        route_steps = (
            {
                "type": "bridge",
                "action": "bridge_apxusd_to_remote",
                "from_chain": monitor.source_chain,
                "to_chain": monitor.target_chain,
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": remote_apxusd.asset_id,
                "to_symbol": remote_apxusd.symbol,
                "amount_in": start_amount,
                "amount_out": remote_start_amount,
                "cost_usd": first_bridge_cost_usd,
            },
            {
                "type": "swap",
                "chain": monitor.target_chain,
                "action": "buy_apyusd_on_remote_chain",
                "from_asset": remote_apxusd.asset_id,
                "from_symbol": remote_apxusd.symbol,
                "to_asset": remote_apyusd.asset_id,
                "to_symbol": remote_apyusd.symbol,
                "amount_in": remote_start_amount,
                "amount_out": bought_apyusd_amount,
            },
            {
                "type": "bridge",
                "action": "bridge_apyusd_back_to_settlement",
                "from_chain": monitor.target_chain,
                "to_chain": monitor.source_chain,
                "from_asset": remote_apyusd.asset_id,
                "from_symbol": remote_apyusd.symbol,
                "to_asset": settlement_apyusd.asset_id,
                "to_symbol": settlement_apyusd.symbol,
                "amount_in": bought_apyusd_amount,
                "amount_out": settlement_apyusd_amount,
                "cost_usd": second_bridge_cost_usd,
            },
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "sell_apyusd_for_apxusd_on_settlement_chain",
                "from_asset": settlement_apyusd.asset_id,
                "from_symbol": settlement_apyusd.symbol,
                "to_asset": settlement_apxusd.asset_id,
                "to_symbol": settlement_apxusd.symbol,
                "amount_in": settlement_apyusd_amount,
                "amount_out": sold_apxusd_amount,
            },
        )

        return self._build_sample(
            monitor=monitor,
            strategy_id=BUY_TARGET_SELL_SOURCE,
            strategy_label=f"{self._display_chain(monitor.target_chain)} 买 apyUSD → {self._display_chain(monitor.source_chain)} 卖 apyUSD",
            settlement_chain=monitor.source_chain,
            remote_chain=monitor.target_chain,
            buy_chain=monitor.target_chain,
            sell_chain=monitor.source_chain,
            start_asset=settlement_apxusd,
            final_asset=settlement_apxusd,
            notional_usd=notional_usd,
            start_amount=start_amount,
            first_leg=first_leg,
            second_leg=second_leg,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_amount=final_amount,
            first_bridge_cost_usd=first_bridge_cost_usd,
            second_bridge_cost_usd=second_bridge_cost_usd,
            route_steps=route_steps,
            recorded_at=recorded_at,
        )

    def _build_sample(
        self,
        *,
        monitor: ArbitrageMonitorDefinition,
        strategy_id: str,
        strategy_label: str,
        settlement_chain: str,
        remote_chain: str,
        buy_chain: str,
        sell_chain: str,
        start_asset: AssetDefinition,
        final_asset: AssetDefinition,
        notional_usd: float,
        start_amount: float,
        first_leg: PendleQuote,
        second_leg: PendleQuote,
        bought_apyusd_amount: float,
        sold_apxusd_amount: float,
        final_amount: float,
        first_bridge_cost_usd: float,
        second_bridge_cost_usd: float,
        route_steps: tuple[dict[str, Any], ...],
        recorded_at: datetime,
    ) -> ArbitrageSample:
        final_value_usd = final_amount * final_asset.price_hint_usd
        gross_profit_usd = final_value_usd - notional_usd
        gas_cost_usd = monitor.source_gas_usd + monitor.target_gas_usd
        total_cost_usd = first_bridge_cost_usd + second_bridge_cost_usd + gas_cost_usd
        net_profit_usd = gross_profit_usd - total_cost_usd

        return ArbitrageSample(
            monitor=monitor,
            strategy_id=strategy_id,
            strategy_label=strategy_label,
            settlement_chain=settlement_chain,
            remote_chain=remote_chain,
            buy_chain=buy_chain,
            sell_chain=sell_chain,
            start_asset=start_asset,
            final_asset=final_asset,
            notional_usd=notional_usd,
            start_amount=start_amount,
            first_leg=first_leg,
            second_leg=second_leg,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_amount=final_amount,
            first_bridge_cost_usd=first_bridge_cost_usd,
            second_bridge_cost_usd=second_bridge_cost_usd,
            gross_profit_usd=gross_profit_usd,
            net_profit_usd=net_profit_usd,
            gross_edge_pct=gross_profit_usd / notional_usd * 100,
            net_edge_pct=net_profit_usd / notional_usd * 100,
            total_cost_usd=total_cost_usd,
            route_steps=route_steps,
            recorded_at=recorded_at,
        )

    async def _quote(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> PendleQuote:
        params = {
            "receiver": monitor.receiver_address,
            "slippage": monitor.slippage_bps / 10000,
            "tokensIn": token_in,
            "tokensOut": token_out,
            "amountsIn": str(amount_in_raw),
            "enableAggregator": "true",
            "aggregators": ",".join(monitor.aggregators),
        }
        response: httpx.Response | None = None
        for attempt in range(QUOTE_RETRY_ATTEMPTS):
            await asyncio.sleep(QUOTE_THROTTLE_SECONDS)
            response = await client.get(f"{PENDLE_SDK_BASE_URL}/{chain_id}/convert", params=params)
            if response.status_code != 429:
                break
            retry_after = self._retry_after_seconds(response)
            await asyncio.sleep(retry_after or QUOTE_RETRY_DELAY_SECONDS * (attempt + 1))
        if response is None:
            raise ValueError("Pendle quote response missing")
        response.raise_for_status()
        payload = response.json()
        routes = payload.get("routes") or []
        if not routes:
            raise ValueError("Pendle route not found")
        route = routes[0]
        outputs = route.get("outputs") or []
        if not outputs:
            raise ValueError("Pendle route output not found")

        output = outputs[0]
        min_out_raw = self._extract_min_out(route)
        contract_param_info = route.get("contractParamInfo") or {}
        return PendleQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=int(output["amount"]),
            min_out_raw=min_out_raw,
            token_in=token_in.lower(),
            token_out=str(output.get("token") or token_out).lower(),
            method=contract_param_info.get("method"),
        )

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = response.headers.get("retry-after")
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            return None

    @staticmethod
    def _extract_min_out(route: dict[str, Any]) -> int | None:
        params = (route.get("contractParamInfo") or {}).get("contractCallParams") or []
        if len(params) < 2 or not isinstance(params[1], list) or not params[1]:
            return None
        min_out = params[1][0].get("minOut")
        return int(min_out) if min_out is not None else None

    @staticmethod
    def _matching_asset(
        asset_map: dict[str, AssetDefinition],
        source_asset: AssetDefinition | None,
        chain: str,
    ) -> AssetDefinition | None:
        if source_asset is None:
            return None
        for asset in asset_map.values():
            if asset.group_id == source_asset.group_id and asset.chain == chain:
                return asset
        return None

    @staticmethod
    def _to_raw_amount(amount: float, decimals: int) -> int:
        return int(Decimal(str(amount)) * Decimal(10) ** decimals)

    @staticmethod
    def _scale_raw_amount(amount_raw: int, source_decimals: int, target_decimals: int) -> int:
        if source_decimals == target_decimals:
            return amount_raw
        if target_decimals > source_decimals:
            return amount_raw * 10 ** (target_decimals - source_decimals)
        return amount_raw // 10 ** (source_decimals - target_decimals)

    @staticmethod
    def _bridge_cost_usd(amount: float, asset: AssetDefinition, monitor: ArbitrageMonitorDefinition) -> float:
        return amount * asset.price_hint_usd * monitor.bridge_fee_bps / 10000 + monitor.bridge_fixed_usd

    @staticmethod
    def _display_chain(chain: str) -> str:
        return {"ethereum": "Ethereum", "base": "Base"}.get(chain, chain)

    def _samples_to_metrics(self, samples: list[ArbitrageSample]) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        best_sample = max(samples, key=lambda sample: sample.net_profit_usd, default=None)

        for sample in samples:
            details = self._sample_details(sample)
            for metric_name, value, unit in (
                ("gross_profit_usd", sample.gross_profit_usd, "usd"),
                ("net_profit_usd", sample.net_profit_usd, "usd"),
                ("gross_edge_pct", sample.gross_edge_pct, "pct"),
                ("net_edge_pct", sample.net_edge_pct, "pct"),
                ("bought_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("sold_apxusd", sample.sold_apxusd_amount, "tokens"),
                ("source_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("target_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("target_apxusd", sample.sold_apxusd_amount, "tokens"),
                ("final_apxusd", sample.final_amount, "tokens"),
                ("intermediate_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("total_cost_usd", sample.total_cost_usd, "usd"),
            ):
                metrics.append(
                    MetricPoint(
                        entity_id=sample.entity_id,
                        entity_type="arbitrage_sample",
                        metric_name=metric_name,
                        value=float(value),
                        unit=unit,
                        source="pendle_sdk",
                        recorded_at=sample.recorded_at,
                        details=details,
                    )
                )

        if best_sample is not None:
            best_details = self._sample_details(best_sample)
            best_details["sample_entity_id"] = best_sample.entity_id
            for metric_name, value, unit in (
                ("best_net_profit_usd", best_sample.net_profit_usd, "usd"),
                ("best_net_edge_pct", best_sample.net_edge_pct, "pct"),
                ("best_notional_usd", best_sample.notional_usd, "usd"),
            ):
                metrics.append(
                    MetricPoint(
                        entity_id=ARBITRAGE_ENTITY_ID,
                        entity_type="arbitrage",
                        metric_name=metric_name,
                        value=float(value),
                        unit=unit,
                        source="pendle_sdk",
                        recorded_at=best_sample.recorded_at,
                        details=best_details,
                    )
                )
        return metrics

    @staticmethod
    def _sample_details(sample: ArbitrageSample) -> dict[str, Any]:
        return {
            "monitor_id": sample.monitor.monitor_id,
            "label": sample.monitor.label,
            "source_chain": sample.monitor.source_chain,
            "target_chain": sample.monitor.target_chain,
            "strategy_id": sample.strategy_id,
            "strategy_label": sample.strategy_label,
            "settlement_chain": sample.settlement_chain,
            "remote_chain": sample.remote_chain,
            "buy_chain": sample.buy_chain,
            "sell_chain": sample.sell_chain,
            "notional_usd": sample.notional_usd,
            "start_asset_id": sample.start_asset.asset_id,
            "final_asset_id": sample.final_asset.asset_id,
            "start_amount": sample.start_amount,
            "bought_apyusd_amount": sample.bought_apyusd_amount,
            "sold_apxusd_amount": sample.sold_apxusd_amount,
            "final_amount": sample.final_amount,
            "bridge_fee_bps": sample.monitor.bridge_fee_bps,
            "bridge_fixed_usd": sample.monitor.bridge_fixed_usd,
            "first_bridge_cost_usd": sample.first_bridge_cost_usd,
            "second_bridge_cost_usd": sample.second_bridge_cost_usd,
            "source_gas_usd": sample.monitor.source_gas_usd,
            "target_gas_usd": sample.monitor.target_gas_usd,
            "slippage_bps": sample.monitor.slippage_bps,
            "route_steps": list(sample.route_steps),
            "first_leg": {
                "token_in": sample.first_leg.token_in,
                "token_out": sample.first_leg.token_out,
                "amount_in_raw": str(sample.first_leg.amount_in_raw),
                "amount_out_raw": str(sample.first_leg.amount_out_raw),
                "min_out_raw": str(sample.first_leg.min_out_raw) if sample.first_leg.min_out_raw else None,
                "rate_raw": sample.first_leg.rate_raw,
                "method": sample.first_leg.method,
            },
            "second_leg": {
                "token_in": sample.second_leg.token_in,
                "token_out": sample.second_leg.token_out,
                "amount_in_raw": str(sample.second_leg.amount_in_raw),
                "amount_out_raw": str(sample.second_leg.amount_out_raw),
                "min_out_raw": str(sample.second_leg.min_out_raw) if sample.second_leg.min_out_raw else None,
                "rate_raw": sample.second_leg.rate_raw,
                "method": sample.second_leg.method,
            },
        }
