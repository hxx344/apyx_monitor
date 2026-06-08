from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx
from sqlmodel import Session, desc, select

from ..config import ArbitrageMonitorDefinition, AssetCatalog, AssetDefinition, Settings
from ..db import engine
from ..models import MetricSnapshot
from .base import BaseCollector, MetricPoint

VELORA_MARKET_API_BASE_URL = "https://api.paraswap.io"
ARBITRAGE_ENTITY_ID = "arb-apyusd-apxusd-crosschain"
BUY_SOURCE_SELL_TARGET = "buy-source-sell-target"
BUY_TARGET_SELL_SOURCE = "buy-target-sell-source"
QUOTE_THROTTLE_SECONDS = 4.0
RATE_LIMIT_COOLDOWN_SECONDS = 600
CURVE_NAV_ENTITY_ID = "curve-apyusd-apxusd"
CURVE_NAV_DEVIATION_METRIC = "curve_rate_vs_nav_deviation_pct"

logger = logging.getLogger(__name__)
_rate_limited_until: datetime | None = None


class VeloraRateLimitedError(RuntimeError):
    pass


class VeloraRouteUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class SwapQuote:
    amount_in_raw: int
    amount_out_raw: int
    min_out_raw: int | None
    token_in: str
    token_out: str
    method: str | None
    routing: dict[str, Any] | None = None

    @property
    def rate_raw(self) -> float:
        return self.amount_out_raw / self.amount_in_raw if self.amount_in_raw else 0.0


QuoteCache = dict[tuple[int, str, str, int, float, str, tuple[str, ...]], SwapQuote]


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
    entry_leg: SwapQuote
    first_leg: SwapQuote
    second_leg: SwapQuote
    exit_leg: SwapQuote
    entry_apxusd_amount: float
    bought_apyusd_amount: float
    sold_apxusd_amount: float
    final_apxusd_amount: float
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
        notional_label = (
            f"{int(self.notional_usd)}"
            if self.notional_usd.is_integer()
            else str(self.notional_usd)
        )
        return f"{self.monitor.monitor_id}-{self.strategy_id}-{notional_label}"


class ArbitrageCollector(BaseCollector):
    name = "arbitrage"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog
        self._next_monitor_index = 0
        self._latest_samples: dict[str, ArbitrageSample] = {}
        self._asset_decimals_by_address = {
            asset.contract_address.lower(): asset.decimals for asset in self.catalog.assets
        }

    async def collect(self) -> list[MetricPoint]:
        global _rate_limited_until
        now = datetime.now(timezone.utc)
        if _rate_limited_until is not None and now < _rate_limited_until:
            logger.warning(
                "arbitrage collector skipped because Velora Market API is rate limited until %s",
                _rate_limited_until.isoformat(),
            )
            return []

        asset_map = {asset.asset_id: asset for asset in self.catalog.assets}
        chain_id_map = {chain.chain: chain.chain_id for chain in self.catalog.chains}
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        samples: list[ArbitrageSample] = []
        quote_cache: QuoteCache = {}
        monitor_contexts: list[
            tuple[
                ArbitrageMonitorDefinition,
                AssetDefinition,
                AssetDefinition,
                AssetDefinition,
                AssetDefinition,
                AssetDefinition,
            ]
        ] = []

        for monitor in self.catalog.arbitrage_monitors:
            if not monitor.enabled:
                continue
            settlement_apxusd = asset_map.get(monitor.start_asset_id)
            settlement_apyusd = asset_map.get(monitor.intermediate_asset_id)
            remote_apxusd = asset_map.get(monitor.final_asset_id)
            remote_apyusd = self._matching_asset(asset_map, settlement_apyusd, monitor.target_chain)
            funding_asset = (
                asset_map.get(monitor.funding_asset_id)
                if monitor.funding_asset_id
                else settlement_apxusd
            )
            if not all(
                [funding_asset, settlement_apxusd, settlement_apyusd, remote_apxusd, remote_apyusd]
            ):
                logger.warning(
                    "arbitrage monitor skipped because assets are missing: monitor=%s",
                    monitor.monitor_id,
                )
                continue
            monitor_contexts.append(
                (
                    monitor,
                    funding_asset,
                    settlement_apxusd,
                    settlement_apyusd,
                    remote_apxusd,
                    remote_apyusd,
                )
            )

        if not monitor_contexts:
            return self._samples_to_metrics(
                samples,
                best_candidates=list(self._latest_samples.values()),
                best_recorded_at=now,
            )

        selected_index = self._next_monitor_index % len(monitor_contexts)
        self._next_monitor_index = (selected_index + 1) % len(monitor_contexts)
        (
            monitor,
            funding_asset,
            settlement_apxusd,
            settlement_apyusd,
            remote_apxusd,
            remote_apyusd,
        ) = monitor_contexts[selected_index]
        logger.info(
            "arbitrage collector sampling monitor %s (%s/%s)",
            monitor.monitor_id,
            selected_index + 1,
            len(monitor_contexts),
        )
        if not self._should_calculate_arbitrage_paths(now):
            return self._samples_to_metrics(
                samples,
                best_candidates=list(self._latest_samples.values()),
                best_recorded_at=now,
            )

        async with httpx.AsyncClient(timeout=timeout) as client:
            for notional_usd in monitor.notionals_usd:
                for strategy_id in (BUY_SOURCE_SELL_TARGET, BUY_TARGET_SELL_SOURCE):
                    try:
                        sample = await self._sample_monitor(
                            client,
                            monitor,
                            chain_id_map,
                            funding_asset,
                            settlement_apxusd,
                            settlement_apyusd,
                            remote_apxusd,
                            remote_apyusd,
                            strategy_id,
                            float(notional_usd),
                            quote_cache,
                        )
                    except VeloraRateLimitedError:
                        _rate_limited_until = datetime.now(timezone.utc) + timedelta(
                            seconds=RATE_LIMIT_COOLDOWN_SECONDS
                        )
                        logger.warning(
                            "arbitrage collector entering Velora Market API rate-limit "
                            "cooldown until %s",
                            _rate_limited_until.isoformat(),
                        )
                        return self._samples_to_metrics(
                            samples,
                            best_candidates=list(self._latest_samples.values()),
                            best_recorded_at=now,
                        )
                    except VeloraRouteUnavailableError as exc:
                        logger.warning(
                            "arbitrage sample skipped because Velora route is unavailable: "
                            "monitor=%s strategy=%s notional=%s error=%s",
                            monitor.monitor_id,
                            strategy_id,
                            notional_usd,
                            exc,
                        )
                        continue
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "arbitrage sample failed: monitor=%s strategy=%s notional=%s",
                            monitor.monitor_id,
                            strategy_id,
                            notional_usd,
                        )
                        continue
                    samples.append(sample)
                    self._latest_samples[sample.entity_id] = sample

        return self._samples_to_metrics(
            samples,
            best_candidates=list(self._latest_samples.values()),
            best_recorded_at=now,
        )

    async def _sample_monitor(
        self,
        client: httpx.AsyncClient,
        monitor: ArbitrageMonitorDefinition,
        chain_id_map: dict[str, int],
        funding_asset: AssetDefinition,
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        strategy_id: str,
        notional_usd: float,
        quote_cache: QuoteCache | None = None,
    ) -> ArbitrageSample:
        if strategy_id == BUY_SOURCE_SELL_TARGET:
            return await self._sample_buy_source_sell_target(
                client,
                monitor,
                chain_id_map,
                funding_asset,
                settlement_apxusd,
                settlement_apyusd,
                remote_apxusd,
                remote_apyusd,
                notional_usd,
                quote_cache,
            )
        if strategy_id == BUY_TARGET_SELL_SOURCE:
            return await self._sample_buy_target_sell_source(
                client,
                monitor,
                chain_id_map,
                funding_asset,
                settlement_apxusd,
                settlement_apyusd,
                remote_apxusd,
                remote_apyusd,
                notional_usd,
                quote_cache,
            )
        raise ValueError(f"Unsupported arbitrage strategy: {strategy_id}")

    async def _sample_buy_source_sell_target(
        self,
        client: httpx.AsyncClient,
        monitor: ArbitrageMonitorDefinition,
        chain_id_map: dict[str, int],
        funding_asset: AssetDefinition,
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        notional_usd: float,
        quote_cache: QuoteCache | None,
    ) -> ArbitrageSample:
        recorded_at = datetime.now(timezone.utc)
        settlement_chain_id = chain_id_map[monitor.source_chain]
        remote_chain_id = chain_id_map[monitor.target_chain]
        funding_raw = self._to_raw_amount(
            notional_usd / funding_asset.price_hint_usd,
            funding_asset.decimals,
        )
        start_amount = funding_raw / 10 ** funding_asset.decimals

        entry_leg = await self._quote_cached(
            client,
            settlement_chain_id,
            monitor,
            funding_asset.contract_address,
            settlement_apxusd.contract_address,
            funding_raw,
            quote_cache,
        )
        entry_apxusd_amount = entry_leg.amount_out_raw / 10 ** settlement_apxusd.decimals
        first_leg = await self._quote_cached(
            client,
            settlement_chain_id,
            monitor,
            settlement_apxusd.contract_address,
            settlement_apyusd.contract_address,
            entry_leg.amount_out_raw,
            quote_cache,
        )
        bought_apyusd_amount = first_leg.amount_out_raw / 10 ** settlement_apyusd.decimals
        remote_apyusd_raw = self._scale_raw_amount(
            first_leg.amount_out_raw,
            settlement_apyusd.decimals,
            remote_apyusd.decimals,
        )
        remote_apyusd_amount = remote_apyusd_raw / 10 ** remote_apyusd.decimals
        second_leg = await self._quote_cached(
            client,
            remote_chain_id,
            monitor,
            remote_apyusd.contract_address,
            remote_apxusd.contract_address,
            remote_apyusd_raw,
            quote_cache,
        )
        sold_apxusd_amount = second_leg.amount_out_raw / 10 ** remote_apxusd.decimals
        final_raw = self._scale_raw_amount(
            second_leg.amount_out_raw,
            remote_apxusd.decimals,
            settlement_apxusd.decimals,
        )
        final_apxusd_amount = final_raw / 10 ** settlement_apxusd.decimals
        exit_leg = self._reverse_entry_quote(
            entry_leg,
            settlement_apxusd.contract_address,
            funding_asset.contract_address,
            final_raw,
        )
        final_amount = exit_leg.amount_out_raw / 10 ** funding_asset.decimals
        first_bridge_cost_usd = self._bridge_cost_usd(
            bought_apyusd_amount,
            settlement_apyusd,
            monitor,
        )
        second_bridge_cost_usd = self._bridge_cost_usd(sold_apxusd_amount, remote_apxusd, monitor)
        route_steps = (
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "enter_apxusd_on_settlement_chain",
                "from_asset": funding_asset.asset_id,
                "from_symbol": funding_asset.symbol,
                "to_asset": settlement_apxusd.asset_id,
                "to_symbol": settlement_apxusd.symbol,
                "amount_in": start_amount,
                "amount_out": entry_apxusd_amount,
                "routing": entry_leg.routing,
            },
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "buy_apyusd_on_settlement_chain",
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": settlement_apyusd.asset_id,
                "to_symbol": settlement_apyusd.symbol,
                "amount_in": entry_apxusd_amount,
                "amount_out": bought_apyusd_amount,
                "routing": first_leg.routing,
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
                "routing": second_leg.routing,
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
                "amount_out": final_apxusd_amount,
                "cost_usd": second_bridge_cost_usd,
            },
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "exit_to_funding_asset_on_settlement_chain",
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": funding_asset.asset_id,
                "to_symbol": funding_asset.symbol,
                "amount_in": final_apxusd_amount,
                "amount_out": final_amount,
                "routing": exit_leg.routing,
            },
        )

        return self._build_sample(
            monitor=monitor,
            strategy_id=BUY_SOURCE_SELL_TARGET,
            strategy_label=(
                f"{self._display_chain(monitor.source_chain)} 买 apyUSD → "
                f"{self._display_chain(monitor.target_chain)} 卖 apyUSD"
            ),
            settlement_chain=monitor.source_chain,
            remote_chain=monitor.target_chain,
            buy_chain=monitor.source_chain,
            sell_chain=monitor.target_chain,
            start_asset=funding_asset,
            final_asset=funding_asset,
            notional_usd=notional_usd,
            start_amount=start_amount,
            entry_leg=entry_leg,
            first_leg=first_leg,
            second_leg=second_leg,
            exit_leg=exit_leg,
            entry_apxusd_amount=entry_apxusd_amount,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_apxusd_amount=final_apxusd_amount,
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
        funding_asset: AssetDefinition,
        settlement_apxusd: AssetDefinition,
        settlement_apyusd: AssetDefinition,
        remote_apxusd: AssetDefinition,
        remote_apyusd: AssetDefinition,
        notional_usd: float,
        quote_cache: QuoteCache | None,
    ) -> ArbitrageSample:
        recorded_at = datetime.now(timezone.utc)
        settlement_chain_id = chain_id_map[monitor.source_chain]
        remote_chain_id = chain_id_map[monitor.target_chain]
        funding_raw = self._to_raw_amount(
            notional_usd / funding_asset.price_hint_usd,
            funding_asset.decimals,
        )
        start_amount = funding_raw / 10 ** funding_asset.decimals
        entry_leg = await self._quote_cached(
            client,
            settlement_chain_id,
            monitor,
            funding_asset.contract_address,
            settlement_apxusd.contract_address,
            funding_raw,
            quote_cache,
        )
        entry_apxusd_amount = entry_leg.amount_out_raw / 10 ** settlement_apxusd.decimals
        remote_apxusd_raw = self._scale_raw_amount(
            entry_leg.amount_out_raw,
            settlement_apxusd.decimals,
            remote_apxusd.decimals,
        )
        remote_start_amount = remote_apxusd_raw / 10 ** remote_apxusd.decimals

        first_leg = await self._quote_cached(
            client,
            remote_chain_id,
            monitor,
            remote_apxusd.contract_address,
            remote_apyusd.contract_address,
            remote_apxusd_raw,
            quote_cache,
            allow_reverse_fallback=True,
        )
        bought_apyusd_amount = first_leg.amount_out_raw / 10 ** remote_apyusd.decimals
        settlement_apyusd_raw = self._scale_raw_amount(
            first_leg.amount_out_raw,
            remote_apyusd.decimals,
            settlement_apyusd.decimals,
        )
        settlement_apyusd_amount = settlement_apyusd_raw / 10 ** settlement_apyusd.decimals
        second_leg = await self._quote_cached(
            client,
            settlement_chain_id,
            monitor,
            settlement_apyusd.contract_address,
            settlement_apxusd.contract_address,
            settlement_apyusd_raw,
            quote_cache,
            allow_reverse_fallback=True,
        )
        sold_apxusd_amount = second_leg.amount_out_raw / 10 ** settlement_apxusd.decimals
        final_apxusd_amount = sold_apxusd_amount
        exit_leg = self._reverse_entry_quote(
            entry_leg,
            settlement_apxusd.contract_address,
            funding_asset.contract_address,
            second_leg.amount_out_raw,
        )
        final_amount = exit_leg.amount_out_raw / 10 ** funding_asset.decimals
        first_bridge_cost_usd = self._bridge_cost_usd(
            entry_apxusd_amount,
            settlement_apxusd,
            monitor,
        )
        second_bridge_cost_usd = self._bridge_cost_usd(bought_apyusd_amount, remote_apyusd, monitor)
        route_steps = (
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "enter_apxusd_on_settlement_chain",
                "from_asset": funding_asset.asset_id,
                "from_symbol": funding_asset.symbol,
                "to_asset": settlement_apxusd.asset_id,
                "to_symbol": settlement_apxusd.symbol,
                "amount_in": start_amount,
                "amount_out": entry_apxusd_amount,
                "routing": entry_leg.routing,
            },
            {
                "type": "bridge",
                "action": "bridge_apxusd_to_remote",
                "from_chain": monitor.source_chain,
                "to_chain": monitor.target_chain,
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": remote_apxusd.asset_id,
                "to_symbol": remote_apxusd.symbol,
                "amount_in": entry_apxusd_amount,
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
                "routing": first_leg.routing,
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
                "routing": second_leg.routing,
            },
            {
                "type": "swap",
                "chain": monitor.source_chain,
                "action": "exit_to_funding_asset_on_settlement_chain",
                "from_asset": settlement_apxusd.asset_id,
                "from_symbol": settlement_apxusd.symbol,
                "to_asset": funding_asset.asset_id,
                "to_symbol": funding_asset.symbol,
                "amount_in": final_apxusd_amount,
                "amount_out": final_amount,
                "routing": exit_leg.routing,
            },
        )

        return self._build_sample(
            monitor=monitor,
            strategy_id=BUY_TARGET_SELL_SOURCE,
            strategy_label=(
                f"{self._display_chain(monitor.target_chain)} 买 apyUSD → "
                f"{self._display_chain(monitor.source_chain)} 卖 apyUSD"
            ),
            settlement_chain=monitor.source_chain,
            remote_chain=monitor.target_chain,
            buy_chain=monitor.target_chain,
            sell_chain=monitor.source_chain,
            start_asset=funding_asset,
            final_asset=funding_asset,
            notional_usd=notional_usd,
            start_amount=start_amount,
            entry_leg=entry_leg,
            first_leg=first_leg,
            second_leg=second_leg,
            exit_leg=exit_leg,
            entry_apxusd_amount=entry_apxusd_amount,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_apxusd_amount=final_apxusd_amount,
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
        entry_leg: SwapQuote,
        first_leg: SwapQuote,
        second_leg: SwapQuote,
        exit_leg: SwapQuote,
        entry_apxusd_amount: float,
        bought_apyusd_amount: float,
        sold_apxusd_amount: float,
        final_apxusd_amount: float,
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
            entry_leg=entry_leg,
            first_leg=first_leg,
            second_leg=second_leg,
            exit_leg=exit_leg,
            entry_apxusd_amount=entry_apxusd_amount,
            bought_apyusd_amount=bought_apyusd_amount,
            sold_apxusd_amount=sold_apxusd_amount,
            final_apxusd_amount=final_apxusd_amount,
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
    ) -> SwapQuote:
        params = {
            "srcToken": token_in,
            "destToken": token_out,
            "amount": str(amount_in_raw),
            "side": "SELL",
            "network": chain_id,
            "version": "6.2",
        }
        src_decimals = self._token_decimals(token_in)
        dest_decimals = self._token_decimals(token_out)
        if src_decimals is not None:
            params["srcDecimals"] = src_decimals
        if dest_decimals is not None:
            params["destDecimals"] = dest_decimals

        await asyncio.sleep(QUOTE_THROTTLE_SECONDS)
        response = await client.get(f"{VELORA_MARKET_API_BASE_URL}/prices", params=params)
        if response.status_code == 429:
            retry_after = self._retry_after_seconds(response) or RATE_LIMIT_COOLDOWN_SECONDS
            raise VeloraRateLimitedError(
                f"Velora Market API rate limited; retry after {retry_after:.0f}s"
            )
        if self._is_route_unavailable(response):
            raise VeloraRouteUnavailableError(
                f"Velora route unavailable: status={response.status_code} "
                f"chain={chain_id} token_in={token_in} token_out={token_out}"
            )
        response.raise_for_status()
        payload = response.json()
        price_route = payload.get("priceRoute") or payload
        dest_amount = price_route.get("destAmount")
        if dest_amount is None:
            raise ValueError("Velora market route output not found")

        return SwapQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=int(dest_amount),
            min_out_raw=None,
            token_in=token_in.lower(),
            token_out=token_out.lower(),
            method=f"velora_market_{price_route.get('version') or '6.2'}",
            routing=self._extract_velora_routing(price_route),
        )

    async def _quote_conversion(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> SwapQuote:
        if token_in.lower() == token_out.lower():
            return SwapQuote(
                amount_in_raw=amount_in_raw,
                amount_out_raw=amount_in_raw,
                min_out_raw=amount_in_raw,
                token_in=token_in.lower(),
                token_out=token_out.lower(),
                method="identity",
            )
        return await self._quote(client, chain_id, monitor, token_in, token_out, amount_in_raw)

    async def _quote_cached(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_cache: QuoteCache | None,
        *,
        allow_reverse_fallback: bool = False,
    ) -> SwapQuote:
        key = (
            chain_id,
            token_in.lower(),
            token_out.lower(),
            amount_in_raw,
            monitor.slippage_bps,
            monitor.receiver_address.lower(),
            "velora_market",
        )
        if quote_cache is not None and key in quote_cache:
            return quote_cache[key]
        try:
            quote = await self._quote_conversion(
                client,
                chain_id,
                monitor,
                token_in,
                token_out,
                amount_in_raw,
            )
        except (VeloraRouteUnavailableError, httpx.HTTPStatusError) as exc:
            response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
            if response is not None and not self._can_use_reverse_fallback(response):
                raise
            failed_status_code = response.status_code if response is not None else 422
            if not allow_reverse_fallback:
                raise
            quote = await self._quote_from_reverse_conversion(
                client,
                chain_id,
                monitor,
                token_in,
                token_out,
                amount_in_raw,
                quote_cache,
                failed_status_code,
            )
        if quote_cache is not None:
            quote_cache[key] = quote
        return quote

    async def _quote_from_reverse_conversion(
        self,
        client: httpx.AsyncClient,
        chain_id: int,
        monitor: ArbitrageMonitorDefinition,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
        quote_cache: QuoteCache | None,
        failed_status_code: int,
    ) -> SwapQuote:
        reverse_quote = await self._quote_cached(
            client,
            chain_id,
            monitor,
            token_out,
            token_in,
            amount_in_raw,
            quote_cache,
        )
        if reverse_quote.amount_out_raw <= 0:
            raise ValueError("Velora reverse route output is zero")

        amount_out_raw = amount_in_raw * reverse_quote.amount_in_raw // reverse_quote.amount_out_raw
        logger.warning(
            "Velora quote %s -> %s on chain %s returned %s; using reverse quote fallback",
            token_in,
            token_out,
            chain_id,
            failed_status_code,
        )
        return SwapQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=amount_out_raw,
            min_out_raw=None,
            token_in=token_in.lower(),
            token_out=token_out.lower(),
            method=f"derived_reverse_http_{failed_status_code}",
            routing={
                "provider": "derived_reverse",
                "failed_status_code": failed_status_code,
                "source": "reverse_quote",
                "reverse_routing": reverse_quote.routing,
            },
        )

    @staticmethod
    def _reverse_entry_quote(
        entry_leg: SwapQuote,
        token_in: str,
        token_out: str,
        amount_in_raw: int,
    ) -> SwapQuote:
        amount_out_raw = amount_in_raw * entry_leg.amount_in_raw // entry_leg.amount_out_raw
        return SwapQuote(
            amount_in_raw=amount_in_raw,
            amount_out_raw=amount_out_raw,
            min_out_raw=None,
            token_in=token_in.lower(),
            token_out=token_out.lower(),
            method="derived_reverse_entry",
            routing={
                "provider": "derived_reverse_entry",
                "source": "entry_leg",
                "entry_routing": entry_leg.routing,
            },
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
    def _can_use_reverse_fallback(response: httpx.Response) -> bool:
        return response.status_code in {400, 404, 422, 500, 501, 502, 503, 504}

    @staticmethod
    def _is_route_unavailable(response: httpx.Response) -> bool:
        return response.status_code in {400, 404, 422}

    def _token_decimals(self, token_address: str) -> int | None:
        return self._asset_decimals_by_address.get(token_address.lower())

    @staticmethod
    def _extract_velora_routing(price_route: dict[str, Any]) -> dict[str, Any]:
        return {
            "provider": "velora",
            "mode": "market",
            "version": price_route.get("version"),
            "network": price_route.get("network"),
            "block_number": price_route.get("blockNumber"),
            "side": price_route.get("side"),
            "contract_method": price_route.get("contractMethod"),
            "contract_address": price_route.get("contractAddress"),
            "token_transfer_proxy": price_route.get("tokenTransferProxy"),
            "gas_cost": price_route.get("gasCost"),
            "gas_cost_usd": price_route.get("gasCostUSD"),
            "src_usd": price_route.get("srcUSD"),
            "dest_usd": price_route.get("destUSD"),
            "dest_amount_after_fee": price_route.get("destAmountAfterFee"),
            "partner_fee": price_route.get("partnerFee"),
            "max_impact_reached": price_route.get("maxImpactReached"),
            "best_route": [
                {
                    "percent": route.get("percent"),
                    "swaps": [
                        {
                            "src_token": swap.get("srcToken"),
                            "src_decimals": swap.get("srcDecimals"),
                            "dest_token": swap.get("destToken"),
                            "dest_decimals": swap.get("destDecimals"),
                            "swap_exchanges": [
                                {
                                    "exchange": exchange.get("exchange"),
                                    "src_amount": exchange.get("srcAmount"),
                                    "dest_amount": exchange.get("destAmount"),
                                    "percent": exchange.get("percent"),
                                    "pool_addresses": exchange.get("poolAddresses") or [],
                                    "pool_identifiers": exchange.get("poolIdentifiers") or [],
                                    "data": exchange.get("data") or {},
                                }
                                for exchange in swap.get("swapExchanges") or []
                            ],
                        }
                        for swap in route.get("swaps") or []
                    ],
                }
                for route in price_route.get("bestRoute") or []
            ],
        }

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
    def _bridge_cost_usd(
        amount: float,
        asset: AssetDefinition,
        monitor: ArbitrageMonitorDefinition,
    ) -> float:
        return (
            amount * asset.price_hint_usd * monitor.bridge_fee_bps / 10000
            + monitor.bridge_fixed_usd
        )

    def _should_calculate_arbitrage_paths(self, now: datetime) -> bool:
        if not self.settings.arbitrage_curve_gate_enabled:
            return True

        snapshots = self._latest_curve_nav_deviation_snapshots(limit=10)
        if not snapshots:
            logger.info(
                "arbitrage collector skipped Velora path calculation because Curve/NAV "
                "deviation is not available yet"
            )
            return False

        latest = snapshots[0]
        latest_recorded_at = self._as_utc(latest.recorded_at)
        age_seconds = (now - latest_recorded_at).total_seconds()
        if age_seconds > self.settings.arbitrage_curve_gate_max_age_seconds:
            logger.info(
                "arbitrage collector skipped Velora path calculation because latest "
                "Curve/NAV deviation is stale: age=%.0fs max_age=%ss",
                age_seconds,
                self.settings.arbitrage_curve_gate_max_age_seconds,
            )
            return False

        latest_deviation_pct = abs(latest.value)
        deviation_triggered = (
            latest_deviation_pct >= self.settings.arbitrage_curve_gate_min_deviation_pct
        )
        change_window_started_at = now - timedelta(
            seconds=self.settings.arbitrage_curve_gate_change_window_seconds
        )
        window_values = [
            snapshot.value
            for snapshot in snapshots
            if self._as_utc(snapshot.recorded_at) >= change_window_started_at
        ]
        change_pct = max(window_values) - min(window_values) if len(window_values) > 1 else 0.0
        change_triggered = change_pct >= self.settings.arbitrage_curve_gate_min_change_pct

        if deviation_triggered or change_triggered:
            logger.info(
                "arbitrage collector entering Velora path calculation: "
                "curve_nav_deviation=%.4f%% change=%.4f%% window=%ss",
                latest.value,
                change_pct,
                self.settings.arbitrage_curve_gate_change_window_seconds,
            )
            return True

        logger.info(
            "arbitrage collector skipped Velora path calculation because Curve/NAV is quiet: "
            "deviation=%.4f%% min_deviation=%.4f%% change=%.4f%% min_change=%.4f%% "
            "window=%ss",
            latest.value,
            self.settings.arbitrage_curve_gate_min_deviation_pct,
            change_pct,
            self.settings.arbitrage_curve_gate_min_change_pct,
            self.settings.arbitrage_curve_gate_change_window_seconds,
        )
        return False

    @staticmethod
    def _latest_curve_nav_deviation_snapshots(limit: int) -> list[MetricSnapshot]:
        with Session(engine) as session:
            statement = (
                select(MetricSnapshot)
                .where(MetricSnapshot.entity_id == CURVE_NAV_ENTITY_ID)
                .where(MetricSnapshot.metric_name == CURVE_NAV_DEVIATION_METRIC)
                .order_by(desc(MetricSnapshot.recorded_at), desc(MetricSnapshot.id))
                .limit(limit)
            )
            return list(session.exec(statement).all())

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _display_chain(chain: str) -> str:
        return {"ethereum": "Ethereum", "base": "Base", "bsc": "BSC"}.get(chain, chain)

    def _samples_to_metrics(
        self,
        samples: list[ArbitrageSample],
        *,
        best_candidates: list[ArbitrageSample] | None = None,
        best_recorded_at: datetime | None = None,
    ) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        candidates = best_candidates if best_candidates is not None else samples
        best_sample = max(candidates, key=lambda sample: sample.net_profit_usd, default=None)
        best_metric_recorded_at = best_recorded_at or (
            best_sample.recorded_at if best_sample is not None else None
        )

        for sample in samples:
            details = self._sample_details(sample)
            for metric_name, value, unit in (
                ("gross_profit_usd", sample.gross_profit_usd, "usd"),
                ("net_profit_usd", sample.net_profit_usd, "usd"),
                ("gross_edge_pct", sample.gross_edge_pct, "pct"),
                ("net_edge_pct", sample.net_edge_pct, "pct"),
                ("entry_apxusd", sample.entry_apxusd_amount, "tokens"),
                ("bought_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("sold_apxusd", sample.sold_apxusd_amount, "tokens"),
                ("source_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("target_apyusd", sample.bought_apyusd_amount, "tokens"),
                ("target_apxusd", sample.sold_apxusd_amount, "tokens"),
                ("final_apxusd", sample.final_apxusd_amount, "tokens"),
                ("final_usdc", sample.final_amount, "tokens"),
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
                        source="velora_market",
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
                        source="velora_market",
                        recorded_at=best_metric_recorded_at or best_sample.recorded_at,
                        details=best_details,
                    )
                )
        return metrics

    @staticmethod
    def _sample_details(sample: ArbitrageSample) -> dict[str, Any]:
        def min_out(value: int | None) -> str | None:
            return str(value) if value is not None else None

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
            "start_symbol": sample.start_asset.symbol,
            "final_asset_id": sample.final_asset.asset_id,
            "final_symbol": sample.final_asset.symbol,
            "start_amount": sample.start_amount,
            "entry_apxusd_amount": sample.entry_apxusd_amount,
            "bought_apyusd_amount": sample.bought_apyusd_amount,
            "sold_apxusd_amount": sample.sold_apxusd_amount,
            "final_apxusd_amount": sample.final_apxusd_amount,
            "final_amount": sample.final_amount,
            "bridge_fee_bps": sample.monitor.bridge_fee_bps,
            "bridge_fixed_usd": sample.monitor.bridge_fixed_usd,
            "first_bridge_cost_usd": sample.first_bridge_cost_usd,
            "second_bridge_cost_usd": sample.second_bridge_cost_usd,
            "source_gas_usd": sample.monitor.source_gas_usd,
            "target_gas_usd": sample.monitor.target_gas_usd,
            "slippage_bps": sample.monitor.slippage_bps,
            "route_steps": list(sample.route_steps),
            "entry_leg": {
                "token_in": sample.entry_leg.token_in,
                "token_out": sample.entry_leg.token_out,
                "amount_in_raw": str(sample.entry_leg.amount_in_raw),
                "amount_out_raw": str(sample.entry_leg.amount_out_raw),
                "min_out_raw": min_out(sample.entry_leg.min_out_raw),
                "rate_raw": sample.entry_leg.rate_raw,
                "method": sample.entry_leg.method,
                "routing": sample.entry_leg.routing,
            },
            "first_leg": {
                "token_in": sample.first_leg.token_in,
                "token_out": sample.first_leg.token_out,
                "amount_in_raw": str(sample.first_leg.amount_in_raw),
                "amount_out_raw": str(sample.first_leg.amount_out_raw),
                "min_out_raw": min_out(sample.first_leg.min_out_raw),
                "rate_raw": sample.first_leg.rate_raw,
                "method": sample.first_leg.method,
                "routing": sample.first_leg.routing,
            },
            "second_leg": {
                "token_in": sample.second_leg.token_in,
                "token_out": sample.second_leg.token_out,
                "amount_in_raw": str(sample.second_leg.amount_in_raw),
                "amount_out_raw": str(sample.second_leg.amount_out_raw),
                "min_out_raw": min_out(sample.second_leg.min_out_raw),
                "rate_raw": sample.second_leg.rate_raw,
                "method": sample.second_leg.method,
                "routing": sample.second_leg.routing,
            },
            "exit_leg": {
                "token_in": sample.exit_leg.token_in,
                "token_out": sample.exit_leg.token_out,
                "amount_in_raw": str(sample.exit_leg.amount_in_raw),
                "amount_out_raw": str(sample.exit_leg.amount_out_raw),
                "min_out_raw": min_out(sample.exit_leg.min_out_raw),
                "rate_raw": sample.exit_leg.rate_raw,
                "method": sample.exit_leg.method,
                "routing": sample.exit_leg.routing,
            },
        }
