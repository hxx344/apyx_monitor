from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint

MORPHO_MARKET_QUERY = """
query MarketState($marketId: String!, $chainId: Int!) {
  marketById(marketId: $marketId, chainId: $chainId) {
    marketId
    loanAsset {
      address
      symbol
      decimals
    }
    collateralAsset {
      address
      symbol
      decimals
    }
    state {
      supplyAssets
      supplyAssetsUsd
      borrowAssets
      borrowAssetsUsd
      liquidityAssets
      liquidityAssetsUsd
      utilization
      borrowApy
      supplyApy
      avgBorrowApy
      avgSupplyApy
    }
    warnings {
      type
      level
    }
  }
}
"""

logger = logging.getLogger(__name__)


class MorphoCollector(BaseCollector):
    name = "morpho"
    graphql_url = "https://api.morpho.org/graphql"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog

    async def collect(self) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for market in self.catalog.morpho_markets:
                if not market.enabled:
                    continue
                try:
                    response = await client.post(
                        self.graphql_url,
                        json={
                            "query": MORPHO_MARKET_QUERY,
                            "variables": {
                                "marketId": market.morpho_market_id,
                                "chainId": market.chain_id,
                            },
                        },
                    )
                    response.raise_for_status()
                except httpx.TimeoutException as exc:
                    logger.warning("跳过 Morpho 市场 │ 原因=接口超时 │ 市场=%s │ 错误=%s", market.market_id, exc)
                    continue
                except httpx.HTTPError as exc:
                    logger.warning("跳过 Morpho 市场 │ 原因=接口请求失败 │ 市场=%s │ 错误=%s", market.market_id, exc)
                    continue
                payload = response.json()
                if payload.get("errors"):
                    raise RuntimeError(f"Morpho query failed: {payload['errors']}")
                market_data = payload.get("data", {}).get("marketById")
                if not market_data:
                    logger.warning("Morpho 市场未找到 │ 市场=%s", market.market_id)
                    continue
                state = market_data.get("state") or {}
                loan_asset = market_data.get("loanAsset") or {}
                loan_decimals = self._to_float(loan_asset.get("decimals"))
                if loan_decimals is None:
                    logger.warning(
                        "跳过 Morpho 市场 │ 原因=借款资产 decimals 缺失 │ 市场=%s",
                        market.market_id,
                    )
                    continue
                recorded_at = datetime.now(timezone.utc)
                details = {
                    "label": market.label,
                    "morpho_market_id": market.morpho_market_id,
                }

                self._append_metric(
                    metrics,
                    market.market_id,
                    "available_to_borrow_assets",
                    self._scale_raw_assets(state.get("liquidityAssets"), int(loan_decimals)),
                    "assets",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "available_to_borrow_usd",
                    self._to_float(state.get("liquidityAssetsUsd")),
                    "usd",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "borrow_apy",
                    self._scale_pct(state.get("borrowApy")),
                    "pct",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "supply_apy",
                    self._scale_pct(state.get("supplyApy")),
                    "pct",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "supply_assets_usd",
                    self._to_float(state.get("supplyAssetsUsd")),
                    "usd",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "borrow_assets_usd",
                    self._to_float(state.get("borrowAssetsUsd")),
                    "usd",
                    recorded_at,
                    details,
                )
                self._append_metric(
                    metrics,
                    market.market_id,
                    "utilization_pct",
                    self._scale_pct(state.get("utilization")),
                    "pct",
                    recorded_at,
                    details,
                )
        return metrics

    @staticmethod
    def _append_metric(
        metrics: list[MetricPoint],
        entity_id: str,
        metric_name: str,
        value: float | None,
        unit: str,
        recorded_at: datetime,
        details: dict[str, str],
    ) -> None:
        if value is None:
            logger.warning(
                "跳过 Morpho 指标 │ 原因=数值缺失 │ 对象=%s │ 指标=%s",
                entity_id,
                metric_name,
            )
            return
        metrics.append(
            MetricPoint(
                entity_id=entity_id,
                entity_type="morpho_market",
                metric_name=metric_name,
                value=value,
                unit=unit,
                source="morpho_api",
                recorded_at=recorded_at,
                details=details,
            )
        )

    @staticmethod
    def _scale_raw_assets(value: Any, decimals: int) -> float | None:
        parsed = MorphoCollector._to_float(value)
        if parsed is None:
            return None
        return parsed / 10**decimals

    @staticmethod
    def _scale_pct(value: Any) -> float | None:
        parsed = MorphoCollector._to_float(value)
        if parsed is None:
            return None
        return parsed * 100

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
