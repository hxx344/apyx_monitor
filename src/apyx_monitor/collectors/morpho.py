from __future__ import annotations

from datetime import datetime, timezone

import httpx

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint


MORPHO_MARKET_QUERY = """
query MarketState($uniqueKey: String!, $chainId: Int!) {
  marketByUniqueKey(uniqueKey: $uniqueKey, chainId: $chainId) {
    uniqueKey
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
                response = await client.post(
                    self.graphql_url,
                    json={
                        "query": MORPHO_MARKET_QUERY,
                        "variables": {"uniqueKey": market.unique_key, "chainId": market.chain_id},
                    },
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("errors"):
                    raise RuntimeError(f"Morpho query failed: {payload['errors']}")
                market_data = payload["data"]["marketByUniqueKey"]
                state = market_data["state"]
                loan_decimals = int(market_data["loanAsset"]["decimals"])
                liquidity_assets = float(state["liquidityAssets"]) / 10 ** loan_decimals
                recorded_at = datetime.now(timezone.utc)

                metrics.extend(
                    [
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="available_to_borrow_assets",
                            value=liquidity_assets,
                            unit="assets",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="available_to_borrow_usd",
                            value=float(state["liquidityAssetsUsd"]),
                            unit="usd",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="borrow_apy",
                            value=float(state["borrowApy"]) * 100,
                            unit="pct",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="supply_apy",
                            value=float(state["supplyApy"]) * 100,
                            unit="pct",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="supply_assets_usd",
                            value=float(state["supplyAssetsUsd"]),
                            unit="usd",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="borrow_assets_usd",
                            value=float(state["borrowAssetsUsd"]),
                            unit="usd",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                        MetricPoint(
                            entity_id=market.market_id,
                            entity_type="morpho_market",
                            metric_name="utilization_pct",
                            value=float(state["utilization"]) * 100,
                            unit="pct",
                            source="morpho_api",
                            recorded_at=recorded_at,
                            details={"label": market.label, "unique_key": market.unique_key},
                        ),
                    ]
                )
        return metrics
