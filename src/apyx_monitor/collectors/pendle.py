from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint


class PendleCollector(BaseCollector):
    name = "pendle"
    base_url = "https://api-v2.pendle.finance/core/v1"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog

    async def collect(self) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for market in self.catalog.pendle_markets:
                if not market.enabled:
                    continue
                url = f"{self.base_url}/{market.chain_id}/markets/{market.market_address}"
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
                recorded_at = self._parse_timestamp(payload.get("dataUpdatedAt"))

                yt_price = self._safe_get(payload, "yt", "price", "usd")
                if yt_price is not None:
                    metrics.append(
                        MetricPoint(
                            entity_id=market.yt_asset_id,
                            entity_type="yt_asset",
                            metric_name="price_usd",
                            value=float(yt_price),
                            unit="usd",
                            source="pendle_api",
                            recorded_at=recorded_at,
                            details={"market_id": market.market_id, "market_address": market.market_address},
                        )
                    )

                implied_apy = payload.get("impliedApy")
                if implied_apy is not None:
                    metrics.append(
                        MetricPoint(
                            entity_id=market.yt_asset_id,
                            entity_type="yt_asset",
                            metric_name="implied_apy",
                            value=float(implied_apy) * 100,
                            unit="pct",
                            source="pendle_api",
                            recorded_at=recorded_at,
                            details={"market_id": market.market_id},
                        )
                    )

                liquidity_usd = self._safe_get(payload, "liquidity", "usd")
                if liquidity_usd is not None:
                    metrics.append(
                        MetricPoint(
                            entity_id=market.yt_asset_id,
                            entity_type="yt_asset",
                            metric_name="market_liquidity_usd",
                            value=float(liquidity_usd),
                            unit="usd",
                            source="pendle_api",
                            recorded_at=recorded_at,
                            details={"market_id": market.market_id},
                        )
                    )

                underlying_apy = payload.get("underlyingApy")
                if underlying_apy is not None:
                    metrics.append(
                        MetricPoint(
                            entity_id=market.underlying_asset_id,
                            entity_type="asset_group",
                            metric_name="underlying_apy",
                            value=float(underlying_apy) * 100,
                            unit="pct",
                            source="pendle_api",
                            recorded_at=recorded_at,
                            details={"market_id": market.market_id, "market_address": market.market_address},
                        )
                    )

                underlying_price = self._safe_get(payload, "underlyingAsset", "price", "usd")
                if underlying_price is not None:
                    metrics.append(
                        MetricPoint(
                            entity_id=market.underlying_asset_id,
                            entity_type="asset_group",
                            metric_name="reference_price_usd",
                            value=float(underlying_price),
                            unit="usd",
                            source="pendle_api",
                            recorded_at=recorded_at,
                            details={"market_id": market.market_id},
                        )
                    )

        return metrics

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)

    @staticmethod
    def _safe_get(payload: dict[str, Any], *path: str) -> Any:
        cursor: Any = payload
        for key in path:
            if not isinstance(cursor, dict):
                return None
            cursor = cursor.get(key)
            if cursor is None:
                return None
        return cursor
