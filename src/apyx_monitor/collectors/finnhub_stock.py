from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint

logger = logging.getLogger(__name__)

FINNHUB_STOCK_ENTITY_ID = "stock-strc"
FINNHUB_STOCK_ENTITY_TYPE = "equity"

MARKET_PHASE_LABELS = {
    "pre-market": "盘前",
    "regular": "盘中",
    "post-market": "盘后",
}

MARKET_PHASE_CODES = {
    "closed": 0.0,
    "pre-market": 1.0,
    "regular": 2.0,
    "post-market": 3.0,
}


class FinnhubStockCollector(BaseCollector):
    name = "finnhub_stock"
    base_url = "https://finnhub.io/api/v1"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog

    async def collect(self) -> list[MetricPoint]:
        token = self.settings.finnhub_api_key
        if not token:
            logger.info("Skipping Finnhub stock polling because FINNHUB_API_KEY is not set")
            return []

        symbol = self.settings.finnhub_stock_symbol.upper()
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            quote_response, status_response = await self._fetch_quote_and_status(
                client,
                symbol,
                token,
            )

        quote = quote_response.json()
        status = status_response.json()
        current_price = self._to_float(quote.get("c"))
        if current_price is None or current_price <= 0:
            raise RuntimeError(f"Finnhub quote has no usable current price for {symbol}: {quote}")

        recorded_at = datetime.now(timezone.utc)
        session = status.get("session") or "closed"
        phase = MARKET_PHASE_LABELS.get(session, "休市")
        details = {
            "symbol": symbol,
            "market_session": session,
            "market_phase": phase,
            "market_is_open": bool(status.get("isOpen")),
            "previous_close": self._to_float(quote.get("pc")),
            "change": self._to_float(quote.get("d")),
            "change_pct": self._to_float(quote.get("dp")),
            "open": self._to_float(quote.get("o")),
            "high": self._to_float(quote.get("h")),
            "low": self._to_float(quote.get("l")),
            "finnhub_quote_timestamp": self._to_float(quote.get("t")),
            "finnhub_status_timestamp": self._to_float(status.get("t")),
        }

        return [
            MetricPoint(
                entity_id=FINNHUB_STOCK_ENTITY_ID,
                entity_type=FINNHUB_STOCK_ENTITY_TYPE,
                metric_name="price_usd",
                value=current_price,
                unit="usd",
                source="finnhub",
                recorded_at=recorded_at,
                details=details,
            ),
            MetricPoint(
                entity_id=FINNHUB_STOCK_ENTITY_ID,
                entity_type=FINNHUB_STOCK_ENTITY_TYPE,
                metric_name="market_phase_code",
                value=MARKET_PHASE_CODES.get(session, 0.0),
                unit="code",
                source="finnhub",
                recorded_at=recorded_at,
                details=details,
            ),
        ]

    async def _fetch_quote_and_status(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        token: str,
    ) -> tuple[httpx.Response, httpx.Response]:
        quote_response, status_response = await self._gather(
            client.get(f"{self.base_url}/quote", params={"symbol": symbol, "token": token}),
            client.get(
                f"{self.base_url}/stock/market-status",
                params={"exchange": "US", "token": token},
            ),
        )
        quote_response.raise_for_status()
        status_response.raise_for_status()
        return quote_response, status_response

    @staticmethod
    async def _gather(*awaitables):
        return await asyncio.gather(*awaitables)

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
