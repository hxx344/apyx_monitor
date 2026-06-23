from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint

logger = logging.getLogger(__name__)

FINNHUB_STOCK_ENTITY_ID = "stock-strc"
FINNHUB_STOCK_ENTITY_TYPE = "equity"

MARKET_PHASE_LABELS = {
    "pre-market": "\u76d8\u524d",
    "regular": "\u76d8\u4e2d",
    "post-market": "\u76d8\u540e",
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
            session = status.get("session") or "closed"
            candle_payload = None
            candle_error = None
            if session in {"pre-market", "post-market"}:
                try:
                    candle_payload = await self._fetch_recent_candle(client, symbol, token)
                except httpx.HTTPError as exc:
                    candle_error = self._safe_http_error(exc)
                    logger.warning(
                        "Finnhub 1m candle unavailable for extended-hours price; falling back to quote: %s",
                        candle_error,
                    )

        quote_price = self._to_float(quote.get("c"))
        candle_price = self._latest_candle_close(candle_payload)
        current_price = candle_price if candle_price is not None else quote_price
        if current_price is None or current_price <= 0:
            raise RuntimeError(f"Finnhub quote has no usable current price for {symbol}: {quote}")

        recorded_at = datetime.now(timezone.utc)
        phase = MARKET_PHASE_LABELS.get(session, "\u4f11\u5e02")
        previous_close = self._to_float(quote.get("pc"))
        change = current_price - previous_close if previous_close else self._to_float(quote.get("d"))
        change_pct = (
            (change / previous_close * 100)
            if previous_close and change is not None
            else self._to_float(quote.get("dp"))
        )
        details = {
            "symbol": symbol,
            "market_session": session,
            "market_phase": phase,
            "market_is_open": bool(status.get("isOpen")),
            "price_source": "stock_candle_1m" if candle_price is not None else "quote",
            "candle_error": candle_error,
            "quote_price": quote_price,
            "previous_close": previous_close,
            "change": change,
            "change_pct": change_pct,
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

    async def _fetch_recent_candle(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        token: str,
    ) -> dict | None:
        now = datetime.now(timezone.utc)
        response = await client.get(
            f"{self.base_url}/stock/candle",
            params={
                "symbol": symbol,
                "resolution": "1",
                "from": int((now - timedelta(hours=6)).timestamp()),
                "to": int(now.timestamp()),
                "token": token,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("s") != "ok":
            return None
        return payload

    @classmethod
    def _latest_candle_close(cls, payload: dict | None) -> float | None:
        if not payload:
            return None
        closes = payload.get("c")
        if not isinstance(closes, list) or not closes:
            return None
        return cls._to_float(closes[-1])

    @staticmethod
    def _safe_http_error(exc: httpx.HTTPError) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            return f"http_{exc.response.status_code}"
        return exc.__class__.__name__

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
