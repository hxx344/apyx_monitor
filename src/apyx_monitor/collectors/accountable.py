from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import Settings
from .base import BaseCollector, MetricPoint

ACCOUNTABLE_DASHBOARD_URL = "https://api.accountable.apyx.fi/dashboard"
APXUSD_REDEMPTION_ENTITY_ID = "apxusd-redemption"
REDEMPTION_VALUE_METRIC = "redemption_value_usd"
ACCOUNTABLE_REQUEST_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://accountable.apyx.fi",
    "referer": "https://accountable.apyx.fi/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
}

logger = logging.getLogger(__name__)


class AccountableCollector(BaseCollector):
    name = "accountable"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def collect(self) -> list[MetricPoint]:
        timeout = httpx.Timeout(self.settings.http_timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(
                ACCOUNTABLE_DASHBOARD_URL,
                headers=ACCOUNTABLE_REQUEST_HEADERS,
            )
            if response.status_code in {403, 429}:
                logger.warning(
                    "Accountable dashboard unavailable; skipping redemption value: http_%s",
                    response.status_code,
                )
                return []
            response.raise_for_status()
            payload = response.json()

        value = self._extract_redemption_value(payload)
        recorded_at = self._extract_recorded_at(payload)
        return [
            MetricPoint(
                entity_id=APXUSD_REDEMPTION_ENTITY_ID,
                entity_type="proof_of_solvency",
                metric_name=REDEMPTION_VALUE_METRIC,
                value=value,
                unit="usd",
                source="accountable_api",
                recorded_at=recorded_at,
                details={"url": ACCOUNTABLE_DASHBOARD_URL},
            )
        ]

    @staticmethod
    def _extract_redemption_value(payload: dict[str, Any]) -> float:
        value = (((payload.get("data") or {}).get("reserves") or {}).get("redemption_value"))
        if value is None:
            raise ValueError("Accountable dashboard response missing data.reserves.redemption_value")
        return float(value)

    @staticmethod
    def _extract_recorded_at(payload: dict[str, Any]) -> datetime:
        ts = ((payload.get("data") or {}).get("ts"))
        try:
            if ts is not None:
                return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass
        return datetime.now(timezone.utc)
