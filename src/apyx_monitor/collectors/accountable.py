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
    "cache-control": "no-cache",
    "origin": "https://accountable.apyx.fi",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://accountable.apyx.fi/",
    "sec-ch-ua": '"Google Chrome";v="126", "Chromium";v="126", "Not/A)Brand";v="8"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
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
        dashboard_url = self.settings.accountable_dashboard_url
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(
                dashboard_url,
                headers=ACCOUNTABLE_REQUEST_HEADERS,
                follow_redirects=True,
            )
            if response.status_code in {403, 429}:
                logger.warning(
                    "Accountable dashboard unavailable; skipping redemption value: "
                    "url=%s status=http_%s",
                    dashboard_url,
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
                details={"url": dashboard_url},
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
