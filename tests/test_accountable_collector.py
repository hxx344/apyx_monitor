from __future__ import annotations

import asyncio
from datetime import timezone

import httpx

from apyx_monitor.collectors.accountable import ACCOUNTABLE_DASHBOARD_URL, AccountableCollector
from apyx_monitor.config import Settings


def test_extract_redemption_value_from_dashboard_payload():
    payload = {"data": {"reserves": {"redemption_value": 0.7907}}}

    assert AccountableCollector._extract_redemption_value(payload) == 0.7907


def test_extract_recorded_at_from_dashboard_payload_milliseconds():
    payload = {"data": {"ts": "1782721084666"}}

    recorded_at = AccountableCollector._extract_recorded_at(payload)

    assert recorded_at.tzinfo == timezone.utc
    assert int(recorded_at.timestamp() * 1000) == 1782721084666


def test_collect_sends_browser_headers(monkeypatch):
    captured_headers = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured_headers.update(request.headers)
        return httpx.Response(
            200,
            json={"data": {"reserves": {"redemption_value": 0.7907}, "ts": "1782721084666"}},
        )

    transport = httpx.MockTransport(handler)
    original_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)

    points = asyncio.run(AccountableCollector(Settings()).collect())

    assert points[0].value == 0.7907
    assert captured_headers["origin"] == "https://accountable.apyx.fi"
    assert captured_headers["referer"] == "https://accountable.apyx.fi/"
    assert "Mozilla/5.0" in captured_headers["user-agent"]


def test_collect_skips_forbidden_response(monkeypatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == ACCOUNTABLE_DASHBOARD_URL
        return httpx.Response(403, text="Forbidden")

    transport = httpx.MockTransport(handler)
    original_client = httpx.AsyncClient

    def mock_client(*args, **kwargs):
        kwargs["transport"] = transport
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", mock_client)

    assert asyncio.run(AccountableCollector(Settings()).collect()) == []
