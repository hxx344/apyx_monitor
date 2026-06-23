from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from apyx_monitor.collectors import finnhub_stock as finnhub_stock_module
from apyx_monitor.collectors import morpho as morpho_module
from apyx_monitor.collectors.finnhub_stock import FinnhubStockCollector
from apyx_monitor.collectors.onchain import (
    APYUSD_HEDGED_NAV_DISCOUNT_ENTITY_ID,
    APYUSD_UNLOCK_DAYS,
    _apyusd_hedged_nav_discount_metrics,
)
from apyx_monitor.config import (
    AssetCatalog,
    ChainDefinition,
    MorphoMarketDefinition,
    Settings,
)


def _catalog(
    *,
    morpho_markets: list[MorphoMarketDefinition] | None = None,
) -> AssetCatalog:
    return AssetCatalog(
        chains=[
            ChainDefinition(
                chain="ethereum",
                chain_id=1,
                rpc_url_env="ETH_RPC",
                default_rpc_url="",
            )
        ],
        assets=[],
        pendle_markets=[],
        morpho_markets=morpho_markets or [],
    )


def test_morpho_collector_skips_null_metric_values(monkeypatch):
    asyncio.run(_run_morpho_null_metric_test(monkeypatch))


def test_apyusd_hedged_nav_discount_metrics_annualize_unlock_return():
    metrics = _apyusd_hedged_nav_discount_metrics(
        recorded_at=datetime.now(timezone.utc),
        convert_to_assets=1.05,
        exchange_rate=1.00,
        fast_scan=True,
    )

    by_name = {metric.metric_name: metric for metric in metrics}
    expected_apy = ((1.05 ** (365 / APYUSD_UNLOCK_DAYS)) - 1) * 100

    assert by_name["unlock_return_pct"].entity_id == APYUSD_HEDGED_NAV_DISCOUNT_ENTITY_ID
    assert round(by_name["unlock_return_pct"].value, 6) == 5.0
    assert round(by_name["annualized_apy_pct"].value, 6) == round(expected_apy, 6)
    assert by_name["annualized_apy_pct"].details["fast_scan"] is True


def test_apyusd_hedged_nav_discount_metrics_skip_invalid_entry_price():
    assert (
        _apyusd_hedged_nav_discount_metrics(
            recorded_at=datetime.now(timezone.utc),
            convert_to_assets=1.05,
            exchange_rate=0,
        )
        == []
    )


def test_finnhub_stock_collector_maps_quote_and_market_phase(monkeypatch):
    asyncio.run(_run_finnhub_stock_collector_test(monkeypatch))


def test_finnhub_stock_collector_falls_back_when_candle_is_unavailable(monkeypatch):
    asyncio.run(_run_finnhub_stock_candle_fallback_test(monkeypatch))


async def _run_finnhub_stock_collector_test(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def get(self, url: str, params: dict) -> httpx.Response:
            request = httpx.Request("GET", url)
            if url.endswith("/quote"):
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "c": 81.25,
                        "pc": 80.0,
                        "d": 1.25,
                        "dp": 1.5625,
                        "o": 80.5,
                        "h": 82.0,
                        "l": 79.8,
                        "t": 1782144000,
                    },
                )
            if url.endswith("/stock/candle"):
                return httpx.Response(
                    200,
                    request=request,
                    json={"s": "ok", "t": [1782144000], "c": [82.5]},
                )
            return httpx.Response(
                200,
                request=request,
                json={"session": "pre-market", "isOpen": False, "t": 1782144000},
            )

    monkeypatch.setattr(finnhub_stock_module.httpx, "AsyncClient", FakeAsyncClient)
    collector = FinnhubStockCollector(
        Settings(FINNHUB_API_KEY="token", FINNHUB_STOCK_SYMBOL="STRC"),
        _catalog(),
    )

    metrics = await collector.collect()
    by_name = {metric.metric_name: metric for metric in metrics}

    assert by_name["price_usd"].value == 82.5
    assert by_name["price_usd"].details["market_phase"] == "盘前"
    assert by_name["price_usd"].details["price_source"] == "stock_candle_1m"
    assert by_name["price_usd"].details["quote_price"] == 81.25
    assert by_name["price_usd"].details["previous_close"] == 80.0
    assert by_name["price_usd"].details["change"] == 2.5
    assert by_name["market_phase_code"].value == 1.0


async def _run_finnhub_stock_candle_fallback_test(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def get(self, url: str, params: dict) -> httpx.Response:
            request = httpx.Request("GET", url)
            if url.endswith("/quote"):
                return httpx.Response(200, request=request, json={"c": 88.79, "pc": 88.59})
            if url.endswith("/stock/candle"):
                return httpx.Response(403, request=request, json={"error": "no access"})
            return httpx.Response(
                200,
                request=request,
                json={"session": "pre-market", "isOpen": False},
            )

    monkeypatch.setattr(finnhub_stock_module.httpx, "AsyncClient", FakeAsyncClient)
    collector = FinnhubStockCollector(
        Settings(FINNHUB_API_KEY="token", FINNHUB_STOCK_SYMBOL="STRC"),
        _catalog(),
    )

    metrics = await collector.collect()
    by_name = {metric.metric_name: metric for metric in metrics}

    assert by_name["price_usd"].value == 88.79
    assert by_name["price_usd"].details["price_source"] == "quote"
    assert by_name["price_usd"].details["candle_error"] == "http_403"


async def _run_morpho_null_metric_test(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def post(self, url: str, json: dict) -> httpx.Response:
            request = httpx.Request("POST", url)
            return httpx.Response(
                200,
                request=request,
                json={
                    "data": {
                        "marketById": {
                            "loanAsset": {"decimals": "6"},
                            "state": {
                                "liquidityAssets": "123000000",
                                "liquidityAssetsUsd": None,
                                "borrowApy": "0.042",
                                "supplyApy": "0.025",
                                "supplyAssetsUsd": "1000",
                                "borrowAssetsUsd": "250",
                                "utilization": "0.25",
                            },
                        }
                    }
                },
            )

    monkeypatch.setattr(morpho_module.httpx, "AsyncClient", FakeAsyncClient)

    collector = morpho_module.MorphoCollector(
        Settings(),
        _catalog(
            morpho_markets=[
                MorphoMarketDefinition(
                    market_id="morpho-test",
                    label="Morpho Test",
                    morpho_market_id="0xabc",
                    chain_id=1,
                )
            ]
        ),
    )

    metrics = await collector.collect()
    metric_names = {metric.metric_name for metric in metrics}

    assert "available_to_borrow_usd" not in metric_names
    assert "available_to_borrow_assets" in metric_names
    assert "borrow_apy" in metric_names
