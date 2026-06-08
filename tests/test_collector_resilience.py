from __future__ import annotations

import asyncio

import httpx

from apyx_monitor.collectors import morpho as morpho_module
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
