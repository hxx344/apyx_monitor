from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import logging

from web3 import Web3

from ..config import AssetCatalog, Settings
from .base import BaseCollector, MetricPoint


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
]

ERC4626_ABI = ERC20_ABI + [
    {
        "constant": True,
        "inputs": [],
        "name": "totalAssets",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"name": "assets", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function",
    }
]

CHAINLINK_FEED_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

APYUSD_ETHEREUM_ASSET_ID = "apyusd-ethereum"
MORPHO_APYUSD_USDC_MARKET_ID = "morpho-apyusd-usdc"
APYX_CAPPED_COLLATERALIZATION_RATIO_FEED = "0x2037a5Eb67aa9B2FBF50042B724D8c4dB80F23b4"


logger = logging.getLogger(__name__)


class OnChainCollector(BaseCollector):
    name = "onchain"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog
        self._providers: dict[str, Web3] = {}

    async def collect(self) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        recorded_at = datetime.now(timezone.utc)
        chain_map = self.catalog.chain_map()
        aggregates: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        for asset in self.catalog.assets:
            if not asset.enabled:
                continue
            try:
                chain = chain_map[asset.chain]
                web3 = self._get_provider(asset.chain, chain.resolve_rpc_url())
                address = Web3.to_checksum_address(asset.contract_address)
                abi = ERC4626_ABI if asset.standard == "erc4626" else ERC20_ABI
                contract = web3.eth.contract(address=address, abi=abi)

                total_supply = contract.functions.totalSupply().call() / 10 ** asset.decimals
                metrics.append(
                    MetricPoint(
                        entity_id=asset.asset_id,
                        entity_type="asset",
                        metric_name="total_supply",
                        value=float(total_supply),
                        unit="tokens",
                        source=f"rpc:{asset.chain}",
                        recorded_at=recorded_at,
                        details={"group_id": asset.group_id, "address": asset.contract_address},
                    )
                )
                aggregates[asset.group_id]["total_supply"] += float(total_supply)

                if asset.standard == "erc4626":
                    fallback_used = False
                    try:
                        total_assets = contract.functions.totalAssets().call() / 10 ** asset.decimals
                    except Exception:  # noqa: BLE001
                        total_assets = float(total_supply)
                        fallback_used = True
                    nav = float(total_assets / total_supply) if total_supply else 0.0
                    tvl_usd = float(total_assets * asset.price_hint_usd)
                    if asset.asset_id == APYUSD_ETHEREUM_ASSET_ID:
                        convert_to_assets = contract.functions.convertToAssets(10 ** asset.decimals).call() / 10 ** asset.decimals
                        metrics.append(
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="convert_to_assets",
                                value=float(convert_to_assets),
                                unit="assets_per_share",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={
                                    "group_id": asset.group_id,
                                    "address": asset.contract_address,
                                    "sample_shares": 10 ** asset.decimals,
                                },
                            )
                        )
                    metrics.extend(
                        [
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="total_assets",
                                value=float(total_assets),
                                unit="assets",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={
                                    "group_id": asset.group_id,
                                    "address": asset.contract_address,
                                    "fallback_used": fallback_used,
                                },
                            ),
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="nav_usd",
                                value=nav,
                                unit="usd",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={
                                    "group_id": asset.group_id,
                                    "address": asset.contract_address,
                                    "fallback_used": fallback_used,
                                },
                            ),
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="tvl_usd",
                                value=tvl_usd,
                                unit="usd",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={
                                    "group_id": asset.group_id,
                                    "address": asset.contract_address,
                                    "fallback_used": fallback_used,
                                },
                            ),
                        ]
                    )
                    aggregates[asset.group_id]["tvl_usd"] += tvl_usd
                    aggregates[asset.group_id]["backing_assets"] += float(total_assets)
                else:
                    tvl_usd = float(total_supply * asset.price_hint_usd)
                    metrics.extend(
                        [
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="nav_usd",
                                value=float(asset.price_hint_usd),
                                unit="usd",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={"group_id": asset.group_id, "address": asset.contract_address},
                            ),
                            MetricPoint(
                                entity_id=asset.asset_id,
                                entity_type="asset",
                                metric_name="tvl_usd",
                                value=tvl_usd,
                                unit="usd",
                                source=f"rpc:{asset.chain}",
                                recorded_at=recorded_at,
                                details={"group_id": asset.group_id, "address": asset.contract_address},
                            ),
                        ]
                    )
                    aggregates[asset.group_id]["tvl_usd"] += tvl_usd
            except Exception as exc:  # noqa: BLE001
                logger.warning("onchain asset %s failed: %s", asset.asset_id, exc)
                continue

        try:
            ethereum_chain = chain_map["ethereum"]
            web3 = self._get_provider("ethereum", ethereum_chain.resolve_rpc_url())
            ratio_feed = web3.eth.contract(
                address=Web3.to_checksum_address(APYX_CAPPED_COLLATERALIZATION_RATIO_FEED),
                abi=CHAINLINK_FEED_ABI,
            )
            decimals = ratio_feed.functions.decimals().call()
            latest_round = ratio_feed.functions.latestRoundData().call()
            ratio_value = float(latest_round[1] / 10 ** decimals)
            metrics.append(
                MetricPoint(
                    entity_id=MORPHO_APYUSD_USDC_MARKET_ID,
                    entity_type="market",
                    metric_name="capped_collateralization_ratio",
                    value=ratio_value,
                    unit="ratio",
                    source="rpc:ethereum",
                    recorded_at=recorded_at,
                    details={
                        "feed_address": APYX_CAPPED_COLLATERALIZATION_RATIO_FEED,
                        "updated_at": int(latest_round[3]),
                        "answered_in_round": int(latest_round[4]),
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("onchain feed %s failed: %s", APYX_CAPPED_COLLATERALIZATION_RATIO_FEED, exc)

        for group_id, values in aggregates.items():
            total_supply = values.get("total_supply", 0.0)
            tvl_usd = values.get("tvl_usd", 0.0)
            metrics.extend(
                [
                    MetricPoint(
                        entity_id=group_id,
                        entity_type="asset_group",
                        metric_name="total_supply",
                        value=float(total_supply),
                        unit="tokens",
                        source="rpc:aggregate",
                        recorded_at=recorded_at,
                        details={"aggregate": True},
                    ),
                    MetricPoint(
                        entity_id=group_id,
                        entity_type="asset_group",
                        metric_name="tvl_usd",
                        value=float(tvl_usd),
                        unit="usd",
                        source="rpc:aggregate",
                        recorded_at=recorded_at,
                        details={"aggregate": True},
                    ),
                ]
            )
            if total_supply:
                metrics.append(
                    MetricPoint(
                        entity_id=group_id,
                        entity_type="asset_group",
                        metric_name="nav_usd",
                        value=float(tvl_usd / total_supply),
                        unit="usd",
                        source="rpc:aggregate",
                        recorded_at=recorded_at,
                        details={"aggregate": True},
                    )
                )

        return metrics

    def _get_provider(self, chain_name: str, rpc_url: str) -> Web3:
        if chain_name not in self._providers:
            self._providers[chain_name] = Web3(Web3.HTTPProvider(rpc_url))
        return self._providers[chain_name]
