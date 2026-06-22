from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone

from sqlmodel import Session, select
from web3 import Web3

from ..config import AssetCatalog, Settings
from ..db import engine
from ..models import OnChainEventCursor
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

CURVE_POOL_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "name": "coins",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "int128", "name": "i", "type": "int128"},
            {"internalType": "int128", "name": "j", "type": "int128"},
            {"internalType": "uint256", "name": "dx", "type": "uint256"},
        ],
        "name": "get_dy",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
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

APYX_RATE_VIEW_ABI = [
    {
        "inputs": [],
        "name": "precision",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "apy",
        "outputs": [{"internalType": "uint256", "name": "percentYield", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "vault",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

APYUSD_GROUP_ID = "apyusd"
APYUSD_ETHEREUM_ASSET_ID = "apyusd-ethereum"
MORPHO_APYUSD_USDC_MARKET_ID = "morpho-apyusd-usdc"
APYX_APYUSD_RATE_VIEW = "0xCABa36EDE2C08e16F3602e8688a8bE94c1B4e484"
APYX_CAPPED_COLLATERALIZATION_RATIO_FEED = "0x2037a5Eb67aa9B2FBF50042B724D8c4dB80F23b4"
CURVE_APYUSD_APXUSD_POOL_ID = "curve-apyusd-apxusd"
COMPOUNDING_PERIODS_PER_YEAR = 12
APYUSD_HEDGED_NAV_DISCOUNT_ENTITY_ID = "apyusd-hedged-nav-discount"
APYUSD_UNLOCK_DAYS = 20
APPROVAL_MONITOR_ENTITY_ID = "eth-approval-cd2a-336555"
APPROVAL_MONITOR_METRIC_NAME = "approval_detected"
APPROVAL_MONITOR_CHAIN = "ethereum"
APPROVAL_MONITOR_OWNER = "0xcd2a3555fae0ed39731c56677c11538d9481a768"
APPROVAL_MONITOR_TOKEN = "0x3365554a61CeFF74A76528f9e86C1E87946d16a5"
APPROVAL_EVENT_TOPIC = Web3.to_hex(Web3.keccak(text="Approval(address,address,uint256)"))


logger = logging.getLogger(__name__)


class OnChainCollector(BaseCollector):
    name = "onchain"

    def __init__(self, settings: Settings, catalog: AssetCatalog) -> None:
        self.settings = settings
        self.catalog = catalog
        self._providers: dict[str, Web3] = {}

    async def collect(self) -> list[MetricPoint]:
        return await asyncio.to_thread(self._collect)

    def _collect(self) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        recorded_at = datetime.now(timezone.utc)
        chain_map = self.catalog.chain_map()
        asset_map = {asset.asset_id: asset for asset in self.catalog.assets}
        aggregates: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        apyusd_convert_to_assets: float | None = None
        capped_ratio_value: float | None = None
        curve_exchange_rate: float | None = None

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
                        apyusd_convert_to_assets = float(convert_to_assets)
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
                logger.warning("链上资产采集失败 │ 资产=%s │ 错误=%s", asset.asset_id, exc)
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
            capped_ratio_value = ratio_value
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
            logger.warning(
                "链上价格源采集失败 │ 地址=%s │ 错误=%s",
                APYX_CAPPED_COLLATERALIZATION_RATIO_FEED,
                exc,
            )

        try:
            ethereum_chain = chain_map["ethereum"]
            web3 = self._get_provider("ethereum", ethereum_chain.resolve_rpc_url())
            rate_view = web3.eth.contract(
                address=Web3.to_checksum_address(APYX_APYUSD_RATE_VIEW),
                abi=APYX_RATE_VIEW_ABI,
            )
            precision = rate_view.functions.precision().call()
            apy_raw = rate_view.functions.apy().call()
            vault_address = rate_view.functions.vault().call()
            apr_rate = apy_raw / precision
            apr_pct = float(apr_rate * 100)
            apy_pct = (
                float((1 + apr_rate / COMPOUNDING_PERIODS_PER_YEAR) ** COMPOUNDING_PERIODS_PER_YEAR - 1)
                * 100
            )
            base_details = {
                "rate_view_address": APYX_APYUSD_RATE_VIEW,
                "vault_address": vault_address,
                "raw_value": int(apy_raw),
                "precision": int(precision),
            }
            metrics.extend(
                [
                    MetricPoint(
                        entity_id=APYUSD_GROUP_ID,
                        entity_type="asset_group",
                        metric_name="underlying_apr",
                        value=apr_pct,
                        unit="pct",
                        source="rpc:ethereum:apyx_rate_view",
                        recorded_at=recorded_at,
                        details=base_details,
                    ),
                    MetricPoint(
                        entity_id=APYUSD_GROUP_ID,
                        entity_type="asset_group",
                        metric_name="underlying_apy",
                        value=apy_pct,
                        unit="pct",
                        source="derived:monthly_compounding:apyx_rate_view",
                        recorded_at=recorded_at,
                        details={
                            **base_details,
                            "apr_pct": apr_pct,
                            "compounding_periods_per_year": COMPOUNDING_PERIODS_PER_YEAR,
                            "formula": "(1 + apr / periods) ** periods - 1",
                        },
                    ),
                ]
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("APYX RateView 采集失败 │ 地址=%s │ 错误=%s", APYX_APYUSD_RATE_VIEW, exc)

        for pool in self.catalog.curve_pools:
            if not pool.enabled:
                continue
            token_in = asset_map.get(pool.token_in_asset_id)
            token_out = asset_map.get(pool.token_out_asset_id)
            if token_in is None or token_out is None:
                logger.warning(
                    "跳过 Curve 池 │ 原因=资产配置缺失 │ 池=%s │ 交易对=%s -> %s",
                    pool.pool_id,
                    pool.token_in_asset_id,
                    pool.token_out_asset_id,
                )
                continue
            try:
                chain = chain_map[pool.chain]
                web3 = self._get_provider(pool.chain, chain.resolve_rpc_url())
                contract = web3.eth.contract(
                    address=Web3.to_checksum_address(pool.contract_address),
                    abi=CURVE_POOL_ABI,
                )
                coin_addresses: list[str] = []
                for index in range(8):
                    try:
                        coin_addresses.append(Web3.to_checksum_address(contract.functions.coins(index).call()))
                    except Exception:  # noqa: BLE001
                        break
                token_in_address = Web3.to_checksum_address(token_in.contract_address)
                token_out_address = Web3.to_checksum_address(token_out.contract_address)
                if token_in_address not in coin_addresses or token_out_address not in coin_addresses:
                    logger.warning(
                        "跳过 Curve 池 │ 原因=池内未找到代币地址 │ 池=%s",
                        pool.pool_id,
                    )
                    continue
                token_in_index = coin_addresses.index(token_in_address)
                token_out_index = coin_addresses.index(token_out_address)
                sample_amount_raw = 10 ** token_in.decimals
                amount_out_raw = contract.functions.get_dy(
                    token_in_index,
                    token_out_index,
                    sample_amount_raw,
                ).call()
                exchange_rate = amount_out_raw / 10 ** token_out.decimals
                if pool.pool_id == CURVE_APYUSD_APXUSD_POOL_ID:
                    curve_exchange_rate = float(exchange_rate)
                metrics.append(
                    MetricPoint(
                        entity_id=pool.pool_id,
                        entity_type="pool",
                        metric_name="exchange_rate",
                        value=float(exchange_rate),
                        unit="token_out_per_token_in",
                        source=f"rpc:{pool.chain}",
                        recorded_at=recorded_at,
                        details={
                            "pool_address": pool.contract_address,
                            "label": pool.label,
                            "token_in_asset_id": token_in.asset_id,
                            "token_out_asset_id": token_out.asset_id,
                            "token_in_symbol": token_in.symbol,
                            "token_out_symbol": token_out.symbol,
                            "token_in_index": token_in_index,
                            "token_out_index": token_out_index,
                            "sample_amount": 1.0,
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Curve 池采集失败 │ 池=%s │ 错误=%s", pool.pool_id, exc)

        metrics.extend(
            _apyusd_hedged_nav_discount_metrics(
                recorded_at,
                apyusd_convert_to_assets,
                curve_exchange_rate,
            )
        )

        if apyusd_convert_to_assets and curve_exchange_rate is not None:
            deviation_pct = abs(curve_exchange_rate / apyusd_convert_to_assets - 1) * 100
            metrics.append(
                MetricPoint(
                    entity_id=CURVE_APYUSD_APXUSD_POOL_ID,
                    entity_type="pool",
                    metric_name="curve_rate_vs_nav_deviation_pct",
                    value=float(deviation_pct),
                    unit="pct",
                    source="derived:onchain",
                    recorded_at=recorded_at,
                    details={
                        "exchange_rate": curve_exchange_rate,
                        "convert_to_assets": apyusd_convert_to_assets,
                        "baseline_metric": "convert_to_assets",
                        "formula": "abs(exchange_rate / convert_to_assets - 1) * 100",
                    },
                )
            )

        if capped_ratio_value is not None:
            ratio_deviation_pct = abs(capped_ratio_value - 1.0) * 100
            metrics.append(
                MetricPoint(
                    entity_id=MORPHO_APYUSD_USDC_MARKET_ID,
                    entity_type="market",
                    metric_name="capped_collateralization_ratio_deviation_pct",
                    value=float(ratio_deviation_pct),
                    unit="pct",
                    source="derived:onchain",
                    recorded_at=recorded_at,
                    details={
                        "capped_collateralization_ratio": capped_ratio_value,
                        "peg_target": 1.0,
                        "formula": "abs(ratio - 1.0) * 100",
                    },
                )
            )

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

        metrics.extend(self._collect_approval_monitor_metrics(recorded_at, chain_map))

        return metrics

    def _collect_approval_monitor_metrics(
        self,
        recorded_at: datetime,
        chain_map: dict,
    ) -> list[MetricPoint]:
        try:
            chain = chain_map[APPROVAL_MONITOR_CHAIN]
            web3 = self._get_provider(APPROVAL_MONITOR_CHAIN, chain.resolve_rpc_url())
            latest_block = int(web3.eth.block_number)
            max_range = max(1, int(self.settings.approval_monitor_max_block_range))
            lookback = max(0, int(self.settings.approval_monitor_initial_lookback_blocks))
            cursor_id = (
                f"{APPROVAL_MONITOR_CHAIN}:approval:"
                f"{APPROVAL_MONITOR_TOKEN.lower()}:{APPROVAL_MONITOR_OWNER.lower()}"
            )

            with Session(engine) as session:
                cursor = session.exec(
                    select(OnChainEventCursor).where(OnChainEventCursor.cursor_id == cursor_id)
                ).first()
                if cursor is None and lookback == 0:
                    session.add(
                        OnChainEventCursor(
                            cursor_id=cursor_id,
                            chain=APPROVAL_MONITOR_CHAIN,
                            last_scanned_block=latest_block,
                            updated_at=recorded_at,
                        )
                    )
                    session.commit()
                    return []

                from_block = (
                    max(0, latest_block - lookback)
                    if cursor is None
                    else cursor.last_scanned_block + 1
                )
                if from_block > latest_block:
                    return []

                to_block = min(latest_block, from_block + max_range - 1)
                logs = web3.eth.get_logs(
                    {
                        "address": Web3.to_checksum_address(APPROVAL_MONITOR_TOKEN),
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "topics": [
                            APPROVAL_EVENT_TOPIC,
                            _address_topic(APPROVAL_MONITOR_OWNER),
                        ],
                    }
                )

                if cursor is None:
                    session.add(
                        OnChainEventCursor(
                            cursor_id=cursor_id,
                            chain=APPROVAL_MONITOR_CHAIN,
                            last_scanned_block=to_block,
                            updated_at=recorded_at,
                        )
                    )
                else:
                    cursor.last_scanned_block = to_block
                    cursor.updated_at = recorded_at
                session.commit()

            if not logs:
                return []

            metrics: list[MetricPoint] = []
            for log in logs:
                tx_hash = _to_0x_hex(log["transactionHash"])
                log_index = int(log["logIndex"])
                details = {
                    "alert_fingerprint": f"{tx_hash}:{log_index}",
                    "chain": APPROVAL_MONITOR_CHAIN,
                    "owner": Web3.to_checksum_address(APPROVAL_MONITOR_OWNER),
                    "token": Web3.to_checksum_address(APPROVAL_MONITOR_TOKEN),
                    "from_block": from_block,
                    "to_block": to_block,
                    "block_number": int(log["blockNumber"]),
                    "tx_hash": tx_hash,
                    "log_index": log_index,
                    "spender": _topic_address(log["topics"][2]),
                    "approval_value_raw": str(_hex_data_to_int(log["data"])),
                    "events_in_scan": len(logs),
                }
                metrics.append(
                    MetricPoint(
                        entity_id=APPROVAL_MONITOR_ENTITY_ID,
                        entity_type="onchain_event",
                        metric_name=APPROVAL_MONITOR_METRIC_NAME,
                        value=1.0,
                        unit="event",
                        source=f"rpc:{APPROVAL_MONITOR_CHAIN}",
                        recorded_at=recorded_at,
                        details=details,
                    )
                )
            return metrics
        except Exception:  # noqa: BLE001
            logger.exception("Approval 事件监控失败")
            return []

    async def collect_nav_curve(self) -> list[MetricPoint]:
        return await asyncio.to_thread(self._collect_nav_curve)

    def _collect_nav_curve(self) -> list[MetricPoint]:
        metrics: list[MetricPoint] = []
        recorded_at = datetime.now(timezone.utc)
        chain_map = self.catalog.chain_map()
        asset_map = {asset.asset_id: asset for asset in self.catalog.assets}
        apyusd_convert_to_assets: float | None = None
        curve_exchange_rate: float | None = None

        asset = asset_map.get(APYUSD_ETHEREUM_ASSET_ID)
        if asset is not None and asset.enabled:
            try:
                chain = chain_map[asset.chain]
                web3 = self._get_provider(asset.chain, chain.resolve_rpc_url())
                contract = web3.eth.contract(
                    address=Web3.to_checksum_address(asset.contract_address),
                    abi=ERC4626_ABI,
                )
                convert_to_assets = (
                    contract.functions.convertToAssets(10 ** asset.decimals).call()
                    / 10 ** asset.decimals
                )
                apyusd_convert_to_assets = float(convert_to_assets)
                metrics.append(
                    MetricPoint(
                        entity_id=asset.asset_id,
                        entity_type="asset",
                        metric_name="convert_to_assets",
                        value=float(convert_to_assets),
                        unit="assets_per_share",
                        source=f"rpc:{asset.chain}:nav_curve_fast",
                        recorded_at=recorded_at,
                        details={
                            "group_id": asset.group_id,
                            "address": asset.contract_address,
                            "sample_shares": 10 ** asset.decimals,
                            "fast_scan": True,
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "NAV/Curve 快扫资产失败 │ 资产=%s │ 错误=%s",
                    APYUSD_ETHEREUM_ASSET_ID,
                    exc,
                )

        for pool in self.catalog.curve_pools:
            if not pool.enabled:
                continue
            token_in = asset_map.get(pool.token_in_asset_id)
            token_out = asset_map.get(pool.token_out_asset_id)
            if token_in is None or token_out is None:
                logger.warning(
                    "跳过 NAV/Curve 池快扫 │ 原因=资产配置缺失 │ 池=%s │ 交易对=%s -> %s",
                    pool.pool_id,
                    pool.token_in_asset_id,
                    pool.token_out_asset_id,
                )
                continue
            try:
                chain = chain_map[pool.chain]
                web3 = self._get_provider(pool.chain, chain.resolve_rpc_url())
                contract = web3.eth.contract(
                    address=Web3.to_checksum_address(pool.contract_address),
                    abi=CURVE_POOL_ABI,
                )
                coin_addresses: list[str] = []
                for index in range(8):
                    try:
                        coin_addresses.append(
                            Web3.to_checksum_address(contract.functions.coins(index).call())
                        )
                    except Exception:  # noqa: BLE001
                        break
                token_in_address = Web3.to_checksum_address(token_in.contract_address)
                token_out_address = Web3.to_checksum_address(token_out.contract_address)
                missing_token = (
                    token_in_address not in coin_addresses
                    or token_out_address not in coin_addresses
                )
                if missing_token:
                    logger.warning(
                        "跳过 NAV/Curve 池快扫 │ 原因=池内未找到代币地址 │ 池=%s",
                        pool.pool_id,
                    )
                    continue
                token_in_index = coin_addresses.index(token_in_address)
                token_out_index = coin_addresses.index(token_out_address)
                amount_out_raw = contract.functions.get_dy(
                    token_in_index,
                    token_out_index,
                    10 ** token_in.decimals,
                ).call()
                exchange_rate = amount_out_raw / 10 ** token_out.decimals
                if pool.pool_id == CURVE_APYUSD_APXUSD_POOL_ID:
                    curve_exchange_rate = float(exchange_rate)
                metrics.append(
                    MetricPoint(
                        entity_id=pool.pool_id,
                        entity_type="pool",
                        metric_name="exchange_rate",
                        value=float(exchange_rate),
                        unit="token_out_per_token_in",
                        source=f"rpc:{pool.chain}:nav_curve_fast",
                        recorded_at=recorded_at,
                        details={
                            "pool_address": pool.contract_address,
                            "label": pool.label,
                            "token_in_asset_id": token_in.asset_id,
                            "token_out_asset_id": token_out.asset_id,
                            "token_in_symbol": token_in.symbol,
                            "token_out_symbol": token_out.symbol,
                            "token_in_index": token_in_index,
                            "token_out_index": token_out_index,
                            "sample_amount": 1.0,
                            "fast_scan": True,
                        },
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("NAV/Curve 池快扫失败 │ 池=%s │ 错误=%s", pool.pool_id, exc)

        metrics.extend(
            _apyusd_hedged_nav_discount_metrics(
                recorded_at,
                apyusd_convert_to_assets,
                curve_exchange_rate,
                fast_scan=True,
            )
        )

        if apyusd_convert_to_assets and curve_exchange_rate is not None:
            deviation_pct = abs(curve_exchange_rate / apyusd_convert_to_assets - 1) * 100
            metrics.append(
                MetricPoint(
                    entity_id=CURVE_APYUSD_APXUSD_POOL_ID,
                    entity_type="pool",
                    metric_name="curve_rate_vs_nav_deviation_pct",
                    value=float(deviation_pct),
                    unit="pct",
                    source="derived:onchain:nav_curve_fast",
                    recorded_at=recorded_at,
                    details={
                        "exchange_rate": curve_exchange_rate,
                        "convert_to_assets": apyusd_convert_to_assets,
                        "baseline_metric": "convert_to_assets",
                        "formula": "abs(exchange_rate / convert_to_assets - 1) * 100",
                        "fast_scan": True,
                    },
                )
            )

        return metrics

    def _get_provider(self, chain_name: str, rpc_url: str) -> Web3:
        if chain_name not in self._providers:
            self._providers[chain_name] = Web3(Web3.HTTPProvider(rpc_url))
        return self._providers[chain_name]


def _apyusd_hedged_nav_discount_metrics(
    recorded_at: datetime,
    convert_to_assets: float | None,
    exchange_rate: float | None,
    *,
    fast_scan: bool = False,
) -> list[MetricPoint]:
    if convert_to_assets is None or exchange_rate is None or exchange_rate <= 0:
        return []

    nav_to_entry_ratio = convert_to_assets / exchange_rate
    unlock_return_pct = (nav_to_entry_ratio - 1) * 100
    annualized_apy_pct = (nav_to_entry_ratio ** (365 / APYUSD_UNLOCK_DAYS) - 1) * 100
    source = "derived:onchain:nav_curve_fast" if fast_scan else "derived:onchain"
    details = {
        "strategy": "long_apyusd_short_strc_apxusd_hedge",
        "unlock_days": APYUSD_UNLOCK_DAYS,
        "exchange_rate": exchange_rate,
        "convert_to_assets": convert_to_assets,
        "formula_return_pct": "(convert_to_assets / exchange_rate - 1) * 100",
        "formula_apy_pct": "(convert_to_assets / exchange_rate) ** (365 / unlock_days) - 1",
    }
    if fast_scan:
        details["fast_scan"] = True

    return [
        MetricPoint(
            entity_id=APYUSD_HEDGED_NAV_DISCOUNT_ENTITY_ID,
            entity_type="strategy",
            metric_name="unlock_return_pct",
            value=float(unlock_return_pct),
            unit="pct",
            source=source,
            recorded_at=recorded_at,
            details=details,
        ),
        MetricPoint(
            entity_id=APYUSD_HEDGED_NAV_DISCOUNT_ENTITY_ID,
            entity_type="strategy",
            metric_name="annualized_apy_pct",
            value=float(annualized_apy_pct),
            unit="pct",
            source=source,
            recorded_at=recorded_at,
            details=details,
        ),
    ]


def _address_topic(address: str) -> str:
    return "0x" + "0" * 24 + Web3.to_checksum_address(address)[2:].lower()


def _topic_address(topic: object) -> str:
    topic_hex = _to_0x_hex(topic)
    return Web3.to_checksum_address("0x" + topic_hex[-40:])


def _hex_data_to_int(data: object) -> int:
    data_hex = _to_0x_hex(data)
    return int(data_hex, 16)


def _to_0x_hex(value: object) -> str:
    if hasattr(value, "to_0x_hex"):
        return value.to_0x_hex()
    return Web3.to_hex(value)
