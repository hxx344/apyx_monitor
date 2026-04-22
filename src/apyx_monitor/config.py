from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"


class Settings(BaseSettings):
    app_env: str = Field(default="dev", alias="APP_ENV")
    database_url: str = Field(
        default=f"sqlite:///{(DATA_DIR / 'apyx_monitor.db').as_posix()}",
        alias="DATABASE_URL",
    )
    collection_interval_seconds: int = Field(default=60, alias="COLLECTION_INTERVAL_SECONDS")
    http_timeout_seconds: int = Field(default=15, alias="HTTP_TIMEOUT_SECONDS")
    feishu_webhook_url: Optional[str] = Field(default=None, alias="FEISHU_WEBHOOK_URL")
    feishu_secret: Optional[str] = Field(default=None, alias="FEISHU_SECRET")

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )


class ChainDefinition(BaseModel):
    chain: str
    chain_id: int
    rpc_url_env: str
    default_rpc_url: str

    def resolve_rpc_url(self) -> str:
        return os.getenv(self.rpc_url_env, self.default_rpc_url)


class AssetDefinition(BaseModel):
    asset_id: str
    group_id: str
    symbol: str
    kind: Literal["base", "yield"]
    chain: str
    contract_address: str
    decimals: int = 18
    standard: Literal["erc20", "erc4626"]
    price_hint_usd: float = 1.0
    enabled: bool = True


class PendleMarketDefinition(BaseModel):
    market_id: str
    label: str
    market_address: str
    chain_id: int
    underlying_asset_id: str
    yt_asset_id: str
    enabled: bool = True


class MorphoMarketDefinition(BaseModel):
    market_id: str
    label: str
    unique_key: str
    chain_id: int
    enabled: bool = True


class CurvePoolDefinition(BaseModel):
    pool_id: str
    label: str
    chain: str
    contract_address: str
    token_in_asset_id: str
    token_out_asset_id: str
    enabled: bool = True


class AssetCatalog(BaseModel):
    chains: list[ChainDefinition]
    assets: list[AssetDefinition]
    pendle_markets: list[PendleMarketDefinition]
    morpho_markets: list[MorphoMarketDefinition]
    curve_pools: list[CurvePoolDefinition] = []

    def chain_map(self) -> dict[str, ChainDefinition]:
        return {chain.chain: chain for chain in self.chains}


class RuleDefinition(BaseModel):
    rule_id: str
    description: str
    entity_id: str
    metric_name: str
    comparator: Literal["lt", "lte", "gt", "gte"]
    threshold: float
    severity: Literal["P1", "P2", "P3"] = "P2"
    cooldown_seconds: int = 900
    required_consecutive_hits: int = 1
    enabled: bool = True


class RuleCatalog(BaseModel):
    rules: list[RuleDefinition]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return Settings()


@lru_cache(maxsize=1)
def get_asset_catalog() -> AssetCatalog:
    with (CONFIG_DIR / "assets.yaml").open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return AssetCatalog.model_validate(raw)


@lru_cache(maxsize=1)
def get_rule_catalog() -> RuleCatalog:
    with (CONFIG_DIR / "rules.yaml").open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    return RuleCatalog.model_validate(raw)
