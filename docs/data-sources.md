# APYX MVP 数据源整理

## 官方入口
- 官网：https://apyx.fi/
- 文档：https://docs.apyx.fi/
- 应用：https://app.apyx.fi/
- GitHub：https://github.com/apyx-labs

## 已确认合约
### Ethereum
- apxUSD: `0x98A878b1Cd98131B271883B390f68D2c90674665`
- apyUSD: `0x38EEb52F0771140d10c4E9A9a72349A329Fe8a6A`

### Base
- apxUSD: `0xD993935E13851dd7517af10687EC7e5022127228`
- apyUSD: `0x2c271ddF484aC0386d216eB7eB9Ff02D4Dc0F6AA`

## Pendle 市场
- apxUSD market: `0x50dce085af29caba28f7308bea57c4043757b491`
- apyUSD market: `0x3c53fae231ad3c0408a8b6d33138bbff1caec330`
- API: `https://api-v2.pendle.finance/core/v1/1/markets/{marketAddress}`
- Hosted SDK Convert API: `https://api-v2.pendle.finance/core/v2/sdk/{chainId}/convert`

关键字段：
- `yt.price.usd`
- `pt.price.usd`
- `impliedApy`
- `underlyingApy`
- `liquidity.usd`
- `underlyingAsset.price.usd`
- `dataUpdatedAt`

套利报价口径：
- 始终以 Ethereum `apxUSD` 作为本金和最终收益资产
- 当 Ethereum 的 `apyUSD/apxUSD` 更低时：Ethereum `apxUSD -> apyUSD`，桥 `apyUSD` 到 Base，Base `apyUSD -> apxUSD`，再桥 `apxUSD` 回 Ethereum
- 当 Base 的 `apyUSD/apxUSD` 更低时：先桥 Ethereum `apxUSD` 到 Base，Base `apxUSD -> apyUSD`，桥 `apyUSD` 回 Ethereum，Ethereum `apyUSD -> apxUSD`
- 当前桥费与 gas 成本默认按 `0` 计入，后续可在 `config/assets.yaml` 的 `arbitrage_monitors` 中调整；桥费会按去程和回程两次扣除

## Morpho 市场
- apyUSD/USDC marketId: `0x9c28c8fa039a8df548a7f27adf062d751b0f2e9b9131931810535543adb23291`
- GraphQL: `https://api.morpho.org/graphql`

关键字段：
- `state.liquidityAssets`
- `state.liquidityAssetsUsd`
- `state.borrowApy`
- `state.supplyApy`
- `state.borrowAssetsUsd`
- `state.supplyAssetsUsd`
- `state.utilization`

## Curve 池
- apyUSD/apxUSD pool: `0xe41be7b340f7c2eda4da1e99b42ee1b228b526b7`
- 数据方式：Ethereum RPC 直接调用池合约

关键字段：
- `coins(0)` / `coins(1)`
- `get_dy(0, 1, 1e18)`

当前口径：
- `Curve apyUSD/apxUSD exchange_rate`: `1 apyUSD` 在池内实时可兑换得到多少 `apxUSD`

## APYX 协议说明
- `apyUSD` 为 ERC-4626 tokenized vault
- 文档提到 NAV dashboard，但未发现稳定公开 API
- 官网与应用存在反爬，MVP 不将 HTML 抓取作为主源
- `ApyUSDRateView`: `0xCABa36EDE2C08e16F3602e8688a8bE94c1B4e484`
- `apyUSD underlying APR`: 链上读取 `ApyUSDRateView.apy()` 原始简单年化
- `apyUSD underlying APY`: 使用 APR 按月复利换算

## MVP 数据口径
- `apxUSD TVL`: 多链 `totalSupply * 1 USD`
- `apyUSD TVL`: 多链 `totalAssets * 1 USD`
- `apyUSD NAV`: `totalAssets / totalSupply`
- `yt-* price`: Pendle `yt.price.usd`
- `apxUSD underlying APY`: Pendle `underlyingApy`
- `apyUSD underlying APR`: 链上 `ApyUSDRateView.apy()`
- `apyUSD underlying APY`: `(1 + APR / 12) ** 12 - 1`
- `Curve exchange_rate`: Curve pool `get_dy(apyUSD -> apxUSD, 1e18)`
- `Morpho available_to_borrow_usd`: `state.liquidityAssetsUsd`
- `Morpho borrow_apy`: `state.borrowApy`
- `best_net_profit_usd`: Ethereum 结算闭环套利策略与本金档位里最高的净利润
- `best_net_edge_pct`: 最高净利润对应的净利率
