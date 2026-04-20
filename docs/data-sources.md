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

关键字段：
- `yt.price.usd`
- `pt.price.usd`
- `impliedApy`
- `underlyingApy`
- `liquidity.usd`
- `underlyingAsset.price.usd`
- `dataUpdatedAt`

## Morpho 市场
- apyUSD/apxUSD uniqueKey: `0xe23380494e365453f72f736f2d941959ae945773eb67a06cf4f538c7c4201264`
- PT-apyUSD-18JUN2026/USDC uniqueKey: `0xa75bb490ecfee90c86a9d22ebc2dde42fb83478b3f18722b9fc6f5f668cab124`
- GraphQL: `https://api.morpho.org/graphql`

关键字段：
- `state.liquidityAssets`
- `state.liquidityAssetsUsd`
- `state.borrowApy`
- `state.supplyApy`
- `state.borrowAssetsUsd`
- `state.supplyAssetsUsd`
- `state.utilization`

## APYX 协议说明
- `apyUSD` 为 ERC-4626 tokenized vault
- 文档提到 NAV dashboard，但未发现稳定公开 API
- 官网与应用存在反爬，MVP 不将 HTML 抓取作为主源

## MVP 数据口径
- `apxUSD TVL`: 多链 `totalSupply * 1 USD`
- `apyUSD TVL`: 多链 `totalAssets * 1 USD`
- `apyUSD NAV`: `totalAssets / totalSupply`
- `yt-* price`: Pendle `yt.price.usd`
- `underlying APY`: Pendle `underlyingApy`
- `Morpho available_to_borrow_usd`: `state.liquidityAssetsUsd`
- `Morpho borrow_apy`: `state.borrowApy`
