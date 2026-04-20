# APYX Monitor MVP

一个面向 APYX 生态的监控服务 MVP，覆盖：

- `apxUSD` / `apyUSD` 的 TVL 与基础链上指标
- `yt-apxUSD` / `yt-apyUSD` 的 Pendle 隐含 APY 与相关市场指标
- Morpho 市场的可借款额、借款利率、利用率
- 阈值触发后的飞书机器人告警
- FastAPI 查询接口与本地 SQLite 持久化

## MVP 方案

### 数据源
- APYX 文档：用于发现合约与市场
- Pendle REST：YT 价格、隐含 APY、流动性、基础资产价格
- Morpho GraphQL：可借款额、借款利率、供给/借款、利用率
- 链上 RPC：`apxUSD` / `apyUSD` 的 `totalSupply` / `totalAssets`

### 当前默认监控对象
- Ethereum + Base 上的 `apxUSD`、`apyUSD`
- Ethereum 上 Pendle 市场
  - `0x50dce085af29caba28f7308bea57c4043757b491` (`YT-apxUSD-18JUN2026`)
  - `0x3c53fae231ad3c0408a8b6d33138bbff1caec330` (`YT-apyUSD-18JUN2026`)
- Ethereum 上 Morpho 市场
  - `0xe23380494e365453f72f736f2d941959ae945773eb67a06cf4f538c7c4201264` (`apyUSD/apxUSD`)

## 快速启动

1. 复制 `.env.example` 为 `.env`
2. 按需填写飞书与 RPC
3. 安装依赖
4. 启动服务：

```bash
uvicorn apyx_monitor.main:app --reload
```

启动后访问：
- `GET /dashboard`
- `GET /healthz`
- `GET /api/v1/metrics/latest`
- `GET /api/v1/metrics/trends?entity_id=apxusd&metric_name=tvl_usd&hours=24&bucket_minutes=15`
- `GET /api/v1/alerts?status=firing`
- `POST /api/v1/jobs/poll`

## 目录结构

- `config/assets.yaml`：资产、合约、Pendle、Morpho 配置
- `config/rules.yaml`：告警规则
- `docs/data-sources.md`：已确认资料来源
- `src/apyx_monitor/`：应用代码

## 说明

- `apxUSD` / `apyUSD` 的 APY 在官方站点存在反爬，MVP 默认优先用 Pendle 市场中的 `underlyingApy` 作为可程序化替代来源。
- `apyUSD` 作为 ERC-4626 vault，TVL 采用 `totalAssets` 近似，NAV 采用 `totalAssets / totalSupply`。
- 默认规则仅为示例值，正式环境需按业务重新标定。
- 已新增简单看板，可查看 TVL、底层 APY、YT 隐含 APY 和 Morpho 指标历史趋势。
