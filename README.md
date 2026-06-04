# APYX Monitor MVP

一个面向 APYX 生态的监控服务 MVP，覆盖：

- `apxUSD` / `apyUSD` 的 TVL 与基础链上指标
- `yt-apxUSD` / `yt-apyUSD` 的 Pendle 隐含 APY 与相关市场指标
- Curve `apyUSD/apxUSD` 池子的实时兑换汇率
- Ethereum / Base `apyUSD` 与 `apxUSD` 的 PendleSwap 闭环跨链套利报价监控
- Morpho 市场的可借款额、借款利率、利用率
- 闭环跨链套利利润率超过阈值时的飞书机器人告警
- Curve 汇率偏离净值告警、Apyx Capped Ratio 脱锚告警
- FastAPI 查询接口与本地 SQLite 持久化

## MVP 方案

### 数据源
- APYX 文档：用于发现合约与市场
- Pendle REST：YT 价格、隐含 APY、流动性、基础资产价格
- Pendle Hosted SDK：`apyUSD` / `apxUSD` 路由报价，用于套利空间估算
- Morpho GraphQL：可借款额、借款利率、供给/借款、利用率
- 链上 RPC：`apxUSD` / `apyUSD` 的 `totalSupply` / `totalAssets`
- 链上 RPC：Curve 池 `get_dy(apyUSD -> apxUSD, 1e18)` 实时汇率
- 链上派生：Curve 汇率相对 `convertToAssets()` 的偏离幅度、Capped Ratio 相对 1.0 的脱锚幅度

### 当前默认监控对象
- Ethereum + Base 上的 `apxUSD`、`apyUSD`
- Ethereum 上 Pendle 市场
  - `0x50dce085af29caba28f7308bea57c4043757b491` (`YT-apxUSD-18JUN2026`)
  - `0x3c53fae231ad3c0408a8b6d33138bbff1caec330` (`YT-apyUSD-18JUN2026`)
- Ethereum 上 Morpho 市场
  - `0x9c28c8fa039a8df548a7f27adf062d751b0f2e9b9131931810535543adb23291` (`apyUSD/USDC`)
- Ethereum 上 Curve 池
  - `0xe41be7b340f7c2eda4da1e99b42ee1b228b526b7` (`apyUSD/apxUSD`)
- Ethereum ↔ Base 闭环跨链套利监控
  - 默认本金档位：Ethereum `10000` USDC
  - Pendle Hosted SDK 存在限流，套利采集遇到 `429` 会进入 10 分钟冷却并保留看板已有数据
  - 结算口径：始终以 Ethereum `USDC` 作为本金和最终收益资产
  - 当 Ethereum 的 `apyUSD/apxUSD` 更低时：Ethereum `USDC -> apxUSD -> apyUSD`，桥 `apyUSD` 到 Base，Base `apyUSD -> apxUSD`，再桥 `apxUSD` 回 Ethereum，最后 `apxUSD -> USDC`
  - 当 Base 的 `apyUSD/apxUSD` 更低时：Ethereum `USDC -> apxUSD`，桥 `apxUSD` 到 Base，Base `apxUSD -> apyUSD`，桥 `apyUSD` 回 Ethereum，Ethereum `apyUSD -> apxUSD -> USDC`
  - 当前桥费与 gas 成本默认按 `0` 计入，收益按闭环后回到 Ethereum 的 `USDC` 计算

## 快速启动

1. 复制 `.env.example` 为 `.env`
2. 修改看板登录账号密码，并按需填写飞书与 RPC
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

### Dashboard 登录

访问 `/dashboard` 会先跳转到登录页。默认账号密码请在 `.env` 中配置：

- `DASHBOARD_USERNAME`：看板账号
- `DASHBOARD_PASSWORD`：看板密码
- `DASHBOARD_SESSION_SECRET`：Cookie 会话签名密钥，生产环境请使用足够长的随机字符串
- `DASHBOARD_SESSION_TTL_SECONDS`：登录有效期，默认 `86400` 秒

## 执行指南

### 本地开发

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn apyx_monitor.main:app --reload
```

如果没有执行 `pip install -e .`，需要先把 `src` 加入 Python 搜索路径：

```bash
PYTHONPATH=src uvicorn apyx_monitor.main:app --reload
```

### 长期运行

生产或服务器长期运行时，建议使用单 worker：

```bash
uvicorn apyx_monitor.main:app --host 0.0.0.0 --port 8000 --workers 1
```

注意事项：

- 不要用多个 worker 跑 SQLite 版本。当前采集调度器在 FastAPI 进程启动时创建，多个 worker 会启动多个定时采集任务，容易同时写入 SQLite。
- `--reload` 只用于本地开发，长期运行不要开启。
- 默认 SQLite 数据库在 `data/apyx_monitor.db`。程序会启用 WAL 和 30 秒 busy timeout，以降低看板查询和后台写入之间的锁冲突。
- 采集间隔由 `COLLECTION_INTERVAL_SECONDS` 控制，默认每 60 秒采集一次。过低的间隔会增加 RPC/API 压力和数据库写入压力。
- NAV/Curve 快速扫描由 `NAV_CURVE_INTERVAL_SECONDS` 控制，默认每 20 秒采集一次 `apyUSD convertToAssets()`、Curve `get_dy()` 和偏离净值指标，不会额外请求 Pendle/Morpho。
- 每次发版后需要重启服务，让数据库 PRAGMA、索引和新代码生效。

### 启动后检查

```bash
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/api/v1/metrics/latest
```

也可以手动触发一次采集：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/jobs/poll
```

### 数据增长与清理

`MetricSnapshot` 会持续保存历史指标，目前没有自动删除策略。长期运行建议定期控制历史数据量，例如只保留最近 30 天或 90 天的分钟级数据；否则 SQLite 文件会持续增长，看板 30 天趋势查询也会越来越慢。

手工清理前请先备份数据库，并尽量在服务停止或低访问时执行：

```bash
sqlite3 data/apyx_monitor.db "DELETE FROM metricsnapshot WHERE recorded_at < datetime('now', '-90 days'); VACUUM;"
```

## 目录结构

- `config/assets.yaml`：资产、合约、Pendle、Morpho 配置
- `config/rules.yaml`：告警规则
- `docs/data-sources.md`：已确认资料来源
- `src/apyx_monitor/`：应用代码

## 说明

- `apyUSD` 的底层 APR 读取官方链上 `ApyUSDRateView.apy()`，底层 APY 使用 APR 按月复利换算；`apxUSD` 暂继续使用 Pendle 市场中的 `underlyingApy`。
- `apyUSD` 作为 ERC-4626 vault，TVL 采用 `totalAssets` 近似，NAV 采用 `totalAssets / totalSupply`。
- 默认规则仅为示例值，正式环境需按业务重新标定。
- 已新增简单看板，可查看 TVL、底层 APY、YT 隐含 APY、Curve 汇率、Morpho 指标和闭环跨链套利报价历史趋势。
- 默认新增风险监控：
    - Curve `apyUSD/apxUSD` 汇率相对 `apyUSD.convertToAssets()` 偏离超过 `1%`
    - `Apyx Capped Ratio` 相对 `1.0` 脱锚超过 `0.5%`
    - 闭环跨链套利最佳利润率超过 `0.2%` 时发送飞书；阈值可在看板调整
  - 其他风险规则仅在看板记录和展示，不发送飞书。
