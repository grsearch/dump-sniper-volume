# Dump Sniper

Solana / Pump.fun 实时交易机器人。当前唯一自动策略是
**迁移 AGE + 实时 FDV + 1 分钟成交额/独立买家 + 5 秒 RSI(7)**。

## 买入策略

程序逐笔处理监控列表内的 swap。只有以下五项同时成立才买入：

1. Pump.fun 迁移 AGE 在 **2～5 分钟**，包含两个端点。
2. 当前 FDV 在 **$50,000～$150,000**，包含两个端点。
3. 最近 60 秒总成交额 **> $10,000**。
4. 最近 60 秒独立买入钱包 **>= 60**。
5. 实时 5 秒 RSI(7) 从 **<=30 上穿到 >30**。

买入 FDV 使用当前链上池状态实时计算并保存在内存中，下单判断不新增
DEX Screener、Birdeye 或 RPC 请求；链上快照暂时不可用时才退回监控注册表中的最近
FDV，两个来源都不可用则拒绝买入。AGE 使用监控注册表中的 Pump.fun 迁移时间。
这两个范围只控制买入，不会缩小监控列表，也不会把不在买入窗口内的代币提前移出。

成交额按 `最近60秒SOL成交量 × ACTIVITY_RSI_SOL_PRICE_USD` 计算，默认
`ACTIVITY_RSI_SOL_PRICE_USD=75.5`。SOL 市价变化后应同步更新该配置；旧的
`SOL_PRICE_USD` 不会覆盖新策略，避免历史配置中的旧价格误改成交额门槛。
独立买入钱包使用每笔 BUY swap 的链上 signer 在同一滚动 60 秒窗口内去重，同一钱包
重复买入只计一个，不增加 API 或 RPC 请求。

RSI 使用每根 5 秒 K 线的最新收盘价和 Wilder 平滑计算，与 TradingView 标准 RSI
一致；它包含当前尚未结束的 5 秒 K 线，以便在最新 swap 到达时立即判断。RSI(7)
至少需要 8 根 5 秒 K 线；历史不足时不会买入。RSI 已经在 30 上方时不会重复触发，
必须先回到 30 或以下，再次上穿。

旧的第一次爆量、TPS 扩张、回撤确认、买卖比、大砸单、多窗口反转、追高过滤、
固定冷却和加仓均不参与买入。

实盘构造 BUY 前会强制读取最新池状态，并使用 PumpSwap SDK 1.19 的
`virtual_quote_reserves` 计算真实报价。买入使用 `buy_exact_quote_in`：每笔最多只花设定的
仓位 SOL，并把剩余价格空间转换成最少到手代币数。刷新后的预估价已经超过信号价 15%
时，本地直接拒绝；上链后价格越过该上限时交易也会拒绝，不会通过放大滑点超额支出。

## 卖出策略

以下任一条件成立即卖出：

- 相对真实成交入场价跌至 **-20%**，立即固定止损；确认卖出后该币进入 **120 秒**冷静期。
- 相对真实成交入场价上涨 **30%** 后激活移动止盈；随后从最新最高价回撤
  **5%** 卖出。

5 秒和 1 分钟 RSI 卖出均关闭。RSI 只参与买入判断；持仓由固定止损、移动止盈或
迁移 AGE 到期退出。

固定止盈、最长持仓、流动反转、稳定期退出、趋势/区间止损、定时止盈、
竞争对手跟卖和其他自动卖出策略均被专用策略分支屏蔽。手动卖出与交易失败处理保留。

## 监控列表

TokenWatchdog 在每个可信链上价格事件后实时计算 FDV 和 LP，并立即执行准入阈值；
数据库和监控页面最多每秒更新一次，避免高频 swap 阻塞交易主路径。DexScreener/Birdeye
仍每分钟补充一次 24 小时成交量等外部行情字段：

- FDV：**$15,000～$1,000,000**。
- LP：**>= $3,000**。
- 实时 FDV = 链上有效价格 × Mint 实际总供应量 × `ACTIVITY_RSI_SOL_PRICE_USD`。
- 实时 LP = PumpSwap 池实际 SOL 储备 × 2 × `ACTIVITY_RSI_SOL_PRICE_USD`；虚拟储备不计入 LP。
- FDV 或 LP 在任一可信 tick 不符合条件时立即移出；有持仓时保留监控，确保退出数据不断流。
- 24 小时成交量：**>= $5,000**。
- AGE 从 Pump.fun 迁移时间开始计算。
- AGE **> 25 分钟**时移出监控；如有持仓，先以 `TOKEN_AGE_EXPIRED` 自动卖出，卖出确认后再移除。
- AGE 使用独立的 1 秒检查，不等待每分钟一次的 FDV/LP 巡检。
- 历史记录缺少迁移时间时，先使用发现时间显示 AGE；拿到 DEX
  交易池创建时间后自动替换为精确时间。

旧的 `MAX_TOKEN_AGE_MS` 不再控制当前策略。监控年龄使用
`BURST_WATCHLIST_MAX_AGE_MS=1500000`。

## Pump.fun 迁移发现

- Helius WebSocket 实时检测 Pump.fun migrate / migrateV2。
- 迁移钱包轮询每 5 秒补漏。
- 从确认交易保存 mint、pool、vault、blockTime、slot 和 signature。
- 符合 FDV 与 LP 条件后加入监控列表，不使用 mint 创建年龄过滤。

## 数据留存

`SWAP_EVENT_LOG_ENABLED=true` 时，每笔已解析的监控代币 swap 都写入 SQLite
`swap_events`，可用于离线重放和阈值回测。

## 关键配置

~~~env
POSITION_SIZE_SOL=0.2
ACTIVITY_RSI_ENABLED=true
ACTIVITY_RSI_VOLUME_WINDOW_MS=60000
ACTIVITY_RSI_MIN_VOLUME_USD=10000
ACTIVITY_RSI_MIN_FDV_USD=50000
ACTIVITY_RSI_MAX_FDV_USD=150000
ACTIVITY_RSI_MIN_MIGRATION_AGE_MS=120000
ACTIVITY_RSI_MAX_MIGRATION_AGE_MS=300000
ACTIVITY_RSI_MIN_UNIQUE_BUYERS_1M=60
ACTIVITY_RSI_SOL_PRICE_USD=75.5
ACTIVITY_RSI_5S_PERIOD=7
ACTIVITY_RSI_BUY_CROSS=30
ACTIVITY_RSI_5S_MIN_BUCKETS=8
ACTIVITY_RSI_MAX_SIGNAL_AGE_MS=5000

BUY_SLIPPAGE_BPS=5000
BUY_MAX_PRICE_DEVIATION_PCT=15
BUY_MAX_POOL_STATE_AGE_MS=500
BUY_FORCE_FRESH_POOL_STATE=true
COMPUTE_UNIT_LIMIT=250000

ACTIVITY_RSI_STOP_LOSS_PCT=-20
STOP_LOSS_REBUY_COOLDOWN_MS=120000
ACTIVITY_RSI_TRAILING_ACTIVATE_PCT=30
ACTIVITY_RSI_TRAILING_DRAWDOWN_PCT=5

BURST_WATCHLIST_MAX_AGE_MS=1500000
WATCHDOG_AGE_CHECK_INTERVAL_MS=1000
WATCHDOG_CHECK_INTERVAL_MS=60000
WATCHDOG_REALTIME_MARKET_PERSIST_MS=1000
MIN_FDV_USD=15000
MAX_FDV_USD=1000000
MIN_LIQUIDITY_USD=3000

ADDON_ENABLED=0
REBUY_COOLDOWN_MS=0
SWAP_EVENT_LOG_ENABLED=true
~~~

启动日志应显示：

~~~text
Entry: ACTIVITY_RSI (AGE 2-5min, FDV $50000-$150000, 1m volume >$10000, buyers >=60, RSI(7,5s) crosses above 30, SOL=$75.5)
Exit only: stop -20%; RSI exit disabled; trailing +30% / drawdown 5% (plus token-age exit)
Executor: ... BUY chain ceiling=50%, signal-price cap=+15%, pool-state max age=500ms, CU=250000
Legacy entries/exits: disabled
Watchdog: ... migrationAge=25min
~~~
