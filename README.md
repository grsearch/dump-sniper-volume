# Dump Sniper

Solana / Pump.fun 实时交易机器人。当前唯一自动策略是 **1 分钟成交额 + 5 秒 RSI(7)**。

## 买入策略

程序逐笔处理监控列表内的 swap。只有以下两项同时成立才买入：

1. 最近 60 秒总成交额 **> $10,000**。
2. 实时 5 秒 RSI(7) 从 **<=30 上穿到 >30**。

成交额按 `最近60秒SOL成交量 × ACTIVITY_RSI_SOL_PRICE_USD` 计算，默认
`ACTIVITY_RSI_SOL_PRICE_USD=75.5`。SOL 市价变化后应同步更新该配置；旧的
`SOL_PRICE_USD` 不会覆盖新策略，避免历史配置中的旧价格误改成交额门槛。

RSI 使用每根 5 秒 K 线的最新收盘价和 Wilder 平滑计算，与 TradingView 标准 RSI
一致；它包含当前尚未结束的 5 秒 K 线，以便在最新 swap 到达时立即判断。RSI(7)
至少需要 8 根 5 秒 K 线；历史不足时不会买入。RSI 已经在 30 上方时不会重复触发，
必须先回到 30 或以下，再次上穿。

旧的第一次爆量、TPS 扩张、回撤确认、买卖比、大砸单、多窗口反转、追高过滤、
固定冷却和加仓均不参与买入。

## 卖出策略

以下任一条件成立即卖出：

- 实时 5 秒 RSI(7) 从 **>=70 下穿到 <70**。
- 实时 5 秒 RSI(7) **>80**。
- 相对真实成交入场价上涨 **20%** 后激活移动止盈；随后从最新最高价回撤
  **10%** 卖出。

如果移动止盈先激活，则不再执行 RSI 下穿 70 或 RSI 大于 80 的卖出；该持仓随后
只由最高价回撤 10% 触发移动止盈卖出。

阈值均按严格定义执行：RSI 恰好等于 80 不触发超买退出；当前 RSI 低于 70
本身也不触发，必须观察到前值不低于 70、当前值低于 70 的真实下穿。

固定止盈、固定止损、最长持仓、流动反转、稳定期退出、趋势/区间止损、定时止盈、
竞争对手跟卖和其他自动卖出策略均被专用策略分支屏蔽。手动卖出与交易失败处理保留。

## 监控列表

TokenWatchdog 每分钟刷新一次 FDV、LP、价格和 24 小时成交量：

- FDV：**$15,000～$1,000,000**。
- LP：**>= $3,000**。
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
ACTIVITY_RSI_ENABLED=true
ACTIVITY_RSI_VOLUME_WINDOW_MS=60000
ACTIVITY_RSI_MIN_VOLUME_USD=10000
ACTIVITY_RSI_SOL_PRICE_USD=75.5
ACTIVITY_RSI_5S_PERIOD=7
ACTIVITY_RSI_BUY_CROSS=30
ACTIVITY_RSI_5S_MIN_BUCKETS=8
ACTIVITY_RSI_MAX_SIGNAL_AGE_MS=5000

ACTIVITY_RSI_EXIT_DOWN_CROSS=70
ACTIVITY_RSI_EXIT_OVERBOUGHT=80
ACTIVITY_RSI_TRAILING_ACTIVATE_PCT=20
ACTIVITY_RSI_TRAILING_DRAWDOWN_PCT=10

BURST_WATCHLIST_MAX_AGE_MS=1500000
WATCHDOG_AGE_CHECK_INTERVAL_MS=1000
WATCHDOG_CHECK_INTERVAL_MS=60000
MIN_FDV_USD=15000
MAX_FDV_USD=1000000
MIN_LIQUIDITY_USD=3000

ADDON_ENABLED=0
REBUY_COOLDOWN_MS=0
SWAP_EVENT_LOG_ENABLED=true
~~~

启动日志应显示：

~~~text
Entry: ACTIVITY_RSI (1m volume >$10000, RSI(7,5s) crosses above 30, SOL=$75.5)
Exit only: RSI(7,5s) crosses below 70 or >80; trailing +20% / drawdown 10%
Legacy entries/exits: disabled
Watchdog: ... migrationAge=25min
~~~
