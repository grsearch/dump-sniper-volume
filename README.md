# Dump Sniper

Solana / Pump.fun 实时交易机器人。当前唯一自动策略是
**迁移早期资金流入场 + EMA/移动止盈/FDV 退出**。

## 买入策略

程序逐笔处理监控列表内的可信 swap。在迁移 AGE **15～25秒**内，第一次同时满足
以下条件时锁定信号：

1. 实时 FDV：**$15,000～$100,000**。
2. 最近10秒价格变化：**-10%～+8%**。
3. 最近1秒净资金流：**> 0 SOL**，计算方式为买入SOL减卖出SOL。
4. 最近5秒独立买入钱包：**>=3**。
5. 最近5秒真实成交笔数：**>=4**。
6. 最近5秒最大单笔买入占总买入量：**<=70%**。
7. 当前价格、池地址、池状态、SOL储备和FDV必须来自同一笔最新可信链上状态。

信号锁定后不再重复判断，也不使用触发信号的那笔成交作为执行成交。程序等待下一笔
可信成交：

- 必须在信号后 **3秒内**到达。
- 预计买入价不得超过信号价 **15%**。
- 超时后本次迁移不再买入。

旧的1分钟成交额、RSI、最低成交量、1分钟买家数、大单占比、爆量、TPS、大砸单和
多窗口反转均不参与买入。每笔仓位默认 **0.2 SOL**，同币已有仓位时不加仓。

买入判断完全复用实时 swap 和内存池状态，不增加 DEX Screener、Birdeye 或 RPC 请求。
构造 BUY 前仍会强制刷新池状态，并使用 PumpSwap SDK 的 `buy_exact_quote_in`；刷新后的
预估价或上链成交保护超过信号价15%时拒绝买入。

## 卖出策略

以下任一条件成立即卖出：

1. **买入后快速判错**，退出原因为 `EARLY_ENTRY_INVALIDATED`：
   - 仅在买入后3～15秒运行；移动止盈激活后永久关闭。
   - 买入后最高涨幅始终小于3%。
   - 当前可信价格低于信号价，且跌破买入前5秒VWAP至少3%。
   - 最近3秒净资金流为负，卖量/买量不低于1.5。
   - 最近3秒独立买家不超过1个，并少于此前3秒。
   - 上述条件必须持续至少500ms，并由至少2笔不同的可信成交确认。
   - 机器人的自身买入不计入资金流和买家数。
2. **15秒已收盘K线 EMA9 下穿 EMA20**。
   - EMA20 至少需要20根已收盘15秒K线。
   - 不超过5分钟的空档使用上一收盘价补齐；更长空档重置EMA。
   - 下穿后等待收盘时刻至少500ms，并在下一笔可信成交上执行。
3. **移动止盈：上涨40%激活，从最高点回撤10%卖出**。
4. **实时 FDV < $10,000** 时应急退出。
5. 迁移 AGE **>30分钟**仍未退出时，以 `TOKEN_AGE_EXPIRED` 平仓并移出监控。

固定止损、固定止盈、RSI卖出、流动反转、趋势/区间止损、防御模式和其他旧自动卖出
策略均关闭。该策略允许单笔出现较大浮亏，应继续使用小仓位验证。

## 监控列表

监控列表范围与买入范围分开：

- FDV：**$15,000～$1,000,000**。
- LP：**>= $3,000**。
- 24小时成交量：**>= $5,000**。
- 实时 FDV = 链上有效价格 × Mint实际总供应量 × `EARLY_FLOW_SOL_PRICE_USD`。
- 实时 LP = PumpSwap池实际SOL储备 × 2 × `EARLY_FLOW_SOL_PRICE_USD`。
- FDV或LP不符合条件时立即移出；有持仓时保留数据流，直到卖出确认。
- AGE从Pump.fun迁移时间开始计算。
- AGE超过30分钟时，有持仓先卖出，无持仓直接移除。

外部行情每分钟补充一次，可信链上价格事件仍会实时更新FDV和LP。

## Pump.fun 迁移发现

- Helius WebSocket实时检测Pump.fun migrate / migrateV2。
- 迁移钱包轮询每5秒补漏。
- 从确认交易保存mint、pool、vault、blockTime、slot和signature。
- 符合监控列表FDV与LP条件后加入，不使用mint创建年龄过滤。

## 数据留存

`SWAP_EVENT_LOG_ENABLED=true` 时，每笔已解析的监控代币swap写入SQLite
`swap_events`，用于离线重放和阈值回测。

## 关键配置

~~~env
POSITION_SIZE_SOL=0.2

EARLY_FLOW_ENABLED=true
EARLY_FLOW_MIN_AGE_MS=15000
EARLY_FLOW_MAX_AGE_MS=25000
EARLY_FLOW_MIN_FDV_USD=15000
EARLY_FLOW_MAX_FDV_USD=100000
EARLY_FLOW_SOL_PRICE_USD=75.5
EARLY_FLOW_PRICE_WINDOW_MS=10000
EARLY_FLOW_MIN_PRICE_CHANGE_PCT=-10
EARLY_FLOW_MAX_PRICE_CHANGE_PCT=8
EARLY_FLOW_NET_FLOW_WINDOW_MS=1000
EARLY_FLOW_ACTIVITY_WINDOW_MS=5000
EARLY_FLOW_MIN_UNIQUE_BUYERS=3
EARLY_FLOW_MIN_TRADE_COUNT=4
EARLY_FLOW_MAX_LARGEST_BUY_SHARE=0.70
EARLY_FLOW_EXECUTION_WINDOW_MS=3000
EARLY_FLOW_MAX_EXECUTION_PRICE_DEVIATION_PCT=15
EARLY_FLOW_MARKET_FRESH_MS=1500

EARLY_FLOW_EMA_EXIT_ENABLED=true
EARLY_FLOW_EMA_FAST_PERIOD=9
EARLY_FLOW_EMA_SLOW_PERIOD=20
EARLY_FLOW_EMA_BAR_MS=15000
EARLY_FLOW_EMA_RESET_GAP_MS=300000
EARLY_FLOW_EMA_EXECUTION_DELAY_MS=500
EARLY_FLOW_TRAILING_ACTIVATE_PCT=40
EARLY_FLOW_TRAILING_DRAWDOWN_PCT=10
EARLY_FLOW_FDV_EXIT_USD=10000

EARLY_WRONG_EXIT_ENABLED=true
EARLY_WRONG_EXIT_MIN_HOLD_MS=3000
EARLY_WRONG_EXIT_MAX_HOLD_MS=15000
EARLY_WRONG_EXIT_MAX_PEAK_PNL_PCT=3
EARLY_WRONG_EXIT_PRICE_BREAK_PCT=-3
EARLY_WRONG_EXIT_FLOW_WINDOW_MS=3000
EARLY_WRONG_EXIT_SELL_BUY_RATIO=1.5
EARLY_WRONG_EXIT_MAX_UNIQUE_BUYERS=1
EARLY_WRONG_EXIT_CONFIRM_MS=500
EARLY_WRONG_EXIT_CONFIRM_TRADES=2

BUY_SLIPPAGE_BPS=5000
BUY_MAX_PRICE_DEVIATION_PCT=15
BUY_MAX_POOL_STATE_AGE_MS=500
BUY_FORCE_FRESH_POOL_STATE=true
COMPUTE_UNIT_LIMIT=250000

BURST_WATCHLIST_MAX_AGE_MS=1800000
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
Entry: EARLY_FLOW (AGE 15-25s, FDV $15000-$100000, change10s -10%..+8%, flow1s>0, buyers5s>=3, tx5s>=4, largestBuyShare<=70%)
Exit only: fixed stop disabled; take profit disabled; RSI exit disabled; EMA9/EMA20 down-cross; trailing +40% / drawdown 10% and FDV <$10000 (plus token-age exit)
Early invalidation: enabled (3-15s, peak<3%, VWAP break<=-3%, sell/buy>=1.5, confirm=2/500ms)
Executor: ... BUY chain ceiling=50%, signal-price cap=+15%, pool-state max age=500ms, CU=250000
Legacy entries/exits: disabled
Watchdog: ... migrationAge=30min
~~~
