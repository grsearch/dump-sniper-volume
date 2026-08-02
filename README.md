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
多窗口反转均不参与买入。风险评分已关闭，基础条件通过后不会再因评分被拒绝。
每笔仓位默认 **0.2 SOL**，同币已有仓位时不加仓。

买入判断完全复用实时 swap 和内存池状态，不增加 DEX Screener、Birdeye 或 RPC 请求。
构造 BUY 前仍会强制刷新池状态，并使用 PumpSwap SDK 的 `buy_exact_quote_in`；刷新后的
预估价或上链成交保护超过信号价15%时拒绝买入。

## 卖出策略

以下任一条件成立即卖出：

1. **15秒已收盘K线 EMA9 下穿 EMA20**。
   - EMA20 至少需要20根已收盘15秒K线。
   - 不超过5分钟的空档使用上一收盘价补齐；更长空档重置EMA。
   - 下穿后等待收盘时刻至少500ms，并在下一笔可信成交上执行。
2. **移动止盈：上涨9%激活，从最高点回撤5%卖出**。
3. **实时 FDV < $10,000** 时应急退出。
4. 迁移 AGE **>30分钟**仍未退出时，以 `TOKEN_AGE_EXPIRED` 平仓并移出监控。

固定止损、确认尾部止损、固定止盈、RSI卖出、流动反转、趋势/区间止损、
慢性弱势退出、防御模式和其他旧自动卖出策略均关闭。亏损保护仅保留可信
单笔成交达到 -50% 时触发的灾难保护。该策略允许单笔出现较大浮亏，应继续
使用小仓位验证。

原“买入后快速判错”条件默认以 `shadow` 模式运行：仍逐笔计算、确认并写入
`EEI_CANDIDATE` / `EEI_SHADOW_TRIGGER`，但不会提交卖单。只有显式设置
`EARLY_WRONG_EXIT_MODE=live` 才恢复 `EARLY_ENTRY_INVALIDATED` 实盘退出。

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
- 迁移安全审计已关闭，不检查`MintTo`、大额转移或同交易买入。
- 发现后的准入只使用监控列表FDV与LP条件，不使用mint创建年龄过滤。

## 数据留存

`SWAP_EVENT_LOG_ENABLED=true` 时，每笔已解析的监控代币swap写入SQLite
`swap_events`，用于离线重放和阈值回测。

`POSITION_RESEARCH_LOG_ENABLED=true` 时，持仓期间每笔可信成交还会写入
`position_research_events`。每行保留：

- 链上时间、接收时间、slot、签名、方向、钱包、成交量和原始/有效价格。
- 池地址、池内代币/SOL、虚拟储备、供应量、FDV、LP和行情采集时间。
- 入场价、信号价、入场前VWAP、持仓PnL、高点、回撤及移动止盈状态。
- 当前与前一段1秒、3秒、5秒、10秒的成交数、买卖量、净流入、买卖比、
  独立买卖钱包、大单占比、VWAP、价格变化和加速度。
- EEI候选/影子触发、移动止盈激活、真实退出触发、FDV退出及最终平仓事件。

按固定截止时间导出分析数据：

~~~bash
npm run export:research -- --since=2026-07-29T00:00:00+08:00 --until=2026-07-30T00:00:00+08:00 --out=reports/research-20260729
~~~

导出目录包含 `manifest.json`、仓位、逐笔研究指标、原始swap、信号、交易、
平仓后走势和代币元数据。`manifest.json.analysisCutoffMs` 是唯一分析截止点；
截止点之后才发生的平仓、PnL和高点会被遮蔽，避免回测看到未来数据。

## 关键配置

### SOL / WSOL 对账

程序启动和每次卖出确认后会做只读余额刷新，并在北京时间每天 `00:00`、`06:00`、`12:00`、`18:00` 执行定时对账：

- 钱包控制的全部 WSOL Token Account（包括钱包界面可能隐藏的辅助账户）都会被统计；余额达到 `WSOL_AUTO_UNWRAP_MIN_SOL` 时，会在定时对账中自动关闭可安全关闭的账户，转回原生 SOL。
- 买入、卖出或解包正在执行时不操作，等待 60 秒后重试。
- 外部 router、vault 或 Jupiter 中转账户不归属于本钱包，不参与资产、盈亏和健康告警统计，也不会被程序关闭。
- 自动解包前会重新核验 WSOL mint、账户 owner 和 close authority，只有钱包有权限关闭的账户才会执行。
- 仪表盘显示钱包 SOL、钱包 WSOL 和 SOL+WSOL 合计；Token Account 租金不计入合计。
- 成交盈亏只按“钱包原生 SOL + 钱包控制的 WSOL”的净变化核算；包装和解包属于内部转换，不重复计为利润。

逐笔报价资产变化写入 `quote_asset_movements`，定时对账写入 `quote_asset_reconciliations`，便于后续导出审计。

```env
WSOL_RECONCILE_ENABLED=true
WSOL_RECONCILE_SCHEDULE_HOURS_CST=0,6,12,18
WSOL_RECONCILE_BUSY_RETRY_MS=60000
WSOL_AUTO_UNWRAP_MIN_SOL=0.01
```

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
EARLY_FLOW_MIN_BUY_SOL_5S=2
EARLY_FLOW_MAX_LARGEST_BUY_SHARE=0.70
EARLY_FLOW_EXECUTION_WINDOW_MS=3000
EARLY_FLOW_MAX_EXECUTION_PRICE_DEVIATION_PCT=15
EARLY_FLOW_MARKET_FRESH_MS=1500

EARLY_FLOW_RISK_FILTER_ENABLED=false
EARLY_FLOW_RISK_REJECT_SCORE=4
EARLY_FLOW_RISK_MIN_UNIQUE_BUYERS_5S=6
EARLY_FLOW_RISK_MIN_BUY_SOL_5S=3
EARLY_FLOW_RISK_MIN_PRICE_CHANGE_PCT=-2
EARLY_FLOW_RISK_MAX_LARGEST_BUY_SHARE=0.45
EARLY_FLOW_RISK_MAX_EXECUTION_DELAY_MS=400
EARLY_FLOW_RISK_MIN_FDV_USD=25000
EARLY_FLOW_RISK_MAX_MIGRATION_AGE_MS=20000

EARLY_FLOW_EMA_EXIT_ENABLED=true
EARLY_FLOW_EMA_FAST_PERIOD=9
EARLY_FLOW_EMA_SLOW_PERIOD=20
EARLY_FLOW_EMA_BAR_MS=15000
EARLY_FLOW_EMA_RESET_GAP_MS=300000
EARLY_FLOW_EMA_EXECUTION_DELAY_MS=500
EARLY_FLOW_TRAILING_ACTIVATE_PCT=9
EARLY_FLOW_TRAILING_DRAWDOWN_PCT=5
EARLY_FLOW_RUNNER_ENABLED=true
EARLY_FLOW_RUNNER_ACTIVATE_PCT=15
EARLY_FLOW_RUNNER_MAX_ACTIVATION_HOLD_MS=60000
EARLY_FLOW_RUNNER_FLOW_WINDOW_MS=3000
EARLY_FLOW_RUNNER_MIN_NET_FLOW_SOL=0
EARLY_FLOW_RUNNER_MIN_BUY_SELL_RATIO=1.2
EARLY_FLOW_RUNNER_MIN_UNIQUE_BUYERS=3
EARLY_FLOW_RUNNER_CONFIRM_MS=500
EARLY_FLOW_RUNNER_CONFIRM_TRADES=2
EARLY_FLOW_RUNNER_TIER_1_DRAWDOWN_PCT=8
EARLY_FLOW_RUNNER_TIER_1_FLOOR_PCT=8
EARLY_FLOW_RUNNER_TIER_2_DRAWDOWN_PCT=10
EARLY_FLOW_RUNNER_TIER_2_FLOOR_PCT=15
EARLY_FLOW_RUNNER_TIER_3_DRAWDOWN_PCT=15
EARLY_FLOW_RUNNER_TIER_3_FLOOR_PCT=30
EARLY_FLOW_FDV_EXIT_USD=10000
EARLY_FLOW_FIXED_STOP_LOSS_PCT=0
EARLY_FLOW_TAIL_STOP_ENABLED=false
EARLY_FLOW_TAIL_STOP_PNL_PCT=-30
EARLY_FLOW_TAIL_STOP_CONFIRM_MS=500
EARLY_FLOW_TAIL_STOP_CONFIRM_TRADES=2
EARLY_FLOW_CATASTROPHIC_STOP_ENABLED=true
EARLY_FLOW_CATASTROPHIC_STOP_PNL_PCT=-50
EARLY_FLOW_SLOW_BLEED_EXIT_ENABLED=false
EARLY_FLOW_SLOW_BLEED_MIN_HOLD_MS=60000
EARLY_FLOW_SLOW_BLEED_MAX_PEAK_PNL_PCT=9
EARLY_FLOW_SLOW_BLEED_MAX_PNL_PCT=-5
EARLY_FLOW_SLOW_BLEED_FLOW_WINDOW_MS=3000
EARLY_FLOW_SLOW_BLEED_SELL_BUY_RATIO=1.5
EARLY_FLOW_SLOW_BLEED_MAX_UNIQUE_BUYERS=2
EARLY_FLOW_SLOW_BLEED_CONFIRM_MS=500
EARLY_FLOW_SLOW_BLEED_CONFIRM_TRADES=2

EARLY_WRONG_EXIT_ENABLED=true
EARLY_WRONG_EXIT_MODE=shadow
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
ADDON_SHADOW_ENABLED=true
ADDON_SHADOW_WINDOW_MS=60000
ADDON_SHADOW_LOW_PNL_PCT=-20
ADDON_SHADOW_MIN_CURRENT_PNL_PCT=-20
ADDON_SHADOW_MAX_CURRENT_PNL_PCT=0
ADDON_SHADOW_MIN_REBOUND_PCT=3
ADDON_SHADOW_MIN_NET_FLOW_3S_SOL=0
ADDON_SHADOW_MIN_BUY_SELL_RATIO_3S=1.5
ADDON_SHADOW_MIN_UNIQUE_BUYERS_3S=2
ADDON_SHADOW_MIN_TRADE_COUNT_3S=6
ADDON_SHADOW_MAX_TRADE_COUNT_3S=16
ADDON_SHADOW_MIN_BUY_ACCELERATION_3S=3
ADDON_SHADOW_SIZE_SOL=0.2
ADDON_SHADOW_MAX_HOLD_MS=1500000
ADDON_SHADOW_TRAILING_ACTIVATE_PCT=50
ADDON_SHADOW_TRAILING_DRAWDOWN_PCT=10
ADDON_SHADOW_EXECUTION_COST_PCT=5
REBUY_COOLDOWN_MS=0
SWAP_EVENT_LOG_ENABLED=true
POSITION_RESEARCH_LOG_ENABLED=true
POSITION_RESEARCH_WINDOW_MS=10000
POSITION_RESEARCH_FLUSH_MS=250
POSITION_RESEARCH_FLUSH_MAX=1000
~~~

启动日志应显示：

### Live Runner trailing

Runner is a live exit mode, not a shadow signal. Normal positions use the
`+9% / 5%` trailing stop. A position that reaches `+15%` within 60 seconds is
upgraded permanently when 3-second order flow stays positive for 500ms across
two distinct transactions, buy/sell is at least 1.2, there are at least three
buyers, and price remains above the 3-second VWAP.

- Peak 15-25%: 8% drawdown with a +8% profit floor.
- Peak 25-50%: 10% drawdown with a +15% profit floor.
- Peak 50% or more: 15% drawdown with a +30% profit floor.

The effective exit price is the tighter of the peak drawdown stop and the
profit floor. FDV, EMA, catastrophic, and token-age exits remain independent.

~~~text
Entry: EARLY_FLOW (...) with risk score disabled
Exit only: fixed stop disabled; take profit disabled; RSI exit disabled; EMA9/EMA20 down-cross; trailing +9% / drawdown 5%; LIVE runner tiers; tail stop disabled; slow bleed disabled; trusted single-swap catastrophe at -50%; FDV <$10000 (plus token-age exit)
Early invalidation: shadow (3-15s, peak<3%, VWAP break<=-3%, sell/buy>=1.5, confirm=2/500ms)
Add-on shadow: research only; first entry and any real add-on exit independently
Position research telemetry: enabled (window=10s, flush=250ms)
Executor: ... BUY chain ceiling=50%, signal-price cap=+15%, pool-state max age=500ms, CU=250000
Legacy entries/exits: disabled
Watchdog: ... migrationAge=30min
~~~
