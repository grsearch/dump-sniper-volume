# Dump Sniper

Solana / Pump.fun 实时交易机器人。当前唯一自动买入策略是
**第一次爆量后的回撤确认**。

## 买入策略

程序对监控列表中每个代币的 swap 逐笔计算滚动窗口。每次收到新交易时，
“当前 5 秒”指 `(当前时刻-5s, 当前时刻]`，“前一段 5 秒”指
`(当前时刻-10s, 当前时刻-5s]`：

1. 识别第一次爆量：
   - 当前 5 秒总成交量 / 前一段 5 秒总成交量 **>= 3**。
   - 当前 5 秒交易笔数（等价于 5 秒平均 TPS）/ 前一段 5 秒交易笔数 **>= 2**。
   - 前一段 5 秒必须有真实交易，禁止从零基数计算扩张。
   - 此前 30 秒没有检测到同时满足上述成交量与 TPS 扩张阈值的同等级爆量。
2. 第一次爆量后最多等待 60 秒，持续更新爆量后最高价。
3. 以下条件同时满足时买入：
   - 最高价相对爆量前价格上涨 **>= 5%**。
   - 当前价格从最高价回撤 **2%～8%**。
   - 当前价格仍高于爆量前价格。
   - 当前 5 秒买入 SOL - 卖出 SOL **> 0**。
   - 当前 5 秒卖出 SOL 低于前一段 5 秒。
   - 当前 5 秒买入 SOL / 前一段 5 秒买入 SOL **>= 1.5**，且买入 SOL 增加。
   - 当前 10 秒的新买家钱包数高于前一段 10 秒。
   - 新买家必须是第一次爆量确认后首次出现的钱包；爆量时已经出现的钱包、
     以及之后重复买入的钱包都不会重复计数。
4. 每个代币在发出一次合格行情信号后冷却 **5 分钟**，冷却期间不再接受
   同币的新爆量行情事件。

旧的 1 分钟量比、RSI 入场、大砸单入场、多窗口反转、追高过滤和加仓
均不参与当前买入路径。大砸单仍可记录分析，但不会触发买入。

## 卖出策略

自动退出仅保留三项：

- 固定止盈：相对真实成交入场价 **+20%**。
- 固定止损：相对真实成交入场价 **-10%**。
- 最长持仓：**120 秒**，到时无条件卖出。
- 实际平仓完成后，同币继续冷静 **5 分钟**再允许重新买入。

RSI、流动反转、移动止盈、稳定期退出、趋势/区间止损、定时止盈、
竞争对手跟卖和加仓均不会参与当前策略。手动卖出仍然可用。

## 监控列表

TokenWatchdog 每分钟刷新一次 FDV、LP、价格和 24 小时成交量：

- FDV：**$15,000～$1,000,000**。
- LP：**>= $3,000**。
- 24 小时成交量：**>= $5,000**。
- AGE 从 Pump.fun 迁移时间开始计算。
- AGE **> 60 分钟**时移出监控；已有持仓会保留订阅，直到平仓后再移除。
- 历史记录缺少迁移时间时，先使用发现时间显示 AGE；拿到 DEX
  交易池创建时间后会自动替换为精确时间。

旧的 **MAX_TOKEN_AGE_MS** 不再控制当前策略。使用
**BURST_WATCHLIST_MAX_AGE_MS=3600000**。

## Pump.fun 迁移发现

- Helius WebSocket 实时检测 Pump.fun migrate / migrateV2。
- 迁移钱包轮询每 5 秒补漏。
- 从确认交易保存 mint、pool、vault、blockTime、slot 和 signature。
- 符合 FDV 与 LP 条件后加入监控列表，不使用 mint 创建年龄过滤。

## 数据留存

**SWAP_EVENT_LOG_ENABLED=true** 时，每笔已解析的监控代币 swap 都写入
SQLite **swap_events**，可用于离线重放和阈值回测。

## 关键配置

~~~env
BURST_PULLBACK_ENABLED=true
BURST_PULLBACK_WINDOW_5S_MS=5000
BURST_PULLBACK_VOLUME_EXPANSION=3
BURST_PULLBACK_TPS_EXPANSION=2
BURST_PULLBACK_QUIET_WINDOW_MS=30000
BURST_PULLBACK_CONFIRM_WINDOW_MS=60000
BURST_PULLBACK_MIN_PEAK_RISE_PCT=5
BURST_PULLBACK_MIN_PULLBACK_PCT=2
BURST_PULLBACK_MAX_PULLBACK_PCT=8
BURST_PULLBACK_MIN_BUYER_ACCELERATION=1.5
BURST_PULLBACK_NEW_BUYER_WINDOW_MS=10000
BURST_PULLBACK_EVENT_COOLDOWN_MS=300000
BURST_PULLBACK_MAX_SIGNAL_AGE_MS=5000

BURST_EXIT_TAKE_PROFIT_PCT=20
BURST_EXIT_STOP_LOSS_PCT=-10
BURST_EXIT_MAX_HOLD_MS=120000

BURST_WATCHLIST_MAX_AGE_MS=3600000
WATCHDOG_CHECK_INTERVAL_MS=60000
MIN_FDV_USD=15000
MAX_FDV_USD=1000000
MIN_LIQUIDITY_USD=3000

ADDON_ENABLED=0
REBUY_COOLDOWN_MS=300000
SWAP_EVENT_LOG_ENABLED=true
~~~

启动日志应显示：

~~~text
Entry: FIRST_BURST_PULLBACK
Exit only: TP +20% / stop -10% / max hold 120s
Legacy entries/exits: disabled
Watchdog: ... migrationAge=1h
~~~
