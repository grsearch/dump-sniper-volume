# Dump Sniper

## Pump.fun Graduation Discovery

The service discovers successful Pump.fun graduations without a webhook:

- Helius WebSocket logs provide the low-latency path; the migration wallet is polled every 5 seconds to fill gaps.
- A transaction is accepted only when it contains the official Pump `migrate` discriminator and targets the official PumpSwap program.
- Mint, pool, vaults, chain `blockTime`, slot, and signature are read from the confirmed transaction and saved in `tokens`.
- All token sources use the same market thresholds: FDV `$15,000-$1,000,000` and liquidity at least `$3,000`.
- Pump graduation admission checks only FDV and liquidity. It records the confirmed migration time and does not request or filter on mint creation age.
- Passing discovery only adds the token to monitoring. The Activity Flow buy strategy remains unchanged.

Configuration is under `PUMP_DISCOVERY_*` in `.env.example`. Set `PUMP_DISCOVERY_ENABLED=false` to disable it.

Monitoring-list FDV, LP, price, and 24h volume are refreshed every minute through
the batched DEX Screener token endpoint, with Birdeye as a fallback. The dashboard
shows the age and source of the last successful market refresh. Webhooks may send
`migrationTime`/`migration_time` (seconds, milliseconds, or ISO time); when an
older webhook row has no migration time, the selected DEX pair creation time is
used to backfill its migration AGE even when that pair does not yet expose complete
FDV/LP data. Legacy `WATCHDOG_CHECK_INTERVAL_MS` values above one minute are clamped
to `60000`.

Solana / Pump.fun 短线交易机器人。当前默认买入策略是 **Activity Flow 1m volume-ratio**：不再看大砸单，也不再用 5s/15s/30s 多窗口反转确认。

## 当前买入策略

每个监控代币收到实时 swap 后，程序立即重算最近 1 分钟窗口：

- 1 分钟总成交量默认必须达到 `$3,000`，按 `SOL_PRICE_USD` 换算成 SOL。`.env.example` 默认 `SOL_PRICE_USD=72`，所以约等于 `41.7 SOL/分钟`。
- 1 分钟买量 / 卖量默认必须 `>= 1.35`。
- 1 分钟交易次数默认必须 `>=25`，过滤低频小量币。
- 1 分钟 RSI(7) 的已收盘值和实时值必须同时 `<50`；按每分钟最后成交价作为收盘价，并采用 Wilder RMA。至少需要 8 根已完成的 1 分钟 K 线，数据不足时拒绝买入但继续监控。
- 启动时最多回看 120 分钟 `swap_events` 重建 RSI；120 分钟是读取上限，不是币龄要求，新币只使用上市后实际存在的数据。
- 最近 5 秒必须至少有 `4` 笔买入、`3` 个不同买家，且买量 / 卖量 `>=1.1`。
- 最近 5 秒最大买家的买量占比不能超过 `50%`，避免单钱包或拆单制造假繁荣。
- 最近 5 秒涨幅不能超过 `6%`，任一单笔买入的价格冲击不能超过 `4%`，避免追入瞬时拉高。
- Activity Flow 信号和普通卖出后的同币冷却均为 `0`；只要没有持仓或在途买单，下一笔合格信号可立即重新判断。
- 触发那一笔必须是 BUY。
- 池子 SOL 默认仍需 `>=30 SOL`。
- 信号必须足够新，默认 `ACTIVITY_FLOW_MAX_SIGNAL_AGE_MS=5000`。

默认入口日志应显示：

```text
Entry: ACTIVITY_FLOW (VOLUME_RATIO_1M: 1m volume>=...SOL (~$3000), buy/sell>=1.35, RSI(7,1m) closed/live<50)
Legacy dumpSignal: suppressed
[main] ActivityFlow enabled: mode=VOLUME_RATIO_1M ...
```

## 当前卖出策略

- 流动反转退出：关闭。
- RSI 超买退出：当前 `1m RSI(7) > 80` 时立即卖出；至少需要 8 根已完成分钟线。
- 移动止盈优先：同币任一仓位一旦激活移动止盈，后续不再使用 RSI 超买退出。
- 移动止盈：上涨 `40%` 激活，从最高点回撤 `10%` 卖出。
- 固定止盈：上涨 `100%` 立即卖出。
- 紧急止损和稳定期紧急止损：关闭。
- TIMEOUT 卖出：关闭。
- 加仓：默认最多加仓 1 次；价格相对第一笔入场价下跌 `20%` 后，新的合格信号才允许加仓。同币任一仓位触发自动退出时，全部仓位按顺序逐笔卖出。
- 卖出冷静期：实际平仓完成后，同币 `5 分钟` 内禁止再次买入；多仓分批卖出时从最后一笔完成卖出重新计时。

## 监控列表过滤

TokenWatchdog 默认每 1 分钟巡检一次 FDV 和 LP：

- FDV 必须在 `$15,000 ~ $1,000,000`
- Birdeye LP 必须 `>= $3,000`
- 24h 交易量必须 `>= $5,000`
- AGE 从 Pump 迁移时间开始计算；迁移超过 `24h` 的代币会被移除，已有持仓会保留监控直到平仓
- 迁移时间未知时 AGE 显示未知并跳过 AGE 移除，不使用 mint 创建时间或添加时间猜测
- 监控列表上限默认 `500` 个；只有新增代币后超过该上限才会触发驱逐

## 数据留存

默认开启 `SWAP_EVENT_LOG_ENABLED=true`。程序会把每一笔已解析的监控币实时 swap 写入 SQLite 的 `swap_events` 表，后续可以基于这张表离线重算窗口并回测阈值。

## 参数优化回测

运行内置优化器：

```bash
npm run optimize:activity -- --hours 168 --iterations 2000
```

优化器会：

- 按时间顺序把数据切成 `60%` 训练、`20%` 验证、`20%` 隔离测试，测试集不参与参数选择。
- 按池地址和价格连续性拆分交易片段，防止 bonding curve 迁移到 AMM 时价格口径跳变产生虚假巨额收益。
- 搜索 1 分钟成交量/买卖比/交易数、5 秒买盘质量、移动止盈和固定止盈阈值；固定使用 1 分钟 RSI(7) `<50`，已关闭的退出不会被推荐配置重新开启。
- 默认按每边 `1%` 执行成本及每笔 `0.0005 SOL` 优先费计算，并额外输出每边 `0.5% / 1% / 2%` 成本压力测试。
- 输出 Markdown、候选参数 CSV、隔离测试逐笔交易 CSV、JSON 和一份候选 `.env` 到 `reports/`，但不会修改线上 `.env`、重启服务或发送交易。
- 数据少于 `72h`、有效 swap 少于 `10,000` 或隔离测试交易少于 `30` 笔时，会明确标记为探索性结果，不宣称已经找到可盈利策略。

常用选项：

```bash
npm run optimize:activity -- --hours 0 --iterations 5000 --min-trades 10 --cost-bps 100
npm run optimize:activity -- --hours 0 --iterations 5000 --max-price-jump-ratio 20
npm run optimize:activity -- --self-test
```

`--hours 0` 表示使用数据库内全部历史。推荐先积累至少 3 到 7 天连续数据，再依据隔离测试和分时段稳定性决定是否采用候选参数。

## 关键配置

```env
ACTIVITY_FLOW_ENABLED=true
ACTIVITY_FLOW_REPLACE_DUMP_SIGNAL=true
ACTIVITY_FLOW_ENTRY_MODE=VOLUME_RATIO_1M
ACTIVITY_FLOW_1M_MIN_VOLUME_USD=3000
ACTIVITY_FLOW_1M_MIN_VOLUME_SOL=
ACTIVITY_FLOW_1M_MIN_BUY_SELL_RATIO=1.35
ACTIVITY_FLOW_1M_MIN_TRADES=25
ACTIVITY_FLOW_RSI_1M_ENABLED=true
ACTIVITY_FLOW_RSI_1M_PERIOD=7
ACTIVITY_FLOW_RSI_1M_MAX=50
ACTIVITY_FLOW_RSI_1M_MIN_BARS=8
ACTIVITY_FLOW_RSI_1M_WARMUP_MAX_MINUTES=120
RSI_PRICE_SCALE_RESET_RATIO=100
ACTIVITY_FLOW_CONFIRM_MIN_BUY_TRADES_5S=4
ACTIVITY_FLOW_CONFIRM_MIN_UNIQUE_BUYERS_5S=3
ACTIVITY_FLOW_CONFIRM_MIN_BUY_SELL_RATIO_5S=1.10
ACTIVITY_FLOW_CONFIRM_MAX_BUYER_SHARE_5S=0.50
ACTIVITY_FLOW_CONFIRM_MAX_PRICE_RISE_5S_PCT=6
ACTIVITY_FLOW_CONFIRM_MAX_SINGLE_BUY_IMPACT_PCT=4
ACTIVITY_FLOW_COOLDOWN_MS=0
ACTIVITY_FLOW_MIN_POOL_QUOTE_SOL=30
ACTIVITY_FLOW_MAX_SIGNAL_AGE_MS=5000

FLOW_REVERSAL_EXIT_ENABLED=false
FLOW_REVERSAL_EXIT_MODE=VOLUME_RATIO_1M
FLOW_REVERSAL_EXIT_WINDOW_MS=60000
FLOW_REVERSAL_EXIT_SELL_BUY_RATIO_1M=1.35
FLOW_REVERSAL_EXIT_MIN_VOLUME_1M_SOL=5
FLOW_REVERSAL_EXIT_MIN_HOLD_MS=10000

TRAILING_ACTIVATE_PCT=40
TRAILING_DRAWDOWN_PCT=10
TAKE_PROFIT_PCT=100
RSI_1M_EXIT_ENABLED=true
RSI_1M_EXIT_THRESHOLD=80
EMERGENCY_STOP_LOSS_PCT=0
STABILIZATION_EMERGENCY_DRAWDOWN_PCT=0
MAX_HOLD_MS=0

ADDON_ENABLED=1
ADDON_DROP_PCT=20

REBUY_COOLDOWN_MS=300000
TOKEN_MAX_AGE_MS=0
MIN_FDV_USD=15000
MAX_FDV_USD=1000000
MIN_LIQUIDITY_USD=3000
WATCHDOG_CHECK_INTERVAL_MS=60000
WATCHDOG_MARKET_STALE_MS=180000
MAX_TOKEN_AGE_MS=86400000
MAX_MINT_AGE_HOURS=0
NEW_COIN_AGE_THRESHOLD_MS=0
MAX_WATCHED_TOKENS=500

BUY_MIN_PRIORITY_FEE_LAMPORTS=500000
BUY_CAP_PRIORITY_FEE_LAMPORTS=500000
BUY_MAX_PRIORITY_FEE_LAMPORTS=500000
MAX_PRIORITY_FEE_LAMPORTS=500000
SWAP_EVENT_LOG_ENABLED=true
```

上线前核对 `.env` 已填好 Helius、Birdeye 和钱包密钥，并确认启动日志显示 `VOLUME_RATIO_1M`。
