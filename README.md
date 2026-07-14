# Dump Sniper

Solana / Pump.fun 短线交易机器人。当前版本保留原来的数据流、下单、持仓、Dashboard 和 SQLite 记录链路，但默认买入策略已经改为 **Activity Flow 活跃成交流**。

## 当前买入策略

默认入口不再看单笔大砸单，也不再要求先出现 `MIN_SELL_SOL` 或 `ORDER_FLOW_MIN_DROP_PCT`。程序会持续统计每个监控代币最近 5 秒、15 秒、30 秒、60 秒的成交结构：

- 60 秒窗口：判断这个币是否像图 1 一样足够热。默认要求 `>=24` 笔成交、`>=12 SOL` 成交量、`>=10` 个不同交易账户。
- 30 秒窗口：二次确认活跃度，默认要求 `>=12` 笔成交、`>=6 SOL` 成交量，且买卖量比 `>=1.05`，避免整体仍是持续卖压。
- 15 秒窗口：判断资金方向，默认要求 `>=8` 笔成交、`>=4 SOL` 成交量、买卖量比 `>=1.45`、净买入失衡 `>=0.20`、不同买家 `>=3`。
- 5 秒窗口：实际触发买入，默认要求 `>=5` 笔成交、`>=2.5 SOL` 成交量、买卖量比 `>=1.40`、净买入失衡 `>=0.25`、不同买家 `>=2`，并且最后一笔必须是买入。

追高保护：5 秒涨幅默认不得超过 `5%`，30 秒和 60 秒涨幅默认都不得超过 `10%`。15 秒价格允许最多 `-3%`，30 秒允许最多 `-20%`，60 秒允许最多 `-30%`，用于保留“前面还在跌、短线刚转强”的反转点。

流动性保护：Activity Flow 买入也会检查池子 SOL，默认 `>=30 SOL` 才允许触发。

新鲜度保护：默认只允许最近 `5000ms` 内收到的成交触发买入，避免 LaserStream 乱序或延迟回放老交易时追进已经结束的行情。

旧 `dumpSignal` 默认被压制，只作为记录或 fallback，不会直接触发买入。启动日志应显示：

```text
Entry: ACTIVITY_FLOW
Legacy dumpSignal: suppressed
[main] ActivityFlow enabled
```

## 监控列表过滤

TokenWatchdog 默认每 15 分钟巡检一次：

- FDV 必须在 `$30,000 ~ $1,000,000`
- 24h 交易量必须 `>= $5,000`
- 不再按代币年龄自动移除

## 卖出策略

- 反转卖出：持仓后 5 秒与 15 秒窗口同时转成卖压，并且价格已从峰值回撤，触发 `FLOW_REVERSAL_EXIT`
- 移动止盈：`+10%` 激活，从最高点回撤 `3%` 卖
- 固定止盈：`+20%` 立即卖
- 固定止损：`-25%` 立即卖
- 兜底超时：持仓 `30min` 仍未触发任一出场则强制平仓

## 数据留存

默认开启 `SWAP_EVENT_LOG_ENABLED=true`。程序会把每一笔已解析的监控币实时 swap 写入 SQLite 的 `swap_events` 表，字段包括时间、mint、买卖方向、SOL 成交量、价格、价格变化、交易钱包、slot、签名和池子 SOL。后续可以基于这张表离线重算 5s/15s/30s/60s 窗口，并扫不同阈值组合做回测。

## 关键配置

所有默认值都在 `.env.example` 和 `src/config.js`。主要入口参数使用 `ACTIVITY_FLOW_*`：

```env
ACTIVITY_FLOW_ENABLED=true
ACTIVITY_FLOW_REPLACE_DUMP_SIGNAL=true
ACTIVITY_FLOW_MIN_TRADES_60S=24
ACTIVITY_FLOW_MIN_VOLUME_60S_SOL=12
ACTIVITY_FLOW_MIN_TRADES_30S=12
ACTIVITY_FLOW_MIN_VOLUME_30S_SOL=6
ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_30S=1.05
ACTIVITY_FLOW_MIN_TRADES_15S=8
ACTIVITY_FLOW_MIN_VOLUME_15S_SOL=4
ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_15S=1.45
ACTIVITY_FLOW_MIN_IMBALANCE_15S=0.20
ACTIVITY_FLOW_MIN_TRADES_5S=5
ACTIVITY_FLOW_MIN_VOLUME_5S_SOL=2.5
ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_5S=1.40
ACTIVITY_FLOW_MIN_IMBALANCE_5S=0.25
ACTIVITY_FLOW_MAX_PRICE_CHANGE_5S_PCT=5
ACTIVITY_FLOW_MAX_PRICE_CHANGE_30S_PCT=10
ACTIVITY_FLOW_MAX_PRICE_CHANGE_60S_PCT=10
ACTIVITY_FLOW_MIN_POOL_QUOTE_SOL=30
ACTIVITY_FLOW_MAX_SIGNAL_AGE_MS=5000
FLOW_REVERSAL_EXIT_ENABLED=true
REBUY_COOLDOWN_MS=0
TOKEN_MAX_AGE_MS=0
MAX_TOKEN_AGE_MS=0
MAX_MINT_AGE_HOURS=0
NEW_COIN_AGE_THRESHOLD_MS=0
BUY_MIN_PRIORITY_FEE_LAMPORTS=500000
BUY_CAP_PRIORITY_FEE_LAMPORTS=500000
BUY_MAX_PRIORITY_FEE_LAMPORTS=500000
MAX_PRIORITY_FEE_LAMPORTS=500000
SWAP_EVENT_LOG_ENABLED=true
```

旧的 `ORDER_FLOW_*` 只保留部分开关兼容，不再作为默认阈值体系。

## 部署

```bash
git clone <your-repo-url> dump-sniper
cd dump-sniper
sudo bash deploy/install.sh
sudo -u ubuntu cp /opt/dump-sniper/.env.example /opt/dump-sniper/.env
sudo -u ubuntu vim /opt/dump-sniper/.env
sudo systemctl restart dump-sniper
sudo journalctl -u dump-sniper -f
```

上线前核对 `.env` 已填好 Helius、Birdeye 和钱包密钥，并确认启动日志显示 `ACTIVITY_FLOW`。
