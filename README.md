# Dump Sniper — 精简基线版 (clean baseline)

Solana / Pump.fun 砸盘反弹狙击机器人。本版本在保留原有**经过实盘验证的底层引擎**
（LaserStream 数据流、Pump AMM 交易构造、Jito 抢跑、Pool 状态缓存、SQLite 记录、Dashboard）
的基础上，把**策略层收敛为一套固定规则**，并把历史上堆积的几十个实验性过滤器/出场机制
**默认全部关闭**。目的是：改参数不再莫名其妙没信号，代码行为可预测。

> 底层下单/数据链路代码没有重写（那部分是月级调试沉淀，重写=拿真金白银做实验）。
> 改动集中在：策略默认值、监控过滤、出场逻辑开关，以及清理无关文件。

---

## 策略（唯一生效的规则）

**监控列表过滤**（TokenWatchdog，每 15s 巡检）
- FDV 必须在 **$30,000 ~ $1,000,000** 之间，超出范围移除
- 代币年龄 **> 24h** 移除
- 24h 交易量 **< $5,000** 移除

**买入：订单流承接确认**
- 先出现卖压 setup：10 秒窗口内卖出量 **≥ 20 SOL**、跌幅 **≥ 12%**、卖单数 **≥ 2**、不同卖出账户 **≥ 2**
- 低点后前 **700ms** 的抢买不算确认，避免把第一波竞对冲单当作反转
- 在低点后 **0.7s ~ 4s** 内等待承接：买入量 **≥ 3 SOL**，且后续买量 / setup 总卖量 **≥ 0.25**
- 同时要求承接窗口内买/卖量比 **≥ 1.25**、净买入失衡 **≥ 0.15**、不同买入账户 **≥ 2**
- 最后确认价格从低点反弹 **1.5% ~ 8%**，才把信号交给 `SignalEngine` 买入

旧的大砸单直买路径默认关闭：`ORDER_FLOW_ENABLED=true` 且 `ORDER_FLOW_REPLACE_DUMP_SIGNAL=true` 时，`dumpSignal` 只作为 setup/记录，不会直接买。保留两条安全护栏：跌幅 > 30% 不接（已 rug）、池子 SOL < 30 不买。

**卖出**
- 快速止盈：稳定期内利润达 **5%** 立即卖
- 移动止盈：涨到 **+10%** 激活，从最高点回撤 **3%** 卖
- 固定止盈：**+20%** 立即卖
- 固定止损：**−25%** 立即卖
- 兜底超时：持仓 **30min** 仍未触发上述任一出场，强制平仓

所有参数都在 `.env` 中，且 `src/config.js` 的 `orderFlow` 默认值已与上述规则一致——
即使某个 `.env` 变量漏写，也会落到正确的默认值，不会退回旧的复杂行为。

---

## 全新服务器部署（Openclaw）

```bash
# 1. 克隆
git clone <your-repo-url> dump-sniper && cd dump-sniper

# 2. 一键安装（装依赖 + 配 systemd + logrotate，默认装到 /opt/dump-sniper）
sudo bash deploy/install.sh

# 3. 配置密钥（策略字段已设好，只需填 6 个密钥）
sudo -u ubuntu cp /opt/dump-sniper/.env.example /opt/dump-sniper/.env
sudo -u ubuntu vim /opt/dump-sniper/.env
#    必填: HELIUS_API_KEY / HELIUS_RPC_URL / HELIUS_STAKED_RPC_URL /
#          HELIUS_LASERSTREAM_TOKEN / BIRDEYE_API_KEY / WALLET_PRIVATE_KEY_BS58
#    （从旧服务器的 .env 直接复制对应值）

# 4. 先 DRY_RUN 跑（.env 里 DRY_RUN=true 是默认值，别动）
sudo systemctl start dump-sniper
sudo journalctl -u dump-sniper -f      # 确认有 "BUY/SELL (DRY_RUN)" 日志和信号

# 5. 验证 OK 后切实盘：把 .env 的 DRY_RUN 改成 false，再重启
sudo -u ubuntu sed -i 's/^DRY_RUN=true/DRY_RUN=false/' /opt/dump-sniper/.env
sudo systemctl restart dump-sniper
sudo systemctl enable dump-sniper      # 开机自启
```

监控列表通过 Dashboard（`http://<server>:3001`）或 webhook 添加，逻辑与旧版一致。

常用命令：
```bash
npm run health       # 健康检查
npm run diagnose     # 诊断
sudo journalctl -u dump-sniper -f   # 实时日志
```

---

## 上线前检查清单

- [ ] `.env` 已填 6 个密钥，`DRY_RUN=true`
- [ ] DRY_RUN 跑至少几小时，日志里能看到信号与（模拟）买卖
- [ ] 钱包 SOL 余额够（POSITION_SIZE_SOL × 并发数 + 手续费）
- [ ] 移动止盈回撤 3%（与历史 2.5–10% 分层一致）、兜底超时 30min，确认符合预期
- [ ] 切 LIVE 后小仓观察首批真实成交

---

## 与旧版的差异（为什么今天会"没信号"）

旧版 `SignalEngine` 入场链路上叠了约 25 个过滤器（RSI、近端拉盘、距高点跌幅、
下跌趋势、波动率、砸盘深度、新/老币分支、signal-FDV…），`PositionManager`
出场链路上叠了约 10 套机制（智能止损、防御模式、分波动率超时、低峰早砍、
区间止损、趋势止损、定时止盈…），其中不少是**代码里默认开启**的。
你只改了砸单大小/跌幅就没信号，是因为提高阈值后，这些过滤器的"与"条件叠加起来
把所有信号都拒了。

本版把它们**全部默认关闭**，只留上面写的那套规则，所以参数改动不会再连锁触发隐藏过滤器。
