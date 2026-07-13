'use strict';

require('dotenv').config({ override: true });

const orderFlowForceDisabled = ['true', '1', 'yes'].includes(
  String(process.env.ORDER_FLOW_FORCE_DISABLED || '').toLowerCase(),
);

const config = {
  // ============ Mode ============
  DRY_RUN: (process.env.DRY_RUN ?? 'true').toLowerCase() === 'true',

  // ============ Strategy ============
  strategy: {
    // 触发条件（DumpDetector）
    // v3.17.20 用户调参：MIN_SELL_SOL 6.0, MIN_PRICE_IMPACT_PCT 10.0
    minSellSol: parseFloat(process.env.MIN_SELL_SOL || '20'),
    minPriceImpactPct: parseFloat(process.env.MIN_PRICE_IMPACT_PCT || '10.0'),
    minTriggerSellCount: parseInt(process.env.MIN_TRIGGER_SELL_COUNT || "2", 10),
    // v3.17.39: 距近期高点跌幅过滤 — 防止"高位接刀"(价格刚从 ATH 小幅回落就追入)
    minDropFromRecentHighPct: parseFloat(process.env.MIN_DROP_FROM_RECENT_HIGH_PCT || '0'),
    minDropLookbackSec: parseInt(process.env.MIN_DROP_LOOKBACK_SEC || '1200', 10),
    // v3.17.30: 短窗口涨幅过滤 — 防秒级脉冲拉盘后接刀 (Backrooms: 30s内翻倍, 信号前刚pump完)
    //   用 RsiCalculator 的 1s 桶价格历史，检测最近 N 秒内的涨幅
    //   涨幅超阈值 → 说明价格刚被暴力拉升，砸单可能是拉盘后的正常回调，不是恐慌抛售
    recentPumpShortSec: parseInt(process.env.RECENT_PUMP_SHORT_SEC || '0', 10),
    recentPumpShortMaxPct: parseFloat(process.env.RECENT_PUMP_SHORT_MAX_PCT || '0'),
    // v3.17.40: 长窗口涨幅过滤 — 防"累积长拉后顶部接刀" (FCH: 3h 缓拉 +67%, 每个 5min 不极端但 30min 看 +30%+)
    recentPumpLongSec: parseInt(process.env.RECENT_PUMP_LONG_SEC || '0', 10),
    recentPumpLongMaxPct: parseFloat(process.env.RECENT_PUMP_LONG_MAX_PCT || '0'),
    // v3.10: 实盘观察 — 阈值过宽抓"伪砸盘"（大池子 10 SOL 卖单价格几乎不动），
    // 也抓"流动性已死"（小池子 30%+ impact 但反弹空间小且滑点巨大）
    // 加这两条过滤
    maxPriceImpactPct: parseFloat(process.env.MAX_PRICE_IMPACT_PCT || '30.0'),
    minPoolQuoteSol: parseFloat(process.env.MIN_POOL_QUOTE_SOL || '30.0'),

    // 仓位
    positionSizeSol: parseFloat(process.env.POSITION_SIZE_SOL || '0.1'),

    // v3.17 止盈策略改造，v3.17.6 实战调参：
    //   1) 主止盈 TAKE_PROFIT_PCT +50%（保留双确认）— 捕捉大反弹
    //   2) 移动止盈 TRAILING_* — 锁中等反弹利润（实战主要止盈来源）
    //      v3.17.6 调参：从 5%/2% 拉到 8%/3%
    //      - 实战发现 AMM 自买入会推高池子价格 5-10%（我们 3 SOL 进 30 SOL 池子约 +10%）
    //      - 这导致 5% activate 太敏感，会被自买入虚高触发
    //      - 但这个问题在 v3.17.6 用 stabilization 期 + 中位数 baseline 已根治
    //      - 所以 8% 是"双保险"：stabilization 过滤瞬态高价 + 8% 阈值再过滤一道
    //      - openclaw 拍脑袋拉到 15%/5% 过于保守，会错过大部分中等反弹
    //   3) 紧急止损 -15% 不变
    //   4) MAX_HOLD_MS:30s (v3.17.19 从 30min 改下来) — 反弹窗口 5-30 秒,30 秒外不会再反弹
    // v3.17.20 用户策略改造：固定止盈 10%，到 10% 立即卖，不等双确认。
    //   优先级高于移动止盈（_checkExit 里先检查 TP 再检查 trailing）。
    //   tpConfirmCount/tpConfirmMinGapMs 保留字段但已不在固定止盈路径使用。
    takeProfitPct: parseFloat(process.env.TAKE_PROFIT_PCT || '20'),
    tpConfirmCount: parseInt(process.env.TP_CONFIRM_COUNT || '2', 10),
    tpConfirmMinGapMs: parseInt(process.env.TP_CONFIRM_MIN_GAP_MS || '300', 10),

    // 移动止盈（v3.17.6 调参）
    //   trailingActivatePct: HWM 涨过 entryPrice × (1 + 此值/100) 才 arm
    //   trailingDrawdownPct: armed 后，价格从 HWM 回撤此 % 立即 SELL
    //   trailingMinHwmAgeMs: HWM 必须稳定至少此毫秒数（防单 tick 污染）
    //   设 trailingActivatePct=0 或 trailingDrawdownPct=0 可禁用移动止盈
    trailingActivatePct: parseFloat(process.env.TRAILING_ACTIVATE_PCT || '10'),
    trailingDrawdownPct: parseFloat(process.env.TRAILING_DRAWDOWN_PCT || '3'),
    trailingMinHwmAgeMs: parseInt(process.env.TRAILING_MIN_HWM_AGE_MS || '2000', 10),

    // v3.17.6: Stabilization 期 —— reconcile 完成后等价格稳定，再开始 trailing 追踪
    //   原理：砸盘后 + 我们自买入 → 池子价格剧烈波动 + 虚高 5-10%
    //         如果 reconcile 完成立刻开始追 HWM，第一个 tick 就是虚高瞬态值
    //         → trailing 立刻 armed → 真实价格回归被误判"回撤" → 误杀
    //   修复：reconcile 完成后进入 stabilization 期（默认 5 秒）：
    //         - 收集所有 priceTick 进 buffer
    //         - 不更新 HWM，不武装 trailing，不检查 TP
    //         - emergency_stop 仍正常工作（救命路径不能屏蔽）
    //         期满取样本中位数作为 HWM 起点，过滤自买入推高和砸盘瞬态
    //   实战权衡：
    //     - 5 秒：覆盖砸盘后短暂剧烈波动（实测多数 < 3 秒就稳定）
    //     - 太短（< 3s）：保护不够，自买入虚高没消化完
    //     - 太长（> 10s）：错过早期快速反弹的入场窗口
    stabilizationMs: parseInt(process.env.STABILIZATION_MS || '5000', 10),

    // v3.17.34: 稳定期内快速止盈
    //   稳定期内利润达 fastProfitExitPct%(默认 5%)就提前卖出
    //   防虚高: 中位数+当前价都>=阈值且样本>=fastProfitMinSamples
    fastProfitExitPct: parseFloat(process.env.FAST_PROFIT_EXIT_PCT || '5.0'),
    fastProfitMinSamples: parseInt(process.env.FAST_PROFIT_MIN_SAMPLES || '3', 10),

    // v3.17.7: stabilization 期内 emergency_stop 的阈值
    //   stabilization 期内"相对 entryPrice 的 PnL"不可靠（自买入推高+回归造成假亏损）
    //   所以期间改用"相对样本最高价的回撤"判断 emergency
    //   - max(samples) ≈ 自买入推高的池子价格峰值
    //   - 从峰值真的跌此 % 才认作灾难（不是简单的相对 entryPrice 跌幅）
    //   - 20% 既能放过"自买入回归"（通常 ≤ 10-12%），又能抓真的暴跌
    //   设 0 禁用 stabilization 期内的 emergency_stop（极端 dangerous，不推荐）
    stabilizationEmergencyDrawdownPct: parseFloat(
      process.env.STABILIZATION_EMERGENCY_DRAWDOWN_PCT || '20.0',
    ),

    // 紧急止损（防止灾难性下跌）
    // 设置为 0 可禁用紧急止损（恢复"硬扛"行为）
    emergencyStopLossPct: parseFloat(process.env.EMERGENCY_STOP_LOSS_PCT || '-25'),

    // v3.17.42: 智能止损 — 分波动率止损阈值
    // 智能规则: trailing已armed时不触发(trailing自行处理回撤), 只救trailing永远不armed的死扛仓位
    // stabilization期内不触发, 持仓>5min后才触发
    // 0=禁用, 负数=止损百分比(如-25表示跌破-25%止损)
    volLowEmergencyStopPct: parseFloat(process.env.VOL_LOW_EMERGENCY_STOP_PCT || '0'),
    volMidEmergencyStopPct: parseFloat(process.env.VOL_MID_EMERGENCY_STOP_PCT || '0'),
    volHighEmergencyStopPct: parseFloat(process.env.VOL_HIGH_EMERGENCY_STOP_PCT || '0'),
    // 智能止损最小持仓时间(ms) — 避免刚买入就被止损
    smartStopGraceMs: parseInt(process.env.SMART_STOP_GRACE_MS || '300000', 10),  // 默认5min

    // 持仓上限时间
    //   v3.17:    30min (1800000ms)
    //   v3.17.19: 30秒 (30000ms) — 反弹窗口通常 5-30 秒
    //   v3.17.20: 设 0 禁用 TIMEOUT 卖出，持仓靠 TP/Trailing/Emergency 退出
    //   v3.17.32: 恢复为 4h 强制退出(数据回测: 4h+ 只有 30% 胜率, 平均亏 -13%)
    //   clean:     30min (1800000ms) — 短线反弹策略, 超时强制退出
    maxHoldMs: parseInt(process.env.MAX_HOLD_MS || '1800000', 10),
    lowPeakTimeoutMs: parseInt(process.env.LOW_PEAK_TIMEOUT_MS || '1800000', 10),  // v3.17.40c: peakPnl<trailingActivate 超时割肉, 默认30min
    slotExitGap: parseInt(process.env.SLOT_EXIT_GAP || '0', 10),  // 0 = disabled

    // v3.17.32: 防御模式 — 持仓超过 defenseActivateMs 后进入防御 trailing
    //   数据回测: 20 分钟是 PnL 拐点, 此后 peak<8% 的单平均亏 -17.8%
    //   防御模式: 即使没涨到 trailingActivatePct(8%), 也激活低门槛 trailing
    //   defenseActivateMs: 持仓超过此时间后激活防御模式 (默认 20min)
    //   defenseTrailingDrawdownPct: 防御 trailing 回撤阈值 (默认 3%)
    //   defenseStopLossPct: 防御模式止损 (PnL% 低于此值立即卖出, 默认 -10%)
    defenseActivateMs: parseInt(process.env.DEFENSE_ACTIVATE_MS || '1200000', 10),
    defenseTrailingDrawdownPct: parseFloat(process.env.DEFENSE_TRAILING_DRAWDOWN_PCT || '3.0'),
    defenseStopLossPct: parseFloat(process.env.DEFENSE_STOP_LOSS_PCT || '-10.0'),
    defenseProfitActivatePct: parseFloat(process.env.DEFENSE_PROFIT_ACTIVATE_PCT || '3.0'),  // v3.17.33: PnL>=3%激活防御trailing

    // 滑点
    buySlippageBps: parseInt(process.env.BUY_SLIPPAGE_BPS || '1500', 10),  // 15%
    sellSlippageBps: parseInt(process.env.SELL_SLIPPAGE_BPS || '2000', 10), // 20%

    // 风控（v3.17 默认 maxConcurrent 5）
    cooldownMsPerToken: parseInt(process.env.COOLDOWN_MS_PER_TOKEN || '60000', 10),
    maxConcurrentPositions: parseInt(process.env.MAX_CONCURRENT_POSITIONS || '10', 10),

    // v3.17.6: 同砸单去重时间窗（毫秒）
    //   防 LaserStream 多 region 跨越 dedup TTL 后重推同一砸单导致二次触发
    //   实战案例：同一 seller_tx 在 2 分钟后被慢 region 重新推送 → 价格已跌 20% → 亏
    //   10 分钟覆盖最慢 region + 重启窗口，且通过 signals 表持久化（启动时恢复）
    sellerTxDedupMs: parseInt(process.env.SELLER_TX_DEDUP_MS || '600000', 10),

    // v3.17.7: 同卖家+同代币 去重窗（毫秒）
    //   防"持续出货"场景反复触发：同一 wallet 短时间内反复砸同一个代币
    //   实战案例：ikG8tz5e 18 秒内对 POSITIONS 砸了 2 次（seller_tx 不同），
    //             2 次都被买入 2 次都亏 — 这表明该卖家在持续出货，不是恐慌抛售
    //   设 0 禁用此检查（恢复旧行为）
    //   推荐 5-10 分钟，跟你的持仓最大时间 MAX_HOLD_MS 匹配
    sellerMintDedupMs: parseInt(process.env.SELLER_MINT_DEDUP_MS || '600000', 10),

    // v3.17.7: 信号过期检查（slot gap 阈值）
    //   砸盘交易的 slot 与当前最新 slot 差超过此值就丢弃信号
    //   实战案例：某些代币 LaserStream 推送延迟 48-88 秒（127-214 slot），
    //             那时候反弹早结束，买在山顶 → emergency_stop 出场
    //
    //   v3.17.16: 默认从 20(~8s)降到 10(~4s)。
    //     上一版 500ms DumpDetector 延迟+解析+发送整条 ≈ 1-2s = 2.5-5 slot
    //     现在 500ms 删了,整条链路应该 ≤ 1s = 2.5 slot
    //     10 slot 给 race + Sender 通道延迟留余量,超过则确实是 LaserStream 慢 region 重推。
    //   设 0 禁用此检查（恢复旧行为）
    maxSignalSlotGap: parseInt(process.env.MAX_SIGNAL_SLOT_GAP || '10', 10),
    // v3.17.29: push lag 阈值 — 砸盘落链到我们收到处理的最大墙钟差(ms)
    // 超过此阈值即拒(反弹已经过了,买在山顶)
    // 设 0 禁用此检查(fallback 旧的 slot gap 路径)
    // 实测:健康 LS 推送 push lag 通常 200-800ms,SS 路径 50-200ms
    // 留 5000ms 余量,足够覆盖正常网络抖动 + worker 偶发积压,又能拦下 20+ 分钟的迟到推送
    maxPushLagMs: parseInt(process.env.MAX_PUSH_LAG_MS || '5000', 10),

    // v3.17.13: 代币监控超时（毫秒），0 = 禁用
    //   v3.17.20: 用户明确不要"监控超时退出"（不要 6 小时到期退出），保持 0
    maxWatchDurationMs: parseInt(process.env.MAX_WATCH_DURATION_MS || '0', 10),
    // v3.17.20: FDV 下限（USD），低于此值自动移除监控（默认开启 $20,000）
    //   Birdeye fdv 字段是 USD 计价。15 秒巡检一次（见 TokenWatchdog）
    minFdVUsd: parseFloat(process.env.MIN_FDV_USD || '30000'),
    // v3.17.20: LP 下限（SOL），低于此值自动移除监控（默认开启 5000 SOL）
    //   用链上池子 quote vault 的实际 SOL 余额，不依赖 Birdeye（新币数据不准）
    minLpSol: parseFloat(process.env.MIN_LP_SOL || '0'),
    // v3.17.20: FDV 上限（USD），设 0 禁用（不因 FDV 过大移除监控）
    maxFdVUsd: parseFloat(process.env.MAX_FDV_USD || '1000000'),
  },

  // ============ Order-flow reversal entry ============
  orderFlow: {
    // Default entry path: dump creates a setup; buy volume confirmation creates the buy signal.
    enabled: !orderFlowForceDisabled && (process.env.ORDER_FLOW_ENABLED ?? 'true').toLowerCase() === 'true',
    replaceDumpSignal:
      !orderFlowForceDisabled && (process.env.ORDER_FLOW_REPLACE_DUMP_SIGNAL ?? 'true').toLowerCase() === 'true',
    windowMs: parseInt(process.env.ORDER_FLOW_WINDOW_MS || '10000', 10),
    confirmWindowMs: parseInt(process.env.ORDER_FLOW_CONFIRM_WINDOW_MS || '4000', 10),
    buyGraceMs: parseInt(process.env.ORDER_FLOW_BUY_GRACE_MS || '700', 10),
    minSellSol: parseFloat(process.env.ORDER_FLOW_MIN_SELL_SOL || process.env.MIN_SELL_SOL || '20'),
    minDropPct: parseFloat(process.env.ORDER_FLOW_MIN_DROP_PCT || '12'),
    maxDropPct: parseFloat(process.env.ORDER_FLOW_MAX_DROP_PCT || process.env.MAX_PRICE_IMPACT_PCT || '30'),
    minSellCount: parseInt(process.env.ORDER_FLOW_MIN_SELL_COUNT || process.env.MIN_TRIGGER_SELL_COUNT || '2', 10),
    minUniqueSellers: parseInt(process.env.ORDER_FLOW_MIN_UNIQUE_SELLERS || '2', 10),
    minBuySol: parseFloat(process.env.ORDER_FLOW_MIN_BUY_SOL || '3'),
    minAbsorbRatio: parseFloat(process.env.ORDER_FLOW_MIN_ABSORB_RATIO || '0.25'),
    minBuySellRatio: parseFloat(process.env.ORDER_FLOW_MIN_BUY_SELL_RATIO || '1.25'),
    minImbalance: parseFloat(process.env.ORDER_FLOW_MIN_IMBALANCE || '0.15'),
    minUniqueBuyers: parseInt(process.env.ORDER_FLOW_MIN_UNIQUE_BUYERS || '2', 10),
    minReboundPct: parseFloat(process.env.ORDER_FLOW_MIN_REBOUND_PCT || '1.5'),
    maxReboundPct: parseFloat(process.env.ORDER_FLOW_MAX_REBOUND_PCT || '8'),
    minLowAgeMs: parseInt(process.env.ORDER_FLOW_MIN_LOW_AGE_MS || '300', 10),
    maxCandidateAgeMs: parseInt(process.env.ORDER_FLOW_MAX_CANDIDATE_AGE_MS || '8000', 10),
    cooldownMs: parseInt(process.env.ORDER_FLOW_COOLDOWN_MS || process.env.COOLDOWN_MS_PER_TOKEN || '60000', 10),
    maxEventsPerMint: parseInt(process.env.ORDER_FLOW_MAX_EVENTS_PER_MINT || '180', 10),
    debug: (process.env.ORDER_FLOW_DEBUG ?? 'false').toLowerCase() === 'true',
  },

  // ============ Price anomaly filter ============
  priceFilter: {
    // 单 tick 价格变化超过 maxJumpRatio 视为可疑
    // 1.5 表示 +50% 或 -33%（1/1.5）以上属于异常
    maxJumpRatio: parseFloat(process.env.PRICE_MAX_JUMP_RATIO || '1.5'),
    // 可疑样本必须在多少毫秒内连续出现并方向一致才接受
    confirmWindowMs: parseInt(process.env.PRICE_CONFIRM_WINDOW_MS || '3000', 10),
    confirmMinSamples: parseInt(process.env.PRICE_CONFIRM_MIN_SAMPLES || '2', 10),
  },

  // ============ Helius ============
  // v3.17: 支持多 region LaserStream + 多 region Sender
  //   - laserstreamEndpoints: 数组，多 region gRPC 订阅，最快的 region 命中即触发（signature 去重）
  //   - senderEndpoints:      数组，多 region Sender 并发提交，Promise.race 取最快返回
  //   - 向后兼容：未配 _ENDPOINTS 时回退到旧的单 endpoint 字段
  helius: {
    apiKey: process.env.HELIUS_API_KEY,
    rpcUrl: process.env.HELIUS_RPC_URL,
    stakedRpcUrl: process.env.HELIUS_STAKED_RPC_URL,

    // ---- LaserStream（多 region 订阅）----
    // 优先读 HELIUS_LASERSTREAM_ENDPOINTS（逗号分隔多个）
    // fallback 到旧的 HELIUS_LASERSTREAM_ENDPOINT（单 endpoint）
    laserstreamEndpoint: process.env.HELIUS_LASERSTREAM_ENDPOINT,
    laserstreamEndpoints: (() => {
      const multi = (process.env.HELIUS_LASERSTREAM_ENDPOINTS || '').trim();
      if (multi) {
        return multi.split(',').map((s) => s.trim()).filter(Boolean);
      }
      const single = (process.env.HELIUS_LASERSTREAM_ENDPOINT || '').trim();
      return single ? [single] : [];
    })(),
    laserstreamToken: process.env.HELIUS_LASERSTREAM_TOKEN,

    // ---- Sender（多 region 提交）----
    // 优先读 HELIUS_SENDER_ENDPOINTS（逗号分隔多个）
    // fallback 到旧的 HELIUS_SENDER_ENDPOINT
    senderEndpoint: process.env.HELIUS_SENDER_ENDPOINT || null,
    senderEndpoints: (() => {
      const multi = (process.env.HELIUS_SENDER_ENDPOINTS || '').trim();
      if (multi) {
        return multi.split(',').map((s) => s.trim()).filter(Boolean);
      }
      const single = (process.env.HELIUS_SENDER_ENDPOINT || '').trim();
      return single ? [single] : [];
    })(),
  },

  // ============ AllenHark ============
  // AllenHark 提供两项核心能力：
  //   1) Yellowstone gRPC 数据流 — 跟 Helius LaserStream 同协议，作为额外 region 降低尾延迟
  //   2) Slipstream 交易中继 — leader-proximity 路由，自动选最快 sender 提交 tx
  allenhark: {
    // ---- gRPC 数据流 ----
    // AllenHark gRPC 端点（IP 白名单制，无需 token）
    // 逗号分隔多个端点，格式同 LaserStream
    // 示例: grpc.allenhark.com:10000
    grpcEndpoints: (() => {
      const raw = (process.env.ALLENHARK_GRPC_ENDPOINTS || '').trim();
      if (!raw) return [];
      return raw.split(',').map((s) => s.trim()).filter(Boolean);
    })(),
    // AllenHark gRPC 的 x-token（如果需要的话，目前官方说是 IP 白名单不需要）
    grpcToken: process.env.ALLENHARK_GRPC_TOKEN || '',

    // ---- Slipstream 交易中继 ----
    // API key (sk_live_*)，从 AllenHark Console 获取
    slipstreamApiKey: process.env.ALLENHARK_SLIPSTREAM_API_KEY || '',
    // 首选 region: us-east, eu-central, ap-northeast 等
    slipstreamRegion: process.env.ALLENHARK_SLIPSTREAM_REGION || '',
    // 是否启用 Slipstream 作为 BUY 提交通道
    // true 时 BUY 会走 Slipstream (leader-proximity routing)，失败再 fallback Helius Sender
    slipstreamEnabled: (process.env.ALLENHARK_SLIPSTREAM_ENABLED ?? 'false').toLowerCase() === 'true',
    // Slipstream 优先级 fee 速度: SLOW, FAST, ULTRA_FAST
    slipstreamFeeSpeed: process.env.ALLENHARK_SLIPSTREAM_FEE_SPEED || 'ULTRA_FAST',
    // Slipstream 最大 tip (SOL)，0 表示不限
    slipstreamMaxTipSol: parseFloat(process.env.ALLENHARK_SLIPSTREAM_MAX_TIP_SOL || '0'),
  },

  // ============ Birdeye ============
  birdeye: {
    apiKey: process.env.BIRDEYE_API_KEY,
    baseUrl: 'https://public-api.birdeye.so',
  },

  // ============ Wallet ============
  wallet: {
    privateKeyBs58: process.env.WALLET_PRIVATE_KEY_BS58,
  },

  // ============ Programs ============
  programs: {
    pumpAmm: 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
    pumpAmmV2: 'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1',
    tokenProgram: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    associatedTokenProgram: 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
    systemProgram: '11111111111111111111111111111111',
    wsol: 'So11111111111111111111111111111111111111112',
  },

  // ============ Server ============
  server: {
    port: parseInt(process.env.DASHBOARD_PORT || '3001', 10),
    bindHost: process.env.BIND_HOST || '0.0.0.0',
    webhookSecret: process.env.WEBHOOK_SECRET || null,
    dashboardToken: process.env.DASHBOARD_TOKEN || null,
  },

  // ============ Storage ============
  storage: {
    dbPath: './data/sniper.db',
    reportsDir: './reports',
    logsDir: './logs',
  },

  // ============ Priority fees ============
  // BUY 和 SELL 分开配置：
  //   - BUY 是抢 slot 的（砸盘后所有 sniper 同抢），需要高 fee
  //   - SELL 是平仓的（晚 1-3 个 slot 落链没差别），低 fee 即可
  // 实战竞争者数据(BABYTROLL slot):
  //   排名1 93kgxYKe: priority fee 0.037 SOL,CU 111K → μL/CU 334M
  //   排名2 3fZftz6m: priority fee 0.012 SOL,CU 110K → μL/CU 113M
  //   我们 v3.17.7: fee 0.01,CU 163K → μL/CU 61M(排名4)
  //   核心:Leader 排序看 priority fee / CU,不看 Jito tip
  priorityFee: {
    // 静态模式（dynamic=false 时使用）
    // v3.17.20: 用户调整 BUY/SELL fee 范围 (BUY 0.001-0.009, SELL 0.0001-0.0003)
    buyMaxLamports: parseInt(process.env.BUY_MAX_PRIORITY_FEE_LAMPORTS || '9000000', 10),  // 0.009 SOL
    sellMaxLamports: parseInt(process.env.SELL_MAX_PRIORITY_FEE_LAMPORTS || '300000', 10),  // 0.0003 SOL

    // 动态模式：用 Helius getPriorityFeeEstimate 查 mempool 实时拥堵
    // 砸盘事件中整网 fee 飙升，动态调整能跟上竞争者节奏
    dynamic: (process.env.PRIORITY_FEE_DYNAMIC ?? 'true').toLowerCase() === 'true',

    // 动态模式参数
    // BUY 用 high (75th) 或 veryHigh (95th)，SELL 用 medium (50th)
    buyLevel: process.env.BUY_PRIORITY_LEVEL || 'veryHigh',  // 抢入用最高级别
    sellLevel: process.env.SELL_PRIORITY_LEVEL || 'medium',  // 卖出用中等

    // 动态模式下限
    // v3.17.20: 用户压低成本设置 — 注意 BUY μL/CU 会从 267M 降到 36M (CU 250K, fee 0.009 上限)
    //   如果出现 BUY_CHAIN_FAILED 增多,先把 BUY_CAP 调到 0.02 SOL 看是否恢复
    buyMinLamports: parseInt(process.env.BUY_MIN_PRIORITY_FEE_LAMPORTS || '1000000', 10),  // 0.001 SOL
    sellMinLamports: parseInt(process.env.SELL_MIN_PRIORITY_FEE_LAMPORTS || '100000', 10),  // 0.0001 SOL

    // 动态查询的上限 (即使 mempool 极拥堵也不超过)
    // v3.17.20: 用户调整,激进压成本
    buyCapLamports: parseInt(process.env.BUY_CAP_PRIORITY_FEE_LAMPORTS || '9000000', 10),   // 0.009 SOL
    sellCapLamports: parseInt(process.env.SELL_CAP_PRIORITY_FEE_LAMPORTS || '300000', 10),  // 0.0003 SOL
  },

  // 旧字段保留，向后兼容（仅用于 fallback）
  maxPriorityFeeLamports: parseInt(process.env.MAX_PRIORITY_FEE_LAMPORTS || '5000000', 10), // 0.005 SOL

  // 启动时是否自动尝试补充缺失的 pool 信息（PoolFinder）
  autoFillPoolsOnStart: (process.env.AUTO_FILL_POOLS_ON_START ?? 'true').toLowerCase() === 'true',
};

function validateConfig() {
  const errors = [];
  if (!config.helius.apiKey) errors.push('HELIUS_API_KEY missing');
  if (!config.helius.rpcUrl) errors.push('HELIUS_RPC_URL missing');
  // v3.17: laserstreamEndpoints 数组非空（旧 _ENDPOINT 也会被收进数组）
  if (!config.helius.laserstreamEndpoints || config.helius.laserstreamEndpoints.length === 0) {
    errors.push('HELIUS_LASERSTREAM_ENDPOINT (or HELIUS_LASERSTREAM_ENDPOINTS) missing');
  }
  if (!config.helius.laserstreamToken) errors.push('HELIUS_LASERSTREAM_TOKEN missing');
  if (!config.birdeye.apiKey) errors.push('BIRDEYE_API_KEY missing');
  if (!config.DRY_RUN && !config.wallet.privateKeyBs58) {
    errors.push('WALLET_PRIVATE_KEY_BS58 required for LIVE mode');
  }
  return errors;
}

module.exports = { config, validateConfig };
