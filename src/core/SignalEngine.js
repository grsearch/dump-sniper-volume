'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
// SignalEngine 只在收到 dump 信号时 beat，没信号时不会心跳。砸盘信号本来就稀疏，
// 阈值 600s（10min）会经常误报。改 1h；如果 1h 没有任何砸盘信号才算异常。
monitor.registerModule('SignalEngine', { staleMs: 3600_000, label: 'Signal Engine' });

/**
 * SignalEngine
 * ============
 * 接收 DumpDetector 的 dumpSignal，应用：
 *   - 自触发过滤（不买自己刚卖出的）
 *   - 同代币冷却（cooldownMsPerToken）
 *   - 同砸单去重（v3.17.6: 同一 seller_tx 在 sellerTxDedupMs 内不重复触发）
 *   - 全局并发限制（maxConcurrentPositions）
 *
 * 通过后发出 buyOrder 事件给 Executor。
 *
 * v3.17.6 同砸单去重：
 *   实战发现:LaserStream 多 region 订阅时，同一笔砸单交易可能跨越 region 推送时间差，
 *   在 dedup TTL 过期后（5min）被某个慢 region 重新推过来。这会让冷却期失效:
 *   - mint 冷却是 60s，到 5+分钟时已经过了
 *   - 同一砸单的 LaserStream 重推 → mint 冷却通过 → 触发第二次 BUY
 *   - 但这时价格已经跌了 20%，根本不是反弹窗口 → 亏
 *
 *   修复思路:在 SignalEngine 层加 seller_tx 去重，同砸单 N 分钟内不重复触发。
 *   持久化:同时记录到 SQLite signals 表(已有 seller_tx 字段)。启动时从 DB
 *           恢复最近 N 分钟内 accepted=1 的 seller_tx 进内存，重启不丢。
 */
class SignalEngine extends EventEmitter {
  constructor({ tradeLogger, positionManager, tickStream = null, dumpDetector = null, rsiCalculator = null, tokenRegistry = null, emaService = null }) {
    super();
    this.tradeLogger = tradeLogger;
    this.positionManager = positionManager;
    // v3.17.7: 可选 tickStream 引用，用于读 latestSlot 做信号过期判断
    //   不传也能工作（fallback：不做过期检查）
    this.tickStream = tickStream;

    this.dumpDetector = dumpDetector;
    // v3.17.17: 可选 RSI 计算器,用于"反弹起点"过滤
    this.rsiCalculator = rsiCalculator;
    // v3.26: tokenRegistry — 用于新币策略（按 token age 区分策略）
    this.tokenRegistry = tokenRegistry;
    this.lastTriggerTs = new Map();    // mint → ts
    this.ourSignatures = new Set();    // 我们自己发出的 tx 签名（避免自触发）
    this.inflightBuys = new Set();     // 正在 buy 但还没 registerOpen 的 mint（防并发超额）
    // v3.17.6: 已经触发过买入的砸单 tx → expireAt
    this.triggeredSellerTxs = new Map();
    // v3.17.7: 已经触发过买入的 (seller wallet × mint) → expireAt
    //   防"同一卖家持续出货"反复触发买入（不同 seller_tx 但同一钱包同一币）
    //   实战案例：ikG8tz5e 18 秒内对 POSITIONS 砸了 2 次，2 次都被买入，2 次都亏
    this.triggeredSellerMintPairs = new Map();
    // v3.24: 同币卖出后冷却 — 避免短时间重复买入同一币
    this._exitCooldowns = new Map(); // mint → cooldownExpireAt

    // v3.17.15: 同卖家短期累计卖出追踪
    //   同一卖家可能拆分多笔 tx 砸盘，每笔 < MIN_SELL_SOL 但合计 > MIN_SELL_SOL
    //   这说明是拆分砸盘，应拒绝买入
    this.sellerRecentSells = new Map(); // seller:mint → [{ sellSol, ts }] (unused, kept for compat)

    // v3.17.40: 长窗口价格采样 — 独立于 RsiCalculator 的轻量采样器
    //   每分钟采样一次，保留 35 分钟，用于 RECENT_PUMP_LONG 检查
    this._longPriceSamples = new Map(); // mint → [{ ts, price }, ...]
    this._longPriceSampleIntervalMs = parseInt(process.env.LONG_PRICE_SAMPLE_INTERVAL_MS || '60000', 10);
    this._longPriceSampleMaxAgeMs = parseInt(process.env.LONG_PRICE_SAMPLE_MAX_AGE_MS || '2100000', 10); // 35min
    this._latestSlotFromSlotUpdate = 0; // v3.17.41: SlotSub 独立 slot（不被 LS/SS tx 污染）
    this._latestLsSlot = 0; // v3.17.41: LS-only slot（不被 SS 污染），laggyReconnect 用

    // 启动时从 DB 恢复最近的 accepted seller_tx，防止重启后 LaserStream 重推同砸单
    this._restoreSellerTxsFromDb();
    this._restorePriceSamplesFromDb();

    // 后台定期清理过期项（避免内存泄漏；setTimeout 也有但 Map 用一个统一清理更可靠）
    this._cleanupInterval = setInterval(() => this._cleanupExpired(), 60_000);
    if (this._cleanupInterval.unref) this._cleanupInterval.unref();
  }

  shutdown() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
  }

  _restoreSellerTxsFromDb() {
    const dedupMs = config.strategy.sellerTxDedupMs;
    try {
      const rows = this.tradeLogger.getRecentAcceptedSellerTxs(dedupMs);
      const now = Date.now();
      let restored = 0;
      for (const row of rows) {
        if (!row.seller_tx) continue;
        const expireAt = row.ts + dedupMs;
        if (expireAt > now) {
          this.triggeredSellerTxs.set(row.seller_tx, expireAt);
          restored += 1;
        }
      }
      if (restored > 0) {
        console.log(
          `[SignalEngine] restored ${restored} triggered seller_tx from DB ` +
            `(within last ${Math.round(dedupMs / 60_000)}min, dedup window)`,
        );
        monitor.set('SignalEngine.sellerTxRestored', restored, 'SignalEngine');
      }
    } catch (err) {
      monitor.recordError('SignalEngine', err, { phase: 'restoreSellerTxs' });
      console.warn(`[SignalEngine] failed to restore seller_tx dedup: ${err.message}`);
    }
  }

  _cleanupExpired() {
    const now = Date.now();
    let cleaned = 0;
    for (const [sig, expireAt] of this.triggeredSellerTxs) {
      if (expireAt <= now) {
        this.triggeredSellerTxs.delete(sig);
        cleaned += 1;
      }
    }
    // v3.17.7: 同样清理 sellerMintPairs
    for (const [key, expireAt] of this.triggeredSellerMintPairs) {
      if (expireAt <= now) {
        this.triggeredSellerMintPairs.delete(key);
        cleaned += 1;
      }
    }
    // v3.17.15: 清理过期的卖家累计卖出
    for (const [key, sells] of this.sellerRecentSells) {
      const cutoff = Date.now() - 30_000;
      while (sells.length > 0 && sells[0].ts < cutoff) sells.shift();
      if (sells.length === 0) this.sellerRecentSells.delete(key);
    }
    // v3.17.40: 清理过期价格采样
    this._cleanupLongPriceSamples();

    if (cleaned > 0) {
      monitor.set('SignalEngine.sellerTxsTracked', this.triggeredSellerTxs.size, 'SignalEngine');
      monitor.set('SignalEngine.sellerMintPairsTracked', this.triggeredSellerMintPairs.size, 'SignalEngine');
    }
  }

  /**
   * 由 main 在调用 executor.buy 前后通知 SignalEngine。
   * 这样 openPositionCount + inflightBuys 一起算"占用槽位"。
   */
  markBuyInflight(mint) {
    this.inflightBuys.add(mint);
  }
  markBuyDone(mint) {
    this.inflightBuys.delete(mint);
  }

  registerOurSignature(sig) {
    if (!sig) return;
    this.ourSignatures.add(sig);
    setTimeout(() => this.ourSignatures.delete(sig), 5 * 60_000);
  }

  async handleDumpSignal(signal) {
    monitor.beat('SignalEngine', 'signal');
    // v3.17.16: 记录信号到达 SignalEngine 的时间,用于事后分析 emit→BUY 延迟
    const _signalReceivedAt = Date.now();
    const { mint, symbol, sellSol, priceImpactPct, seller, signature, ts, slot } = signal;


    // 1. 自触发过滤
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    // 2. v3.17.15: 协调出货检测 — 同 slot（相同 GAS）3+ 卖家砸盘
    //    必须同时满足：3+ 不同卖家 + 同一个 slot（相同 GAS）+ 合计 > MIN_SELL_SOL
    //    只有同 slot 才能认定是协调出货（Jito bundle 打包），跨 slot 的不算
    const _sellers = signal._sellers;
    if (_sellers && _sellers.length >= 999 && signal._aggregated) {
      const minSellSol = config.strategy.minSellSol;
      if (sellSol > minSellSol) {
        monitor.inc('SignalEngine.rejectedCoordinatedDump', 1, 'SignalEngine');
        this._logReject(
          signal,
          `COORDINATED_DUMP: ${_sellers.length} sellers in same slot (same GAS), total ${sellSol.toFixed(1)} SOL — coordinated exit`,
        );
        return;
      }
    }

    // 3. (SELLER_SPLIT_DUMP removed v3.17.15)
    //    同卖家多笔大单 = 大砸盘，应该买入不是拒绝
    const minSellSol = config.strategy.minSellSol;

    // 4. slot 过期检查 — 砸盘 slot 太老就丢弃
    //    根因：LaserStream 多 region 仍然可能对某些代币推送延迟 48-88 秒
    //    （127+ slot），那时候反弹早结束，买在山顶 → emergency_stop 出场
    //    例：POSITIONS 监测到 9 笔信号，3 笔慢的 slot gap 121-214（延迟 48-88s），全亏
    //    设 maxSignalSlotGap=0 可禁用此检查（fallback 旧行为）
    // v3.17.41: SlotSub 熔断 + 冷却期
    const slotGapCooldownSec = 15; // v3.17.41: 180→15s, SlotSub 熔断已保护
    const maxSlotGap = config.strategy.maxSignalSlotGap;
    const uptimeSec = process.uptime();
    if (maxSlotGap > 0 && slot && this.tickStream && uptimeSec > slotGapCooldownSec) {
      // v3.17.41→v3.27: 用 SlotSub 独立 slot 算 gap，SlotSub 断连时 fallback 到 SS slot
      // SS (ShredStream) 是独立 UDP 数据源，不受 LS gRPC 断连影响
      // SS median 领先 35s (87 slots)，slot gap 正常值 ~87-170，MAX_SIGNAL_SLOT_GAP=200
      // v3.32: 用 _latestSlot（含SS更新），不用 _latestSlotFromSlotUpdate（仅LS，太慢）
      //   SS比LS快21s+，用LS slot算gap会误杀所有SS源信号
      let refSlot = this.tickStream ? (this.tickStream._latestSlot || 0) : 0;
      let refSource = 'latestSlot';
      if (refSlot === 0) {
        refSlot = this.tickStream ? (this.tickStream._latestSlotFromSlotUpdate || 0) : 0;
        refSource = 'ss';
      }
      if (refSlot === 0) {
        // 两个源都没数据 → 跳过 gap 检查（不熔断，避免误杀所有信号）
        monitor.inc('SignalEngine.skippedSlotGap_noSlotSub', 1, 'SignalEngine');
      } else {
      const slotGap = refSlot - slot;
      if (slotGap > maxSlotGap) {
        monitor.inc('SignalEngine.rejectedSlotGapTooLarge', 1, 'SignalEngine');
        this._logReject(
          signal,
          `slot gap too large: dump@${slot}, ${refSource}@${refSlot}, gap=${slotGap} (>${maxSlotGap}, ~${(slotGap * 0.4).toFixed(0)}s late)`,
        );
        return;
      }
      } // end else (refSlot > 0)
    } else if (maxSlotGap > 0 && uptimeSec <= slotGapCooldownSec && slot && this.tickStream) {
      // 启动冷却期内跳过 slot gap 检查
      let refSlot = this.tickStream ? (this.tickStream._latestSlot || 0) : 0;
      if (refSlot === 0) refSlot = this.tickStream ? (this.tickStream._latestSlotFromSlotUpdate || 0) : 0;
      if (refSlot > 0) {
        const slotGap = refSlot - slot;
        if (slotGap > maxSlotGap) {
          monitor.inc('SignalEngine.skippedSlotGap_cooldown', 1, 'SignalEngine');
        }
      }
    }

    // v3.32b: push lag 检查 — 墙钟差 > 阈值 → 信号太旧拒绝
    //   EMA策略同一bug的修复方案：不用 slotGap×400ms（SS天然领先87-170 slots会误杀），
    //   直接用 Date.now() - signal.ts 计算真实墙钟延迟
    const maxPushLagMs = config.strategy.maxPushLagMs || 0;
    if (maxPushLagMs > 0 && signal.ts) {
      const pushLagMs = Date.now() - signal.ts;
      if (pushLagMs > maxPushLagMs) {
        monitor.inc('SignalEngine.rejectedPushLag', 1, 'SignalEngine');
        this._logReject(signal, `push lag too large: ${pushLagMs}ms > ${maxPushLagMs}ms (signal stale)`);
        return;
      }
    }

    // 3. v3.17.6: 同砸单去重 — 同一 seller_tx 在 sellerTxDedupMs 内不重复触发
    //    防止 LaserStream 多 region 跨越 dedup TTL 后重推同一砸单
    if (signature && this.triggeredSellerTxs.has(signature)) {
      const expireAt = this.triggeredSellerTxs.get(signature);
      if (expireAt > Date.now()) {
        monitor.inc('SignalEngine.rejectedDuplicateSellerTx', 1, 'SignalEngine');
        this._logReject(signal, `duplicate seller_tx (already triggered, expires in ${Math.round((expireAt - Date.now()) / 1000)}s)`);
        return;
      }
      this.triggeredSellerTxs.delete(signature);
    }

    // 4. v3.17.7: 同卖家×同mint去重 — 防"持续出货"场景反复触发
    //    实战案例：同一卖家 ikG8tz5e 18 秒内对 POSITIONS 砸了 2 次
    //    （seller_tx 不同，但 seller wallet + mint 相同），2 次都被买入 2 次都亏
    //    这表明该卖家在持续出货,不是一次性恐慌抛售,买入反弹概率小
    //    设 sellerMintDedupMs=0 可禁用此检查
    if (seller && mint && config.strategy.sellerMintDedupMs > 0) {
      const key = `${seller}:${mint}`;
      const expireAt = this.triggeredSellerMintPairs.get(key);
      if (expireAt && expireAt > Date.now()) {
        monitor.inc('SignalEngine.rejectedSellerMintPair', 1, 'SignalEngine');
        this._logReject(
          signal,
          `same seller+mint cooldown (seller ${seller.slice(0, 6)}.. dumped ${symbol || mint.slice(0, 6)} again, expires in ${Math.round((expireAt - Date.now()) / 1000)}s)`,
        );
        return;
      }
      if (expireAt) {
        this.triggeredSellerMintPairs.delete(key);
      }
    }

    // 5. 冷却
    //    买入后冷却 + 卖出后冷却（防止同一根K线买卖）
    const triggerCooldownMs = signal._activityFlow ? 0 : config.strategy.cooldownMsPerToken;
    const last = this.lastTriggerTs.get(mint);
    if (triggerCooldownMs > 0 && last && Date.now() - last < triggerCooldownMs) {
      monitor.inc('SignalEngine.rejectedCooldown', 1, 'SignalEngine');
      this._logReject(signal, `cooldown (${Math.round((Date.now() - last) / 1000)}s ago)`);
      return;
    }

    // v3.17.40: 采样价格到长窗口缓存
    if (signal.priceAfter) {
      this._sampleLongPrice(mint, signal.priceAfter);
    }

    // Activity Flow entry uses TradingView-style RSI(7) on 1-minute candle closes.
    // Fail closed until enough bars exist so a restart cannot bypass the filter.
    if (signal._activityFlow && config.activityFlow.rsi1mEnabled) {
      const minBars = Math.max(config.activityFlow.rsi1mPeriod + 1, config.activityFlow.rsi1mMinBars);
      const snap = this.rsiCalculator ? this.rsiCalculator.snapshot(mint) : null;
      if (!snap || !Number.isFinite(snap.rsi1m) || snap.bucketCount1m < minBars) {
        monitor.inc('SignalEngine.rejectedRsi1mNotReady', 1, 'SignalEngine');
        this._logReject(
          signal,
          `RSI_1M_NOT_READY: bars=${snap?.bucketCount1m || 0}/${minBars}`,
        );
        return;
      }
      if (snap.rsi1m >= config.activityFlow.rsi1mMax) {
        monitor.inc('SignalEngine.rejectedRsi1mHigh', 1, 'SignalEngine');
        this._logReject(
          signal,
          `RSI_1M_HIGH: RSI(${config.activityFlow.rsi1mPeriod},1m)=` +
            `${snap.rsi1m.toFixed(1)} >= ${config.activityFlow.rsi1mMax}`,
        );
        return;
      }
      signal._rsi1m = snap.rsi1m;
    }

    // v3.17.38: 在任何过滤判断之前,先取"砸单前 RSI"
    let rsiPreDump = null;
    let rsi1sPreDump = null;
    let rsi30sPreDump = null;
    if (this.rsiCalculator) {
      const preSnap = this.rsiCalculator.getSnapshotBeforeLast(mint);
      // v3.17.42: 放宽条件 — 30s桶>=4(2min数据)即可，覆盖更多币种
      if (preSnap && preSnap.poolHealthy && (preSnap.bucketCount5s >= 8 || preSnap.bucketCount30s >= 4)) {
        rsiPreDump = preSnap.rsi5s;
        rsi1sPreDump = preSnap.rsi1s;
        rsi30sPreDump = preSnap.rsi30s;
      }
    }
    signal._rsiPreDump = rsiPreDump;
    signal._rsi1sPreDump = rsi1sPreDump;
    signal._rsi30sPreDump = rsi30sPreDump;

    // v3.17.39: 距近期高点跌幅过滤
    const minDropFromHighPct = config.strategy.minDropFromRecentHighPct;
    const lookbackSec = config.strategy.minDropLookbackSec || 1200;
    if (minDropFromHighPct > 0 && this.rsiCalculator) {
      const prices = this.rsiCalculator.getRecentPriceHistory(mint, lookbackSec, '5s');
      if (prices && prices.length > 0) {
        const recentHigh = Math.max(...prices);
        const dropFromHighPct = ((recentHigh - signal.priceAfter) / recentHigh) * 100;
        if (dropFromHighPct < minDropFromHighPct) {
          monitor.inc('SignalEngine.rejectedShallowDropFromHigh', 1, 'SignalEngine');
          this._logReject(signal, 'near-ATH dump: drop from high only ' + dropFromHighPct.toFixed(1) + '% < ' + minDropFromHighPct + '%');
          return;
        }
      }
    }

    // v3.17.30: 短窗口涨幅过滤（防秒级脉冲拉盘后接刀）
    //   实战案例: Backrooms 30s内从1.4e-6拉到2.0e-6(+42%), 砸单信号在拉盘顶部触发
    //   长窗口(30min)采样粒度太粗,根本看不到秒级脉冲
    //   用 RsiCalculator 的 1s 桶价格历史检测短窗口涨幅
    const recentPumpShortSec = config.strategy.recentPumpShortSec;
    const recentPumpShortMaxPct = config.strategy.recentPumpShortMaxPct;
    if (recentPumpShortSec > 0 && recentPumpShortMaxPct > 0 && this.rsiCalculator) {
      const prices = this.rsiCalculator.getRecentPriceHistory(mint, recentPumpShortSec, '1s');
      if (prices && prices.length >= 2) {
        const oldest = prices[0];
        const newest = prices[prices.length - 1];
        if (oldest > 0) {
          const shortPumpPct = ((newest - oldest) / oldest) * 100;
          if (shortPumpPct > recentPumpShortMaxPct) {
            monitor.inc('SignalEngine.rejectedRecentPumpShort', 1, 'SignalEngine');
            this._logReject(signal, 'short-window pump: ' + shortPumpPct.toFixed(1) + '% > ' + recentPumpShortMaxPct + '% (last ' + recentPumpShortSec + 's)');
            return;
          }
        }
      }
    }

    // v3.17.40: 长窗口涨幅过滤
    const recentPumpLongSec = config.strategy.recentPumpLongSec;
    const recentPumpLongMaxPct = config.strategy.recentPumpLongMaxPct;
    if (recentPumpLongSec > 0 && recentPumpLongMaxPct > 0) {
      const pumpPct = this._getLongPumpPct(mint, recentPumpLongSec * 1000);
      if (pumpPct !== null && pumpPct > recentPumpLongMaxPct) {
        monitor.inc('SignalEngine.rejectedRecentPumpLong', 1, 'SignalEngine');
        this._logReject(signal, 'long-window pump: ' + pumpPct.toFixed(1) + '% > ' + recentPumpLongMaxPct + '%');
        return;
      }
    }

    // v3.17.36: 连环抛过滤
    const minSellCount = config.strategy.minTriggerSellCount;
    const sellCount10s = signal._sellCount10s || 1;
    if (!signal._activityFlow && minSellCount > 0 && sellCount10s < minSellCount) {
      monitor.inc('SignalEngine.rejectedLowSellCount', 1, 'SignalEngine');
      this._logReject(signal, 'recent 10s sell count ' + sellCount10s + ' < ' + minSellCount);
      return;
    }

    // v3.18: 本地 FDV 估算 — 零 RPC
    const maxFdvUsd = parseFloat(process.env.SIGNAL_MAX_FDV_USD ?? '0');
    const minFdvUsd = parseFloat(process.env.SIGNAL_MIN_FDV_USD ?? '0');
    if ((maxFdvUsd > 0 || minFdvUsd > 0) && signal.priceAfter > 0 && signal.baseDecimals > 0) {
      const solPriceUsd = parseFloat(process.env.SOL_PRICE_USD || '170');
      // FDV = totalSupply * priceInSol * solPriceUsd
      //   safeTokenAmount 返回人类可读值，priceAfter = SOL/token
      //   Pump.fun 币 totalSupply = 1e9 tokens
      const fdvEstUsd = signal.priceAfter * 1e9 * solPriceUsd;
      console.log(`[SignalEngine] FDV check: ${signal.symbol} priceAfter=${signal.priceAfter?.toExponential(4)} fdvEst=$${(fdvEstUsd/1000).toFixed(1)}k maxFdv=$${(maxFdvUsd/1000).toFixed(0)}k solPrice=$${solPriceUsd}`);
      if (maxFdvUsd > 0 && fdvEstUsd > maxFdvUsd) {
        monitor.inc('SignalEngine.rejectedFdvTooHigh', 1, 'SignalEngine');
        this._logReject(signal, 'FDV estimate too high');
        return;
      }
      if (minFdvUsd > 0 && fdvEstUsd < minFdvUsd) {
        monitor.inc('SignalEngine.rejectedFdvTooLow', 1, 'SignalEngine');
        this._logReject(signal, 'FDV estimate too low');
        return;
      }
    }

    // v3.32d: 币龄上限过滤 — creation_time 已在 registry 缓存, 零 RPC
    const maxAgeH = parseFloat(process.env.MAX_MINT_AGE_HOURS || '0'); // 0=禁用
    if (maxAgeH > 0 && this.tokenRegistry) {
      const tokenInfo = this.tokenRegistry.getToken(mint);
      const ct = tokenInfo?.creation_time; // ms
      // ct 为 null(还没回填) → 不进 if, 自动放行(保速度, 不阻塞热路径等数据)
      if (ct && (Date.now() - ct) > maxAgeH * 3600 * 1000) {
        monitor.inc('SignalEngine.rejectedOldMint', 1, 'SignalEngine');
        this._logReject(signal, `mint age > ${maxAgeH}h`);
        return;
      }
    }

    // 6. 并发限制（同时计算已开仓 + 正在 buy 的）
    const openCount = this.positionManager.openPositionCount();
    const inflightCount = this.inflightBuys.size;
    const totalSlotsUsed = openCount + inflightCount;
    if (totalSlotsUsed >= config.strategy.maxConcurrentPositions) {
      monitor.inc('SignalEngine.rejectedMaxConcurrent', 1, 'SignalEngine');
      this._logReject(
        signal,
        `max concurrent (${openCount} open + ${inflightCount} inflight / ${config.strategy.maxConcurrentPositions})`,
      );
      return;
    }

    // 7. v3.17.20: 关闭加仓 — 同一代币已有持仓或正在买入时，新信号直接拒绝
    //    （用户需求：不再对同币加仓，避免单币过度集中 + 重复买在下跌途中）
    // v3.24: 同币卖出后冷却检查 — 避免短时间重复买入同一币
    {
      const rebuyCooldownMs = parseInt(process.env.REBUY_COOLDOWN_MS || '0', 10);
      const exitCooldown = rebuyCooldownMs > 0 ? this._exitCooldowns.get(mint) : 0;
      if (exitCooldown && Date.now() < exitCooldown) {
        monitor.inc('SignalEngine.rejectedRebuyCooldown', 1, 'SignalEngine');
        this._logReject(signal, 'REBUY_COOLDOWN: sold this mint recently, cooldown ' + Math.round((exitCooldown - Date.now()) / 1000) + 's remaining');
        return;
      }
    }

    if (this.inflightBuys.has(mint)) {
      monitor.inc('SignalEngine.rejectedInflightBuy', 1, 'SignalEngine');
      this._logReject(signal, 'buy in-flight (no add-on)');
      return;
    }
    const mintOpenCount = this.positionManager.openPositionCountByMint ? this.positionManager.openPositionCountByMint(mint) : (this.positionManager.hasOpenPosition(mint) ? 1 : 0);
    if (mintOpenCount >= 1) {
      const addon = this.positionManager.canAddOn(mint);
      if (addon.allowed) {
        monitor.inc('SignalEngine.addonAllowed', 1, 'SignalEngine');
      } else {
        monitor.inc('SignalEngine.rejectedAddonCondition', 1, 'SignalEngine');
        this._logReject(signal, 'add-on blocked: ' + addon.reason);
        return;
      }
    }

    // 8. 短窗口累计跌幅检查 — 防止买入 RUG
    //    单笔砸盘 impact 可能在 8-30% 范围内，但如果最近 10 秒内累计跌幅 > MAX_PRICE_IMPACT_PCT
    //    说明是连续砸盘（RUG），买入会亏
    // v3.17.41: CUMULATIVE_RUG DISABLED — 连续砸盘也允许反弹
    /*
    if (this.dumpDetector) {
      const stats = this.dumpDetector.getRecentDumpStats(mint);
      if (stats && stats.sellCount >= 3 && stats.cumImpactPct > config.strategy.maxPriceImpactPct) {
        monitor.inc('SignalEngine.rejectedCumulativeRug', 1, 'SignalEngine');
        this._logReject(
          signal,
          `CUMULATIVE_RUG: ${stats.sellCount} sells in 10s, cumImpact=${stats.cumImpactPct.toFixed(1)}% > ${config.strategy.maxPriceImpactPct}%, totalSell=${stats.totalSellSol.toFixed(1)} SOL`,
        );
        return;
      }
    }
    */

    // 9. v3.17.17 (revised v2): RSI 过滤 — 只保留 PEAK 兜底
    //    ⚠️ 经过实战分析,任何看「RSI 低位」的拒绝逻辑都会跟 sniper "+1 slot 抢入"
    //       策略冲突 — 砸盘瞬间 RSI 必然极低,这就是我们想抓的状态,
    //       没办法靠 RSI 数值区分「好砸盘」和「真 rug」(数据特征一样)。
    //    ✅ 唯一安全的过滤: RSI_PEAK
    //       RSI > 92 说明价格刚刚连涨 30+ 秒还没回调,这时来的「砸盘」
    //       通常是大户测试卖出 / 假突破回调,反弹空间很小,拒掉合理。
    //
    //    模式:
    //      off       — 完全跳过(默认,推荐)
    //      peak      — 只拒绝 rsi5s > RSI_PEAK_MAX(默认 92)
    //      slope     — 旧"反弹起点"逻辑 ⚠️ 跟 sniper 矛盾,不推荐
    const rsiMode = process.env.RSI_FILTER || 'off';
    if (this.rsiCalculator && rsiMode !== 'off') {
      const rsiMinPoolSol = parseFloat(process.env.RSI_MIN_POOL_SOL || '20');
      const snap = this.rsiCalculator.snapshot(mint, rsiMinPoolSol);

      if (snap && snap.poolHealthy && snap.bucketCount5s >= 8) {
        if (rsiMode === 'peak') {
          // 唯一推荐的兜底模式
          const peakMax = parseFloat(process.env.RSI_PEAK_MAX || '92');
          if (snap.rsi5s != null && snap.rsi5s > peakMax) {
            monitor.inc('SignalEngine.rejectedRsiPeak', 1, 'SignalEngine');
            this._logReject(signal,
              `RSI_PEAK: rsi5s=${snap.rsi5s.toFixed(1)} > ${peakMax} ` +
              `(price still pumping for 30+s, suspicious "dump", little rebound room)`);
            return;
          }
        } else if (rsiMode === 'slope') {
          // ⚠️ 不推荐 — 跟 sniper 矛盾,只为向后兼容保留
          if (snap.bucketCount1s >= 15) {
            const rsi5sMax = parseFloat(process.env.RSI_5S_MAX || '75');
            if (snap.rsi5s != null && snap.rsi5s > rsi5sMax) {
              monitor.inc('SignalEngine.rejectedRsiOverbought', 1, 'SignalEngine');
              this._logReject(signal, `RSI_OVERBOUGHT: rsi5s=${snap.rsi5s.toFixed(1)} > ${rsi5sMax}`);
              return;
            }
            const slopeMin = parseFloat(process.env.RSI_1S_SLOPE_MIN || '-5');
            if (snap.rsi1sSlope != null && snap.rsi1sSlope < slopeMin) {
              monitor.inc('SignalEngine.rejectedRsiStillFalling', 1, 'SignalEngine');
              this._logReject(signal,
                `RSI_STILL_FALLING: rsi1s=${snap.rsi1s?.toFixed(1)} slope=${snap.rsi1sSlope.toFixed(1)} < ${slopeMin}`);
              return;
            }
          }
        }
      } else if (snap && !snap.poolHealthy && process.env.RSI_DEBUG === 'true') {
        const rsiMinPoolSol = parseFloat(process.env.RSI_MIN_POOL_SOL || '20');
        console.log(`[SignalEngine] ⚠️ RSI skipped (pool too small: ${snap.lastPoolQuoteSol?.toFixed(0)} SOL < ${rsiMinPoolSol})`);
      }
    }

    // ============ v3.17.42: RSI(7,30s) oversold filter ============
    // 数据支撑(7天回测1143笔):
    //   RSI(7,30s) < 35: 净-22.6 SOL, 拒绝后少赚13.2 少亏35.8 净+22.6 SOL/7天
    //   RSI < 25 = 持续急跌，不要抄底
    //   RSI 35-40 = 最佳反弹区间
    //   Pump.fun币RSI低不是"超卖反弹"，而是"还在跌"
    {
      const rsi30sMin = parseFloat(process.env.RSI_30S_MIN || '0');
      if (rsi30sMin > 0 && this.rsiCalculator) {
        const snap = this.rsiCalculator.snapshot(mint, 20);
        // v3.17.42: debug — 记录RSI过滤判断
        if (process.env.RSI_DEBUG === 'true' || !snap || !snap.poolHealthy || snap.rsi30s == null || snap.bucketCount30s < 8) {
          const reason = !snap ? 'no_snap' : !snap.poolHealthy ? `pool=${snap.lastPoolQuoteSol?.toFixed(0)}<20` : snap.rsi30s == null ? 'rsi30s_null' : `buckets=${snap.bucketCount30s}<8`;
          if (snap) console.log(`[SignalEngine] RSI_30S skip ${symbol || mint.slice(0,6)}: ${reason} rsi30s=${snap.rsi30s?.toFixed(1)} rsi5s=${snap.rsi5s?.toFixed(1)}`);
          else console.log(`[SignalEngine] RSI_30S skip ${symbol || mint.slice(0,6)}: no snapshot`);
        }
        if (snap && snap.rsi30s != null && snap.bucketCount30s >= 7) {
          // v3.17.42: poolHealthy 不可靠(warmup只有feedTick没poolSol)
          //   如果该仓位有entryPoolSol，用那个判断；否则poolHealthy为null时也放行
          const poolOk = snap.poolHealthy || snap.lastPoolQuoteSol == null;
          if (poolOk && snap.rsi30s < rsi30sMin) {
            monitor.inc('SignalEngine.rejectedRsi30sOversold', 1, 'SignalEngine');
            this._logReject(signal,
              `RSI_30S_OVERSOLD: rsi30s=${snap.rsi30s.toFixed(1)} < ${rsi30sMin} ` +
              `(sustained downtrend, not a bounce entry; rsi5s=${snap.rsi5s?.toFixed(1)})`);
            return;
          }
        }
      }
    }

    // ============ v3.23: 砸盘深度过滤 ============
    // 砸盘深度 = (砸单前5min均价 - 买入价) / 均价 * 100
    // 数据支撑(7天回测): >50%深度 avgPnL -10%, 17笔深亏; 10-25%深度 WR 60-68%
    {
      const maxDumpDepthPct = parseFloat(process.env.MAX_DUMP_DEPTH_PCT || '0');
      const minDumpDepthPct = parseFloat(process.env.MIN_DUMP_DEPTH_PCT || '0');
      if (maxDumpDepthPct > 0 || minDumpDepthPct > 0) {
        const samples = this._longPriceSamples.get(mint);
        if (samples && samples.length >= 3) {
          // 取最近5分钟(300s)的采样
          const now5m = Date.now() - 300000;
          const recent = samples.filter(s => s.ts >= now5m && s.price > 0);
          if (recent.length >= 3) {
            const avgPrice = recent.reduce((a, s) => a + s.price, 0) / recent.length;
            const buyPrice = signal.priceAfter;
            if (avgPrice > 0 && buyPrice > 0) {
              const dumpDepth = (avgPrice - buyPrice) / avgPrice * 100;
              signal._dumpDepth = +dumpDepth.toFixed(2);
              if (maxDumpDepthPct > 0 && dumpDepth > maxDumpDepthPct) {
                monitor.inc('SignalEngine.rejectedDumpDepthTooDeep', 1, 'SignalEngine');
                this._logReject(signal,
                  'DUMP_DEPTH_TOO_DEEP: depth=' + dumpDepth.toFixed(1) + '% > ' + maxDumpDepthPct + '% (price dropped too much, rebound unlikely)');
                return;
              }
              if (minDumpDepthPct > 0 && dumpDepth < minDumpDepthPct) {
                monitor.inc('SignalEngine.rejectedDumpDepthTooShallow', 1, 'SignalEngine');
                this._logReject(signal,
                  'DUMP_DEPTH_TOO_SHALLOW: depth=' + dumpDepth.toFixed(1) + '% < ' + minDumpDepthPct + '% (not enough drop)');
                return;
              }
            }
          }
        }
        // 无采样数据时不阻止买入（采样覆盖率还在提升中）
      }
    }

    // ============ v3.26→v3.28: 入场前5min波动率过滤 ============
    // v3.28: 扩展为全局过滤（新币+老币），vol=null 也拒绝
    // 数据支撑(14天回测):
    //   vol=null: 42笔 SS死币 亏-21.60SOL (盲买=高风险)
    //   vol>=15%: 18笔 SS死币 亏-10.62SOL (高波动弹不回来)
    //   vol<15%: SS死币仅13笔 亏-3.10SOL
    //   过滤后总PnL: -71.89→-35.70 SOL (+36.19 SOL改善)
    //   新币PnL: -35.84→-9.07 SOL (+26.77 SOL改善)
    {
      const maxPreVol5m = parseFloat(process.env.MAX_PRE_VOL_5M_PCT || '0');
      if (maxPreVol5m > 0) {
        const samples = this._longPriceSamples.get(mint);
        if (samples && samples.length >= 3) {
          const now5m = Date.now() - 300000;
          const recent5m = samples.filter(s => s.ts >= now5m && s.price > 0);
          if (recent5m.length >= 3) {
            const prices = recent5m.map(s => s.price);
            const high = Math.max(...prices);
            const low = Math.min(...prices);
            const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
            if (avg > 0) {
              const vol5m = (high - low) / avg * 100;
              signal._preVol5m = +vol5m.toFixed(2);
              if (vol5m >= maxPreVol5m) {
                monitor.inc('SignalEngine.rejectedHighVol', 1, 'SignalEngine');
                this._logReject(signal,
                  'HIGH_VOL_5M: vol5m=' + vol5m.toFixed(1) + '% >= ' + maxPreVol5m + '% (high volatility, catching falling knife)');
                return;
              }
            }
          }
        }
        // v3.28→v3.32: vol=null 不再拒绝（新加入监控的币首笔信号无价格历史是正常的）
        // 保留 _preVol5m 标记供日志参考
        if (signal._preVol5m == null) {
          signal._preVol5m = null; // explicit
          // monitor.inc('SignalEngine.rejectedNoVolData', 1, 'SignalEngine');
          // this._logReject(signal,
          //   'NO_VOL_DATA: vol5m=null (no price history, blind buy risk)');
          // return;
        }
      }
    }

    // ============ v3.27: 新币(<24h) vol 过滤 — 已合并到全局 v3.28 ============
    // v3.28 将 vol=null 和 vol>=15% 过滤提升为全局（新币+老币），
    // 不再需要单独的新币 vol 过滤。NEW_COIN_MAX_VOL_PCT 保留但不再生效。

    // ============ v3.24: 趋势过滤 — 5分钟跌+1分钟跌时跳过买入 ============
    // 数据支撑(7天): 5m跌+1m跌 WR=45%, PF=0.07, 深亏35%; 跳过后PF从0.44→0.69
    {
      const trendFilterEnabled = process.env.TREND_FILTER_ENABLED === '1';
      if (trendFilterEnabled) {
        const samples = this._longPriceSamples.get(mint);
        if (samples && samples.length >= 5) {
          const now5m = Date.now() - 300000;
          const recent5m = samples.filter(s => s.ts >= now5m && s.price > 0);
          if (recent5m.length >= 5) {
            // 5分钟趋势：首尾价格变化
            const trend5m = recent5m[0].price > 0 ? (recent5m[recent5m.length-1].price - recent5m[0].price) / recent5m[0].price * 100 : 0;
            // 1分钟趋势
            const now1m = Date.now() - 60000;
            const recent1m = samples.filter(s => s.ts >= now1m && s.price > 0);
            let trend1m = 0;
            if (recent1m.length >= 3 && recent1m[0].price > 0) {
              trend1m = (recent1m[recent1m.length-1].price - recent1m[0].price) / recent1m[0].price * 100;
            }
            // v3.25: 分段趋势过滤 — 只拦重度接飞刀，轻度回调放行
            // 数据: 5m跌-1~-10%的9笔拦截中95%后续反弹赚钱(误杀)
            //       5m跌<-20%的24笔才是真正接飞刀
            // 规则: 5m跌<-20% + 1m跌>-10% → 拦(重度暴跌)
            //       5m跌>-20% → 放行(轻度/中度回调, 多数反弹)
            if (trend5m < -20 && trend1m < -10) {
              monitor.inc('SignalEngine.rejectedDownTrend', 1, 'SignalEngine');
              this._logReject(signal,
                'DOWN_TREND: 5m=' + trend5m.toFixed(1) + '% 1m=' + trend1m.toFixed(1) + '% (severe drop, catching falling knife)');
              return;
            }
          }
        }
        // 无采样数据时不阻止买入
      }
    }

    // ============ v3.27: 老币(>=24h) pool 过滤 ============
    // 竞对数据: 老币 pool>=100 + impact>=5% PF=7.93, 是最优策略
    // 我们的池子太小(30 SOL)的老币亏损严重, peak才3-4%涨不动
    {
      const oldCoinMinPoolSol = parseFloat(process.env.OLD_COIN_MIN_POOL_SOL || '0');
      if (oldCoinMinPoolSol > 0 && this.tokenRegistry) {
        const tokenInfo = this.tokenRegistry.getToken(mint);
        if (tokenInfo && tokenInfo.added_at) {
          const tokenAgeMs = Date.now() - tokenInfo.added_at;
          const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '0');
          if (newCoinThresholdMs > 0 && tokenAgeMs >= newCoinThresholdMs) {
            // 老币: pool 太小的不买
            const poolSol = signal.poolQuoteSol || 0;
            if (poolSol > 0 && poolSol < oldCoinMinPoolSol) {
              monitor.inc('SignalEngine.rejectedOldCoinLowPool', 1, 'SignalEngine');
              this._logReject(signal,
                'OLD_COIN_LOW_POOL: pool=' + poolSol.toFixed(0) + 'SOL < ' + oldCoinMinPoolSol + 'SOL (old coin >=24h, low liquidity skip)');
              return;
            }
          }
        }
      }
    }

    // ============ 通过 → 触发买入 ============
    monitor.inc('SignalEngine.signalsAccepted', 1, 'SignalEngine');
    this.inflightBuys.add(mint);  // v3.23: 在emit前就标记，防并发同币双买
    this.lastTriggerTs.set(mint, Date.now());

    // v3.17.6: 记录此 seller_tx，避免后续 N 分钟内被同一砸单二次触发
    if (signature) {
      const dedupMs = config.strategy.sellerTxDedupMs;
      this.triggeredSellerTxs.set(signature, Date.now() + dedupMs);
      monitor.set('SignalEngine.sellerTxsTracked', this.triggeredSellerTxs.size, 'SignalEngine');
    }
    // v3.17.7: 记录此 seller+mint pair
    if (seller && mint && config.strategy.sellerMintDedupMs > 0) {
      const key = `${seller}:${mint}`;
      this.triggeredSellerMintPairs.set(key, Date.now() + config.strategy.sellerMintDedupMs);
      monitor.set('SignalEngine.sellerMintPairsTracked', this.triggeredSellerMintPairs.size, 'SignalEngine');
    }

    // v3.17.7: 日志带上 slot 和 slot gap（用于事后分析延迟分布）
    const latestSlot = this.tickStream ? (this.tickStream.latestSlot || 0) : 0;
    const slotGap = (slot && latestSlot) ? (latestSlot - slot) : null;
    const flow = signal._flow || null;
    const activityReason = signal._activityFlow && flow
      ? `activity_flow_1m: ${flow.s60.tradeCount}tx/${flow.s60.volumeSol.toFixed(2)}SOL ` +
        `buy=${flow.s60.buySol.toFixed(2)} sell=${flow.s60.sellSol.toFixed(2)} ` +
        `r=${flow.s60.buySellRatio.toFixed(2)} ` +
        `rsi1m=${Number.isFinite(signal._rsi1m) ? signal._rsi1m.toFixed(1) : 'n/a'}`
      : null;

    // v3.10: 先 emit buyOrder（让 Executor 立即开始工作），再异步写 DB
    // SQLite WAL 模式下写入也要 1-3ms，省下来给关键路径
    this.emit('buyOrder', {
      ...signal,
      reason: activityReason || `dump: sell ${sellSol.toFixed(2)} SOL, impact -${priceImpactPct.toFixed(2)}%`,
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt,
      rsiPreDump: signal._rsiPreDump,
      rsi1sPreDump: signal._rsi1sPreDump,
      rsi30sPreDump: signal._rsi30sPreDump,
      rsi1m: signal._rsi1m,
      preVol5m: signal._preVol5m,
      dumpDepth: signal._dumpDepth,
    });

    // v3.17.16: 监控 signal 到 emit buyOrder 的耗时(应该 < 5ms)
    const inSignalEngineMs = Date.now() - _signalReceivedAt;
    monitor.set('SignalEngine.lastInEngineMs', inSignalEngineMs, 'SignalEngine');
    if (inSignalEngineMs > 20) {
      console.warn(`[SignalEngine] ⚠️ slow path: ${inSignalEngineMs}ms in handleDumpSignal for ${symbol || mint.slice(0,6)}`);
    }

    if (activityReason) {
      console.log(
        `[SignalEngine] BUY_SIGNAL ${symbol || mint.slice(0, 6)}: ${activityReason}` +
          (slotGap !== null ? `, slot_gap=${slotGap}` : ''),
      );
    } else {
      console.log(
        `[SignalEngine] ✅ BUY_SIGNAL ${symbol || mint.slice(0, 6)}: sell=${sellSol.toFixed(
          2,
        )} SOL, impact=-${priceImpactPct.toFixed(2)}%, seller=${seller ? seller.slice(0, 6) + '..' : 'n/a'}, ` +
          `seller_tx=${signature ? signature.slice(0, 8) + '..' : 'n/a'}` +
          (slotGap !== null ? `, slot_gap=${slotGap}` : ''),
      );
    }

    // 异步写 DB（不阻塞 BUY 路径）
    // 写入时 accepted=1 + seller_tx，启动时 _restoreSellerTxsFromDb 就靠这个恢复
    setImmediate(() => {
      try {
        this.tradeLogger.logSignal({
          ts,
          mint,
          symbol,
          kind: 'BUY_SIGNAL',
          sellSol,
          priceImpactPct,
          seller,
          sellerTx: signature,
          notes: (activityReason || `accepted; sellSol=${sellSol.toFixed(2)}, impact=${priceImpactPct.toFixed(2)}%`) +
                 (slotGap !== null ? `, slot_gap=${slotGap}` : ''),
          accepted: true,
        });
      } catch (err) {
        monitor.recordError('SignalEngine', err, { phase: 'logSignal_async' });
      }
    });
  }

  //   出场：EMA9下穿EMA20清仓 / 20%止盈 / 10%激活3%回撤移动止盈
  //   加仓：首仓价跌>=15% 允许加仓1次，独立止盈
  //   无止损、无其他过滤
  async _handleEmaStrategy(signal, _signalReceivedAt) {
    const { mint, symbol, sellSol, priceImpactPct, seller, signature, ts, slot } = signal;

    // v3.30: 在最开头标记 inflight — 防止两个 async 信号同时通过后续检查
    if (this.inflightBuys.has(mint)) {
      monitor.inc('SignalEngine.rejectedInflightBuy', 1, 'SignalEngine');
      this._logReject(signal, 'buy in-flight (EMA)');
      return;
    }
    this.inflightBuys.add(mint);
    console.log(`[SignalEngine] 🔒 inflightBuys.add(${symbol || mint.slice(0,6)}) sig=${signature?.slice(0,12)}.. sellSol=${sellSol?.toFixed(1)} slot=${slot} inflight=[${[...this.inflightBuys].join(',')}]`);
    try {

    // 1. 自触发过滤（保留）
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    // 2. SLOT 延迟过滤 — 信号太旧就拒绝，避免高位接盘
    //    默认 100 slots = ~40s，可通过 EMA_MAX_SLOT_LAG 覆盖
    const latestSlot = this.tickStream ? (this.tickStream.latestSlot || 0) : 0;
    const slotGap = (slot && latestSlot) ? (latestSlot - slot) : null;
    const emaMaxSlotLag = parseInt(process.env.EMA_MAX_SLOT_LAG || '100', 10);
    if (slotGap !== null && slotGap > emaMaxSlotLag) {
      monitor.inc('SignalEngine.rejectedEmaSlotLag', 1, 'SignalEngine');
      this._logReject(signal, `EMA:slot_lag:${slotGap}>${emaMaxSlotLag} (~${Math.round(slotGap * 0.4)}s late)`);
      return;
    }

    // 3. 同币冷却 — 避免短时间内重复买入同一币
    //    默认 60 秒，可通过 EMA_COOLDOWN_MS 覆盖
    const emaCooldownMs = parseInt(process.env.EMA_COOLDOWN_MS || '60000', 10);
    if (emaCooldownMs > 0 && this.positionManager) {
      const recentClosed = this.positionManager.listRecentlyClosed(mint, emaCooldownMs);
      if (recentClosed.length > 0) {
        monitor.inc('SignalEngine.rejectedEmaCooldown', 1, 'SignalEngine');
        this._logReject(signal, `EMA:cooldown:${recentClosed.length} recent exits in ${emaCooldownMs / 1000}s`);
        return;
      }
    }

    // 3.5 DB级别同币持仓检查 — 防止多实例或内存不一致导致同币多仓
    //    EMA_MAX_ADDONS=0 时不允许任何同币重复买入
    if (this.positionManager && this.tradeLogger) {
      const emaMaxAddOns = parseInt(process.env.EMA_MAX_ADDONS || '0');
      if (emaMaxAddOns <= 0) {
        try {
          const openCount = this.tradeLogger.db.prepare(
            'SELECT COUNT(*) as cnt FROM positions WHERE mint = ? AND status IN (?, ?, ?, ?)'
          ).get(mint, 'open', 'sell_pending', 'sell_confirming', 'stuck');
          if (openCount && openCount.cnt > 0) {
            monitor.inc('SignalEngine.rejectedEmaDuplicate', 1, 'SignalEngine');
            this._logReject(signal, `EMA:has_open_position:${openCount.cnt}`);
            return;
          }
        } catch (_) {}
      }
    }

    // 4. 砸单金额 >= 6 SOL
    const emaMinSellSol = parseFloat(process.env.EMA_MIN_SELL_SOL || '6');
    if (sellSol < emaMinSellSol) {
      monitor.inc('SignalEngine.rejectedSellSol', 1, 'SignalEngine');
      this._logReject(signal, `EMA:size:${sellSol.toFixed(1)}<${emaMinSellSol}`);
      return;
    }

    // 5. 跌幅 >= 8%
    const emaMinImpact = parseFloat(process.env.EMA_MIN_IMPACT_PCT || '8');
    if (priceImpactPct < emaMinImpact) {
      monitor.inc('SignalEngine.rejectedImpact', 1, 'SignalEngine');
      this._logReject(signal, `EMA:impact:${priceImpactPct.toFixed(1)}%<${emaMinImpact}%`);
      return;
    }

    // 6. EMA 状态检查（v2: 同步查内存，零延迟）

    // 7. 加仓逻辑
    const openPositions = this.positionManager ? this.positionManager.listOpen() : [];
    const existingPos = openPositions.find(p => p.mint === mint);

    if (existingPos) {
      // 已有仓位 — 检查是否可以加仓
      const emaAddOnDropPct = parseFloat(process.env.EMA_ADDON_DROP_PCT || '15');
      const emaMaxAddOns = parseInt(process.env.EMA_MAX_ADDONS || '1');
      const currentAddonCount = openPositions.filter(p => p.mint === mint && p.isAddOn).length;
      
      if (currentAddonCount >= emaMaxAddOns) {
        monitor.inc('SignalEngine.rejectedAddonMax', 1, 'SignalEngine');
        this._logReject(signal, `EMA:addon:max_addons`);
        return;
      }

      // 用 signal 的 priceAfter 作为当前价格，和首仓 entry_price 比较
      const dropFromEntry = ((existingPos.entry_price - signal.priceAfter) / existingPos.entry_price) * 100;
      if (dropFromEntry < emaAddOnDropPct) {
        monitor.inc('SignalEngine.rejectedAddonDrop', 1, 'SignalEngine');
        this._logReject(signal, `EMA:addon:drop:${dropFromEntry.toFixed(1)}%<${emaAddOnDropPct}%`);
        return;
      }

      // 加仓！标记为 addon
      signal._isAddOn = true;
    }

    // ======== 通过所有检查，emit 买入信号 ========
    this.emit('buyOrder', {
      ...signal,
      reason: `EMA:dump ${sellSol.toFixed(2)} SOL, impact -${priceImpactPct.toFixed(2)}%`,
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt,
      _isAddOn: signal._isAddOn || false,
      slotGap,
    });

    // 写入 DB
    if (this.tradeLogger) {
      this.tradeLogger.logSignal({
        ts, mint, symbol, sell_sol: sellSol, price_impact_pct: priceImpactPct,
        seller, seller_tx: signature, kind: 'DUMP_DETECTED',
        accepted: 1, reject_reason: null,
      });
    }

    } finally {
      // reject 路径：inflightBuys 在方法最开头已 add，如果 reject 了需要清除
      // accept 路径：markBuyDone 在 index.js buyOrder handler 的 finally 里清除
      //   但如果 accept 后 inflight 还在（还没 markBuyDone），说明正在买
      //   所以只在 openPositions 里没有同币时才清除（说明没买成功）
      const openPositions = this.positionManager ? this.positionManager.listOpen() : [];
      const hasOpenPos = openPositions.some(p => p.mint === mint);
      if (!hasOpenPos) {
        this.inflightBuys.delete(mint);
      }
    }
  }

  _logReject(signal, reason) {
    this.tradeLogger.logSignal({
      ts: signal.ts,
      mint: signal.mint,
      symbol: signal.symbol,
      kind: signal._activityFlow ? 'ACTIVITY_FLOW' : 'DUMP_DETECTED',
      sellSol: signal.sellSol,
      priceImpactPct: signal.priceImpactPct,
      seller: signal.seller,
      sellerTx: signal.signature,
      notes: 'detected but rejected',
      accepted: false,
      rejectReason: reason,
    });
    console.log(
      `[SignalEngine] ⏭  rejected ${signal.symbol || signal.mint.slice(0, 6)}: ${reason}`,
    );
  }

  // v3.17.40: 长窗口价格采样
  _sampleLongPrice(mint, price) {
    if (!Number.isFinite(price) || price <= 0) return;
    const now = Date.now();
    let samples = this._longPriceSamples.get(mint);
    if (!samples) {
      samples = [];
      this._longPriceSamples.set(mint, samples);
    }
    // v3.17.41-fix: 价格跳变检测 — 50x ratio 清旧数据
    if (samples.length > 0) {
      const lastPrice = samples[samples.length - 1].price;
      if (lastPrice > 0) {
        const ratio = price / lastPrice;
        if (ratio > 50 || ratio < 0.02) {
          samples.length = 0;
        }
      }
    }
    if (samples.length > 0 && (now - samples[samples.length - 1].ts) < this._longPriceSampleIntervalMs) return;
    samples.push({ ts: now, price });
    // v3.17.41: 持久化到 DB
    if (this.tradeLogger) {
      this.tradeLogger.savePriceSample(mint, now, price);
    }
  }

  _cleanupLongPriceSamples() {
    const cutoff = Date.now() - this._longPriceSampleMaxAgeMs;
    for (const [mint, samples] of this._longPriceSamples) {
      while (samples.length > 0 && samples[0].ts < cutoff) samples.shift();
      if (samples.length === 0) this._longPriceSamples.delete(mint);
    }
    // v3.17.41: 同步清理 DB
    if (this.tradeLogger) {
      try { this.tradeLogger.cleanOldPriceSamples(Date.now() - this._longPriceSampleMaxAgeMs); } catch (_) {}
    }
  }

  _getLongPumpPct(mint, lookbackMs) {
    const samples = this._longPriceSamples.get(mint);
    if (!samples || samples.length < 2) return null;
    const cutoff = Date.now() - lookbackMs;
    const recent = samples.filter(s => s.ts >= cutoff);
    if (recent.length < 2) return null;
    // v3.17.41-fix: 50x ratio guard against price jumps
    for (let i = 1; i < recent.length; i++) {
      if (recent[i].price > 0 && recent[i-1].price > 0) {
        const ratio = recent[i].price / recent[i-1].price;
        if (ratio > 50 || ratio < 0.02) return null; // jump detected, can't trust
      }
    }
    const oldest = recent[0].price;
    const newest = recent[recent.length - 1].price;
    if (oldest <= 0) return null;
    return ((newest - oldest) / oldest) * 100;
  }

  // v3.17.41: Restore price samples from DB on startup
  _restorePriceSamplesFromDb() {
    if (!this.tradeLogger) return;
    try {
      const map = this.tradeLogger.loadRecentPriceSamples(this._longPriceSampleMaxAgeMs);
      let restored = 0;
      for (const [mint, rawSamples] of map) {
        // Find last jump point, only keep samples after it
        let lastJumpIdx = -1;
        for (let i = 1; i < rawSamples.length; i++) {
          if (rawSamples[i-1].price > 0 && rawSamples[i].price > 0) {
            const ratio = rawSamples[i].price / rawSamples[i-1].price;
            if (ratio > 50 || ratio < 0.02) {
              lastJumpIdx = i;
            }
          }
        }
        const samples = lastJumpIdx >= 0 ? rawSamples.slice(lastJumpIdx) : rawSamples;
        if (samples.length > 0) {
          this._longPriceSamples.set(mint, samples);
          restored += samples.length;
        }
      }
      if (restored > 0) {
        console.log(`[SignalEngine] restored ${restored} price samples from DB`);
      }
    } catch (err) {
      console.warn(`[SignalEngine] failed to restore price samples: ${err.message}`);
    }
  }

}

module.exports = SignalEngine;
