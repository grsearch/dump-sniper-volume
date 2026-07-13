'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
// SignalEngine ???? dump ??? beat????????????????????
// ?? 600s?10min???????? 1h??? 1h ?????????????
monitor.registerModule('SignalEngine', { staleMs: 3600_000, label: 'Signal Engine' });

/**
 * SignalEngine
 * ============
 * ?? DumpDetector ? dumpSignal????
 *   - ???????????????
 *   - ??????cooldownMsPerToken?
 *   - ??????v3.17.6: ?? seller_tx ? sellerTxDedupMs ???????
 *   - ???????maxConcurrentPositions?
 *
 * ????? buyOrder ??? Executor?
 *
 * v3.17.6 ??????
 *   ????:LaserStream ? region ??????????????? region ??????
 *   ? dedup TTL ????5min????? region ??????????????:
 *   - mint ??? 60s?? 5+???????
 *   - ????? LaserStream ?? ? mint ???? ? ????? BUY
 *   - ????????? 20%????????? ? ?
 *
 *   ????:? SignalEngine ?? seller_tx ?????? N ?????????
 *   ???:????? SQLite signals ?(?? seller_tx ??)????? DB
 *           ???? N ??? accepted=1 ? seller_tx ?????????
 */
class SignalEngine extends EventEmitter {
  constructor({ tradeLogger, positionManager, tickStream = null, dumpDetector = null, rsiCalculator = null, tokenRegistry = null, emaService = null }) {
    super();
    this.tradeLogger = tradeLogger;
    this.positionManager = positionManager;
    // v3.17.7: ?? tickStream ?????? latestSlot ???????
    //   ???????fallback????????
    this.tickStream = tickStream;

    this.dumpDetector = dumpDetector;
    // v3.17.17: ?? RSI ???,??"????"??
    this.rsiCalculator = rsiCalculator;
    // v3.26: tokenRegistry ? ???????? token age ?????
    this.tokenRegistry = tokenRegistry;
    this.lastTriggerTs = new Map();    // mint ? ts
    this.ourSignatures = new Set();    // ??????? tx ?????????
    this.inflightBuys = new Set();     // ?? buy ??? registerOpen ? mint???????
    // v3.17.6: ?????????? tx ? expireAt
    this.triggeredSellerTxs = new Map();
    // v3.17.7: ???????? (seller wallet ? mint) ? expireAt
    //   ?"????????"????????? seller_tx ?????????
    //   ?????ikG8tz5e 18 ??? POSITIONS ?? 2 ??2 ??????2 ???
    this.triggeredSellerMintPairs = new Map();
    // v3.24: ??????? ? ????????????
    this._exitCooldowns = new Map(); // mint ? cooldownExpireAt

    // v3.17.15: ???????????
    //   ?????????? tx ????? < MIN_SELL_SOL ??? > MIN_SELL_SOL
    //   ??????????????
    this.sellerRecentSells = new Map(); // seller:mint ? [{ sellSol, ts }] (unused, kept for compat)

    // v3.17.40: ??????? ? ??? RsiCalculator ??????
    //   ?????????? 35 ????? RECENT_PUMP_LONG ??
    this._longPriceSamples = new Map(); // mint ? [{ ts, price }, ...]
    this._longPriceSampleIntervalMs = parseInt(process.env.LONG_PRICE_SAMPLE_INTERVAL_MS || '60000', 10);
    this._longPriceSampleMaxAgeMs = parseInt(process.env.LONG_PRICE_SAMPLE_MAX_AGE_MS || '2100000', 10); // 35min
    this._latestSlotFromSlotUpdate = 0; // v3.17.41: SlotSub ?? slot??? LS/SS tx ???
    this._latestLsSlot = 0; // v3.17.41: LS-only slot??? SS ????laggyReconnect ?

    // ???? DB ????? accepted seller_tx?????? LaserStream ?????
    this._restoreSellerTxsFromDb();
    this._restorePriceSamplesFromDb();

    // ?????????????????setTimeout ??? Map ???????????
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
    // v3.17.7: ???? sellerMintPairs
    for (const [key, expireAt] of this.triggeredSellerMintPairs) {
      if (expireAt <= now) {
        this.triggeredSellerMintPairs.delete(key);
        cleaned += 1;
      }
    }
    // v3.17.15: ???????????
    for (const [key, sells] of this.sellerRecentSells) {
      const cutoff = Date.now() - 30_000;
      while (sells.length > 0 && sells[0].ts < cutoff) sells.shift();
      if (sells.length === 0) this.sellerRecentSells.delete(key);
    }
    // v3.17.40: ????????
    this._cleanupLongPriceSamples();

    if (cleaned > 0) {
      monitor.set('SignalEngine.sellerTxsTracked', this.triggeredSellerTxs.size, 'SignalEngine');
      monitor.set('SignalEngine.sellerMintPairsTracked', this.triggeredSellerMintPairs.size, 'SignalEngine');
    }
  }

  /**
   * ? main ??? executor.buy ???? SignalEngine?
   * ?? openPositionCount + inflightBuys ???"????"?
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
    // v3.17.16: ?????? SignalEngine ???,?????? emit?BUY ??
    const _signalReceivedAt = Date.now();
    const { mint, symbol, sellSol, priceImpactPct, seller, signature, ts, slot } = signal;


    // 1. ?????
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    // 2. v3.17.15: ?????? ? ? slot??? GAS?3+ ????
    //    ???????3+ ???? + ??? slot??? GAS?+ ?? > MIN_SELL_SOL
    //    ??? slot ??????????Jito bundle ????? slot ???
    const _sellers = signal._sellers;
    if (_sellers && _sellers.length >= 999 && signal._aggregated) {
      const minSellSol = config.strategy.minSellSol;
      if (sellSol > minSellSol) {
        monitor.inc('SignalEngine.rejectedCoordinatedDump', 1, 'SignalEngine');
        this._logReject(
          signal,
          `COORDINATED_DUMP: ${_sellers.length} sellers in same slot (same GAS), total ${sellSol.toFixed(1)} SOL ? coordinated exit`,
        );
        return;
      }
    }

    // 3. (SELLER_SPLIT_DUMP removed v3.17.15)
    //    ??????? = ????????????
    const minSellSol = config.strategy.minSellSol;

    // 4. slot ???? ? ?? slot ?????
    //    ???LaserStream ? region ????????????? 48-88 ?
    //    ?127+ slot??????????????? ? emergency_stop ??
    //    ??POSITIONS ??? 9 ????3 ??? slot gap 121-214??? 48-88s????
    //    ? maxSignalSlotGap=0 ???????fallback ????
    // v3.17.41: SlotSub ?? + ???
    const slotGapCooldownSec = 15; // v3.17.41: 180?15s, SlotSub ?????
    const maxSlotGap = config.strategy.maxSignalSlotGap;
    const uptimeSec = process.uptime();
    if (maxSlotGap > 0 && slot && this.tickStream && uptimeSec > slotGapCooldownSec) {
      // v3.17.41?v3.27: ? SlotSub ?? slot ? gap?SlotSub ??? fallback ? SS slot
      // SS (ShredStream) ??? UDP ?????? LS gRPC ????
      // SS median ?? 35s (87 slots)?slot gap ??? ~87-170?MAX_SIGNAL_SLOT_GAP=200
      // v3.32: ? _latestSlot??SS?????? _latestSlotFromSlotUpdate??LS????
      //   SS?LS?21s+??LS slot?gap?????SS???
      let refSlot = this.tickStream ? (this.tickStream._latestSlot || 0) : 0;
      let refSource = 'latestSlot';
      if (refSlot === 0) {
        refSlot = this.tickStream ? (this.tickStream._latestSlotFromSlotUpdate || 0) : 0;
        refSource = 'ss';
      }
      if (refSlot === 0) {
        // ??????? ? ?? gap ????????????????
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
      // ???????? slot gap ??
      let refSlot = this.tickStream ? (this.tickStream._latestSlot || 0) : 0;
      if (refSlot === 0) refSlot = this.tickStream ? (this.tickStream._latestSlotFromSlotUpdate || 0) : 0;
      if (refSlot > 0) {
        const slotGap = refSlot - slot;
        if (slotGap > maxSlotGap) {
          monitor.inc('SignalEngine.skippedSlotGap_cooldown', 1, 'SignalEngine');
        }
      }
    }

    // v3.32b: push lag ?? ? ??? > ?? ? ??????
    //   EMA????bug???????? slotGap?400ms?SS????87-170 slots?????
    //   ??? Date.now() - signal.ts ????????
    const maxPushLagMs = config.strategy.maxPushLagMs || 0;
    if (maxPushLagMs > 0 && signal.ts) {
      const pushLagMs = Date.now() - signal.ts;
      if (pushLagMs > maxPushLagMs) {
        monitor.inc('SignalEngine.rejectedPushLag', 1, 'SignalEngine');
        this._logReject(signal, `push lag too large: ${pushLagMs}ms > ${maxPushLagMs}ms (signal stale)`);
        return;
      }
    }

    // 3. v3.17.6: ????? ? ?? seller_tx ? sellerTxDedupMs ??????
    //    ?? LaserStream ? region ?? dedup TTL ???????
    if (signature && this.triggeredSellerTxs.has(signature)) {
      const expireAt = this.triggeredSellerTxs.get(signature);
      if (expireAt > Date.now()) {
        monitor.inc('SignalEngine.rejectedDuplicateSellerTx', 1, 'SignalEngine');
        this._logReject(signal, `duplicate seller_tx (already triggered, expires in ${Math.round((expireAt - Date.now()) / 1000)}s)`);
        return;
      }
      this.triggeredSellerTxs.delete(signature);
    }

    // 4. v3.17.7: ?????mint?? ? ?"????"??????
    //    ????????? ikG8tz5e 18 ??? POSITIONS ?? 2 ?
    //    ?seller_tx ???? seller wallet + mint ????2 ????? 2 ???
    //    ???????????,?????????,???????
    //    ? sellerMintDedupMs=0 ??????
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

    // 5. ??
    //    ????? + ???????????K????
    const last = this.lastTriggerTs.get(mint);
    if (last && Date.now() - last < config.strategy.cooldownMsPerToken) {
      monitor.inc('SignalEngine.rejectedCooldown', 1, 'SignalEngine');
      this._logReject(signal, `cooldown (${Math.round((Date.now() - last) / 1000)}s ago)`);
      return;
    }

    // v3.17.40: ??????????
    if (signal.priceAfter) {
      this._sampleLongPrice(mint, signal.priceAfter);
    }

    // v3.17.38: ?????????,??"??? RSI"
    let rsiPreDump = null;
    let rsi1sPreDump = null;
    let rsi30sPreDump = null;
    if (this.rsiCalculator) {
      const preSnap = this.rsiCalculator.getSnapshotBeforeLast(mint);
      // v3.17.42: ???? ? 30s?>=4(2min??)?????????
      if (preSnap && preSnap.poolHealthy && (preSnap.bucketCount5s >= 8 || preSnap.bucketCount30s >= 4)) {
        rsiPreDump = preSnap.rsi5s;
        rsi1sPreDump = preSnap.rsi1s;
        rsi30sPreDump = preSnap.rsi30s;
      }
    }
    signal._rsiPreDump = rsiPreDump;
    signal._rsi1sPreDump = rsi1sPreDump;
    signal._rsi30sPreDump = rsi30sPreDump;

    // v3.17.39: ?????????
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

    // v3.17.30: ???????????????????
    //   ????: Backrooms 30s??1.4e-6??2.0e-6(+42%), ???????????
    //   ???(30min)??????,?????????
    //   ? RsiCalculator ? 1s ????????????
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

    // v3.17.40: ???????
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

    // v3.17.36: ?????
    const minSellCount = config.strategy.minTriggerSellCount;
    const sellCount10s = signal._sellCount10s || 1;
    if (minSellCount > 0 && sellCount10s < minSellCount) {
      monitor.inc('SignalEngine.rejectedLowSellCount', 1, 'SignalEngine');
      this._logReject(signal, 'recent 10s sell count ' + sellCount10s + ' < ' + minSellCount);
      return;
    }

    // v3.18: ?? FDV ?? ? ? RPC
    const maxFdvUsd = parseFloat(process.env.SIGNAL_MAX_FDV_USD ?? '0');
    const minFdvUsd = parseFloat(process.env.SIGNAL_MIN_FDV_USD ?? '0');
    if ((maxFdvUsd > 0 || minFdvUsd > 0) && signal.priceAfter > 0 && signal.baseDecimals > 0) {
      const solPriceUsd = parseFloat(process.env.SOL_PRICE_USD || '170');
      // FDV = totalSupply * priceInSol * solPriceUsd
      //   safeTokenAmount ????????priceAfter = SOL/token
      //   Pump.fun ? totalSupply = 1e9 tokens
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

    // v3.32d: ?????? ? creation_time ?? registry ??, ? RPC
    const maxAgeH = parseFloat(process.env.MAX_MINT_AGE_HOURS || '0'); // 0=??
    if (maxAgeH > 0 && this.tokenRegistry) {
      const tokenInfo = this.tokenRegistry.getToken(mint);
      const ct = tokenInfo?.creation_time; // ms
      // ct ? null(????) ? ?? if, ????(???, ?????????)
      if (ct && (Date.now() - ct) > maxAgeH * 3600 * 1000) {
        monitor.inc('SignalEngine.rejectedOldMint', 1, 'SignalEngine');
        this._logReject(signal, `mint age > ${maxAgeH}h`);
        return;
      }
    }

    // 6. ???????????? + ?? buy ??
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

    // 7. v3.17.20: ???? ? ??????????????????????
    //    ?????????????????????? + ?????????
    // v3.24: ????????? ? ????????????
    {
      const rebuyCooldownMs = parseInt(process.env.REBUY_COOLDOWN_MS || '300000', 10); // default 5min
      const exitCooldown = this._exitCooldowns.get(mint);
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

    // 8. ????????? ? ???? RUG
    //    ???? impact ??? 8-30% ????????? 10 ?????? > MAX_PRICE_IMPACT_PCT
    //    ????????RUG??????
    // v3.17.41: CUMULATIVE_RUG DISABLED ? ?????????
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

    // 9. v3.17.17 (revised v2): RSI ?? ? ??? PEAK ??
    //    ?? ??????,????RSI ??????????? sniper "+1 slot ??"
    //       ???? ? ???? RSI ????,??????????,
    //       ???? RSI ???????????? rug?(??????)?
    //    ? ???????: RSI_PEAK
    //       RSI > 92 ???????? 30+ ?????,????????
    //       ????????? / ?????,??????,?????
    //
    //    ??:
    //      off       ? ????(??,??)
    //      peak      ? ??? rsi5s > RSI_PEAK_MAX(?? 92)
    //      slope     ? ?"????"?? ?? ? sniper ??,???
    const rsiMode = process.env.RSI_FILTER || 'off';
    if (this.rsiCalculator && rsiMode !== 'off') {
      const rsiMinPoolSol = parseFloat(process.env.RSI_MIN_POOL_SOL || '20');
      const snap = this.rsiCalculator.snapshot(mint, rsiMinPoolSol);

      if (snap && snap.poolHealthy && snap.bucketCount5s >= 8) {
        if (rsiMode === 'peak') {
          // ?????????
          const peakMax = parseFloat(process.env.RSI_PEAK_MAX || '92');
          if (snap.rsi5s != null && snap.rsi5s > peakMax) {
            monitor.inc('SignalEngine.rejectedRsiPeak', 1, 'SignalEngine');
            this._logReject(signal,
              `RSI_PEAK: rsi5s=${snap.rsi5s.toFixed(1)} > ${peakMax} ` +
              `(price still pumping for 30+s, suspicious "dump", little rebound room)`);
            return;
          }
        } else if (rsiMode === 'slope') {
          // ?? ??? ? ? sniper ??,????????
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
        console.log(`[SignalEngine] ?? RSI skipped (pool too small: ${snap.lastPoolQuoteSol?.toFixed(0)} SOL < ${rsiMinPoolSol})`);
      }
    }

    // ============ v3.17.42: RSI(7,30s) oversold filter ============
    // ????(7???1143?):
    //   RSI(7,30s) < 35: ?-22.6 SOL, ?????13.2 ??35.8 ?+22.6 SOL/7?
    //   RSI < 25 = ?????????
    //   RSI 35-40 = ??????
    //   Pump.fun?RSI???"????"???"???"
    {
      const rsi30sMin = parseFloat(process.env.RSI_30S_MIN || '0');
      if (rsi30sMin > 0 && this.rsiCalculator) {
        const snap = this.rsiCalculator.snapshot(mint, 20);
        // v3.17.42: debug ? ??RSI????
        if (process.env.RSI_DEBUG === 'true' || !snap || !snap.poolHealthy || snap.rsi30s == null || snap.bucketCount30s < 8) {
          const reason = !snap ? 'no_snap' : !snap.poolHealthy ? `pool=${snap.lastPoolQuoteSol?.toFixed(0)}<20` : snap.rsi30s == null ? 'rsi30s_null' : `buckets=${snap.bucketCount30s}<8`;
          if (snap) console.log(`[SignalEngine] RSI_30S skip ${symbol || mint.slice(0,6)}: ${reason} rsi30s=${snap.rsi30s?.toFixed(1)} rsi5s=${snap.rsi5s?.toFixed(1)}`);
          else console.log(`[SignalEngine] RSI_30S skip ${symbol || mint.slice(0,6)}: no snapshot`);
        }
        if (snap && snap.rsi30s != null && snap.bucketCount30s >= 7) {
          // v3.17.42: poolHealthy ???(warmup??feedTick?poolSol)
          //   ??????entryPoolSol?????????poolHealthy?null????
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

    // ============ v3.23: ?????? ============
    // ???? = (???5min?? - ???) / ?? * 100
    // ????(7???): >50%?? avgPnL -10%, 17???; 10-25%?? WR 60-68%
    {
      const maxDumpDepthPct = parseFloat(process.env.MAX_DUMP_DEPTH_PCT || '0');
      const minDumpDepthPct = parseFloat(process.env.MIN_DUMP_DEPTH_PCT || '0');
      if (maxDumpDepthPct > 0 || minDumpDepthPct > 0) {
        const samples = this._longPriceSamples.get(mint);
        if (samples && samples.length >= 3) {
          // ???5??(300s)???
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
        // ???????????????????????
      }
    }

    // ============ v3.26?v3.28: ???5min????? ============
    // v3.28: ??????????+????vol=null ???
    // ????(14???):
    //   vol=null: 42? SS?? ?-21.60SOL (??=???)
    //   vol>=15%: 18? SS?? ?-10.62SOL (???????)
    //   vol<15%: SS???13? ?-3.10SOL
    //   ????PnL: -71.89?-35.70 SOL (+36.19 SOL??)
    //   ??PnL: -35.84?-9.07 SOL (+26.77 SOL??)
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
        // v3.28?v3.32: vol=null ??????????????????????????
        // ?? _preVol5m ???????
        if (signal._preVol5m == null) {
          signal._preVol5m = null; // explicit
          // monitor.inc('SignalEngine.rejectedNoVolData', 1, 'SignalEngine');
          // this._logReject(signal,
          //   'NO_VOL_DATA: vol5m=null (no price history, blind buy risk)');
          // return;
        }
      }
    }

    // ============ v3.27: ??(<24h) vol ?? ? ?????? v3.28 ============
    // v3.28 ? vol=null ? vol>=15% ??????????+????
    // ????????? vol ???NEW_COIN_MAX_VOL_PCT ????????

    // ============ v3.24: ???? ? 5???+1???????? ============
    // ????(7?): 5m?+1m? WR=45%, PF=0.07, ??35%; ???PF?0.44?0.69
    {
      const trendFilterEnabled = process.env.TREND_FILTER_ENABLED === '1';
      if (trendFilterEnabled) {
        const samples = this._longPriceSamples.get(mint);
        if (samples && samples.length >= 5) {
          const now5m = Date.now() - 300000;
          const recent5m = samples.filter(s => s.ts >= now5m && s.price > 0);
          if (recent5m.length >= 5) {
            // 5???????????
            const trend5m = recent5m[0].price > 0 ? (recent5m[recent5m.length-1].price - recent5m[0].price) / recent5m[0].price * 100 : 0;
            // 1????
            const now1m = Date.now() - 60000;
            const recent1m = samples.filter(s => s.ts >= now1m && s.price > 0);
            let trend1m = 0;
            if (recent1m.length >= 3 && recent1m[0].price > 0) {
              trend1m = (recent1m[recent1m.length-1].price - recent1m[0].price) / recent1m[0].price * 100;
            }
            // v3.25: ?????? ? ??????????????
            // ??: 5m?-1~-10%?9????95%??????(??)
            //       5m?<-20%?24????????
            // ??: 5m?<-20% + 1m?>-10% ? ?(????)
            //       5m?>-20% ? ??(??/????, ????)
            if (trend5m < -20 && trend1m < -10) {
              monitor.inc('SignalEngine.rejectedDownTrend', 1, 'SignalEngine');
              this._logReject(signal,
                'DOWN_TREND: 5m=' + trend5m.toFixed(1) + '% 1m=' + trend1m.toFixed(1) + '% (severe drop, catching falling knife)');
              return;
            }
          }
        }
        // ???????????
      }
    }

    // ============ v3.27: ??(>=24h) pool ?? ============
    // ????: ?? pool>=100 + impact>=5% PF=7.93, ?????
    // ???????(30 SOL)???????, peak?3-4%???
    {
      const oldCoinMinPoolSol = parseFloat(process.env.OLD_COIN_MIN_POOL_SOL || '0');
      if (oldCoinMinPoolSol > 0 && this.tokenRegistry) {
        const tokenInfo = this.tokenRegistry.getToken(mint);
        if (tokenInfo && tokenInfo.added_at) {
          const tokenAgeMs = Date.now() - tokenInfo.added_at;
          const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');
          if (tokenAgeMs >= newCoinThresholdMs) {
            // ??: pool ?????
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

    // ============ ?? ? ???? ============
    monitor.inc('SignalEngine.signalsAccepted', 1, 'SignalEngine');
    this.inflightBuys.add(mint);  // v3.23: ?emit????????????
    this.lastTriggerTs.set(mint, Date.now());

    // v3.17.6: ??? seller_tx????? N ????????????
    if (signature) {
      const dedupMs = config.strategy.sellerTxDedupMs;
      this.triggeredSellerTxs.set(signature, Date.now() + dedupMs);
      monitor.set('SignalEngine.sellerTxsTracked', this.triggeredSellerTxs.size, 'SignalEngine');
    }
    // v3.17.7: ??? seller+mint pair
    if (seller && mint && config.strategy.sellerMintDedupMs > 0) {
      const key = `${seller}:${mint}`;
      this.triggeredSellerMintPairs.set(key, Date.now() + config.strategy.sellerMintDedupMs);
      monitor.set('SignalEngine.sellerMintPairsTracked', this.triggeredSellerMintPairs.size, 'SignalEngine');
    }

    // v3.17.7: ???? slot ? slot gap????????????
    const latestSlot = this.tickStream ? (this.tickStream.latestSlot || 0) : 0;
    const slotGap = (slot && latestSlot) ? (latestSlot - slot) : null;

    // v3.10: ? emit buyOrder?? Executor ???????????? DB
    // SQLite WAL ??????? 1-3ms?????????
    this.emit('buyOrder', {
      ...signal,
      reason: `dump: sell ${sellSol.toFixed(2)} SOL, impact -${priceImpactPct.toFixed(2)}%`,
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt,
      rsiPreDump: signal._rsiPreDump,
      rsi1sPreDump: signal._rsi1sPreDump,
      rsi30sPreDump: signal._rsi30sPreDump,
      preVol5m: signal._preVol5m,
      dumpDepth: signal._dumpDepth,
    });

    // v3.17.16: ?? signal ? emit buyOrder ???(?? < 5ms)
    const inSignalEngineMs = Date.now() - _signalReceivedAt;
    monitor.set('SignalEngine.lastInEngineMs', inSignalEngineMs, 'SignalEngine');
    if (inSignalEngineMs > 20) {
      console.warn(`[SignalEngine] ?? slow path: ${inSignalEngineMs}ms in handleDumpSignal for ${symbol || mint.slice(0,6)}`);
    }

    console.log(
      `[SignalEngine] ? BUY_SIGNAL ${symbol || mint.slice(0, 6)}: sell=${sellSol.toFixed(
        2,
      )} SOL, impact=-${priceImpactPct.toFixed(2)}%, seller=${seller ? seller.slice(0, 6) + '..' : 'n/a'}, ` +
        `seller_tx=${signature ? signature.slice(0, 8) + '..' : 'n/a'}` +
        (slotGap !== null ? `, slot_gap=${slotGap}` : ''),
    );

    // ??? DB???? BUY ???
    // ??? accepted=1 + seller_tx???? _restoreSellerTxsFromDb ??????
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
          notes: `accepted; sellSol=${sellSol.toFixed(2)}, impact=${priceImpactPct.toFixed(2)}%` +
                 (slotGap !== null ? `, slot_gap=${slotGap}` : ''),
          accepted: true,
        });
      } catch (err) {
        monitor.recordError('SignalEngine', err, { phase: 'logSignal_async' });
      }
    });
  }

  //   ???EMA9??EMA20?? / 20%?? / 10%??3%??????
  //   ???????>=15% ????1??????
  //   ?????????
  async _handleEmaStrategy(signal, _signalReceivedAt) {
    const { mint, symbol, sellSol, priceImpactPct, seller, signature, ts, slot } = signal;

    // v3.30: ?????? inflight ? ???? async ??????????
    if (this.inflightBuys.has(mint)) {
      monitor.inc('SignalEngine.rejectedInflightBuy', 1, 'SignalEngine');
      this._logReject(signal, 'buy in-flight (EMA)');
      return;
    }
    this.inflightBuys.add(mint);
    console.log(`[SignalEngine] ?? inflightBuys.add(${symbol || mint.slice(0,6)}) sig=${signature?.slice(0,12)}.. sellSol=${sellSol?.toFixed(1)} slot=${slot} inflight=[${[...this.inflightBuys].join(',')}]`);
    try {

    // 1. ?????????
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    // 2. SLOT ???? ? ??????????????
    //    ?? 100 slots = ~40s???? EMA_MAX_SLOT_LAG ??
    const latestSlot = this.tickStream ? (this.tickStream.latestSlot || 0) : 0;
    const slotGap = (slot && latestSlot) ? (latestSlot - slot) : null;
    const emaMaxSlotLag = parseInt(process.env.EMA_MAX_SLOT_LAG || '100', 10);
    if (slotGap !== null && slotGap > emaMaxSlotLag) {
      monitor.inc('SignalEngine.rejectedEmaSlotLag', 1, 'SignalEngine');
      this._logReject(signal, `EMA:slot_lag:${slotGap}>${emaMaxSlotLag} (~${Math.round(slotGap * 0.4)}s late)`);
      return;
    }

    // 3. ???? ? ?????????????
    //    ?? 60 ????? EMA_COOLDOWN_MS ??
    const emaCooldownMs = parseInt(process.env.EMA_COOLDOWN_MS || '60000', 10);
    if (emaCooldownMs > 0 && this.positionManager) {
      const recentClosed = this.positionManager.listRecentlyClosed(mint, emaCooldownMs);
      if (recentClosed.length > 0) {
        monitor.inc('SignalEngine.rejectedEmaCooldown', 1, 'SignalEngine');
        this._logReject(signal, `EMA:cooldown:${recentClosed.length} recent exits in ${emaCooldownMs / 1000}s`);
        return;
      }
    }

    // 3.5 DB???????? ? ?????????????????
    //    EMA_MAX_ADDONS=0 ????????????
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

    // 4. ???? >= 6 SOL
    const emaMinSellSol = parseFloat(process.env.EMA_MIN_SELL_SOL || '6');
    if (sellSol < emaMinSellSol) {
      monitor.inc('SignalEngine.rejectedSellSol', 1, 'SignalEngine');
      this._logReject(signal, `EMA:size:${sellSol.toFixed(1)}<${emaMinSellSol}`);
      return;
    }

    // 5. ?? >= 8%
    const emaMinImpact = parseFloat(process.env.EMA_MIN_IMPACT_PCT || '8');
    if (priceImpactPct < emaMinImpact) {
      monitor.inc('SignalEngine.rejectedImpact', 1, 'SignalEngine');
      this._logReject(signal, `EMA:impact:${priceImpactPct.toFixed(1)}%<${emaMinImpact}%`);
      return;
    }

    // 6. EMA ?????v2: ??????????

    // 7. ????
    const openPositions = this.positionManager ? this.positionManager.listOpen() : [];
    const existingPos = openPositions.find(p => p.mint === mint);

    if (existingPos) {
      // ???? ? ????????
      const emaAddOnDropPct = parseFloat(process.env.EMA_ADDON_DROP_PCT || '15');
      const emaMaxAddOns = parseInt(process.env.EMA_MAX_ADDONS || '1');
      const currentAddonCount = openPositions.filter(p => p.mint === mint && p.isAddOn).length;
      
      if (currentAddonCount >= emaMaxAddOns) {
        monitor.inc('SignalEngine.rejectedAddonMax', 1, 'SignalEngine');
        this._logReject(signal, `EMA:addon:max_addons`);
        return;
      }

      // ? signal ? priceAfter ?????????? entry_price ??
      const dropFromEntry = ((existingPos.entry_price - signal.priceAfter) / existingPos.entry_price) * 100;
      if (dropFromEntry < emaAddOnDropPct) {
        monitor.inc('SignalEngine.rejectedAddonDrop', 1, 'SignalEngine');
        this._logReject(signal, `EMA:addon:drop:${dropFromEntry.toFixed(1)}%<${emaAddOnDropPct}%`);
        return;
      }

      // ?????? addon
      signal._isAddOn = true;
    }

    // ======== ???????emit ???? ========
    this.emit('buyOrder', {
      ...signal,
      reason: `EMA:dump ${sellSol.toFixed(2)} SOL, impact -${priceImpactPct.toFixed(2)}%`,
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt,
      _isAddOn: signal._isAddOn || false,
      slotGap,
    });

    // ?? DB
    if (this.tradeLogger) {
      this.tradeLogger.logSignal({
        ts, mint, symbol, sell_sol: sellSol, price_impact_pct: priceImpactPct,
        seller, seller_tx: signature, kind: 'DUMP_DETECTED',
        accepted: 1, reject_reason: null,
      });
    }

    } finally {
      // reject ???inflightBuys ??????? add??? reject ?????
      // accept ???markBuyDone ? index.js buyOrder handler ? finally ???
      //   ??? accept ? inflight ????? markBuyDone???????
      //   ???? openPositions ?????????????????
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
      kind: 'DUMP_DETECTED',
      sellSol: signal.sellSol,
      priceImpactPct: signal.priceImpactPct,
      seller: signal.seller,
      sellerTx: signal.signature,
      notes: 'detected but rejected',
      accepted: false,
      rejectReason: reason,
    });
    console.log(
      `[SignalEngine] ?  rejected ${signal.symbol || signal.mint.slice(0, 6)}: ${reason}`,
    );
  }

  // v3.17.40: ???????
  _sampleLongPrice(mint, price) {
    if (!Number.isFinite(price) || price <= 0) return;
    const now = Date.now();
    let samples = this._longPriceSamples.get(mint);
    if (!samples) {
      samples = [];
      this._longPriceSamples.set(mint, samples);
    }
    // v3.17.41-fix: ?????? ? 50x ratio ????
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
    // v3.17.41: ???? DB
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
    // v3.17.41: ???? DB
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
