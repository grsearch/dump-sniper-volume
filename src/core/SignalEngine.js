'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { evaluateEarlyFlowEntryRisk } = require('./EarlyFlowEntryRisk');

const monitor = getMonitor();
monitor.registerModule('SignalEngine', { staleMs: 3600_000, label: 'Signal Engine' });

class SignalEngine extends EventEmitter {
  constructor({
    tradeLogger,
    positionManager,
    tickStream = null,
    tokenRegistry = null,
    entryMarketProvider = null,
  }) {
    super();
    this.tradeLogger = tradeLogger;
    this.positionManager = positionManager;
    this.tickStream = tickStream;
    this.tokenRegistry = tokenRegistry;
    this.entryMarketProvider = typeof entryMarketProvider === 'function'
      ? entryMarketProvider
      : null;

    this.lastTriggerTs = new Map();
    this.ourSignatures = new Set();
    this.inflightBuys = new Set();

    // Surrounding execution code records failed-pool cooldowns here. The
    // activity/RSI entry strategy deliberately does not apply a time cooldown.
    this._exitCooldowns = new Map();

    this._cleanupInterval = setInterval(() => this._cleanupExpired(), 60_000);
    if (this._cleanupInterval.unref) this._cleanupInterval.unref();
  }

  shutdown() {
    if (this._cleanupInterval) clearInterval(this._cleanupInterval);
  }

  _cleanupExpired() {
    const now = Date.now();
    for (const [mint, expireAt] of this._exitCooldowns) {
      if (expireAt <= now) this._exitCooldowns.delete(mint);
    }
  }

  markBuyInflight(mint) {
    this.inflightBuys.add(mint);
  }

  markBuyDone(mint) {
    this.inflightBuys.delete(mint);
  }

  setEntryMarketProvider(provider) {
    this.entryMarketProvider = typeof provider === 'function' ? provider : null;
  }

  _resolveEntryFdvUsd(mint) {
    try {
      const realtime = this.entryMarketProvider?.(mint);
      const realtimeFdv = Number(realtime?.fdvUsd ?? realtime?.fdv);
      if (Number.isFinite(realtimeFdv) && realtimeFdv > 0) {
        return { fdvUsd: realtimeFdv, source: 'chain_realtime' };
      }
    } catch (err) {
      monitor.recordError('SignalEngine', err, { phase: 'entry_fdv_realtime', mint });
    }

    const token = this.tokenRegistry?.getToken?.(mint);
    const registryFdv = Number(token?.fdv ?? token?.market_cap);
    if (Number.isFinite(registryFdv) && registryFdv > 0) {
      return { fdvUsd: registryFdv, source: token?.market_source || 'registry' };
    }
    return { fdvUsd: null, source: 'unavailable' };
  }

  setExecutionCooldown(mint, durationMs, reason = 'execution') {
    const duration = Number(durationMs);
    if (!mint || !Number.isFinite(duration) || duration <= 0) return 0;

    const now = Date.now();
    const cooldownUntil = Math.max(
      Number(this._exitCooldowns.get(mint)) || 0,
      now + duration,
    );
    this._exitCooldowns.set(mint, cooldownUntil);
    console.log(
      `[SignalEngine] cooldown ${mint.slice(0, 8)}.. ` +
        `${Math.ceil((cooldownUntil - now) / 1000)}s reason=${reason}`,
    );
    return cooldownUntil;
  }

  setPositionExitCooldown(position, {
    rebuyCooldownMs = 0,
    stopLossRebuyCooldownMs = 0,
  } = {}) {
    const mint = position?.mint;
    if (!mint) return 0;

    const exitReason = String(position.exitReason || '');
    const isFixedStopLoss = exitReason === 'FIXED_STOP_LOSS' ||
      exitReason.startsWith('FIXED_STOP_LOSS_');
    const durationMs = isFixedStopLoss
      ? Math.max(Number(rebuyCooldownMs) || 0, Number(stopLossRebuyCooldownMs) || 0)
      : Number(rebuyCooldownMs) || 0;
    if (durationMs <= 0) return 0;

    return this.setExecutionCooldown(
      mint,
      durationMs,
      isFixedStopLoss ? 'fixed_stop_loss' : 'position_closed',
    );
  }

  registerOurSignature(signature) {
    if (!signature) return;
    this.ourSignatures.add(signature);
    const timer = setTimeout(() => this.ourSignatures.delete(signature), 5 * 60_000);
    if (timer.unref) timer.unref();
  }

  async _handleEarlyFlowSignal(signal, signalReceivedAt) {
    const { mint, symbol, signature, ts, slot } = signal;
    const now = Date.now();
    const details = signal._earlyFlowDetails || {};
    const signalPrice = Number(details.signalPrice);
    const executionPrice = Number(details.executionPrice);
    const executionDelayMs = Number(details.executionDelayMs);
    const signalAgeMs = Number(details.signalMigrationAgeMs);
    const fdvUsd = Number(details.fdvUsd);
    const priceChangePct = Number(details.priceChangePct);
    const netFlow1sSol = Number(details.netFlow1sSol);
    const uniqueBuyers5s = Number(details.uniqueBuyers5s);
    const tradeCount5s = Number(details.tradeCount5s);
    const buySol5s = Number(details.buySol5s);
    const largestBuyShare5s = Number(details.largestBuyShare5s);
    const maxSignalAgeMs = Math.max(config.earlyFlow.executionWindowMs, 5_000);

    if (
      !mint ||
      !Number.isFinite(signalPrice) ||
      signalPrice <= 0 ||
      !Number.isFinite(executionPrice) ||
      executionPrice <= 0
    ) {
      this._logReject(signal, 'invalid early-flow signal');
      return;
    }
    if (ts && now - ts > maxSignalAgeMs) {
      monitor.inc('SignalEngine.rejectedPushLag', 1, 'SignalEngine');
      this._logReject(signal, `signal stale: ${now - ts}ms > ${maxSignalAgeMs}ms`);
      return;
    }
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    const executionCooldownUntil = Number(this._exitCooldowns.get(mint)) || 0;
    if (executionCooldownUntil > now) {
      monitor.inc('SignalEngine.rejectedExecutionCooldown', 1, 'SignalEngine');
      this._logReject(
        signal,
        `buy execution cooldown: ${Math.ceil((executionCooldownUntil - now) / 1000)}s remaining`,
      );
      return;
    }
    if (executionCooldownUntil > 0) this._exitCooldowns.delete(mint);

    const openCount = this.positionManager.openPositionCount();
    const inflightCount = this.inflightBuys.size;
    if (openCount + inflightCount >= config.strategy.maxConcurrentPositions) {
      monitor.inc('SignalEngine.rejectedMaxConcurrent', 1, 'SignalEngine');
      this._logReject(
        signal,
        `max concurrent (${openCount} open + ${inflightCount} inflight / ` +
          `${config.strategy.maxConcurrentPositions})`,
      );
      return;
    }
    if (this.inflightBuys.has(mint)) {
      monitor.inc('SignalEngine.rejectedInflightBuy', 1, 'SignalEngine');
      this._logReject(signal, 'buy in-flight');
      return;
    }
    const mintOpenCount = this.positionManager.openPositionCountByMint
      ? this.positionManager.openPositionCountByMint(mint)
      : (this.positionManager.hasOpenPosition(mint) ? 1 : 0);
    if (mintOpenCount > 0) {
      monitor.inc('SignalEngine.rejectedAddonCondition', 1, 'SignalEngine');
      this._logReject(signal, 'existing position; add-on disabled');
      return;
    }

    if (
      !Number.isFinite(signalAgeMs) ||
      signalAgeMs < config.earlyFlow.minMigrationAgeMs ||
      signalAgeMs > config.earlyFlow.maxMigrationAgeMs
    ) {
      this._logReject(
        signal,
        `ENTRY_AGE_OUT_OF_RANGE: ${this._numberLabel(signalAgeMs / 1000, 2)}s`,
      );
      return;
    }
    if (
      !Number.isFinite(fdvUsd) ||
      fdvUsd < config.earlyFlow.minFdvUsd ||
      fdvUsd > config.earlyFlow.maxFdvUsd
    ) {
      this._logReject(signal, `ENTRY_FDV_OUT_OF_RANGE: $${this._numberLabel(fdvUsd, 0)}`);
      return;
    }
    if (
      !Number.isFinite(priceChangePct) ||
      priceChangePct < config.earlyFlow.minPriceChangePct ||
      priceChangePct > config.earlyFlow.maxPriceChangePct
    ) {
      this._logReject(
        signal,
        `PRICE_CHANGE_10S_OUT_OF_RANGE: ${this._numberLabel(priceChangePct, 2)}%`,
      );
      return;
    }
    if (!(netFlow1sSol > 0)) {
      this._logReject(signal, `NET_FLOW_1S_NOT_POSITIVE: ${this._numberLabel(netFlow1sSol, 3)}SOL`);
      return;
    }
    if (
      !Number.isFinite(uniqueBuyers5s) ||
      uniqueBuyers5s < config.earlyFlow.minUniqueBuyers
    ) {
      this._logReject(signal, `UNIQUE_BUYERS_5S_LOW: ${uniqueBuyers5s}`);
      return;
    }
    if (
      !Number.isFinite(tradeCount5s) ||
      tradeCount5s < config.earlyFlow.minTradeCount
    ) {
      this._logReject(signal, `TRADE_COUNT_5S_LOW: ${tradeCount5s}`);
      return;
    }
    if (
      !Number.isFinite(buySol5s) ||
      buySol5s < config.earlyFlow.minBuySol5s
    ) {
      this._logReject(
        signal,
        `BUY_VOLUME_5S_LOW: ${this._numberLabel(buySol5s, 3)}SOL`,
      );
      return;
    }
    if (
      !Number.isFinite(largestBuyShare5s) ||
      largestBuyShare5s > config.earlyFlow.maxLargestBuyShare
    ) {
      this._logReject(
        signal,
        `LARGEST_BUY_SHARE_5S_HIGH: ${this._numberLabel(largestBuyShare5s * 100, 1)}%`,
      );
      return;
    }
    if (
      !Number.isFinite(executionDelayMs) ||
      executionDelayMs < 0 ||
      executionDelayMs > config.earlyFlow.executionWindowMs
    ) {
      this._logReject(signal, `EXECUTION_WINDOW_MISSED: ${executionDelayMs}ms`);
      return;
    }
    const maxExecutionPrice = signalPrice *
      (1 + config.earlyFlow.maxExecutionPriceDeviationPct / 100);
    if (executionPrice > maxExecutionPrice) {
      this._logReject(
        signal,
        `EXECUTION_PRICE_HIGH: ${((executionPrice / signalPrice - 1) * 100).toFixed(2)}%`,
      );
      return;
    }
    const entryRisk = evaluateEarlyFlowEntryRisk(details, config.earlyFlow);
    details.entryRiskScore = entryRisk.score;
    details.entryRiskRejectScore = entryRisk.rejectScore;
    details.entryRiskBlocked = entryRisk.blocked;
    details.entryRiskReasons = entryRisk.reasons;
    if (entryRisk.blocked) {
      monitor.inc('SignalEngine.rejectedEntryRisk', 1, 'SignalEngine');
      this._logReject(
        signal,
        `ENTRY_RISK_SCORE_HIGH: ${entryRisk.score}/${entryRisk.rejectScore} ` +
          `[${entryRisk.reasons.join(',')}]`,
      );
      return;
    }

    const reason =
      `early_flow: age=${(signalAgeMs / 1000).toFixed(1)}s ` +
      `fdv=$${fdvUsd.toFixed(0)} change10s=${priceChangePct.toFixed(2)}% ` +
      `flow1s=${netFlow1sSol.toFixed(3)}SOL buyers5s=${uniqueBuyers5s} ` +
      `tx5s=${tradeCount5s} buy5s=${buySol5s.toFixed(3)}SOL ` +
      `largestBuyShare=${(largestBuyShare5s * 100).toFixed(1)}% ` +
      `execution=${executionDelayMs}ms/${((executionPrice / signalPrice - 1) * 100).toFixed(2)}% ` +
      `risk=${entryRisk.score}/${entryRisk.rejectScore}`;

    this.inflightBuys.add(mint);
    this.lastTriggerTs.set(mint, now);
    monitor.inc('SignalEngine.signalsAccepted', 1, 'SignalEngine');
    this.emit('buyOrder', {
      ...signal,
      reason,
      entryFdvUsd: fdvUsd,
      entryFdvSource: 'chain_realtime_signal',
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt: signalReceivedAt,
    });
    console.log(
      `[SignalEngine] BUY_SIGNAL ${symbol || mint.slice(0, 6)}: ${reason}` +
        (slot ? ` slot=${slot}` : ''),
    );

    setImmediate(() => {
      try {
        this.tradeLogger.logSignal({
          ts,
          mint,
          symbol,
          kind: 'EARLY_FLOW',
          sellSol: 0,
          priceImpactPct: 0,
          seller: null,
          sellerTx: signature,
          notes: reason,
          accepted: true,
        });
      } catch (err) {
        monitor.recordError('SignalEngine', err, { phase: 'logEarlyFlowSignal_async' });
      }
    });
  }

  async handleEarlyFlowSignal(signal) {
    monitor.beat('SignalEngine', 'signal');
    const signalReceivedAt = Date.now();
    if (!signal || !signal._earlyFlow) {
      throw new Error('SignalEngine only accepts early-flow entry signals');
    }
    return this._handleEarlyFlowSignal(signal, signalReceivedAt);
  }

  async _handleActivityRsiSignal(signal, signalReceivedAt) {
    const { mint, symbol, signature, ts, slot } = signal;
    const now = Date.now();
    const maxSignalAgeMs = config.activityRsi.maxSignalAgeMs;
    const priceAfter = Number(signal.priceAfter);

    if (!mint || !Number.isFinite(priceAfter) || priceAfter <= 0) {
      this._logReject(signal, 'invalid activity-RSI signal');
      return;
    }
    if (maxSignalAgeMs > 0 && ts && now - ts > maxSignalAgeMs) {
      monitor.inc('SignalEngine.rejectedPushLag', 1, 'SignalEngine');
      this._logReject(signal, `signal stale: ${now - ts}ms > ${maxSignalAgeMs}ms`);
      return;
    }
    if (signature && this.ourSignatures.has(signature)) {
      monitor.inc('SignalEngine.rejectedSelfTrigger', 1, 'SignalEngine');
      this._logReject(signal, 'self-triggered');
      return;
    }

    // This map is only populated by execution failures or an explicitly
    // configured post-sale cooldown. Normal activity/RSI signals remain free
    // of a same-token strategy cooldown.
    const executionCooldownUntil = Number(this._exitCooldowns.get(mint)) || 0;
    if (executionCooldownUntil > now) {
      monitor.inc('SignalEngine.rejectedExecutionCooldown', 1, 'SignalEngine');
      this._logReject(
        signal,
        `buy execution cooldown: ${Math.ceil((executionCooldownUntil - now) / 1000)}s remaining`,
      );
      return;
    }
    if (executionCooldownUntil > 0) this._exitCooldowns.delete(mint);

    const openCount = this.positionManager.openPositionCount();
    const inflightCount = this.inflightBuys.size;
    if (openCount + inflightCount >= config.strategy.maxConcurrentPositions) {
      monitor.inc('SignalEngine.rejectedMaxConcurrent', 1, 'SignalEngine');
      this._logReject(
        signal,
        `max concurrent (${openCount} open + ${inflightCount} inflight / ` +
          `${config.strategy.maxConcurrentPositions})`,
      );
      return;
    }
    if (this.inflightBuys.has(mint)) {
      monitor.inc('SignalEngine.rejectedInflightBuy', 1, 'SignalEngine');
      this._logReject(signal, 'buy in-flight');
      return;
    }

    const mintOpenCount = this.positionManager.openPositionCountByMint
      ? this.positionManager.openPositionCountByMint(mint)
      : (this.positionManager.hasOpenPosition(mint) ? 1 : 0);
    if (mintOpenCount > 0) {
      monitor.inc('SignalEngine.rejectedAddonCondition', 1, 'SignalEngine');
      this._logReject(signal, 'existing position; add-on disabled');
      return;
    }

    const activity = signal._activity || {};
    const volumeUsd = Number(activity.volumeUsd);
    const uniqueBuyers1m = Number(activity.uniqueBuyers1m);
    const previousRsi = Number(activity.previousRsi5s);
    const currentRsi = Number(activity.currentRsi5s);
    if (!(volumeUsd > config.activityRsi.minVolumeUsd)) {
      this._logReject(
        signal,
        `VOLUME_1M_LOW: $${this._numberLabel(volumeUsd, 0)} <= ` +
          `$${config.activityRsi.minVolumeUsd}`,
      );
      return;
    }
    if (!(
      Number.isFinite(previousRsi) &&
      Number.isFinite(currentRsi) &&
      previousRsi <= config.activityRsi.rsiBuyCross &&
      currentRsi > config.activityRsi.rsiBuyCross
    )) {
      this._logReject(
        signal,
        `RSI_5S_NO_UP_CROSS: ${this._numberLabel(previousRsi, 1)}->` +
          `${this._numberLabel(currentRsi, 1)} threshold=${config.activityRsi.rsiBuyCross}`,
      );
      return;
    }
    if (!(
      Number.isFinite(uniqueBuyers1m) &&
      uniqueBuyers1m >= config.activityRsi.minUniqueBuyers1m
    )) {
      this._logReject(
        signal,
        `UNIQUE_BUYERS_1M_LOW: ${this._numberLabel(uniqueBuyers1m, 0)} < ` +
          `${config.activityRsi.minUniqueBuyers1m}`,
      );
      return;
    }

    const minEntryFdvUsd = Number(config.activityRsi.minFdvUsd) || 0;
    const maxEntryFdvUsd = Number(config.activityRsi.maxFdvUsd) || 0;
    const entryMarket = this._resolveEntryFdvUsd(mint);
    if (minEntryFdvUsd > 0 && !(entryMarket.fdvUsd > 0)) {
      monitor.inc('SignalEngine.rejectedEntryFdvUnavailable', 1, 'SignalEngine');
      this._logReject(signal, 'ENTRY_FDV_UNAVAILABLE: no realtime or cached FDV');
      return;
    }
    if (minEntryFdvUsd > 0 && entryMarket.fdvUsd < minEntryFdvUsd) {
      monitor.inc('SignalEngine.rejectedEntryFdvLow', 1, 'SignalEngine');
      this._logReject(
        signal,
        `ENTRY_FDV_LOW: $${this._numberLabel(entryMarket.fdvUsd, 0)} < ` +
          `$${minEntryFdvUsd} source=${entryMarket.source}`,
      );
      return;
    }
    if (maxEntryFdvUsd > 0 && entryMarket.fdvUsd > maxEntryFdvUsd) {
      monitor.inc('SignalEngine.rejectedEntryFdvHigh', 1, 'SignalEngine');
      this._logReject(
        signal,
        `ENTRY_FDV_HIGH: $${this._numberLabel(entryMarket.fdvUsd, 0)} > ` +
          `$${maxEntryFdvUsd} source=${entryMarket.source}`,
      );
      return;
    }

    const token = this.tokenRegistry?.getToken?.(mint);
    const migrationTime = Number(token?.migration_time);
    const migrationAgeMs = Number.isFinite(migrationTime) && migrationTime > 0
      ? now - migrationTime
      : null;
    if (!Number.isFinite(migrationAgeMs) || migrationAgeMs < 0) {
      monitor.inc('SignalEngine.rejectedMigrationAgeUnavailable', 1, 'SignalEngine');
      this._logReject(signal, 'MIGRATION_AGE_UNAVAILABLE');
      return;
    }
    if (migrationAgeMs < config.activityRsi.minMigrationAgeMs) {
      monitor.inc('SignalEngine.rejectedMigrationTooYoung', 1, 'SignalEngine');
      this._logReject(
        signal,
        `MIGRATION_TOO_YOUNG: ${Math.floor(migrationAgeMs / 1000)}s < ` +
          `${Math.ceil(config.activityRsi.minMigrationAgeMs / 1000)}s`,
      );
      return;
    }
    if (
      config.activityRsi.maxMigrationAgeMs > 0 &&
      migrationAgeMs > config.activityRsi.maxMigrationAgeMs
    ) {
      monitor.inc('SignalEngine.rejectedMigrationTooOld', 1, 'SignalEngine');
      this._logReject(
        signal,
        `MIGRATION_TOO_OLD: ${Math.ceil(migrationAgeMs / 1000)}s > ` +
          `${Math.floor(config.activityRsi.maxMigrationAgeMs / 1000)}s`,
      );
      return;
    }

    const reason =
      `activity_rsi: volume1m=$${volumeUsd.toFixed(0)} ` +
      `(${Number(activity.volumeSol || 0).toFixed(2)}SOL) ` +
      `buyers1m=${uniqueBuyers1m} ` +
      `fdv=$${this._numberLabel(entryMarket.fdvUsd, 0)} ` +
      `age=${Math.floor(migrationAgeMs / 1000)}s ` +
      `rsi5s=${previousRsi.toFixed(1)}->${currentRsi.toFixed(1)} ` +
      `cross>${config.activityRsi.rsiBuyCross}`;

    this.inflightBuys.add(mint);
    this.lastTriggerTs.set(mint, now);
    monitor.inc('SignalEngine.signalsAccepted', 1, 'SignalEngine');
    this.emit('buyOrder', {
      ...signal,
      reason,
      entryFdvUsd: entryMarket.fdvUsd,
      entryFdvSource: entryMarket.source,
      sizeSol: config.strategy.positionSizeSol,
      _signalReceivedAt: signalReceivedAt,
    });
    console.log(
      `[SignalEngine] BUY_SIGNAL ${symbol || mint.slice(0, 6)}: ${reason}` +
        (slot ? ` slot=${slot}` : ''),
    );

    setImmediate(() => {
      try {
        this.tradeLogger.logSignal({
          ts,
          mint,
          symbol,
          kind: 'ACTIVITY_RSI',
          sellSol: signal.sellSol || 0,
          priceImpactPct: 0,
          seller: null,
          sellerTx: signature,
          notes: reason,
          accepted: true,
        });
      } catch (err) {
        monitor.recordError('SignalEngine', err, { phase: 'logActivityRsiSignal_async' });
      }
    });
  }

  async handleActivityRsiSignal(signal) {
    monitor.beat('SignalEngine', 'signal');
    const signalReceivedAt = Date.now();
    if (!signal || !signal._activityRsi) {
      throw new Error('SignalEngine only accepts activity/RSI entry signals');
    }
    return this._handleActivityRsiSignal(signal, signalReceivedAt);
  }

  _numberLabel(value, decimals) {
    return Number.isFinite(value) ? value.toFixed(decimals) : 'n/a';
  }

  _logReject(signal, reason) {
    if (this.tradeLogger) {
      this.tradeLogger.logSignal({
        ts: signal.ts,
        mint: signal.mint,
        symbol: signal.symbol,
        kind: signal._earlyFlow
          ? 'EARLY_FLOW'
          : (signal._activityRsi ? 'ACTIVITY_RSI' : 'LEGACY_ENTRY'),
        sellSol: signal.sellSol,
        priceImpactPct: signal.priceImpactPct,
        seller: signal.seller,
        sellerTx: signal.signature,
        notes: 'detected but rejected',
        accepted: false,
        rejectReason: reason,
      });
    }
    console.log(
      `[SignalEngine] rejected ${signal.symbol || signal.mint.slice(0, 6)}: ${reason}`,
    );
  }
}

module.exports = SignalEngine;
