'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
monitor.registerModule('SignalEngine', { staleMs: 3600_000, label: 'Signal Engine' });

class SignalEngine extends EventEmitter {
  constructor({ tradeLogger, positionManager, tickStream = null }) {
    super();
    this.tradeLogger = tradeLogger;
    this.positionManager = positionManager;
    this.tickStream = tickStream;

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

    const isFixedStopLoss = position.exitReason === 'FIXED_STOP_LOSS';
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

    const reason =
      `activity_rsi: volume1m=$${volumeUsd.toFixed(0)} ` +
      `(${Number(activity.volumeSol || 0).toFixed(2)}SOL) ` +
      `rsi5s=${previousRsi.toFixed(1)}->${currentRsi.toFixed(1)} ` +
      `cross>${config.activityRsi.rsiBuyCross}`;

    this.inflightBuys.add(mint);
    this.lastTriggerTs.set(mint, now);
    monitor.inc('SignalEngine.signalsAccepted', 1, 'SignalEngine');
    this.emit('buyOrder', {
      ...signal,
      reason,
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
        kind: signal._activityRsi ? 'ACTIVITY_RSI' : 'LEGACY_ENTRY',
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
