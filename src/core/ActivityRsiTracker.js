'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();

/**
 * Emits an entry signal only on a real-time RSI(7, 5s) upward cross of 30
 * while the rolling 60-second notional volume is above the configured USD
 * threshold. RSI is supplied by RsiCalculator after the triggering swap has
 * already been added to the live 5-second bucket.
 */
class ActivityRsiTracker extends EventEmitter {
  constructor({ rsiCalculator, ...opts } = {}) {
    super();
    const strategy = config.activityRsi || {};
    this.rsiCalculator = rsiCalculator || null;
    this.enabled = opts.enabled ?? strategy.enabled ?? true;
    this.replaceDumpSignal = true;
    this.volumeWindowMs = opts.volumeWindowMs ?? strategy.volumeWindowMs ?? 60_000;
    this.minVolumeUsd = opts.minVolumeUsd ?? strategy.minVolumeUsd ?? 10_000;
    this.solPriceUsd = opts.solPriceUsd ?? strategy.solPriceUsd ?? 75.5;
    this.rsi5sPeriod = opts.rsi5sPeriod ?? strategy.rsi5sPeriod ?? 7;
    this.rsiBuyCross = opts.rsiBuyCross ?? strategy.rsiBuyCross ?? 30;
    this.rsi5sMinBuckets = opts.rsi5sMinBuckets ?? strategy.rsi5sMinBuckets ?? 8;
    this.maxSignalAgeMs = opts.maxSignalAgeMs ?? strategy.maxSignalAgeMs ?? 5_000;
    // The 60-second time window bounds memory. A count cap would understate
    // volume on the most active tokens, which are the ones this strategy seeks.
    this.maxEventsPerMint = opts.maxEventsPerMint ?? strategy.maxEventsPerMint ?? 0;
    this.debug = opts.debug ?? strategy.debug ?? false;
    this.states = new Map();
  }

  handleSwap(swap) {
    if (!this.enabled || !this.rsiCalculator || !swap?.mint) return;
    const solVolume = Number(swap.solVolume);
    const price = Number(swap.price);
    const ts = Number.isFinite(Number(swap.ts)) ? Number(swap.ts) : Date.now();
    if (!Number.isFinite(solVolume) || solVolume <= 0 || !Number.isFinite(price) || price <= 0) {
      return;
    }

    const state = this._stateOf(swap.mint);
    state.events.push({ ts, solVolume });
    if (state.events.length > 1 && ts < state.events[state.events.length - 2].ts) {
      state.events.sort((a, b) => a.ts - b.ts);
    }
    state.symbol = swap.symbol || state.symbol;
    state.poolAddress = swap.poolAddress || state.poolAddress;
    state.poolQuoteAfter = Number(swap.poolQuoteAfter) > 0
      ? Number(swap.poolQuoteAfter)
      : state.poolQuoteAfter;
    this._prune(state, ts);

    const snapshot = this.rsiCalculator.snapshot(swap.mint, 0);
    const currentRsi = snapshot?.rsi5s == null ? null : Number(snapshot.rsi5s);
    const bucketCount = Number(snapshot?.bucketCount5s || 0);
    if (!Number.isFinite(currentRsi) || bucketCount < this.rsi5sMinBuckets) return;

    const previousRsi = state.lastRsi5s;
    state.lastRsi5s = currentRsi;
    if (!Number.isFinite(previousRsi)) return;

    const crossedUp = previousRsi <= this.rsiBuyCross && currentRsi > this.rsiBuyCross;
    if (!crossedUp) return;
    monitor.inc('ActivityRsi.crossesAbove30', 1, 'ActivityRsi');

    const volumeSol = state.events.reduce((sum, item) => sum + item.solVolume, 0);
    const volumeUsd = volumeSol * this.solPriceUsd;
    if (!(volumeUsd > this.minVolumeUsd)) {
      this._debug(
        swap.mint,
        `RSI crossed ${this.rsiBuyCross}, volume $${volumeUsd.toFixed(0)} <= $${this.minVolumeUsd}`,
      );
      return;
    }

    const wallNow = Date.now();
    if (this.maxSignalAgeMs > 0 && wallNow - ts > this.maxSignalAgeMs) {
      this._debug(swap.mint, `signal stale by ${wallNow - ts}ms`);
      return;
    }

    const details = {
      volumeWindowMs: this.volumeWindowMs,
      volumeSol,
      volumeUsd,
      solPriceUsd: this.solPriceUsd,
      previousRsi5s: previousRsi,
      currentRsi5s: currentRsi,
      rsiPeriod: this.rsi5sPeriod,
      rsiCross: this.rsiBuyCross,
      rsiBucketCount: bucketCount,
    };
    const signal = {
      mint: swap.mint,
      symbol: state.symbol || swap.symbol,
      sellSol: 0,
      priceImpactPct: 0,
      poolQuoteAfter: Number(swap.poolQuoteAfter) || state.poolQuoteAfter,
      poolQuoteSol: Number(swap.poolQuoteAfter) || state.poolQuoteAfter,
      seller: null,
      signature: `activity-rsi:${swap.signature || `${swap.mint}:${ts}`}`,
      ts,
      slot: Number(swap.slot) || 0,
      poolAddress: swap.poolAddress || state.poolAddress,
      priceAfter: price,
      priceBefore: Number(swap.priceBefore) > 0 ? Number(swap.priceBefore) : price,
      _aggregated: true,
      _activityRsi: true,
      _activity: details,
    };

    console.log(
      `[ActivityRsi] BUY_CROSS ${signal.symbol || swap.mint.slice(0, 6)} ` +
        `volume1m=$${volumeUsd.toFixed(0)} (${volumeSol.toFixed(2)}SOL @ $${this.solPriceUsd}) ` +
        `RSI(${this.rsi5sPeriod},5s)=${previousRsi.toFixed(1)}->${currentRsi.toFixed(1)}`,
    );
    monitor.inc('ActivityRsi.signalsEmitted', 1, 'ActivityRsi');
    this.emit('activityRsiSignal', signal);
  }

  reset(mint) {
    if (mint) this.states.delete(mint);
  }

  cleanup(activeMints = null) {
    const keep = activeMints ? new Set(activeMints) : null;
    for (const mint of this.states.keys()) {
      if (keep && !keep.has(mint)) this.states.delete(mint);
    }
  }

  _stateOf(mint) {
    let state = this.states.get(mint);
    if (!state) {
      state = {
        events: [],
        lastRsi5s: null,
        symbol: null,
        poolAddress: null,
        poolQuoteAfter: null,
      };
      this.states.set(mint, state);
    }
    return state;
  }

  _prune(state, now) {
    const cutoff = now - this.volumeWindowMs;
    while (state.events.length && state.events[0].ts <= cutoff) state.events.shift();
    if (this.maxEventsPerMint > 0 && state.events.length > this.maxEventsPerMint) {
      state.events.splice(0, state.events.length - this.maxEventsPerMint);
    }
  }

  _debug(mint, message) {
    if (this.debug) console.log(`[ActivityRsi] ${mint.slice(0, 6)} ${message}`);
  }
}

module.exports = ActivityRsiTracker;
