'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
monitor.registerModule('Ema15sTracker', { staleMs: 600_000, label: 'EMA 15s Tracker' });

class Ema15sTracker extends EventEmitter {
  constructor(opts = {}) {
    super();
    const strategy = config.strategy || {};
    this.enabled = opts.enabled ?? strategy.emaExitEnabled ?? true;
    this.fastPeriod = opts.fastPeriod ?? strategy.emaFastPeriod ?? 9;
    this.slowPeriod = opts.slowPeriod ?? strategy.emaSlowPeriod ?? 20;
    this.barMs = opts.barMs ?? strategy.emaBarMs ?? 15_000;
    this.resetGapMs = opts.resetGapMs ?? strategy.emaResetGapMs ?? 300_000;
    this.executionDelayMs = opts.executionDelayMs ??
      strategy.emaExecutionDelayMs ?? 500;
    this.states = new Map();
  }

  handlePriceTick(tick) {
    if (!this.enabled || !tick?.mint) return;
    const ts = Number(tick.ts);
    const price = Number(tick.price);
    if (!Number.isFinite(ts) || !Number.isFinite(price) || price <= 0) return;

    const state = this._stateOf(tick.mint);
    const bucketTs = Math.floor(ts / this.barMs) * this.barMs;
    if (state.currentBucketTs == null) {
      state.currentBucketTs = bucketTs;
      state.currentClose = price;
      return;
    }
    if (bucketTs < state.currentBucketTs) return;
    if (bucketTs === state.currentBucketTs) {
      state.currentClose = price;
      this._emitPendingIfDue(state, tick);
      return;
    }

    const previousBucketTs = state.currentBucketTs;
    const previousClose = state.currentClose;
    this._commitClose(state, previousBucketTs, previousClose);

    const gapMs = bucketTs - previousBucketTs;
    if (gapMs > this.resetGapMs) {
      this._resetEma(state);
    } else {
      for (
        let fillBucketTs = previousBucketTs + this.barMs;
        fillBucketTs < bucketTs;
        fillBucketTs += this.barMs
      ) {
        this._commitClose(state, fillBucketTs, previousClose);
      }
    }

    state.currentBucketTs = bucketTs;
    state.currentClose = price;
    this._emitPendingIfDue(state, tick);
  }

  reset(mint) {
    if (mint) this.states.delete(mint);
  }

  cleanup(activeMints = null) {
    if (!activeMints) return;
    const keep = new Set(activeMints);
    for (const mint of this.states.keys()) {
      if (!keep.has(mint)) this.states.delete(mint);
    }
  }

  _commitClose(state, bucketTs, close) {
    const previousValidFast = state.validFast;
    const previousValidSlow = state.validSlow;
    const fastAlpha = 2 / (this.fastPeriod + 1);
    const slowAlpha = 2 / (this.slowPeriod + 1);

    if (state.count === 0) {
      state.emaFast = close;
      state.emaSlow = close;
    } else {
      state.emaFast = fastAlpha * close + (1 - fastAlpha) * state.emaFast;
      state.emaSlow = slowAlpha * close + (1 - slowAlpha) * state.emaSlow;
    }
    state.count++;

    if (state.count >= this.slowPeriod) {
      state.validFast = state.emaFast;
      state.validSlow = state.emaSlow;
      if (
        !state.pendingCross &&
        Number.isFinite(previousValidFast) &&
        Number.isFinite(previousValidSlow) &&
        previousValidFast >= previousValidSlow &&
        state.validFast < state.validSlow
      ) {
        const crossAt = bucketTs + this.barMs;
        state.pendingCross = {
          crossAt,
          dueAt: crossAt + this.executionDelayMs,
          emaFast: state.validFast,
          emaSlow: state.validSlow,
        };
        monitor.inc('Ema15sTracker.downCrosses', 1, 'Ema15sTracker');
      }
    }
  }

  _emitPendingIfDue(state, tick) {
    const pending = state.pendingCross;
    const ts = Number(tick.ts);
    if (!pending || ts < pending.dueAt) return;
    state.pendingCross = null;
    this.emit('downCross', {
      mint: tick.mint,
      price: Number(tick.price),
      ts,
      slot: Number(tick.slot) || 0,
      signature: tick.signature || null,
      poolAddress: tick.poolAddress || null,
      crossAt: pending.crossAt,
      dueAt: pending.dueAt,
      emaFast: pending.emaFast,
      emaSlow: pending.emaSlow,
    });
  }

  _resetEma(state) {
    state.count = 0;
    state.emaFast = null;
    state.emaSlow = null;
    state.validFast = null;
    state.validSlow = null;
    state.pendingCross = null;
  }

  _stateOf(mint) {
    let state = this.states.get(mint);
    if (!state) {
      state = {
        currentBucketTs: null,
        currentClose: null,
        count: 0,
        emaFast: null,
        emaSlow: null,
        validFast: null,
        validSlow: null,
        pendingCross: null,
      };
      this.states.set(mint, state);
    }
    return state;
  }
}

module.exports = Ema15sTracker;
