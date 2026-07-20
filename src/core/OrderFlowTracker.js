'use strict';

const EventEmitter = require('events');
const { config } = require('../config');

function sumVolume(events, side = null) {
  return events.reduce((total, event) => {
    if (side && event.side !== side) return total;
    return total + event.solVolume;
  }, 0);
}

function uniqueSigners(events, side = null) {
  const signers = new Set();
  for (const event of events) {
    if (side && event.side !== side) continue;
    if (event.signer) signers.add(event.signer);
  }
  return signers;
}

function round(value, digits = 3) {
  if (!Number.isFinite(value)) return 0;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

class BurstPullbackTracker extends EventEmitter {
  constructor(opts = {}) {
    super();
    const strategy = config.burstPullback || {};

    this.enabled = opts.enabled ?? strategy.enabled ?? true;
    this.replaceDumpSignal = true;
    this.window5Ms = opts.window5Ms ?? strategy.window5Ms ?? 5_000;
    this.volumeExpansion = opts.volumeExpansion ?? strategy.volumeExpansion ?? 3;
    this.tpsExpansion = opts.tpsExpansion ?? strategy.tpsExpansion ?? 2;
    this.quietWindowMs = opts.quietWindowMs ?? strategy.quietWindowMs ?? 30_000;
    this.confirmWindowMs = opts.confirmWindowMs ?? strategy.confirmWindowMs ?? 60_000;
    this.minPeakRisePct = opts.minPeakRisePct ?? strategy.minPeakRisePct ?? 5;
    this.minPullbackPct = opts.minPullbackPct ?? strategy.minPullbackPct ?? 2;
    this.maxPullbackPct = opts.maxPullbackPct ?? strategy.maxPullbackPct ?? 8;
    this.minBuyerAcceleration =
      opts.minBuyerAcceleration ?? strategy.minBuyerAcceleration ?? 1.5;
    this.newBuyerWindowMs = opts.newBuyerWindowMs ?? strategy.newBuyerWindowMs ?? 10_000;
    this.cooldownMs = opts.cooldownMs ?? strategy.cooldownMs ?? 300_000;
    this.maxSignalAgeMs = opts.maxSignalAgeMs ?? strategy.maxSignalAgeMs ?? 5_000;
    this.maxEventsPerMint = opts.maxEventsPerMint ?? strategy.maxEventsPerMint ?? 2_000;
    this.debug = opts.debug ?? strategy.debug ?? false;

    this.retentionMs = Math.max(
      this.quietWindowMs + (this.window5Ms * 2),
      this.confirmWindowMs + (this.newBuyerWindowMs * 2),
    ) + 1_000;
    this.states = new Map();
    this.cooldowns = new Map();
    this._lastDebugLog = new Map();
  }

  handleSwap(swap) {
    if (!this.enabled || !swap?.mint) return;
    const side = String(swap.side || '').toUpperCase();
    const price = Number(swap.price);
    const solVolume = Number(swap.solVolume);
    if (
      (side !== 'BUY' && side !== 'SELL') ||
      !Number.isFinite(price) ||
      price <= 0 ||
      !Number.isFinite(solVolume) ||
      solVolume <= 0
    ) return;

    const event = {
      mint: swap.mint,
      symbol: swap.symbol || null,
      signer: swap.signer || null,
      side,
      solVolume,
      price,
      priceBefore: Number(swap.priceBefore) > 0 ? Number(swap.priceBefore) : null,
      ts: Number.isFinite(Number(swap.ts)) ? Number(swap.ts) : Date.now(),
      slot: Number(swap.slot) || 0,
      signature: swap.signature || null,
      poolAddress: swap.poolAddress || null,
      poolQuoteAfter: Number(swap.poolQuoteAfter) > 0 ? Number(swap.poolQuoteAfter) : null,
    };

    const state = this._stateOf(event.mint);
    const previousLastEvent = state.events[state.events.length - 1];
    state.events.push(event);
    if (previousLastEvent && event.ts < previousLastEvent.ts) {
      state.events.sort((a, b) => a.ts - b.ts);
    }
    state.symbol = event.symbol || state.symbol;
    state.poolAddress = event.poolAddress || state.poolAddress;
    state.lastPoolQuoteAfter = event.poolQuoteAfter || state.lastPoolQuoteAfter;
    this._prune(state, event.ts);

    if (state.burst) {
      this._trackBurst(state, event);
      return;
    }

    this._detectFirstBurst(state, event);
  }

  noteSuppressedDumpSignal() {
    // Legacy dump signals are intentionally ignored by this strategy.
  }

  _stateOf(mint) {
    let state = this.states.get(mint);
    if (!state) {
      state = {
        events: [],
        symbol: null,
        poolAddress: null,
        lastPoolQuoteAfter: null,
        lastEquivalentBurstAt: 0,
        burst: null,
      };
      this.states.set(mint, state);
    }
    return state;
  }

  _prune(state, now) {
    const cutoff = now - this.retentionMs;
    while (state.events.length && state.events[0].ts <= cutoff) state.events.shift();
    if (state.events.length > this.maxEventsPerMint) {
      state.events.splice(0, state.events.length - this.maxEventsPerMint);
    }
  }

  _rangeEvents(state, startExclusive, endInclusive) {
    return state.events.filter(
      (event) => event.ts > startExclusive && event.ts <= endInclusive,
    );
  }

  _stats(state, startExclusive, endInclusive) {
    const events = this._rangeEvents(state, startExclusive, endInclusive);
    const buys = events.filter((event) => event.side === 'BUY');
    const sells = events.filter((event) => event.side === 'SELL');
    const buySol = sumVolume(buys);
    const sellSol = sumVolume(sells);
    return {
      events,
      tradeCount: events.length,
      buyCount: buys.length,
      sellCount: sells.length,
      buySol,
      sellSol,
      volumeSol: buySol + sellSol,
      netFlowSol: buySol - sellSol,
      uniqueBuyers: uniqueSigners(buys).size,
      firstPrice: events[0]?.price || 0,
      lastPrice: events[events.length - 1]?.price || 0,
    };
  }

  _detectFirstBurst(state, event) {
    const current = this._stats(state, event.ts - this.window5Ms, event.ts);
    const previous = this._stats(
      state,
      event.ts - (this.window5Ms * 2),
      event.ts - this.window5Ms,
    );

    // A multiplier from an empty baseline is not a real expansion.
    if (previous.volumeSol <= 0 || previous.tradeCount <= 0) return;
    const volumeMultiple = current.volumeSol / previous.volumeSol;
    const tpsMultiple = current.tradeCount / previous.tradeCount;
    if (volumeMultiple < this.volumeExpansion || tpsMultiple < this.tpsExpansion) return;

    const previousEquivalentBurstAt = state.lastEquivalentBurstAt;
    state.lastEquivalentBurstAt = event.ts;
    if (
      previousEquivalentBurstAt > 0 &&
      event.ts - previousEquivalentBurstAt < this.quietWindowMs
    ) {
      this._debug(event.mint, event.ts, 'same-level burst inside quiet window');
      return;
    }

    const preBurstPrice = previous.lastPrice || event.priceBefore;
    if (!Number.isFinite(preBurstPrice) || preBurstPrice <= 0) return;
    const peakPrice = Math.max(event.price, ...current.events.map((item) => item.price));
    state.burst = {
      detectedAt: event.ts,
      expiresAt: event.ts + this.confirmWindowMs,
      preBurstPrice,
      peakPrice,
      peakTs: event.ts,
      baselineVolumeSol: previous.volumeSol,
      baselineTrades: previous.tradeCount,
      burstVolumeSol: current.volumeSol,
      burstTrades: current.tradeCount,
      volumeMultiple,
      tpsMultiple,
      buyersKnownAtBurst: uniqueSigners(
        state.events.filter((item) => item.ts <= event.ts),
        'BUY',
      ),
    };

    console.log(
      `[BurstPullback] FIRST_BURST ${state.symbol || event.mint.slice(0, 6)} ` +
        `volume=${current.volumeSol.toFixed(2)}SOL x${volumeMultiple.toFixed(2)} ` +
        `trades=${current.tradeCount} x${tpsMultiple.toFixed(2)} ` +
        `pre=${preBurstPrice.toExponential(4)} wait=${this.confirmWindowMs / 1000}s`,
    );
  }

  _trackBurst(state, event) {
    const burst = state.burst;
    if (event.ts > burst.expiresAt) {
      this._debug(event.mint, event.ts, 'confirmation window expired');
      state.burst = null;
      return;
    }

    if (event.price > burst.peakPrice) {
      burst.peakPrice = event.price;
      burst.peakTs = event.ts;
    }

    const current5 = this._stats(state, event.ts - this.window5Ms, event.ts);
    const previous5 = this._stats(
      state,
      event.ts - (this.window5Ms * 2),
      event.ts - this.window5Ms,
    );
    if (
      previous5.volumeSol > 0 &&
      previous5.tradeCount > 0 &&
      current5.volumeSol / previous5.volumeSol >= this.volumeExpansion &&
      current5.tradeCount / previous5.tradeCount >= this.tpsExpansion
    ) {
      state.lastEquivalentBurstAt = event.ts;
    }

    const currentNewBuyers = this._newBuyerCount(
      state,
      burst,
      event.ts - this.newBuyerWindowMs,
      event.ts,
    );
    const previousNewBuyers = this._newBuyerCount(
      state,
      burst,
      event.ts - (this.newBuyerWindowMs * 2),
      event.ts - this.newBuyerWindowMs,
    );
    const peakRisePct = ((burst.peakPrice - burst.preBurstPrice) / burst.preBurstPrice) * 100;
    const pullbackPct = ((burst.peakPrice - event.price) / burst.peakPrice) * 100;
    const buyerAcceleration = previous5.buySol > 0
      ? current5.buySol / previous5.buySol
      : 0;

    const checks = [
      peakRisePct >= this.minPeakRisePct,
      pullbackPct >= this.minPullbackPct,
      pullbackPct <= this.maxPullbackPct,
      event.price > burst.preBurstPrice,
      current5.netFlowSol > 0,
      current5.sellSol < previous5.sellSol,
      previous5.buySol > 0,
      current5.buySol > previous5.buySol,
      buyerAcceleration >= this.minBuyerAcceleration,
      currentNewBuyers > previousNewBuyers,
    ];
    if (!checks.every(Boolean)) return;

    const wallNow = Date.now();
    if (this.maxSignalAgeMs > 0 && wallNow - event.ts > this.maxSignalAgeMs) {
      this._debug(event.mint, event.ts, `signal stale by ${wallNow - event.ts}ms`);
      return;
    }
    if ((this.cooldowns.get(event.mint) || 0) > wallNow) {
      state.burst = null;
      return;
    }

    const details = {
      volumeMultiple: round(burst.volumeMultiple),
      tpsMultiple: round(burst.tpsMultiple),
      peakRisePct: round(peakRisePct),
      pullbackPct: round(pullbackPct),
      current5BuySol: round(current5.buySol, 4),
      current5SellSol: round(current5.sellSol, 4),
      previous5BuySol: round(previous5.buySol, 4),
      previous5SellSol: round(previous5.sellSol, 4),
      netFlow5sSol: round(current5.netFlowSol, 4),
      buyerAcceleration: round(buyerAcceleration),
      currentNewBuyers,
      previousNewBuyers,
      detectedAt: burst.detectedAt,
    };
    const signal = {
      mint: event.mint,
      symbol: state.symbol || event.symbol,
      sellSol: details.current5SellSol,
      priceImpactPct: details.pullbackPct,
      poolQuoteAfter: event.poolQuoteAfter || state.lastPoolQuoteAfter,
      poolQuoteSol: event.poolQuoteAfter || state.lastPoolQuoteAfter,
      seller: null,
      signature: `burst:${event.signature || `${event.mint}:${event.ts}`}`,
      ts: event.ts,
      slot: event.slot,
      poolAddress: event.poolAddress || state.poolAddress,
      priceAfter: event.price,
      priceBefore: burst.preBurstPrice,
      _aggregated: true,
      _burstPullback: true,
      _burst: details,
    };

    this.cooldowns.set(event.mint, wallNow + this.cooldownMs);
    state.burst = null;
    console.log(
      `[BurstPullback] BUY_CONFIRM ${signal.symbol || event.mint.slice(0, 6)} ` +
        `peak=+${peakRisePct.toFixed(2)}% pullback=${pullbackPct.toFixed(2)}% ` +
        `flow5s=${current5.netFlowSol.toFixed(2)}SOL ` +
        `sell=${previous5.sellSol.toFixed(2)}->${current5.sellSol.toFixed(2)}SOL ` +
        `buyAccel=x${buyerAcceleration.toFixed(2)} ` +
        `newBuyers10s=${previousNewBuyers}->${currentNewBuyers}`,
    );
    this.emit('burstPullbackSignal', signal);
  }

  _newBuyerCount(state, burst, startExclusive, endInclusive) {
    const firstSeenAfterBurst = new Map();
    for (const event of state.events) {
      if (
        event.ts <= burst.detectedAt ||
        event.side !== 'BUY' ||
        !event.signer ||
        burst.buyersKnownAtBurst.has(event.signer) ||
        firstSeenAfterBurst.has(event.signer)
      ) continue;
      firstSeenAfterBurst.set(event.signer, event.ts);
    }
    let count = 0;
    for (const firstSeenAt of firstSeenAfterBurst.values()) {
      if (firstSeenAt > startExclusive && firstSeenAt <= endInclusive) count += 1;
    }
    return count;
  }

  _debug(mint, ts, message) {
    if (!this.debug) return;
    const last = this._lastDebugLog.get(mint) || 0;
    if (ts - last < 2_000) return;
    this._lastDebugLog.set(mint, ts);
    console.log(`[BurstPullback] skip ${mint.slice(0, 6)}: ${message}`);
  }
}

module.exports = BurstPullbackTracker;
