'use strict';

const EventEmitter = require('events');
const { config } = require('../config');

function boolEnv(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return String(raw).toLowerCase() === 'true' || raw === '1';
}

function numEnv(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

function uniqueCount(items, field) {
  const set = new Set();
  for (const item of items) {
    const v = item[field];
    if (v) set.add(v);
  }
  return set.size;
}

function sumVolume(items) {
  return items.reduce((sum, x) => sum + (Number.isFinite(x.solVolume) ? x.solVolume : 0), 0);
}

class OrderFlowTracker extends EventEmitter {
  constructor(opts = {}) {
    super();
    this.enabled = opts.enabled ?? boolEnv('ORDER_FLOW_ENABLED', true);
    this.windowMs = opts.windowMs ?? numEnv('ORDER_FLOW_WINDOW_MS', 10_000);
    this.confirmWindowMs = opts.confirmWindowMs ?? numEnv('ORDER_FLOW_CONFIRM_WINDOW_MS', 3_000);
    this.minSellSol = opts.minSellSol ?? numEnv('ORDER_FLOW_MIN_SELL_SOL', config.strategy.minSellSol || 20);
    this.minDropPct = opts.minDropPct ?? numEnv('ORDER_FLOW_MIN_DROP_PCT', Math.max(6, Math.min(config.strategy.minPriceImpactPct || 10, 10)));
    this.maxDropPct = opts.maxDropPct ?? numEnv('ORDER_FLOW_MAX_DROP_PCT', config.strategy.maxPriceImpactPct || 30);
    this.minSellCount = opts.minSellCount ?? numEnv('ORDER_FLOW_MIN_SELL_COUNT', config.strategy.minTriggerSellCount || 2);
    this.minUniqueSellers = opts.minUniqueSellers ?? numEnv('ORDER_FLOW_MIN_UNIQUE_SELLERS', 2);
    this.minBuySol = opts.minBuySol ?? numEnv('ORDER_FLOW_MIN_BUY_SOL', 3);
    this.minBuySellRatio = opts.minBuySellRatio ?? numEnv('ORDER_FLOW_MIN_BUY_SELL_RATIO', 1.25);
    this.minImbalance = opts.minImbalance ?? numEnv('ORDER_FLOW_MIN_IMBALANCE', 0.15);
    this.minUniqueBuyers = opts.minUniqueBuyers ?? numEnv('ORDER_FLOW_MIN_UNIQUE_BUYERS', 2);
    this.minReboundPct = opts.minReboundPct ?? numEnv('ORDER_FLOW_MIN_REBOUND_PCT', 1.5);
    this.maxReboundPct = opts.maxReboundPct ?? numEnv('ORDER_FLOW_MAX_REBOUND_PCT', 10);
    this.minLowAgeMs = opts.minLowAgeMs ?? numEnv('ORDER_FLOW_MIN_LOW_AGE_MS', 300);
    this.maxCandidateAgeMs = opts.maxCandidateAgeMs ?? numEnv('ORDER_FLOW_MAX_CANDIDATE_AGE_MS', 8_000);
    this.cooldownMs = opts.cooldownMs ?? numEnv('ORDER_FLOW_COOLDOWN_MS', config.strategy.cooldownMsPerToken || 60_000);
    this.maxEventsPerMint = opts.maxEventsPerMint ?? numEnv('ORDER_FLOW_MAX_EVENTS_PER_MINT', 180);
    this.debug = opts.debug ?? boolEnv('ORDER_FLOW_DEBUG', false);

    this.states = new Map();
    this.cooldowns = new Map();
    this._lastStatsLog = new Map();
  }

  handleSwap(swap) {
    if (!this.enabled || !swap || !swap.mint) return;
    const side = String(swap.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;
    const price = Number(swap.price);
    const solVolume = Number(swap.solVolume);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(solVolume) || solVolume <= 0) return;

    const ev = {
      mint: swap.mint,
      symbol: swap.symbol || null,
      signer: swap.signer || null,
      side,
      solVolume,
      price,
      ts: Number.isFinite(swap.ts) ? swap.ts : Date.now(),
      slot: swap.slot || 0,
      signature: swap.signature || null,
      poolAddress: swap.poolAddress || null,
    };

    const state = this._stateOf(ev.mint);
    state.events.push(ev);
    state.symbol = ev.symbol || state.symbol;
    state.poolAddress = ev.poolAddress || state.poolAddress;
    this._prune(state, ev.ts);

    const candidate = this._buildCandidate(state, ev.ts);
    if (candidate) {
      state.candidate = candidate;
      this._maybeLogCandidate(ev.mint, candidate);
    }

    if (ev.side === 'BUY') {
      this._tryConfirm(state, ev);
    }
  }

  noteSuppressedDumpSignal(signal) {
    if (!signal || !signal.mint) return;
    const state = this._stateOf(signal.mint);
    state.lastDumpSignal = signal;
  }

  _stateOf(mint) {
    let state = this.states.get(mint);
    if (!state) {
      state = { events: [], candidate: null, symbol: null, poolAddress: null, lastDumpSignal: null };
      this.states.set(mint, state);
    }
    return state;
  }

  _prune(state, now) {
    const cutoff = now - Math.max(this.windowMs, this.confirmWindowMs, this.maxCandidateAgeMs) - 1_000;
    while (state.events.length > 0 && state.events[0].ts < cutoff) state.events.shift();
    if (state.events.length > this.maxEventsPerMint) {
      state.events.splice(0, state.events.length - this.maxEventsPerMint);
    }
    if (state.candidate && now - state.candidate.startTs > this.maxCandidateAgeMs) {
      state.candidate = null;
    }
  }

  _buildCandidate(state, now) {
    const windowStart = now - this.windowMs;
    const events = state.events.filter((ev) => ev.ts >= windowStart);
    if (events.length < 2) return null;

    const sells = events.filter((ev) => ev.side === 'SELL');
    if (sells.length < this.minSellCount) return null;

    const sellSol = sumVolume(sells);
    if (sellSol < this.minSellSol) return null;

    const uniqueSellers = uniqueCount(sells, 'signer');
    if (uniqueSellers < this.minUniqueSellers) return null;

    let highPrice = 0;
    let lowPrice = Infinity;
    let lowTs = 0;
    let lowSlot = 0;
    for (const ev of events) {
      if (ev.price > highPrice) highPrice = ev.price;
      if (ev.price < lowPrice) {
        lowPrice = ev.price;
        lowTs = ev.ts;
        lowSlot = ev.slot || 0;
      }
    }
    if (!Number.isFinite(lowPrice) || highPrice <= 0 || lowPrice <= 0) return null;

    const dropPct = ((highPrice - lowPrice) / highPrice) * 100;
    if (dropPct < this.minDropPct || dropPct > this.maxDropPct) return null;

    const firstSell = sells[0];
    return {
      mint: firstSell.mint,
      symbol: state.symbol || firstSell.symbol,
      startTs: sells[0].ts,
      updatedTs: now,
      highPrice,
      lowPrice,
      lowTs,
      lowSlot,
      dropPct,
      sellSol,
      sellCount: sells.length,
      uniqueSellers,
      sellers: [...new Set(sells.map((x) => x.signer).filter(Boolean))],
      firstSeller: firstSell.signer || null,
      firstSellSignature: firstSell.signature || null,
      poolAddress: state.poolAddress || firstSell.poolAddress || null,
    };
  }

  _tryConfirm(state, ev) {
    const candidate = state.candidate;
    if (!candidate) return;

    const cooldownUntil = this.cooldowns.get(ev.mint) || 0;
    if (cooldownUntil > ev.ts) return;

    const lowAgeMs = ev.ts - candidate.lowTs;
    if (lowAgeMs < this.minLowAgeMs) return;
    if (ev.ts - candidate.startTs > this.maxCandidateAgeMs) {
      state.candidate = null;
      return;
    }

    const confirmStart = Math.max(candidate.lowTs, ev.ts - this.confirmWindowMs);
    const confirmEvents = state.events.filter((x) => x.ts >= confirmStart && x.ts <= ev.ts);
    const buys = confirmEvents.filter((x) => x.side === 'BUY');
    const sells = confirmEvents.filter((x) => x.side === 'SELL');
    const buySol = sumVolume(buys);
    const sellSol = sumVolume(sells);
    const uniqueBuyers = uniqueCount(buys, 'signer');
    const buySellRatio = buySol / Math.max(sellSol, 0.001);
    const imbalance = (buySol - sellSol) / Math.max(buySol + sellSol, 0.001);
    const reboundPct = ((ev.price - candidate.lowPrice) / candidate.lowPrice) * 100;

    if (buySol < this.minBuySol) return;
    if (uniqueBuyers < this.minUniqueBuyers) return;
    if (buySellRatio < this.minBuySellRatio) return;
    if (imbalance < this.minImbalance) return;
    if (reboundPct < this.minReboundPct || reboundPct > this.maxReboundPct) return;

    const flow = {
      sellSol: +candidate.sellSol.toFixed(4),
      buySol: +buySol.toFixed(4),
      confirmSellSol: +sellSol.toFixed(4),
      buySellRatio: +buySellRatio.toFixed(3),
      imbalance: +imbalance.toFixed(3),
      dropPct: +candidate.dropPct.toFixed(3),
      reboundPct: +reboundPct.toFixed(3),
      sellCount: candidate.sellCount,
      uniqueSellers: candidate.uniqueSellers,
      uniqueBuyers,
      lowAgeMs,
      confirmWindowMs: ev.ts - confirmStart,
    };

    const signature = `flow:${candidate.firstSellSignature || candidate.startTs}:${ev.signature || ev.ts}`;
    const signal = {
      mint: ev.mint,
      symbol: candidate.symbol || ev.symbol,
      sellSol: candidate.sellSol,
      priceImpactPct: candidate.dropPct,
      poolQuoteAfter: null,
      seller: candidate.firstSeller,
      signature,
      ts: ev.ts,
      slot: ev.slot || candidate.lowSlot || 0,
      poolAddress: ev.poolAddress || candidate.poolAddress,
      priceAfter: ev.price,
      priceBefore: candidate.highPrice,
      _aggregated: true,
      _orderFlow: true,
      _sellCount: candidate.sellCount,
      _sellCount10s: candidate.sellCount,
      _totalSellSol10s: candidate.sellSol,
      _sellers: candidate.sellers,
      _flow: flow,
    };

    console.log(
      `[OrderFlow] BUY_CONFIRM ${signal.symbol || ev.mint.slice(0, 6)} ` +
        `drop=${flow.dropPct.toFixed(1)}% rebound=${flow.reboundPct.toFixed(1)}% ` +
        `sell=${flow.sellSol.toFixed(1)}SOL buy=${flow.buySol.toFixed(1)}SOL ` +
        `ratio=${flow.buySellRatio.toFixed(2)} imbalance=${flow.imbalance.toFixed(2)} ` +
        `buyers=${flow.uniqueBuyers} sellers=${flow.uniqueSellers}`,
    );

    this.cooldowns.set(ev.mint, ev.ts + this.cooldownMs);
    state.candidate = null;
    this.emit('flowReversalSignal', signal);
  }

  _maybeLogCandidate(mint, candidate) {
    if (!this.debug) return;
    const last = this._lastStatsLog.get(mint) || 0;
    if (candidate.updatedTs - last < 1_000) return;
    this._lastStatsLog.set(mint, candidate.updatedTs);
    console.log(
      `[OrderFlow] candidate ${candidate.symbol || mint.slice(0, 6)} ` +
        `sell=${candidate.sellSol.toFixed(1)}SOL drop=${candidate.dropPct.toFixed(1)}% ` +
        `sellers=${candidate.uniqueSellers} lowAge=${candidate.updatedTs - candidate.lowTs}ms`,
    );
  }
}

module.exports = OrderFlowTracker;
