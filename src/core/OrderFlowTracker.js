'use strict';

const EventEmitter = require('events');
const { config } = require('../config');

function boolEnv(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return String(raw).toLowerCase() === 'true' || raw === '1' || String(raw).toLowerCase() === 'yes';
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

function round(n, digits = 3) {
  if (!Number.isFinite(n)) return 0;
  const m = 10 ** digits;
  return Math.round(n * m) / m;
}

class OrderFlowTracker extends EventEmitter {
  constructor(opts = {}) {
    super();
    const flowConfig = config.activityFlow || {};

    this.enabled =
      opts.enabled ?? flowConfig.enabled ?? boolEnv('ACTIVITY_FLOW_ENABLED', boolEnv('ORDER_FLOW_ENABLED', true));
    this.replaceDumpSignal =
      opts.replaceDumpSignal ??
      flowConfig.replaceDumpSignal ??
      boolEnv('ACTIVITY_FLOW_REPLACE_DUMP_SIGNAL', boolEnv('ORDER_FLOW_REPLACE_DUMP_SIGNAL', true));

    this.window5Ms = opts.window5Ms ?? flowConfig.window5Ms ?? numEnv('ACTIVITY_FLOW_WINDOW_5S_MS', 5_000);
    this.window15Ms = opts.window15Ms ?? flowConfig.window15Ms ?? numEnv('ACTIVITY_FLOW_WINDOW_15S_MS', 15_000);
    this.window30Ms = opts.window30Ms ?? flowConfig.window30Ms ?? numEnv('ACTIVITY_FLOW_WINDOW_30S_MS', 30_000);
    this.window60Ms = opts.window60Ms ?? flowConfig.window60Ms ?? numEnv('ACTIVITY_FLOW_WINDOW_60S_MS', 60_000);

    this.minTrades60s =
      opts.minTrades60s ?? flowConfig.minTrades60s ?? numEnv('ACTIVITY_FLOW_MIN_TRADES_60S', 24);
    this.minVolume60sSol =
      opts.minVolume60sSol ?? flowConfig.minVolume60sSol ?? numEnv('ACTIVITY_FLOW_MIN_VOLUME_60S_SOL', 12);
    this.minUniqueTraders60s =
      opts.minUniqueTraders60s ??
      flowConfig.minUniqueTraders60s ??
      numEnv('ACTIVITY_FLOW_MIN_UNIQUE_TRADERS_60S', 10);

    this.minTrades30s =
      opts.minTrades30s ?? flowConfig.minTrades30s ?? numEnv('ACTIVITY_FLOW_MIN_TRADES_30S', 12);
    this.minVolume30sSol =
      opts.minVolume30sSol ?? flowConfig.minVolume30sSol ?? numEnv('ACTIVITY_FLOW_MIN_VOLUME_30S_SOL', 6);
    this.minRatio30s =
      opts.minRatio30s ?? flowConfig.minRatio30s ?? numEnv('ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_30S', 0.85);

    this.minTrades15s =
      opts.minTrades15s ?? flowConfig.minTrades15s ?? numEnv('ACTIVITY_FLOW_MIN_TRADES_15S', 8);
    this.minVolume15sSol =
      opts.minVolume15sSol ?? flowConfig.minVolume15sSol ?? numEnv('ACTIVITY_FLOW_MIN_VOLUME_15S_SOL', 4);
    this.minRatio15s =
      opts.minRatio15s ?? flowConfig.minRatio15s ?? numEnv('ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_15S', 1.25);
    this.minImbalance15s =
      opts.minImbalance15s ?? flowConfig.minImbalance15s ?? numEnv('ACTIVITY_FLOW_MIN_IMBALANCE_15S', 0.12);
    this.minUniqueBuyers15s =
      opts.minUniqueBuyers15s ??
      flowConfig.minUniqueBuyers15s ??
      numEnv('ACTIVITY_FLOW_MIN_UNIQUE_BUYERS_15S', 3);
    this.minPriceChange15sPct =
      opts.minPriceChange15sPct ??
      flowConfig.minPriceChange15sPct ??
      numEnv('ACTIVITY_FLOW_MIN_PRICE_CHANGE_15S_PCT', -3);

    this.minTrades5s = opts.minTrades5s ?? flowConfig.minTrades5s ?? numEnv('ACTIVITY_FLOW_MIN_TRADES_5S', 3);
    this.minVolume5sSol =
      opts.minVolume5sSol ?? flowConfig.minVolume5sSol ?? numEnv('ACTIVITY_FLOW_MIN_VOLUME_5S_SOL', 1.5);
    this.minRatio5s =
      opts.minRatio5s ?? flowConfig.minRatio5s ?? numEnv('ACTIVITY_FLOW_MIN_BUY_SELL_RATIO_5S', 1.35);
    this.minImbalance5s =
      opts.minImbalance5s ?? flowConfig.minImbalance5s ?? numEnv('ACTIVITY_FLOW_MIN_IMBALANCE_5S', 0.2);
    this.minUniqueBuyers5s =
      opts.minUniqueBuyers5s ?? flowConfig.minUniqueBuyers5s ?? numEnv('ACTIVITY_FLOW_MIN_UNIQUE_BUYERS_5S', 2);
    this.minPriceChange5sPct =
      opts.minPriceChange5sPct ??
      flowConfig.minPriceChange5sPct ??
      numEnv('ACTIVITY_FLOW_MIN_PRICE_CHANGE_5S_PCT', 0.2);

    this.maxPriceChange5sPct =
      opts.maxPriceChange5sPct ??
      flowConfig.maxPriceChange5sPct ??
      numEnv('ACTIVITY_FLOW_MAX_PRICE_CHANGE_5S_PCT', 8);
    this.maxPriceChange30sPct =
      opts.maxPriceChange30sPct ??
      flowConfig.maxPriceChange30sPct ??
      numEnv('ACTIVITY_FLOW_MAX_PRICE_CHANGE_30S_PCT', 30);
    this.maxPriceChange60sPct =
      opts.maxPriceChange60sPct ??
      flowConfig.maxPriceChange60sPct ??
      numEnv('ACTIVITY_FLOW_MAX_PRICE_CHANGE_60S_PCT', 60);

    this.cooldownMs =
      opts.cooldownMs ??
      flowConfig.cooldownMs ??
      numEnv('ACTIVITY_FLOW_COOLDOWN_MS', config.strategy.cooldownMsPerToken || 60_000);
    this.maxSignalAgeMs =
      opts.maxSignalAgeMs ?? flowConfig.maxSignalAgeMs ?? numEnv('ACTIVITY_FLOW_MAX_SIGNAL_AGE_MS', config.strategy.maxPushLagMs || 5_000);
    this.maxEventsPerMint =
      opts.maxEventsPerMint ?? flowConfig.maxEventsPerMint ?? numEnv('ACTIVITY_FLOW_MAX_EVENTS_PER_MINT', 600);
    this.debug = opts.debug ?? flowConfig.debug ?? boolEnv('ACTIVITY_FLOW_DEBUG', false);

    this.maxWindowMs = Math.max(this.window5Ms, this.window15Ms, this.window30Ms, this.window60Ms);
    this.states = new Map();
    this.cooldowns = new Map();
    this._lastDebugLog = new Map();
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
      poolQuoteAfter: Number.isFinite(Number(swap.poolQuoteAfter)) ? Number(swap.poolQuoteAfter) : null,
    };

    const state = this._stateOf(ev.mint);
    state.events.push(ev);
    state.symbol = ev.symbol || state.symbol;
    state.poolAddress = ev.poolAddress || state.poolAddress;
    state.lastPoolQuoteAfter = ev.poolQuoteAfter || state.lastPoolQuoteAfter || null;
    this._prune(state, ev.ts);

    if (ev.side === 'BUY') {
      this._trySignal(state, ev);
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
      state = {
        events: [],
        symbol: null,
        poolAddress: null,
        lastPoolQuoteAfter: null,
        lastDumpSignal: null,
      };
      this.states.set(mint, state);
    }
    return state;
  }

  _prune(state, now) {
    const cutoff = now - this.maxWindowMs - 1_000;
    while (state.events.length > 0 && state.events[0].ts < cutoff) state.events.shift();
    if (state.events.length > this.maxEventsPerMint) {
      state.events.splice(0, state.events.length - this.maxEventsPerMint);
    }
  }

  _windowEvents(state, now, windowMs) {
    const start = now - windowMs;
    return state.events
      .filter((ev) => ev.ts >= start && ev.ts <= now)
      .sort((a, b) => (a.ts - b.ts) || ((a.slot || 0) - (b.slot || 0)));
  }

  _stats(state, now, windowMs) {
    const events = this._windowEvents(state, now, windowMs);
    const buys = events.filter((ev) => ev.side === 'BUY');
    const sells = events.filter((ev) => ev.side === 'SELL');
    const buySol = sumVolume(buys);
    const sellSol = sumVolume(sells);
    const volumeSol = buySol + sellSol;
    const first = events[0] || null;
    const last = events[events.length - 1] || null;
    const firstPrice = first ? first.price : 0;
    const lastPrice = last ? last.price : 0;
    const priceChangePct = firstPrice > 0 && lastPrice > 0 ? ((lastPrice - firstPrice) / firstPrice) * 100 : 0;

    return {
      windowMs,
      events,
      tradeCount: events.length,
      buyCount: buys.length,
      sellCount: sells.length,
      buySol,
      sellSol,
      volumeSol,
      buySellRatio: buySol / Math.max(sellSol, 0.001),
      buyCountRatio: buys.length / Math.max(sells.length, 1),
      imbalance: (buySol - sellSol) / Math.max(volumeSol, 0.001),
      uniqueBuyers: uniqueCount(buys, 'signer'),
      uniqueSellers: uniqueCount(sells, 'signer'),
      uniqueTraders: uniqueCount(events, 'signer'),
      firstPrice,
      lastPrice,
      priceChangePct,
      lastSide: last ? last.side : null,
    };
  }

  _trySignal(state, ev) {
    const wallNow = Date.now();
    if (this.maxSignalAgeMs > 0 && wallNow - ev.ts > this.maxSignalAgeMs) {
      this._debugReject(ev.mint, ev.ts, `signal age ${wallNow - ev.ts}ms>${this.maxSignalAgeMs}ms`, null, null, null, null);
      return;
    }

    const cooldownUntil = this.cooldowns.get(ev.mint) || 0;
    if (cooldownUntil > wallNow) return;

    const s5 = this._stats(state, ev.ts, this.window5Ms);
    const s15 = this._stats(state, ev.ts, this.window15Ms);
    const s30 = this._stats(state, ev.ts, this.window30Ms);
    const s60 = this._stats(state, ev.ts, this.window60Ms);
    const reject = this._firstReject(s5, s15, s30, s60);
    if (reject) {
      this._debugReject(ev.mint, ev.ts, reject, s5, s15, s30, s60);
      return;
    }

    const flow = {
      s5: this._compactStats(s5),
      s15: this._compactStats(s15),
      s30: this._compactStats(s30),
      s60: this._compactStats(s60),
    };

    const signal = {
      mint: ev.mint,
      symbol: state.symbol || ev.symbol,
      sellSol: round(s15.sellSol, 4),
      priceImpactPct: round(Math.max(0, -s15.priceChangePct), 3),
      poolQuoteAfter: ev.poolQuoteAfter || state.lastPoolQuoteAfter || null,
      poolQuoteSol: ev.poolQuoteAfter || state.lastPoolQuoteAfter || null,
      seller: null,
      signature: `activity:${ev.signature || `${ev.mint}:${ev.ts}`}`,
      ts: ev.ts,
      slot: ev.slot || 0,
      poolAddress: ev.poolAddress || state.poolAddress,
      priceAfter: ev.price,
      priceBefore: s15.firstPrice || s30.firstPrice || s60.firstPrice || ev.price,
      _aggregated: true,
      _activityFlow: true,
      _sellCount: s15.sellCount,
      _sellCount10s: s15.sellCount,
      _totalSellSol10s: round(s15.sellSol, 4),
      _sellers: [...new Set(s15.events.filter((x) => x.side === 'SELL').map((x) => x.signer).filter(Boolean))],
      _flow: flow,
    };

    console.log(
      `[ActivityFlow] BUY_CONFIRM ${signal.symbol || ev.mint.slice(0, 6)} ` +
        `5s=${flow.s5.tradeCount}tx/${flow.s5.volumeSol.toFixed(1)}SOL ` +
        `r=${flow.s5.buySellRatio.toFixed(2)} imb=${flow.s5.imbalance.toFixed(2)} chg=${flow.s5.priceChangePct.toFixed(1)}% ` +
        `| 15s=${flow.s15.tradeCount}tx/${flow.s15.volumeSol.toFixed(1)}SOL ` +
        `r=${flow.s15.buySellRatio.toFixed(2)} imb=${flow.s15.imbalance.toFixed(2)} chg=${flow.s15.priceChangePct.toFixed(1)}% ` +
        `| 60s=${flow.s60.tradeCount}tx/${flow.s60.volumeSol.toFixed(1)}SOL traders=${flow.s60.uniqueTraders}`,
    );

    this.cooldowns.set(ev.mint, wallNow + this.cooldownMs);
    this.emit('flowReversalSignal', signal);
  }

  _firstReject(s5, s15, s30, s60) {
    if (s60.tradeCount < this.minTrades60s) return `60s trades ${s60.tradeCount}<${this.minTrades60s}`;
    if (s60.volumeSol < this.minVolume60sSol) return `60s volume ${s60.volumeSol.toFixed(2)}<${this.minVolume60sSol}`;
    if (s60.uniqueTraders < this.minUniqueTraders60s) {
      return `60s traders ${s60.uniqueTraders}<${this.minUniqueTraders60s}`;
    }
    if (s30.tradeCount < this.minTrades30s) return `30s trades ${s30.tradeCount}<${this.minTrades30s}`;
    if (s30.volumeSol < this.minVolume30sSol) return `30s volume ${s30.volumeSol.toFixed(2)}<${this.minVolume30sSol}`;
    if (s30.buySellRatio < this.minRatio30s) {
      return `30s buy/sell ${s30.buySellRatio.toFixed(2)}<${this.minRatio30s}`;
    }

    if (s15.tradeCount < this.minTrades15s) return `15s trades ${s15.tradeCount}<${this.minTrades15s}`;
    if (s15.volumeSol < this.minVolume15sSol) return `15s volume ${s15.volumeSol.toFixed(2)}<${this.minVolume15sSol}`;
    if (s15.buySellRatio < this.minRatio15s) {
      return `15s buy/sell ${s15.buySellRatio.toFixed(2)}<${this.minRatio15s}`;
    }
    if (s15.imbalance < this.minImbalance15s) {
      return `15s imbalance ${s15.imbalance.toFixed(2)}<${this.minImbalance15s}`;
    }
    if (s15.uniqueBuyers < this.minUniqueBuyers15s) {
      return `15s buyers ${s15.uniqueBuyers}<${this.minUniqueBuyers15s}`;
    }
    if (s15.priceChangePct < this.minPriceChange15sPct) {
      return `15s price ${s15.priceChangePct.toFixed(1)}%<${this.minPriceChange15sPct}%`;
    }

    if (s5.tradeCount < this.minTrades5s) return `5s trades ${s5.tradeCount}<${this.minTrades5s}`;
    if (s5.volumeSol < this.minVolume5sSol) return `5s volume ${s5.volumeSol.toFixed(2)}<${this.minVolume5sSol}`;
    if (s5.buySellRatio < this.minRatio5s) return `5s buy/sell ${s5.buySellRatio.toFixed(2)}<${this.minRatio5s}`;
    if (s5.imbalance < this.minImbalance5s) return `5s imbalance ${s5.imbalance.toFixed(2)}<${this.minImbalance5s}`;
    if (s5.uniqueBuyers < this.minUniqueBuyers5s) return `5s buyers ${s5.uniqueBuyers}<${this.minUniqueBuyers5s}`;
    if (s5.lastSide !== 'BUY') return 'last side is not BUY';
    if (s5.priceChangePct < this.minPriceChange5sPct) {
      return `5s price ${s5.priceChangePct.toFixed(1)}%<${this.minPriceChange5sPct}%`;
    }

    if (s5.priceChangePct > this.maxPriceChange5sPct) {
      return `5s price ${s5.priceChangePct.toFixed(1)}%>${this.maxPriceChange5sPct}%`;
    }
    if (s30.priceChangePct > this.maxPriceChange30sPct) {
      return `30s price ${s30.priceChangePct.toFixed(1)}%>${this.maxPriceChange30sPct}%`;
    }
    if (s60.priceChangePct > this.maxPriceChange60sPct) {
      return `60s price ${s60.priceChangePct.toFixed(1)}%>${this.maxPriceChange60sPct}%`;
    }
    return null;
  }

  _compactStats(stats) {
    return {
      windowMs: stats.windowMs,
      tradeCount: stats.tradeCount,
      buyCount: stats.buyCount,
      sellCount: stats.sellCount,
      buySol: round(stats.buySol, 4),
      sellSol: round(stats.sellSol, 4),
      volumeSol: round(stats.volumeSol, 4),
      buySellRatio: round(stats.buySellRatio, 3),
      buyCountRatio: round(stats.buyCountRatio, 3),
      imbalance: round(stats.imbalance, 3),
      uniqueBuyers: stats.uniqueBuyers,
      uniqueSellers: stats.uniqueSellers,
      uniqueTraders: stats.uniqueTraders,
      priceChangePct: round(stats.priceChangePct, 3),
    };
  }

  _debugReject(mint, ts, reason, s5, s15, s30, s60) {
    if (!this.debug) return;
    const last = this._lastDebugLog.get(mint) || 0;
    if (ts - last < 2_000) return;
    this._lastDebugLog.set(mint, ts);
    if (!s5 || !s15 || !s30 || !s60) {
      console.log(`[ActivityFlow] skip ${mint.slice(0, 6)}: ${reason}`);
      return;
    }
    console.log(
      `[ActivityFlow] skip ${mint.slice(0, 6)}: ${reason} ` +
        `5s=${s5.tradeCount}tx/${s5.volumeSol.toFixed(1)}SOL r=${s5.buySellRatio.toFixed(2)} ` +
        `15s=${s15.tradeCount}tx/${s15.volumeSol.toFixed(1)}SOL r=${s15.buySellRatio.toFixed(2)} ` +
        `30s=${s30.tradeCount}tx/${s30.volumeSol.toFixed(1)}SOL ` +
        `60s=${s60.tradeCount}tx/${s60.volumeSol.toFixed(1)}SOL`,
    );
  }
}

module.exports = OrderFlowTracker;
