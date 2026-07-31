'use strict';

const EventEmitter = require('events');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { evaluateEarlyFlowEntryRisk } = require('./EarlyFlowEntryRisk');

const monitor = getMonitor();
monitor.registerModule('EarlyFlowEntry', { staleMs: 600_000, label: 'Early Flow Entry' });

class EarlyFlowEntryTracker extends EventEmitter {
  constructor({ tokenRegistry, marketProvider = null, ...opts } = {}) {
    super();
    const strategy = config.earlyFlow || {};
    this.tokenRegistry = tokenRegistry || null;
    this.marketProvider = typeof marketProvider === 'function' ? marketProvider : null;
    this.enabled = opts.enabled ?? strategy.enabled ?? true;
    this.minMigrationAgeMs = opts.minMigrationAgeMs ??
      strategy.minMigrationAgeMs ?? 15_000;
    this.maxMigrationAgeMs = opts.maxMigrationAgeMs ??
      strategy.maxMigrationAgeMs ?? 25_000;
    this.minFdvUsd = opts.minFdvUsd ?? strategy.minFdvUsd ?? 15_000;
    this.maxFdvUsd = opts.maxFdvUsd ?? strategy.maxFdvUsd ?? 100_000;
    this.priceWindowMs = opts.priceWindowMs ?? strategy.priceWindowMs ?? 10_000;
    this.minPriceChangePct = opts.minPriceChangePct ??
      strategy.minPriceChangePct ?? -10;
    this.maxPriceChangePct = opts.maxPriceChangePct ??
      strategy.maxPriceChangePct ?? 8;
    this.netFlowWindowMs = opts.netFlowWindowMs ??
      strategy.netFlowWindowMs ?? 1_000;
    this.activityWindowMs = opts.activityWindowMs ??
      strategy.activityWindowMs ?? 5_000;
    this.minUniqueBuyers = opts.minUniqueBuyers ??
      strategy.minUniqueBuyers ?? 3;
    this.minTradeCount = opts.minTradeCount ?? strategy.minTradeCount ?? 4;
    this.minBuySol5s = opts.minBuySol5s ?? strategy.minBuySol5s ?? 2;
    this.maxLargestBuyShare = opts.maxLargestBuyShare ??
      strategy.maxLargestBuyShare ?? 0.70;
    this.executionWindowMs = opts.executionWindowMs ??
      strategy.executionWindowMs ?? 3_000;
    this.maxExecutionPriceDeviationPct = opts.maxExecutionPriceDeviationPct ??
      strategy.maxExecutionPriceDeviationPct ?? 15;
    this.marketFreshMs = opts.marketFreshMs ?? strategy.marketFreshMs ?? 1_500;
    this.riskConfig = {
      riskEnabled: opts.riskEnabled ?? strategy.riskEnabled ?? true,
      riskRejectScore: opts.riskRejectScore ?? strategy.riskRejectScore ?? 4,
      riskMinUniqueBuyers5s:
        opts.riskMinUniqueBuyers5s ?? strategy.riskMinUniqueBuyers5s ?? 6,
      riskMinBuySol5s: opts.riskMinBuySol5s ?? strategy.riskMinBuySol5s ?? 3,
      riskMinPriceChangePct:
        opts.riskMinPriceChangePct ?? strategy.riskMinPriceChangePct ?? -2,
      riskMaxLargestBuyShare:
        opts.riskMaxLargestBuyShare ?? strategy.riskMaxLargestBuyShare ?? 0.45,
      riskMaxExecutionDelayMs:
        opts.riskMaxExecutionDelayMs ?? strategy.riskMaxExecutionDelayMs ?? 400,
      riskMinFdvUsd: opts.riskMinFdvUsd ?? strategy.riskMinFdvUsd ?? 25_000,
      riskMaxMigrationAgeMs:
        opts.riskMaxMigrationAgeMs ?? strategy.riskMaxMigrationAgeMs ?? 20_000,
    };
    this.debug = opts.debug ?? false;
    this.states = new Map();
  }

  setMarketProvider(provider) {
    this.marketProvider = typeof provider === 'function' ? provider : null;
  }

  reset(mint) {
    if (mint) this.states.delete(mint);
  }

  cleanup(activeMints = null, now = Date.now()) {
    const keep = activeMints ? new Set(activeMints) : null;
    for (const [mint, state] of this.states) {
      if (
        (keep && !keep.has(mint)) ||
        (state.migrationTime > 0 && now - state.migrationTime > 60 * 60_000)
      ) {
        this.states.delete(mint);
      }
    }
  }

  handleSwap(swap) {
    if (!this.enabled || !swap?.mint) return;
    const ts = Number(swap.ts);
    const price = Number(swap.price);
    const solVolume = Number(swap.solVolume);
    const side = String(swap.side || '').toUpperCase();
    if (
      !Number.isFinite(ts) ||
      !Number.isFinite(price) ||
      price <= 0 ||
      !Number.isFinite(solVolume) ||
      solVolume <= 0 ||
      (side !== 'BUY' && side !== 'SELL')
    ) {
      return;
    }

    const state = this._stateOf(swap);
    state.events.push({
      ts,
      price,
      solVolume,
      side,
      signer: swap.signer ? String(swap.signer) : null,
      signature: swap.signature || null,
    });
    if (state.events.length > 1 && ts < state.events[state.events.length - 2].ts) {
      state.events.sort((a, b) => a.ts - b.ts);
    }
    this._prune(state, ts);

    if (state.done) return;
    if (state.pending) {
      this._tryExecute(state, swap);
      return;
    }

    const migrationTime = this._migrationTime(state, swap.mint);
    if (!(migrationTime > 0)) return;
    const migrationAgeMs = ts - migrationTime;
    if (migrationAgeMs < this.minMigrationAgeMs) return;
    if (migrationAgeMs > this.maxMigrationAgeMs) {
      state.done = true;
      return;
    }

    const market = this._trustedMarket(swap);
    if (!market) {
      this._debug(swap.mint, 'market or pool state is not current/trusted');
      return;
    }
    const fdvUsd = Number(market.fdvUsd);
    if (fdvUsd < this.minFdvUsd || fdvUsd > this.maxFdvUsd) return;

    const metrics = this._metrics(state.events, ts, price);
    if (!metrics) return;
    if (
      metrics.priceChangePct < this.minPriceChangePct ||
      metrics.priceChangePct > this.maxPriceChangePct ||
      !(metrics.netFlow1sSol > 0) ||
      metrics.uniqueBuyers5s < this.minUniqueBuyers ||
      metrics.tradeCount5s < this.minTradeCount ||
      metrics.buySol5s < this.minBuySol5s ||
      metrics.largestBuyShare5s > this.maxLargestBuyShare
    ) {
      return;
    }

    state.pending = {
      signalTs: ts,
      deadlineTs: ts + this.executionWindowMs,
      signalPrice: price,
      signalSlot: Number(swap.slot) || 0,
      signalSignature: swap.signature || null,
      fdvUsd,
      migrationTime,
      migrationAgeMs,
      metrics,
    };
    monitor.inc('EarlyFlowEntry.signalsArmed', 1, 'EarlyFlowEntry');
    console.log(
      `[EarlyFlowEntry] ARMED ${state.symbol || swap.mint.slice(0, 6)} ` +
        `age=${(migrationAgeMs / 1000).toFixed(1)}s fdv=$${fdvUsd.toFixed(0)} ` +
        `change10s=${metrics.priceChangePct.toFixed(2)}% ` +
        `flow1s=${metrics.netFlow1sSol.toFixed(3)}SOL ` +
        `buyers5s=${metrics.uniqueBuyers5s} tx5s=${metrics.tradeCount5s} ` +
        `buy5s=${metrics.buySol5s.toFixed(3)}SOL ` +
        `largestBuyShare=${(metrics.largestBuyShare5s * 100).toFixed(1)}%`,
    );
  }

  _tryExecute(state, swap) {
    const pending = state.pending;
    const ts = Number(swap.ts);
    const price = Number(swap.price);
    if (!pending || !Number.isFinite(ts) || !Number.isFinite(price) || price <= 0) return;
    if (ts > pending.deadlineTs) {
      state.pending = null;
      state.done = true;
      monitor.inc('EarlyFlowEntry.executionExpired', 1, 'EarlyFlowEntry');
      this._debug(swap.mint, `execution expired after ${ts - pending.signalTs}ms`);
      return;
    }

    const market = this._trustedMarket(swap);
    if (!market) return;
    const maxPrice = pending.signalPrice *
      (1 + this.maxExecutionPriceDeviationPct / 100);
    if (price > maxPrice) {
      monitor.inc('EarlyFlowEntry.executionPriceHigh', 1, 'EarlyFlowEntry');
      return;
    }

    state.pending = null;
    state.done = true;
    const executionMetrics = this._metrics(state.events, ts, price) || pending.metrics;
    const details = {
      ...pending.metrics,
      fdvUsd: pending.fdvUsd,
      migrationTime: pending.migrationTime,
      signalMigrationAgeMs: pending.migrationAgeMs,
      signalTs: pending.signalTs,
      signalPrice: pending.signalPrice,
      executionTs: ts,
      executionPrice: price,
      executionDelayMs: ts - pending.signalTs,
      maxExecutionPriceDeviationPct: this.maxExecutionPriceDeviationPct,
      preEntryVwap5s: executionMetrics.priceVwap5s,
      preEntryUniqueBuyers3s: executionMetrics.uniqueBuyers3s,
    };
    const entryRisk = evaluateEarlyFlowEntryRisk(details, this.riskConfig);
    details.entryRiskScore = entryRisk.score;
    details.entryRiskRejectScore = entryRisk.rejectScore;
    details.entryRiskBlocked = entryRisk.blocked;
    details.entryRiskReasons = entryRisk.reasons;
    const signal = {
      mint: swap.mint,
      symbol: state.symbol || swap.symbol,
      signature: swap.signature || `early-flow:${swap.mint}:${ts}`,
      ts,
      slot: Number(swap.slot) || 0,
      priceAfter: pending.signalPrice,
      priceBefore: pending.signalPrice,
      executionPrice: price,
      poolAddress: swap.poolAddress || state.poolAddress,
      poolQuoteAfter: Number(swap.poolQuoteAfter) || null,
      sellSol: 0,
      priceImpactPct: 0,
      _aggregated: true,
      _earlyFlow: true,
      _earlyFlowDetails: details,
    };

    monitor.inc('EarlyFlowEntry.signalsEmitted', 1, 'EarlyFlowEntry');
    console.log(
      `[EarlyFlowEntry] EXECUTABLE ${signal.symbol || swap.mint.slice(0, 6)} ` +
        `delay=${details.executionDelayMs}ms ` +
        `priceMove=${((price / pending.signalPrice - 1) * 100).toFixed(2)}% ` +
        `risk=${entryRisk.score}/${entryRisk.rejectScore}` +
        (entryRisk.reasons.length ? ` [${entryRisk.reasons.join(',')}]` : ''),
    );
    this.emit('earlyFlowSignal', signal);
  }

  _trustedMarket(swap) {
    let market = null;
    try {
      market = this.marketProvider?.(swap.mint) || null;
    } catch (err) {
      monitor.recordError('EarlyFlowEntry', err, {
        phase: 'market_provider',
        mint: swap.mint,
      });
      return null;
    }
    const now = Date.now();
    const fetchedAt = Number(market?.fetchedAt);
    const marketPrice = Number(market?.priceSol);
    const swapPrice = Number(swap.price);
    const fdvUsd = Number(market?.fdvUsd);
    const poolQuoteSol = Number(market?.poolQuoteSol);
    if (
      !Number.isFinite(fetchedAt) ||
      fetchedAt <= 0 ||
      now - fetchedAt > this.marketFreshMs ||
      !Number.isFinite(marketPrice) ||
      marketPrice <= 0 ||
      !Number.isFinite(fdvUsd) ||
      fdvUsd <= 0 ||
      !Number.isFinite(poolQuoteSol) ||
      poolQuoteSol <= 0
    ) {
      return null;
    }
    if (swap.poolAddress && market.poolAddress && swap.poolAddress !== market.poolAddress) {
      return null;
    }
    const priceGapPct = Math.abs((marketPrice / swapPrice - 1) * 100);
    if (!Number.isFinite(priceGapPct) || priceGapPct > 0.5) return null;
    return market;
  }

  _metrics(events, ts, currentPrice) {
    const priceCutoff = ts - this.priceWindowMs;
    let baseline = null;
    for (const event of events) {
      if (event.ts <= priceCutoff) baseline = event;
      else break;
    }
    if (!baseline) baseline = events.find((event) => event.ts >= priceCutoff) || null;
    if (!baseline || !(baseline.price > 0)) return null;

    const flowEvents = events.filter((event) => event.ts > ts - this.netFlowWindowMs);
    const activityEvents = events.filter((event) => event.ts > ts - this.activityWindowMs);
    const buys = activityEvents.filter((event) => event.side === 'BUY');
    const totalBuySol = buys.reduce((sum, event) => sum + event.solVolume, 0);
    const totalVolumeSol = activityEvents.reduce(
      (sum, event) => sum + event.solVolume,
      0,
    );
    const weightedPrice = activityEvents.reduce(
      (sum, event) => sum + (event.price * event.solVolume),
      0,
    );
    const largestBuySol = buys.reduce(
      (max, event) => Math.max(max, event.solVolume),
      0,
    );
    const uniqueBuyers = new Set(
      buys.map((event) => event.signer).filter(Boolean),
    ).size;
    const uniqueBuyers3s = new Set(
      events
        .filter((event) => event.ts > ts - 3_000 && event.side === 'BUY')
        .map((event) => event.signer)
        .filter(Boolean),
    ).size;
    const netFlow1sSol = flowEvents.reduce(
      (sum, event) => sum + (event.side === 'BUY' ? event.solVolume : -event.solVolume),
      0,
    );

    return {
      priceChangePct: ((currentPrice - baseline.price) / baseline.price) * 100,
      priceBaseline: baseline.price,
      netFlow1sSol,
      uniqueBuyers5s: uniqueBuyers,
      tradeCount5s: activityEvents.length,
      buySol5s: totalBuySol,
      largestBuySol5s: largestBuySol,
      largestBuyShare5s: totalBuySol > 0 ? largestBuySol / totalBuySol : Infinity,
      priceVwap5s: totalVolumeSol > 0 ? weightedPrice / totalVolumeSol : currentPrice,
      uniqueBuyers3s,
    };
  }

  _migrationTime(state, mint) {
    if (state.migrationTime > 0) return state.migrationTime;
    const token = this.tokenRegistry?.getToken?.(mint);
    const migrationTime = Number(token?.migration_time);
    if (Number.isFinite(migrationTime) && migrationTime > 0) {
      state.migrationTime = migrationTime;
      state.symbol = token.symbol || state.symbol;
      state.poolAddress = token.pool_address || state.poolAddress;
      return migrationTime;
    }
    return null;
  }

  _stateOf(swap) {
    let state = this.states.get(swap.mint);
    if (!state) {
      state = {
        migrationTime: 0,
        symbol: swap.symbol || null,
        poolAddress: swap.poolAddress || null,
        events: [],
        pending: null,
        done: false,
      };
      this.states.set(swap.mint, state);
    } else {
      state.symbol = swap.symbol || state.symbol;
      state.poolAddress = swap.poolAddress || state.poolAddress;
    }
    return state;
  }

  _prune(state, now) {
    const keepMs = Math.max(this.priceWindowMs, this.activityWindowMs, this.netFlowWindowMs) +
      5_000;
    const cutoff = now - keepMs;
    while (state.events.length && state.events[0].ts < cutoff) state.events.shift();
  }

  _debug(mint, message) {
    if (this.debug) console.log(`[EarlyFlowEntry] ${mint.slice(0, 6)} ${message}`);
  }
}

module.exports = EarlyFlowEntryTracker;
