'use strict';

const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function unixMs(value, fallback = Date.now()) {
  const number = finiteNumber(value);
  if (!(number > 0)) return fallback;
  return number < 10_000_000_000 ? Math.trunc(number * 1000) : Math.trunc(number);
}

function pctChange(from, to) {
  return from > 0 && Number.isFinite(to) ? ((to - from) / from) * 100 : null;
}

function summarizeEvents(events) {
  const ordered = [...events].sort((left, right) => left.ts - right.ts);
  const buys = ordered.filter((event) => event.side === 'BUY');
  const sells = ordered.filter((event) => event.side === 'SELL');
  const buySol = buys.reduce((sum, event) => sum + event.solVolume, 0);
  const sellSol = sells.reduce((sum, event) => sum + event.solVolume, 0);
  const buyers = new Set(buys.map((event) => event.signer).filter(Boolean));
  const sellers = new Set(sells.map((event) => event.signer).filter(Boolean));
  const signatures = new Set(ordered.map((event) => event.signature).filter(Boolean));
  const buyerTotals = new Map();
  for (const event of buys) {
    if (!event.signer) continue;
    buyerTotals.set(event.signer, (buyerTotals.get(event.signer) || 0) + event.solVolume);
  }
  const largestBuy = buys.reduce((max, event) => Math.max(max, event.solVolume), 0);
  const topBuyerSol = Math.max(0, ...buyerTotals.values());
  const prices = ordered.map((event) => event.price).filter((price) => price > 0);
  const poolQuotes = ordered
    .map((event) => event.poolQuoteAfter)
    .filter((value) => value > 0);
  const fdvs = ordered.map((event) => event.fdvUsd).filter((value) => value > 0);
  let peakPrice = null;
  let maxDrawdownPct = null;
  let maxSingleTradeDropPct = null;
  for (let index = 0; index < prices.length; index++) {
    const price = prices[index];
    peakPrice = peakPrice == null ? price : Math.max(peakPrice, price);
    const drawdown = pctChange(peakPrice, price);
    if (drawdown != null) {
      maxDrawdownPct = maxDrawdownPct == null ? drawdown : Math.min(maxDrawdownPct, drawdown);
    }
    if (index > 0) {
      const tradeChange = pctChange(prices[index - 1], price);
      maxSingleTradeDropPct = maxSingleTradeDropPct == null
        ? tradeChange
        : Math.min(maxSingleTradeDropPct, tradeChange);
    }
  }

  return {
    eventCount: ordered.length,
    signatureCount: signatures.size,
    buyCount: buys.length,
    sellCount: sells.length,
    buySol,
    sellSol,
    netFlowSol: buySol - sellSol,
    buySellRatio: sellSol > 0 ? buySol / sellSol : (buySol > 0 ? null : 0),
    uniqueBuyers: buyers.size,
    uniqueSellers: sellers.size,
    largestBuyShare: buySol > 0 ? largestBuy / buySol : null,
    topBuyerConcentration: buySol > 0 ? topBuyerSol / buySol : null,
    firstPrice: prices[0] ?? null,
    lastPrice: prices.at(-1) ?? null,
    peakPrice: prices.length ? Math.max(...prices) : null,
    troughPrice: prices.length ? Math.min(...prices) : null,
    priceReturnPct: prices.length ? pctChange(prices[0], prices.at(-1)) : null,
    peakReturnPct: prices.length ? pctChange(prices[0], Math.max(...prices)) : null,
    troughReturnPct: prices.length ? pctChange(prices[0], Math.min(...prices)) : null,
    maxDrawdownPct,
    maxSingleTradeDropPct,
    firstPoolQuoteSol: poolQuotes[0] ?? null,
    lastPoolQuoteSol: poolQuotes.at(-1) ?? null,
    minPoolQuoteSol: poolQuotes.length ? Math.min(...poolQuotes) : null,
    maxPoolQuoteSol: poolQuotes.length ? Math.max(...poolQuotes) : null,
    poolQuoteChangePct: poolQuotes.length
      ? pctChange(poolQuotes[0], poolQuotes.at(-1))
      : null,
    firstFdvUsd: fdvs[0] ?? null,
    lastFdvUsd: fdvs.at(-1) ?? null,
    minFdvUsd: fdvs.length ? Math.min(...fdvs) : null,
    maxFdvUsd: fdvs.length ? Math.max(...fdvs) : null,
  };
}

class MigrationRugTelemetry {
  constructor({ tradeLogger, scanner, settings = {} } = {}) {
    this.tradeLogger = tradeLogger;
    this.scanner = scanner;
    this.enabled = settings.rugTelemetryEnabled !== false;
    this.windowMs = Math.max(1_000, finiteNumber(settings.rugTelemetryWindowMs) || 10_000);
    this.preSlots = Math.max(1, Math.trunc(finiteNumber(settings.rugTelemetryPreSlots) || 32));
    this.maxEvents = Math.max(100, Math.trunc(finiteNumber(settings.rugTelemetryMaxEvents) || 2_000));
    this.maxConcurrentScans = Math.max(
      1,
      Math.trunc(finiteNumber(settings.rugTelemetryMaxConcurrentScans) || 1),
    );
    this.states = new Map();
    this.finalizeQueue = [];
    this.activeFinalizes = 0;
  }

  observeMigration(migration) {
    if (!this.enabled || !migration?.mint) return;
    const migrationTime = unixMs(migration.migrationTime);
    const previous = this.states.get(migration.mint);
    if (previous) {
      if (migrationTime < previous.migrationTime) {
        previous.migration = { ...migration, migrationTime };
        previous.migrationTime = migrationTime;
      }
      return previous;
    }
    const state = {
      migration: { ...migration, migrationTime },
      migrationTime,
      token: null,
      screening: null,
      accepted: false,
      events: [],
      cleanupTimer: null,
      finalizeTimer: null,
    };
    const finalizeDelay = Math.max(
      0,
      state.migrationTime + this.windowMs - Date.now(),
      3_000,
    );
    state.finalizeTimer = setTimeout(() => {
      this._enqueueFinalize(migration.mint);
    }, finalizeDelay);
    if (state.finalizeTimer.unref) state.finalizeTimer.unref();
    state.cleanupTimer = setTimeout(() => {
      const current = this.states.get(migration.mint);
      if (current === state) this.states.delete(migration.mint);
    }, 60_000);
    if (state.cleanupTimer.unref) state.cleanupTimer.unref();
    this.states.set(migration.mint, state);
    return state;
  }

  markAccepted({ token, migration, screening } = {}) {
    if (!this.enabled || !migration?.mint) return;
    const state = this.states.get(migration.mint);
    if (!state) return;
    state.accepted = true;
    state.token = token || null;
    state.screening = screening || null;
  }

  _enqueueFinalize(mint) {
    if (!this.states.has(mint) || this.finalizeQueue.includes(mint)) return;
    this.finalizeQueue.push(mint);
    this._drainFinalizeQueue();
  }

  _drainFinalizeQueue() {
    while (this.activeFinalizes < this.maxConcurrentScans && this.finalizeQueue.length > 0) {
      const mint = this.finalizeQueue.shift();
      this.activeFinalizes++;
      this.finalizeNow(mint)
        .catch((err) => {
          monitor.recordError('MigrationRugTelemetry', err, {
            phase: 'finalize',
            mint,
          });
        })
        .finally(() => {
          this.activeFinalizes--;
          this._drainFinalizeQueue();
        });
    }
  }

  handleSwap(swap) {
    if (!this.enabled || !swap?.mint) return;
    const state = this.states.get(swap.mint);
    if (!state) return;
    const ts = unixMs(swap.ts || swap.receivedAt);
    if (ts < state.migrationTime - this.windowMs || ts > state.migrationTime + this.windowMs) return;
    if (state.events.length >= this.maxEvents) return;
    const side = String(swap.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;
    state.events.push({
      ts,
      side,
      signer: swap.signer || null,
      signature: swap.signature || null,
      solVolume: Math.max(0, finiteNumber(swap.solVolume) || 0),
      price: finiteNumber(swap.price),
      poolQuoteAfter: finiteNumber(swap.poolQuoteAfter),
      fdvUsd: finiteNumber(swap.fdvUsd),
    });
  }

  async finalizeNow(mint) {
    const state = this.states.get(mint);
    if (!state) return null;
    if (state.finalizeTimer) clearTimeout(state.finalizeTimer);
    const startTimeMs = state.migrationTime - this.windowMs;
    const endTimeMs = state.migrationTime + this.windowMs;
    const persisted = this.tradeLogger.getSwapEventsForMintInRange?.(
      mint,
      startTimeMs,
      endTimeMs,
    ) || [];
    const normalizePersisted = (event) => ({
      ts: unixMs(event.ts),
      side: String(event.side || '').toUpperCase(),
      signer: event.signer || null,
      signature: event.signature || null,
      solVolume: Math.max(0, finiteNumber(event.sol_volume ?? event.solVolume) || 0),
      price: finiteNumber(event.price),
      poolQuoteAfter: finiteNumber(event.pool_quote_after ?? event.poolQuoteAfter),
      fdvUsd: finiteNumber(event.fdv_usd ?? event.fdvUsd),
      source: event.source || 'swap_events',
    });

    let audit = {
      allowed: true,
      skipped: true,
      reason: 'scanner_unavailable',
      matches: [],
      activityEvents: [],
    };
    if (this.scanner) {
      const scan = (slotRadius) => this.scanner.audit(state.migration, {
        force: true,
        refreshDetectionSlot: true,
        preSlots: slotRadius,
        postSlots: slotRadius,
        maxMatches: 500,
        startTimeMs,
        endTimeMs,
      });
      audit = await scan(this.preSlots);
      const firstScanned = finiteNumber(audit.summary?.firstScannedBlockTimeMs);
      const lastScanned = finiteNumber(audit.summary?.lastScannedBlockTimeMs);
      const needsWiderScan = audit.reasonCode !== 'audit_incomplete' && (
        firstScanned == null || firstScanned > startTimeMs + 1_000 ||
        lastScanned == null || lastScanned < endTimeMs - 1_000
      );
      if (needsWiderScan) {
        const expandedSlots = Math.max(
          this.preSlots + 1,
          this.preSlots * 2,
          Math.ceil(this.windowMs / 250) + 4,
        );
        audit = await scan(expandedSlots);
      }
    }
    const replayed = Array.isArray(audit.activityEvents) ? audit.activityEvents : [];
    const merged = [
      ...state.events.map((event) => ({ ...event, source: event.source || 'live' })),
      ...persisted.map(normalizePersisted),
      ...replayed,
    ];
    const deduped = new Map();
    for (const event of merged) {
      const ts = finiteNumber(event.ts);
      if (ts == null || ts < startTimeMs || ts > endTimeMs) continue;
      const key = event.signature
        ? `${event.signature}:${event.side}`
        : `no-signature:${event.side}:${event.signer || ''}:${ts}:${event.solVolume || 0}`;
      const existing = deduped.get(key);
      if (!existing || existing.source === 'chain_replay') deduped.set(key, { ...event, ts });
    }
    const inRange = [...deduped.values()].sort((a, b) => a.ts - b.ts);
    const windows = {
      pre10s: summarizeEvents(inRange.filter((event) => event.ts < state.migrationTime)),
      post1s: summarizeEvents(inRange.filter(
        (event) => event.ts >= state.migrationTime && event.ts <= state.migrationTime + 1_000,
      )),
      post3s: summarizeEvents(inRange.filter(
        (event) => event.ts >= state.migrationTime && event.ts <= state.migrationTime + 3_000,
      )),
      post5s: summarizeEvents(inRange.filter(
        (event) => event.ts >= state.migrationTime && event.ts <= state.migrationTime + 5_000,
      )),
      post10s: summarizeEvents(inRange.filter((event) => event.ts >= state.migrationTime)),
    };
    const matches = Array.isArray(audit.matches) ? audit.matches : [];
    const phaseFor = (match) => {
      const matchTime = finiteNumber(match.blockTimeMs);
      if (matchTime != null) {
        if (matchTime < state.migrationTime) return 'pre';
        if (matchTime > state.migrationTime + 999) return 'post';
        return 'same_second';
      }
      const slot = finiteNumber(match.slot);
      if (slot == null || slot === finiteNumber(state.migration.slot)) return 'same_slot';
      return slot < Number(state.migration.slot) ? 'pre' : 'post';
    };
    const chain = {
      auditIncomplete: audit.reasonCode === 'audit_incomplete',
      mintToCount: matches.filter((match) => match.type === 'mint_to').length,
      largeTransferCount: matches.filter((match) => match.type === 'large_transfer').length,
      sameTxBuyCount: matches.filter((match) => match.type === 'same_tx_buy_migrate').length,
      maxLargeTransferSupplyPct: Math.max(
        0,
        ...matches.map((match) => finiteNumber(match.supplyPct) || 0),
      ),
      matchesByPhase: matches.reduce((counts, match) => {
        const phase = phaseFor(match);
        counts[phase] = (counts[phase] || 0) + 1;
        return counts;
      }, {}),
      matches,
      summary: audit.summary || null,
    };
    const offsets = inRange.map((event) => event.ts - state.migrationTime);
    const firstScannedBlockTimeMs = finiteNumber(audit.summary?.firstScannedBlockTimeMs);
    const lastScannedBlockTimeMs = finiteNumber(audit.summary?.lastScannedBlockTimeMs);
    const preWindowScanned = firstScannedBlockTimeMs != null &&
      firstScannedBlockTimeMs <= startTimeMs + 1_000;
    const postWindowScanned = lastScannedBlockTimeMs != null &&
      lastScannedBlockTimeMs >= endTimeMs - 1_000;
    const coverageComplete = !chain.auditIncomplete && preWindowScanned && postWindowScanned;
    const incompleteReasons = [];
    if (chain.auditIncomplete) incompleteReasons.push('chain_audit_incomplete');
    if (!preWindowScanned) incompleteReasons.push('pre_window_not_scanned');
    if (!postWindowScanned) incompleteReasons.push('post_window_not_scanned');
    const metrics = {
      observeOnly: true,
      migration: {
        slot: finiteNumber(state.migration.slot),
        time: state.migrationTime,
        detectionPath: state.migration.detectionPath || null,
      },
      screening: {
        accepted: state.accepted,
        fdvUsd: finiteNumber(state.screening?.market?.fdv),
        liquidityUsd: finiteNumber(state.screening?.market?.liquidity),
      },
      coverage: {
        eventCount: inRange.length,
        truncated: state.events.length >= this.maxEvents,
        firstOffsetMs: offsets.length ? Math.min(...offsets) : null,
        lastOffsetMs: offsets.length ? Math.max(...offsets) : null,
        hasPreMigrationSwapCoverage: offsets.some((offset) => offset < 0),
        preWindowScanned,
        postWindowScanned,
        complete: coverageComplete,
        incompleteReasons,
        liveEventCount: inRange.filter((event) => event.source === 'live').length,
        persistedEventCount: inRange.filter((event) => event.source === 'swap_events').length,
        replayEventCount: inRange.filter((event) => event.source === 'chain_replay').length,
      },
      windows,
      chain,
    };
    const post = windows.post10s;
    const row = {
      mint,
      symbol: state.token?.symbol || null,
      migrationSignature: state.migration.signature ||
        `${mint}:${state.migration.slot || state.migrationTime}`,
      migrationSlot: finiteNumber(state.migration.slot),
      migrationTime: state.migrationTime,
      capturedAt: Date.now(),
      windowBeforeMs: this.windowMs,
      windowAfterMs: this.windowMs,
      swapEventCount: inRange.length,
      buyCount: post.buyCount,
      sellCount: post.sellCount,
      buySol: post.buySol,
      sellSol: post.sellSol,
      netFlowSol: post.netFlowSol,
      uniqueBuyers: post.uniqueBuyers,
      uniqueSellers: post.uniqueSellers,
      largestBuyShare: post.largestBuyShare,
      priceReturnPct: post.priceReturnPct,
      peakReturnPct: post.peakReturnPct,
      troughReturnPct: post.troughReturnPct,
      maxDrawdownPct: post.maxDrawdownPct,
      poolQuoteChangePct: post.poolQuoteChangePct,
      mintToCount: chain.mintToCount,
      largeTransferCount: chain.largeTransferCount,
      sameTxBuyCount: chain.sameTxBuyCount,
      auditIncomplete: coverageComplete ? 0 : 1,
      metrics,
    };
    this.tradeLogger.logMigrationRiskSnapshot(row);
    monitor.inc('MigrationRugTelemetry.snapshots', 1, 'MigrationRugTelemetry');
    if (state.cleanupTimer) clearTimeout(state.cleanupTimer);
    this.states.delete(mint);
    return row;
  }

  stop() {
    for (const state of this.states.values()) {
      if (state.cleanupTimer) clearTimeout(state.cleanupTimer);
      if (state.finalizeTimer) clearTimeout(state.finalizeTimer);
    }
    this.finalizeQueue.length = 0;
    this.states.clear();
  }
}

module.exports = MigrationRugTelemetry;
module.exports.summarizeEvents = summarizeEvents;
