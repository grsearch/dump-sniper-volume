'use strict';

require('dotenv').config({ override: true });

const Database = require('better-sqlite3');
const { config } = require('../src/config');
const RsiCalculator = require('../src/core/RsiCalculator');

function numList(name, fallback) {
  const raw = process.env[name];
  if (!raw) return fallback;
  return raw.split(',').map((x) => Number(x.trim())).filter((x) => Number.isFinite(x));
}

function envNumber(name, fallback) {
  const raw = process.env[name];
  return raw == null || raw === '' ? Number(fallback) : Number(raw);
}

function uniqueCount(items, field) {
  const s = new Set();
  for (const item of items) {
    if (item[field]) s.add(item[field]);
  }
  return s.size;
}

function sum(items, field) {
  return items.reduce((acc, item) => acc + (Number.isFinite(item[field]) ? item[field] : 0), 0);
}

function largestVolumeShare(items, total, field) {
  const volumes = new Map();
  for (const item of items) {
    const key = item[field] || '__unknown__';
    volumes.set(key, (volumes.get(key) || 0) + (Number.isFinite(item.solVolume) ? item.solVolume : 0));
  }
  const largest = volumes.size > 0 ? Math.max(...volumes.values()) : 0;
  return largest / Math.max(total, 0.001);
}

function windowStats(events, idx, windowMs) {
  const now = events[idx].ts;
  const rows = [];
  for (let i = idx; i >= 0; i--) {
    if (now - events[i].ts > windowMs) break;
    rows.push(events[i]);
  }
  rows.reverse();

  const buys = rows.filter((x) => x.side === 'BUY');
  const sells = rows.filter((x) => x.side === 'SELL');
  const buySol = sum(buys, 'solVolume');
  const sellSol = sum(sells, 'solVolume');
  const volumeSol = buySol + sellSol;
  const first = rows[0] || null;
  const last = rows[rows.length - 1] || null;
  const firstPrice = first ? first.price : 0;
  const lastPrice = last ? last.price : 0;

  return {
    tradeCount: rows.length,
    buyCount: buys.length,
    buySol,
    sellSol,
    volumeSol,
    buySellRatio: buySol / Math.max(sellSol, 0.001),
    imbalance: (buySol - sellSol) / Math.max(volumeSol, 0.001),
    uniqueBuyers: uniqueCount(buys, 'signer'),
    uniqueTraders: uniqueCount(rows, 'signer'),
    largestBuyerShare: largestVolumeShare(buys, buySol, 'signer'),
    maxSingleBuyImpactPct: buys.reduce(
      (maxImpact, buy) => Math.max(maxImpact, Number.isFinite(buy.priceChangePct) ? buy.priceChangePct : 0),
      0,
    ),
    priceChangePct: firstPrice > 0 && lastPrice > 0 ? ((lastPrice - firstPrice) / firstPrice) * 100 : 0,
    lastSide: last ? last.side : null,
  };
}

function passesEntry(cfg, s5, s15, s30, s60, event) {
  const poolQuoteAfter = event.poolQuoteAfter;
  if (poolQuoteAfter && cfg.minPoolQuoteSol > 0 && poolQuoteAfter < cfg.minPoolQuoteSol) return false;
  if (cfg.entryMode === 'VOLUME_RATIO_1M') {
    if (cfg.minTrades1m > 0 && s60.tradeCount < cfg.minTrades1m) return false;
    if (s60.volumeSol < cfg.minVolume1mSol) return false;
    if (s60.buySellRatio < cfg.minRatio1m) return false;
    if (cfg.rsi1mEnabled) {
      const minBars = Math.max(cfg.rsi1mPeriod + 1, cfg.rsi1mMinBars);
      if (
        event.rsi1mBars < minBars ||
        !Number.isFinite(event.rsi1mClosed) ||
        !Number.isFinite(event.rsi1mLive)
      ) return false;
      if (event.rsi1mClosed >= cfg.rsi1mMax || event.rsi1mLive >= cfg.rsi1mMax) return false;
    }
    if (s5.buyCount < cfg.confirmMinBuyTrades5s) return false;
    if (s5.uniqueBuyers < cfg.confirmMinUniqueBuyers5s) return false;
    if (s5.buySellRatio < cfg.confirmMinRatio5s) return false;
    if (s5.largestBuyerShare > cfg.confirmMaxBuyerShare5s) return false;
    if (s5.priceChangePct > cfg.confirmMaxPriceRise5sPct) return false;
    if (s5.maxSingleBuyImpactPct > cfg.confirmMaxSingleBuyImpactPct) return false;
    if (s60.lastSide !== 'BUY') return false;
    return true;
  }

  if (s60.tradeCount < cfg.minTrades60s) return false;
  if (s60.volumeSol < cfg.minVolume60sSol) return false;
  if (s60.uniqueTraders < cfg.minUniqueTraders60s) return false;
  if (s30.tradeCount < cfg.minTrades30s) return false;
  if (s30.volumeSol < cfg.minVolume30sSol) return false;
  if (s30.buySellRatio < cfg.minRatio30s) return false;
  if (s30.priceChangePct < cfg.minPriceChange30sPct) return false;
  if (s60.priceChangePct < cfg.minPriceChange60sPct) return false;
  if (s15.tradeCount < cfg.minTrades15s) return false;
  if (s15.volumeSol < cfg.minVolume15sSol) return false;
  if (s15.buySellRatio < cfg.minRatio15s) return false;
  if (s15.imbalance < cfg.minImbalance15s) return false;
  if (s15.uniqueBuyers < cfg.minUniqueBuyers15s) return false;
  if (s15.priceChangePct < cfg.minPriceChange15sPct) return false;
  if (s5.tradeCount < cfg.minTrades5s) return false;
  if (s5.volumeSol < cfg.minVolume5sSol) return false;
  if (s5.buySellRatio < cfg.minRatio5s) return false;
  if (s5.imbalance < cfg.minImbalance5s) return false;
  if (s5.uniqueBuyers < cfg.minUniqueBuyers5s) return false;
  if (s5.lastSide !== 'BUY') return false;
  if (s5.priceChangePct < cfg.minPriceChange5sPct) return false;
  if (s5.priceChangePct > cfg.maxPriceChange5sPct) return false;
  if (s30.priceChangePct > cfg.maxPriceChange30sPct) return false;
  if (s60.priceChangePct > cfg.maxPriceChange60sPct) return false;
  return true;
}

function postEntryWindowStats(events, entryIdx, idx, windowMs) {
  const now = events[idx].ts;
  const rows = [];
  for (let i = idx; i > entryIdx; i--) {
    if (now - events[i].ts > windowMs) break;
    rows.push(events[i]);
  }
  const buys = rows.filter((x) => x.side === 'BUY');
  const sells = rows.filter((x) => x.side === 'SELL');
  const buySol = sum(buys, 'solVolume');
  const sellSol = sum(sells, 'solVolume');
  return {
    buySol,
    sellSol,
    volumeSol: buySol + sellSol,
    sellBuyRatio: sellSol / Math.max(buySol, 0.001),
  };
}

function simulateExit(events, entryIdx, model) {
  const entry = events[entryIdx];
  const entryPrice = entry.price;
  const deadline = model.maxHoldMs > 0 ? entry.ts + model.maxHoldMs : Infinity;
  let hwm = entryPrice;
  let trailingArmed = false;
  let last = entry;

  for (let i = entryIdx + 1; i < events.length; i++) {
    const ev = events[i];
    if (ev.ts > deadline) break;
    if (!Number.isFinite(ev.price) || ev.price <= 0) continue;
    last = ev;
    if (ev.price > hwm) hwm = ev.price;

    const pnlPct = ((ev.price - entryPrice) / entryPrice) * 100;
    if (model.flowExitEnabled && ev.side === 'SELL' && ev.ts - entry.ts >= model.flowExitMinHoldMs) {
      const st = postEntryWindowStats(events, entryIdx, i, model.flowExitWindowMs);
      if (
        st.volumeSol >= model.flowExitMinVolumeSol &&
        st.sellSol > st.buySol &&
        st.sellBuyRatio >= model.flowExitSellBuyRatio
      ) {
        return { exitIdx: i, exitTs: ev.ts, pnlPct, reason: 'FLOW_REVERSAL_EXIT' };
      }
    }

    if (model.takeProfitPct > 0 && pnlPct >= model.takeProfitPct) {
      return { exitIdx: i, exitTs: ev.ts, pnlPct, reason: 'TAKE_PROFIT' };
    }
    if (model.stopLossPct < 0 && pnlPct <= model.stopLossPct) {
      return { exitIdx: i, exitTs: ev.ts, pnlPct, reason: 'FIXED_STOP_LOSS' };
    }

    const peakPnlPct = ((hwm - entryPrice) / entryPrice) * 100;
    if (model.trailingActivatePct > 0 && peakPnlPct >= model.trailingActivatePct) trailingArmed = true;
    if (trailingArmed && model.trailingDrawdownPct > 0) {
      const drawdownPct = ((hwm - ev.price) / hwm) * 100;
      if (drawdownPct >= model.trailingDrawdownPct) {
        return { exitIdx: i, exitTs: ev.ts, pnlPct, reason: 'TRAILING_STOP' };
      }
    }

    if (
      model.rsiExitEnabled &&
      !trailingArmed &&
      ev.rsi1mBars >= model.rsiExitMinBars &&
      Number.isFinite(ev.rsi1mLive) &&
      ev.rsi1mLive > model.rsiExitThreshold
    ) {
      return { exitIdx: i, exitTs: ev.ts, pnlPct, reason: 'RSI_1M_EXIT' };
    }
  }

  if (model.maxHoldMs <= 0) return null;
  const pnlPct = last && last.price > 0 ? ((last.price - entryPrice) / entryPrice) * 100 : 0;
  return { exitIdx: Math.max(entryIdx, events.indexOf(last)), exitTs: last.ts, pnlPct, reason: 'TIMEOUT' };
}

function makeConfigs() {
  const base = {
    entryMode: config.activityFlow.entryMode || 'VOLUME_RATIO_1M',
    minVolume1mSol: config.activityFlow.minVolume1mSol,
    minRatio1m: config.activityFlow.minRatio1m,
    minTrades1m: config.activityFlow.minTrades1m,
    rsi1mEnabled: config.activityFlow.rsi1mEnabled,
    rsi1mPeriod: config.activityFlow.rsi1mPeriod,
    rsi1mMax: config.activityFlow.rsi1mMax,
    rsi1mMinBars: config.activityFlow.rsi1mMinBars,
    confirmMinBuyTrades5s: config.activityFlow.confirmMinBuyTrades5s,
    confirmMinUniqueBuyers5s: config.activityFlow.confirmMinUniqueBuyers5s,
    confirmMinRatio5s: config.activityFlow.confirmMinRatio5s,
    confirmMaxBuyerShare5s: config.activityFlow.confirmMaxBuyerShare5s,
    confirmMaxPriceRise5sPct: config.activityFlow.confirmMaxPriceRise5sPct,
    confirmMaxSingleBuyImpactPct: config.activityFlow.confirmMaxSingleBuyImpactPct,
    minTrades60s: config.activityFlow.minTrades60s,
    minVolume60sSol: config.activityFlow.minVolume60sSol,
    minUniqueTraders60s: config.activityFlow.minUniqueTraders60s,
    minTrades30s: config.activityFlow.minTrades30s,
    minVolume30sSol: config.activityFlow.minVolume30sSol,
    minTrades15s: config.activityFlow.minTrades15s,
    minVolume15sSol: config.activityFlow.minVolume15sSol,
    minUniqueBuyers15s: config.activityFlow.minUniqueBuyers15s,
    minPriceChange15sPct: config.activityFlow.minPriceChange15sPct,
    minPriceChange30sPct: config.activityFlow.minPriceChange30sPct,
    minPriceChange60sPct: config.activityFlow.minPriceChange60sPct,
    minUniqueBuyers5s: config.activityFlow.minUniqueBuyers5s,
    minPriceChange5sPct: config.activityFlow.minPriceChange5sPct,
    maxPriceChange5sPct: config.activityFlow.maxPriceChange5sPct,
    maxPriceChange30sPct: config.activityFlow.maxPriceChange30sPct,
    maxPriceChange60sPct: config.activityFlow.maxPriceChange60sPct,
    minPoolQuoteSol: config.activityFlow.minPoolQuoteSol,
  };

  if (base.entryMode === 'VOLUME_RATIO_1M') {
    const grids = {
      minVolume1mSol: numList('BT_MIN_VOLUME_1M_SOL', [
        Math.max(1, base.minVolume1mSol * 0.75),
        base.minVolume1mSol,
        base.minVolume1mSol * 1.25,
        base.minVolume1mSol * 1.5,
      ]),
      minRatio1m: numList('BT_MIN_RATIO_1M', [1.2, 1.35, 1.5]),
    };

    let configs = [base];
    for (const [key, values] of Object.entries(grids)) {
      const next = [];
      for (const cfg of configs) {
        for (const value of values) next.push({ ...cfg, [key]: value });
      }
      configs = next;
    }
    return configs;
  }

  const grids = {
    minRatio30s: numList('BT_MIN_RATIO_30S', [1.0, 1.05, 1.15]),
    minRatio15s: numList('BT_MIN_RATIO_15S', [1.35, 1.45, 1.6]),
    minImbalance15s: numList('BT_MIN_IMBALANCE_15S', [0.16, 0.2, 0.25]),
    minTrades5s: numList('BT_MIN_TRADES_5S', [4, 5, 6]),
    minVolume5sSol: numList('BT_MIN_VOLUME_5S_SOL', [2, 2.5, 3]),
    minRatio5s: numList('BT_MIN_RATIO_5S', [1.35, 1.4, 1.5]),
    minImbalance5s: numList('BT_MIN_IMBALANCE_5S', [0.2, 0.25, 0.3]),
  };

  let configs = [base];
  for (const [key, values] of Object.entries(grids)) {
    const next = [];
    for (const cfg of configs) {
      for (const value of values) next.push({ ...cfg, [key]: value });
    }
    configs = next;
  }
  return configs;
}

function evaluate(cfg, byMint, model) {
  let trades = 0;
  let wins = 0;
  let pnlPctSum = 0;
  let openPositions = 0;
  const reasons = {};

  for (const events of byMint.values()) {
    let cooldownUntil = 0;
    for (let i = 0; i < events.length; i++) {
      const ev = events[i];
      if (ev.side !== 'BUY' || ev.ts < cooldownUntil || !ev.price) continue;

      const s5 = windowStats(events, i, 5_000);
      const s15 = windowStats(events, i, 15_000);
      const s30 = windowStats(events, i, 30_000);
      const s60 = windowStats(events, i, 60_000);
      if (!passesEntry(cfg, s5, s15, s30, s60, ev)) continue;

      const exit = simulateExit(events, i, model);
      if (!exit) {
        openPositions += 1;
        break;
      }
      trades += 1;
      if (exit.pnlPct > 0) wins += 1;
      pnlPctSum += exit.pnlPct;
      reasons[exit.reason] = (reasons[exit.reason] || 0) + 1;
      cooldownUntil = exit.exitTs + model.cooldownMs;
      i = Math.max(i, exit.exitIdx);
    }
  }

  return {
    trades,
    wins,
    winRate: trades ? (wins / trades) * 100 : 0,
    pnlPctSum,
    avgPnlPct: trades ? pnlPctSum / trades : 0,
    approxSol: (pnlPctSum / 100) * model.positionSol,
    openPositions,
    reasons,
  };
}

function main() {
  const db = new Database(config.storage.dbPath, { readonly: true });
  const sinceMs = Number(process.env.BT_SINCE_MS || (Date.now() - 24 * 60 * 60 * 1000));
  const untilMs = Number(process.env.BT_UNTIL_MS || Date.now());
  let rows;
  try {
    rows = db.prepare(`
      SELECT mint, symbol, signer, side, sol_volume AS solVolume, price, price_before AS priceBefore,
             price_change_pct AS priceChangePct, ts, slot, signature, pool_quote_after AS poolQuoteAfter
      FROM swap_events
      WHERE ts >= ? AND ts < ?
      ORDER BY mint, ts ASC
    `).all(sinceMs, untilMs).map((row) => ({
      ...row,
      side: String(row.side || '').toUpperCase(),
    }));
  } catch (err) {
    if (String(err.message || '').includes('no such table')) {
      console.log('No swap_events table found. Restart the service once with this version to create it.');
      return;
    }
    throw err;
  }

  if (rows.length === 0) {
    console.log('No swap_events found. Keep the service running with SWAP_EVENT_LOG_ENABLED=true first.');
    return;
  }

  const byMint = new Map();
  for (const row of rows) {
    if (!byMint.has(row.mint)) byMint.set(row.mint, []);
    byMint.get(row.mint).push(row);
  }

  for (const [mint, events] of byMint) {
    const rsi = new RsiCalculator({ period60: config.activityFlow.rsi1mPeriod });
    for (const event of events) {
      rsi.feedTick(mint, event.price, event.ts);
      const snapshot = rsi.snapshot(mint);
      event.rsi1m = snapshot?.rsi1mLive ?? null;
      event.rsi1mLive = snapshot?.rsi1mLive ?? null;
      event.rsi1mClosed = snapshot?.rsi1mClosed ?? null;
      event.rsi1mBars = snapshot?.rsi1mClosedBars || 0;
    }
  }

  const model = {
    takeProfitPct: envNumber('BT_TAKE_PROFIT_PCT', config.strategy.takeProfitPct),
    trailingActivatePct: envNumber('BT_TRAILING_ACTIVATE_PCT', config.strategy.trailingActivatePct),
    trailingDrawdownPct: envNumber('BT_TRAILING_DRAWDOWN_PCT', config.strategy.trailingDrawdownPct),
    rsiExitEnabled: String(process.env.BT_RSI_1M_EXIT_ENABLED ?? config.strategy.rsi1mExitEnabled).toLowerCase() === 'true',
    rsiExitThreshold: envNumber('BT_RSI_1M_EXIT_THRESHOLD', config.strategy.rsi1mExitThreshold),
    rsiExitMinBars: config.activityFlow.rsi1mMinBars,
    stopLossPct: envNumber('BT_STOP_LOSS_PCT', config.strategy.fixedStopLossPct),
    maxHoldMs: envNumber('BT_MAX_HOLD_MS', config.strategy.maxHoldMs),
    cooldownMs: Number(process.env.BT_COOLDOWN_MS ?? config.activityFlow.cooldownMs ?? 0),
    positionSol: Number(process.env.BT_POSITION_SOL || config.strategy.positionSizeSol || 1),
    flowExitEnabled: String(process.env.BT_FLOW_EXIT_ENABLED ?? config.strategy.flowReversalExitEnabled ?? 'true').toLowerCase() !== 'false',
    flowExitWindowMs: Number(process.env.BT_FLOW_EXIT_WINDOW_MS || config.strategy.flowReversalExitWindowMs || 60_000),
    flowExitSellBuyRatio: Number(process.env.BT_FLOW_EXIT_SELL_BUY_RATIO || config.strategy.flowReversalExitSellBuyRatio1m || 1.35),
    flowExitMinVolumeSol: Number(process.env.BT_FLOW_EXIT_MIN_VOLUME_SOL || config.strategy.flowReversalExitMinVolume1mSol || 5),
    flowExitMinHoldMs: Number(process.env.BT_FLOW_EXIT_MIN_HOLD_MS || config.strategy.flowReversalExitMinHoldMs || 10_000),
  };

  const results = makeConfigs()
    .map((cfg) => ({ cfg, result: evaluate(cfg, byMint, model) }))
    .filter((x) => x.result.trades >= Number(process.env.BT_MIN_TRADES || 3))
    .sort((a, b) => b.result.approxSol - a.result.approxSol)
    .slice(0, Number(process.env.BT_TOP_N || 20))
    .map(({ cfg, result }) => ({
      trades: result.trades,
      winRate: `${result.winRate.toFixed(1)}%`,
      approxSol: +result.approxSol.toFixed(4),
      avgPct: +result.avgPnlPct.toFixed(2),
      open: result.openPositions,
      mode: cfg.entryMode,
      vol1m: cfg.minVolume1mSol ? +cfg.minVolume1mSol.toFixed(2) : undefined,
      r1m: cfg.minRatio1m,
      buys5: cfg.confirmMinBuyTrades5s,
      buyers5: cfg.confirmMinUniqueBuyers5s,
      topBuyer5: cfg.confirmMaxBuyerShare5s,
      rise5: cfg.confirmMaxPriceRise5sPct,
      impact1: cfg.confirmMaxSingleBuyImpactPct,
      r30: cfg.minRatio30s,
      r15: cfg.minRatio15s,
      imb15: cfg.minImbalance15s,
      tx5: cfg.minTrades5s,
      vol5: cfg.minVolume5sSol,
      r5: cfg.minRatio5s,
      imb5: cfg.minImbalance5s,
      exits: JSON.stringify(result.reasons),
    }));

  console.log(`Loaded ${rows.length} swap events across ${byMint.size} mints.`);
  console.log(
    `Exit model: TP ${model.takeProfitPct}%, trailing ${model.trailingActivatePct}/${model.trailingDrawdownPct}%, ` +
    `flowExit ${model.flowExitEnabled ? 'enabled' : 'disabled'}, ` +
    `stop ${model.stopLossPct < 0 ? model.stopLossPct + '%' : 'disabled'}, ` +
    `maxHold ${model.maxHoldMs > 0 ? Math.round(model.maxHoldMs / 1000) + 's' : 'disabled'}.`,
  );
  console.table(results);
}

main();
