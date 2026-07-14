'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const HOUR_MS = 60 * 60 * 1000;
const MINUTE_MS = 60 * 1000;
const BAD_SCORE = -1e12;

const FALLBACK = {
  dbPath: './data/sniper.db',
  reportsDir: './reports',
  solPriceUsd: 72,
  positionSol: 1,
  costBps: 100,
  priorityFeeSol: 0.0005,
  baseline: {
    entryMinVolumeUsd: 3000,
    entryMinRatio1m: 1.35,
    entryMinTrades1m: 25,
    entryMinBuyTrades5s: 4,
    entryMinUniqueBuyers5s: 3,
    entryMinRatio5s: 1.10,
    entryMaxBuyerShare5s: 0.50,
    entryMaxRise5sPct: 6,
    entryMaxSingleBuyImpactPct: 4,
    minPoolQuoteSol: 30,
    flowExitRatio: 1.35,
    flowExitMinVolumeSol: 5,
    flowExitMinHoldMs: 10_000,
    trailingActivatePct: 60,
    trailingDrawdownPct: 10,
    takeProfitPct: 200,
    stopLossPct: -25,
    maxHoldMs: 30 * MINUTE_MS,
    stabilizationMs: 5_000,
    stopLossMinHoldMs: 30_000,
    trailingMinHwmAgeMs: 2_000,
  },
};

const PARAM_SPACE = {
  entryMinVolumeUsd: [1500, 2000, 2500, 3000, 3500, 4000, 5000],
  entryMinRatio1m: [1.05, 1.10, 1.20, 1.25, 1.35, 1.50, 1.60],
  entryMinTrades1m: [10, 15, 20, 25, 30, 40, 50],
  entryMinBuyTrades5s: [2, 3, 4, 5, 6, 8],
  entryMinUniqueBuyers5s: [2, 3, 4, 5, 6],
  entryMinRatio5s: [1.0, 1.05, 1.10, 1.20, 1.35, 1.50],
  entryMaxBuyerShare5s: [0.35, 0.45, 0.50, 0.60, 0.70, 0.80],
  entryMaxRise5sPct: [3, 4, 5, 6, 8, 10, 12],
  entryMaxSingleBuyImpactPct: [2, 3, 4, 5, 6, 8, 10],
  flowExitRatio: [1.0, 1.10, 1.20, 1.35, 1.50, 1.75, 2.0],
  flowExitMinVolumeSol: [1, 2, 3, 5, 8, 10, 15],
  flowExitMinHoldMs: [0, 5_000, 10_000, 15_000, 30_000],
  trailingActivatePct: [20, 30, 40, 60, 80, 100],
  trailingDrawdownPct: [5, 8, 10, 12, 15, 20],
  takeProfitPct: [50, 80, 100, 150, 200, 250],
  stopLossPct: [-8, -10, -12, -15, -20, -25],
  maxHoldMs: [60_000, 120_000, 180_000, 300_000, 600_000, 1_800_000],
};

function numberOr(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseArgs(argv) {
  const args = {
    hours: 168,
    iterations: 1200,
    top: 10,
    minTrades: 8,
    minTestTrades: 5,
    seed: 20260715,
    costBps: null,
    priorityFeeSol: null,
    solPriceUsd: null,
    positionSol: null,
    dbPath: null,
    reportsDir: null,
    selfTest: false,
    help: false,
  };
  const names = {
    '--hours': 'hours',
    '--iterations': 'iterations',
    '--top': 'top',
    '--min-trades': 'minTrades',
    '--min-test-trades': 'minTestTrades',
    '--seed': 'seed',
    '--cost-bps': 'costBps',
    '--priority-fee-sol': 'priorityFeeSol',
    '--sol-price-usd': 'solPriceUsd',
    '--position-sol': 'positionSol',
    '--db': 'dbPath',
    '--reports-dir': 'reportsDir',
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === '--self-test') {
      args.selfTest = true;
      continue;
    }
    if (token === '--help' || token === '-h') {
      args.help = true;
      continue;
    }
    const eq = token.indexOf('=');
    const name = eq >= 0 ? token.slice(0, eq) : token;
    if (!names[name]) throw new Error(`Unknown option: ${name}`);
    const raw = eq >= 0 ? token.slice(eq + 1) : argv[++i];
    if (raw == null) throw new Error(`Missing value for ${name}`);
    const key = names[name];
    args[key] = key === 'dbPath' || key === 'reportsDir' ? raw : numberOr(raw, NaN);
    if (typeof args[key] === 'number' && !Number.isFinite(args[key])) {
      throw new Error(`Invalid number for ${name}: ${raw}`);
    }
  }
  args.iterations = Math.max(1, Math.floor(args.iterations));
  args.top = Math.max(1, Math.floor(args.top));
  args.minTrades = Math.max(1, Math.floor(args.minTrades));
  args.minTestTrades = Math.max(1, Math.floor(args.minTestTrades));
  return args;
}

function printHelp() {
  console.log(`Activity Flow optimizer

Usage:
  npm run optimize:activity -- --hours 168 --iterations 2000

Options:
  --hours N                History window; 0 means all rows (default 168)
  --iterations N           Random parameter candidates (default 1200)
  --top N                  Candidates written to reports (default 10)
  --min-trades N           Minimum train/validation trades (default 8)
  --min-test-trades N      Minimum test trades for a robust verdict (default 5)
  --cost-bps N             Execution cost per side, including slippage (default 100)
  --priority-fee-sol N     Fixed priority fee per transaction (default 0.0005)
  --sol-price-usd N        SOL/USD used for the USD volume threshold
  --position-sol N         Position size used for PnL in SOL
  --db PATH                SQLite database path
  --reports-dir PATH       Output directory
  --seed N                 Deterministic random seed
  --self-test              Run synthetic tests without reading the database
`);
}

function loadRuntime() {
  try {
    require('dotenv').config({ override: true });
  } catch (_) {
    // Self-test can run in a checkout without installed dependencies.
  }

  try {
    const { config } = require('../src/config');
    const solPriceUsd = numberOr(process.env.SOL_PRICE_USD, FALLBACK.solPriceUsd);
    const priorityLamports = numberOr(
      process.env.BUY_MAX_PRIORITY_FEE_LAMPORTS || process.env.MAX_PRIORITY_FEE_LAMPORTS,
      500_000,
    );
    return {
      dbPath: config.storage.dbPath,
      reportsDir: config.storage.reportsDir,
      solPriceUsd,
      positionSol: config.strategy.positionSizeSol,
      costBps: numberOr(process.env.BT_EXECUTION_COST_BPS, FALLBACK.costBps),
      priorityFeeSol: priorityLamports / 1e9,
      baseline: {
        entryMinVolumeUsd: config.activityFlow.minVolume1mUsd,
        entryMinRatio1m: config.activityFlow.minRatio1m,
        entryMinTrades1m: config.activityFlow.minTrades1m,
        entryMinBuyTrades5s: config.activityFlow.confirmMinBuyTrades5s,
        entryMinUniqueBuyers5s: config.activityFlow.confirmMinUniqueBuyers5s,
        entryMinRatio5s: config.activityFlow.confirmMinRatio5s,
        entryMaxBuyerShare5s: config.activityFlow.confirmMaxBuyerShare5s,
        entryMaxRise5sPct: config.activityFlow.confirmMaxPriceRise5sPct,
        entryMaxSingleBuyImpactPct: config.activityFlow.confirmMaxSingleBuyImpactPct,
        minPoolQuoteSol: config.activityFlow.minPoolQuoteSol,
        flowExitRatio: config.strategy.flowReversalExitSellBuyRatio1m,
        flowExitMinVolumeSol: config.strategy.flowReversalExitMinVolume1mSol,
        flowExitMinHoldMs: config.strategy.flowReversalExitMinHoldMs,
        trailingActivatePct: config.strategy.trailingActivatePct,
        trailingDrawdownPct: config.strategy.trailingDrawdownPct,
        takeProfitPct: config.strategy.takeProfitPct,
        stopLossPct: config.strategy.emergencyStopLossPct,
        maxHoldMs: config.strategy.maxHoldMs,
        stabilizationMs: config.strategy.stabilizationMs,
        stopLossMinHoldMs: numberOr(process.env.EMERGENCY_STOP_GRACE_MS, 30_000),
        trailingMinHwmAgeMs: config.strategy.trailingMinHwmAgeMs,
      },
    };
  } catch (_) {
    return JSON.parse(JSON.stringify(FALLBACK));
  }
}

function mulberry32(seed) {
  let value = seed >>> 0;
  return function random() {
    value += 0x6D2B79F5;
    let t = value;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pick(values, random) {
  return values[Math.floor(random() * values.length)];
}

function candidateKey(candidate) {
  return Object.keys(PARAM_SPACE).map((key) => candidate[key]).join('|');
}

function generateCandidates(baseline, count, seed) {
  const random = mulberry32(seed);
  const candidates = [{ ...baseline, id: 'baseline' }];
  const seen = new Set([candidateKey(baseline)]);
  let attempts = 0;
  while (candidates.length < count + 1 && attempts < count * 20) {
    attempts += 1;
    const candidate = { ...baseline };
    for (const [key, values] of Object.entries(PARAM_SPACE)) candidate[key] = pick(values, random);
    if (candidate.trailingDrawdownPct >= candidate.trailingActivatePct) continue;
    const key = candidateKey(candidate);
    if (seen.has(key)) continue;
    seen.add(key);
    candidate.id = `random-${candidates.length}`;
    candidates.push(candidate);
  }
  return candidates;
}

function lowerBound(events, ts) {
  let lo = 0;
  let hi = events.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (events[mid].ts < ts) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function computeWindows(events, windowMs, detailed) {
  const stats = new Array(events.length);
  const buyerVolumes = new Map();
  const maxImpactDeque = [];
  let start = 0;
  let buySol = 0;
  let sellSol = 0;
  let buyCount = 0;
  let sellCount = 0;

  function add(ev, idx) {
    if (ev.side === 'BUY') {
      buySol += ev.solVolume;
      buyCount += 1;
      if (detailed) {
        const buyer = ev.signer || '__unknown__';
        buyerVolumes.set(buyer, (buyerVolumes.get(buyer) || 0) + ev.solVolume);
        while (
          maxImpactDeque.length > 0 &&
          events[maxImpactDeque[maxImpactDeque.length - 1]].priceChangePct <= ev.priceChangePct
        ) maxImpactDeque.pop();
        maxImpactDeque.push(idx);
      }
    } else {
      sellSol += ev.solVolume;
      sellCount += 1;
    }
  }

  function remove(ev, idx) {
    if (ev.side === 'BUY') {
      buySol -= ev.solVolume;
      buyCount -= 1;
      if (detailed) {
        const buyer = ev.signer || '__unknown__';
        const next = (buyerVolumes.get(buyer) || 0) - ev.solVolume;
        if (next <= 1e-9) buyerVolumes.delete(buyer);
        else buyerVolumes.set(buyer, next);
        if (maxImpactDeque[0] === idx) maxImpactDeque.shift();
      }
    } else {
      sellSol -= ev.solVolume;
      sellCount -= 1;
    }
  }

  for (let i = 0; i < events.length; i += 1) {
    add(events[i], i);
    while (start < i && events[i].ts - events[start].ts > windowMs) {
      remove(events[start], start);
      start += 1;
    }
    while (detailed && maxImpactDeque.length > 0 && maxImpactDeque[0] < start) maxImpactDeque.shift();
    const volumeSol = buySol + sellSol;
    let uniqueBuyers = 0;
    let largestBuyerSol = 0;
    if (detailed) {
      for (const [buyer, amount] of buyerVolumes) {
        if (buyer !== '__unknown__') uniqueBuyers += 1;
        if (amount > largestBuyerSol) largestBuyerSol = amount;
      }
    }
    const firstPrice = events[start].price;
    const lastPrice = events[i].price;
    stats[i] = {
      start,
      tradeCount: i - start + 1,
      buyCount,
      sellCount,
      buySol,
      sellSol,
      volumeSol,
      buySellRatio: buySol / Math.max(sellSol, 0.001),
      uniqueBuyers,
      largestBuyerShare: largestBuyerSol / Math.max(buySol, 0.001),
      maxSingleBuyImpactPct:
        detailed && maxImpactDeque.length > 0 ? Math.max(0, events[maxImpactDeque[0]].priceChangePct) : 0,
      priceChangePct: firstPrice > 0 ? ((lastPrice - firstPrice) / firstPrice) * 100 : 0,
    };
  }
  return stats;
}

function prepareMint(events) {
  events.sort((a, b) => (a.ts - b.ts) || (a.id - b.id));
  const buyPrefix = new Float64Array(events.length + 1);
  const sellPrefix = new Float64Array(events.length + 1);
  for (let i = 0; i < events.length; i += 1) {
    buyPrefix[i + 1] = buyPrefix[i] + (events[i].side === 'BUY' ? events[i].solVolume : 0);
    sellPrefix[i + 1] = sellPrefix[i] + (events[i].side === 'SELL' ? events[i].solVolume : 0);
  }
  return {
    events,
    w5: computeWindows(events, 5_000, true),
    w60: computeWindows(events, 60_000, false),
    buyPrefix,
    sellPrefix,
  };
}

function prepareRows(rows) {
  const byMint = new Map();
  let invalidRows = 0;
  let missingPoolQuote = 0;
  let duplicateSignatures = 0;
  const signatures = new Set();
  const globalTimes = [];

  for (const row of rows) {
    const side = String(row.side || '').toUpperCase();
    const ts = Number(row.ts);
    const price = Number(row.price);
    const solVolume = Number(row.solVolume);
    if (!row.mint || (side !== 'BUY' && side !== 'SELL') || !Number.isFinite(ts) ||
        !Number.isFinite(price) || price <= 0 || !Number.isFinite(solVolume) || solVolume <= 0) {
      invalidRows += 1;
      continue;
    }
    let priceChangePct = Number(row.priceChangePct);
    const priceBefore = Number(row.priceBefore);
    if (!Number.isFinite(priceChangePct)) {
      priceChangePct = Number.isFinite(priceBefore) && priceBefore > 0
        ? ((price - priceBefore) / priceBefore) * 100
        : 0;
    }
    const poolQuoteAfter = Number(row.poolQuoteAfter);
    if (!Number.isFinite(poolQuoteAfter) || poolQuoteAfter <= 0) missingPoolQuote += 1;
    if (row.signature) {
      const sigKey = `${row.signature}:${row.mint}:${side}`;
      if (signatures.has(sigKey)) duplicateSignatures += 1;
      else signatures.add(sigKey);
    }
    const ev = {
      id: numberOr(row.id, 0),
      mint: row.mint,
      symbol: row.symbol || null,
      signer: row.signer || null,
      side,
      solVolume,
      price,
      priceBefore: Number.isFinite(priceBefore) ? priceBefore : null,
      priceChangePct,
      poolQuoteAfter: Number.isFinite(poolQuoteAfter) && poolQuoteAfter > 0 ? poolQuoteAfter : null,
      ts,
    };
    if (!byMint.has(ev.mint)) byMint.set(ev.mint, []);
    byMint.get(ev.mint).push(ev);
    globalTimes.push(ts);
  }

  const prepared = new Map();
  for (const [mint, events] of byMint) prepared.set(mint, prepareMint(events));
  globalTimes.sort((a, b) => a - b);
  let gapCount = 0;
  let maxGapMs = 0;
  for (let i = 1; i < globalTimes.length; i += 1) {
    const gap = globalTimes[i] - globalTimes[i - 1];
    if (gap > 5 * MINUTE_MS) gapCount += 1;
    if (gap > maxGapMs) maxGapMs = gap;
  }
  return {
    prepared,
    quality: {
      rawRows: rows.length,
      validRows: globalTimes.length,
      invalidRows,
      mints: prepared.size,
      missingPoolQuote,
      duplicateSignatures,
      startTs: globalTimes[0] || 0,
      endTs: globalTimes[globalTimes.length - 1] || 0,
      gapCount,
      maxGapMs,
    },
  };
}

function passesEntry(data, idx, candidate, solPriceUsd) {
  const ev = data.events[idx];
  if (ev.side !== 'BUY') return false;
  const s5 = data.w5[idx];
  const s60 = data.w60[idx];
  const minVolumeSol = candidate.entryMinVolumeUsd / Math.max(solPriceUsd, 0.001);
  if (candidate.minPoolQuoteSol > 0 && (!ev.poolQuoteAfter || ev.poolQuoteAfter < candidate.minPoolQuoteSol)) return false;
  if (s60.tradeCount < candidate.entryMinTrades1m) return false;
  if (s60.volumeSol < minVolumeSol) return false;
  if (s60.buySellRatio < candidate.entryMinRatio1m) return false;
  if (s5.buyCount < candidate.entryMinBuyTrades5s) return false;
  if (s5.uniqueBuyers < candidate.entryMinUniqueBuyers5s) return false;
  if (s5.buySellRatio < candidate.entryMinRatio5s) return false;
  if (s5.largestBuyerShare > candidate.entryMaxBuyerShare5s) return false;
  if (s5.priceChangePct > candidate.entryMaxRise5sPct) return false;
  if (s5.maxSingleBuyImpactPct > candidate.entryMaxSingleBuyImpactPct) return false;
  return true;
}

function flowExitStats(data, entryIdx, idx) {
  const windowStart = Math.max(entryIdx + 1, data.w60[idx].start);
  const buySol = data.buyPrefix[idx + 1] - data.buyPrefix[windowStart];
  const sellSol = data.sellPrefix[idx + 1] - data.sellPrefix[windowStart];
  return {
    buySol,
    sellSol,
    volumeSol: buySol + sellSol,
    sellBuyRatio: sellSol / Math.max(buySol, 0.001),
  };
}

function makeTrade(data, entryIdx, exitIdx, reason, candidate, options) {
  const entry = data.events[entryIdx];
  const exit = data.events[exitIdx];
  const rawPnlPct = ((exit.price - entry.price) / entry.price) * 100;
  const costRate = options.costBps / 10_000;
  const entryExecutionPrice = entry.price * (1 + costRate);
  const exitExecutionPrice = exit.price * (1 - costRate);
  const netMultiple = exitExecutionPrice / entryExecutionPrice;
  const netSol = options.positionSol * (netMultiple - 1) - options.priorityFeeSol * 2;
  return {
    mint: entry.mint,
    symbol: entry.symbol,
    entryTs: entry.ts,
    exitTs: exit.ts,
    holdMs: exit.ts - entry.ts,
    entryPrice: entry.price,
    exitPrice: exit.price,
    rawPnlPct,
    netPnlPct: (netSol / options.positionSol) * 100,
    netSol,
    reason,
    candidateId: candidate.id,
  };
}

function simulateExit(data, entryIdx, splitEnd, candidate, options) {
  const events = data.events;
  const entry = events[entryIdx];
  const deadline = entry.ts + candidate.maxHoldMs;
  let hwm = entry.price;
  let hwmTs = entry.ts;
  let trailingArmed = false;
  let lastIdx = entryIdx;

  for (let i = entryIdx + 1; i < events.length; i += 1) {
    const ev = events[i];
    if (ev.ts >= splitEnd) break;
    if (ev.ts > deadline) {
      if (lastIdx > entryIdx) return makeTrade(data, entryIdx, lastIdx, 'MAX_HOLD', candidate, options);
      return null;
    }
    lastIdx = i;
    const holdMs = ev.ts - entry.ts;
    const rawPnlPct = ((ev.price - entry.price) / entry.price) * 100;

    if (ev.side === 'SELL' && holdMs >= candidate.flowExitMinHoldMs) {
      const st = flowExitStats(data, entryIdx, i);
      if (st.volumeSol >= candidate.flowExitMinVolumeSol && st.sellSol > st.buySol &&
          st.sellBuyRatio >= candidate.flowExitRatio) {
        return makeTrade(data, entryIdx, i, 'FLOW_REVERSAL_EXIT', candidate, options);
      }
    }

    if (holdMs < candidate.stabilizationMs) continue;
    if (ev.price > hwm) {
      hwm = ev.price;
      hwmTs = ev.ts;
    }
    if (candidate.takeProfitPct > 0 && rawPnlPct >= candidate.takeProfitPct) {
      return makeTrade(data, entryIdx, i, 'TAKE_PROFIT', candidate, options);
    }
    if (candidate.stopLossPct < 0 && holdMs >= candidate.stopLossMinHoldMs && rawPnlPct <= candidate.stopLossPct) {
      return makeTrade(data, entryIdx, i, 'EMERGENCY_STOP', candidate, options);
    }
    const peakPnlPct = ((hwm - entry.price) / entry.price) * 100;
    if (candidate.trailingActivatePct > 0 && peakPnlPct >= candidate.trailingActivatePct) trailingArmed = true;
    if (trailingArmed && candidate.trailingDrawdownPct > 0) {
      const drawdownPct = ((hwm - ev.price) / hwm) * 100;
      if (drawdownPct >= candidate.trailingDrawdownPct && ev.ts - hwmTs >= candidate.trailingMinHwmAgeMs) {
        return makeTrade(data, entryIdx, i, 'TRAILING_STOP', candidate, options);
      }
    }
  }

  if (deadline < splitEnd && lastIdx > entryIdx && events[lastIdx].ts <= deadline) {
    return makeTrade(data, entryIdx, lastIdx, 'MAX_HOLD', candidate, options);
  }
  return null;
}

function median(values) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function summarizeTrades(trades, openPositions, minTrades) {
  const ordered = [...trades].sort((a, b) => a.exitTs - b.exitTs);
  let equity = 0;
  let peak = 0;
  let maxDrawdownSol = 0;
  let grossProfit = 0;
  let grossLoss = 0;
  let worstLossStreak = 0;
  let lossStreak = 0;
  const reasons = {};
  const mintPnl = new Map();

  for (const trade of ordered) {
    equity += trade.netSol;
    if (equity > peak) peak = equity;
    maxDrawdownSol = Math.max(maxDrawdownSol, peak - equity);
    if (trade.netSol > 0) {
      grossProfit += trade.netSol;
      lossStreak = 0;
    } else {
      grossLoss += Math.abs(trade.netSol);
      lossStreak += 1;
      worstLossStreak = Math.max(worstLossStreak, lossStreak);
    }
    reasons[trade.reason] = (reasons[trade.reason] || 0) + 1;
    mintPnl.set(trade.mint, (mintPnl.get(trade.mint) || 0) + trade.netSol);
  }

  const netSol = ordered.reduce((sum, trade) => sum + trade.netSol, 0);
  const wins = ordered.filter((trade) => trade.netSol > 0).length;
  const positiveMintPnl = [...mintPnl.values()].filter((value) => value > 0);
  const totalPositiveMintPnl = positiveMintPnl.reduce((sum, value) => sum + value, 0);
  const topMintShare = totalPositiveMintPnl > 0 ? Math.max(...positiveMintPnl) / totalPositiveMintPnl : 1;
  const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : (grossProfit > 0 ? 99 : 0);
  const openPenalty = openPositions * 0.02;
  const concentrationPenalty = Math.max(0, topMintShare - 0.50) * Math.max(Math.abs(netSol), 0.1);
  const score = ordered.length >= minTrades
    ? netSol - 0.75 * maxDrawdownSol - concentrationPenalty - openPenalty
    : BAD_SCORE;

  return {
    trades: ordered.length,
    wins,
    winRate: ordered.length ? (wins / ordered.length) * 100 : 0,
    netSol,
    avgNetSol: ordered.length ? netSol / ordered.length : 0,
    avgNetPct: ordered.length ? ordered.reduce((sum, trade) => sum + trade.netPnlPct, 0) / ordered.length : 0,
    medianNetPct: median(ordered.map((trade) => trade.netPnlPct)),
    profitFactor,
    maxDrawdownSol,
    worstLossStreak,
    avgHoldMs: ordered.length ? ordered.reduce((sum, trade) => sum + trade.holdMs, 0) / ordered.length : 0,
    topMintShare,
    openPositions,
    reasons,
    score,
    tradeRows: ordered,
  };
}

function evaluateCandidate(prepared, split, candidate, options) {
  const trades = [];
  let openPositions = 0;
  for (const data of prepared.values()) {
    const events = data.events;
    let i = lowerBound(events, split.start);
    while (i < events.length && events[i].ts < split.end) {
      if (!passesEntry(data, i, candidate, options.solPriceUsd)) {
        i += 1;
        continue;
      }
      const trade = simulateExit(data, i, split.end, candidate, options);
      if (!trade) {
        openPositions += 1;
        break;
      }
      trades.push(trade);
      i = lowerBound(events, trade.exitTs + 1);
    }
  }
  return summarizeTrades(trades, openPositions, options.minTrades);
}

function scoreValidation(train, validation) {
  if (train.score <= BAD_SCORE || validation.score <= BAD_SCORE) return BAD_SCORE;
  let score = validation.score + train.score * 0.20;
  if (train.netSol <= 0 || validation.netSol <= 0) score -= Math.abs(train.netSol) + Math.abs(validation.netSol) + 1;
  if (Math.sign(train.avgNetSol) !== Math.sign(validation.avgNetSol)) score -= 1;
  return score;
}

function makeSplits(startTs, endTs) {
  const span = Math.max(1, endTs - startTs);
  const trainEnd = startTs + span * 0.60;
  const validationEnd = startTs + span * 0.80;
  return {
    train: { name: 'train', start: startTs, end: trainEnd },
    validation: { name: 'validation', start: trainEnd, end: validationEnd },
    test: { name: 'test', start: validationEnd, end: endTs + 1 },
  };
}

function bootstrapNetSol(trades, seed, samples = 500) {
  if (trades.length === 0) return { p05: 0, p50: 0, p95: 0 };
  const random = mulberry32(seed ^ 0xA5A5A5A5);
  const totals = [];
  for (let sample = 0; sample < samples; sample += 1) {
    let total = 0;
    for (let i = 0; i < trades.length; i += 1) total += trades[Math.floor(random() * trades.length)].netSol;
    totals.push(total);
  }
  totals.sort((a, b) => a - b);
  const at = (q) => totals[Math.min(totals.length - 1, Math.floor(totals.length * q))];
  return { p05: at(0.05), p50: at(0.50), p95: at(0.95) };
}

function foldResults(prepared, startTs, endTs, candidate, options, folds = 4) {
  const span = endTs - startTs;
  const results = [];
  for (let i = 0; i < folds; i += 1) {
    results.push(evaluateCandidate(
      prepared,
      { start: startTs + span * (i / folds), end: startTs + span * ((i + 1) / folds) },
      candidate,
      { ...options, minTrades: 1 },
    ));
  }
  return results;
}

function round(value, digits = 3) {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function formatDate(ts) {
  return ts ? new Date(ts).toISOString() : 'n/a';
}

function metricRow(result) {
  return {
    trades: result.trades,
    netSol: round(result.netSol, 4),
    winRate: `${round(result.winRate, 1)}%`,
    avgPct: round(result.avgNetPct, 2),
    medianPct: round(result.medianNetPct, 2),
    profitFactor: round(result.profitFactor, 2),
    maxDdSol: round(result.maxDrawdownSol, 4),
    worstLossStreak: result.worstLossStreak,
    avgHoldSec: round(result.avgHoldMs / 1000, 1),
    topMintShare: `${round(result.topMintShare * 100, 1)}%`,
    open: result.openPositions,
  };
}

function envBlock(candidate) {
  return [
    `ACTIVITY_FLOW_1M_MIN_VOLUME_USD=${candidate.entryMinVolumeUsd}`,
    'ACTIVITY_FLOW_1M_MIN_VOLUME_SOL=',
    `ACTIVITY_FLOW_1M_MIN_BUY_SELL_RATIO=${candidate.entryMinRatio1m}`,
    `ACTIVITY_FLOW_1M_MIN_TRADES=${candidate.entryMinTrades1m}`,
    `ACTIVITY_FLOW_CONFIRM_MIN_BUY_TRADES_5S=${candidate.entryMinBuyTrades5s}`,
    `ACTIVITY_FLOW_CONFIRM_MIN_UNIQUE_BUYERS_5S=${candidate.entryMinUniqueBuyers5s}`,
    `ACTIVITY_FLOW_CONFIRM_MIN_BUY_SELL_RATIO_5S=${candidate.entryMinRatio5s}`,
    `ACTIVITY_FLOW_CONFIRM_MAX_BUYER_SHARE_5S=${candidate.entryMaxBuyerShare5s}`,
    `ACTIVITY_FLOW_CONFIRM_MAX_PRICE_RISE_5S_PCT=${candidate.entryMaxRise5sPct}`,
    `ACTIVITY_FLOW_CONFIRM_MAX_SINGLE_BUY_IMPACT_PCT=${candidate.entryMaxSingleBuyImpactPct}`,
    `FLOW_REVERSAL_EXIT_SELL_BUY_RATIO_1M=${candidate.flowExitRatio}`,
    `FLOW_REVERSAL_EXIT_MIN_VOLUME_1M_SOL=${candidate.flowExitMinVolumeSol}`,
    `FLOW_REVERSAL_EXIT_MIN_HOLD_MS=${candidate.flowExitMinHoldMs}`,
    `TRAILING_ACTIVATE_PCT=${candidate.trailingActivatePct}`,
    `TRAILING_DRAWDOWN_PCT=${candidate.trailingDrawdownPct}`,
    `TAKE_PROFIT_PCT=${candidate.takeProfitPct}`,
    `EMERGENCY_STOP_LOSS_PCT=${candidate.stopLossPct}`,
    `MAX_HOLD_MS=${candidate.maxHoldMs}`,
  ].join('\n');
}

function markdownTable(headers, rows) {
  const esc = (value) => String(value == null ? '' : value).replace(/\|/g, '\\|');
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...rows.map((row) => `| ${row.map(esc).join(' | ')} |`),
  ].join('\n');
}

function csvEscape(value) {
  const str = String(value == null ? '' : value);
  return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
}

function writeReports(output, runtime, args) {
  const dir = path.resolve(args.reportsDir || runtime.reportsDir);
  fs.mkdirSync(dir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const base = `activity-flow-optimizer-${stamp}`;
  const jsonPath = path.join(dir, `${base}.json`);
  const csvPath = path.join(dir, `${base}.csv`);
  const mdPath = path.join(dir, `${base}.md`);
  const envPath = path.join(dir, `${base}.recommended.env`);

  fs.writeFileSync(jsonPath, JSON.stringify(output, null, 2));
  const csvHeaders = [
    'rank', 'id', 'entryMinVolumeUsd', 'entryMinRatio1m', 'entryMinTrades1m',
    'entryMinBuyTrades5s', 'entryMinUniqueBuyers5s', 'entryMinRatio5s',
    'entryMaxBuyerShare5s', 'entryMaxRise5sPct', 'entryMaxSingleBuyImpactPct',
    'flowExitRatio', 'flowExitMinVolumeSol', 'flowExitMinHoldMs',
    'trailingActivatePct', 'trailingDrawdownPct', 'takeProfitPct', 'stopLossPct', 'maxHoldMs',
    'trainTrades', 'trainNetSol', 'validationTrades', 'validationNetSol', 'testTrades', 'testNetSol',
    'testProfitFactor', 'testMaxDrawdownSol',
  ];
  const csvRows = output.topCandidates.map((item, index) => {
    const c = item.candidate;
    return [
      index + 1, c.id, c.entryMinVolumeUsd, c.entryMinRatio1m, c.entryMinTrades1m,
      c.entryMinBuyTrades5s, c.entryMinUniqueBuyers5s, c.entryMinRatio5s,
      c.entryMaxBuyerShare5s, c.entryMaxRise5sPct, c.entryMaxSingleBuyImpactPct,
      c.flowExitRatio, c.flowExitMinVolumeSol, c.flowExitMinHoldMs,
      c.trailingActivatePct, c.trailingDrawdownPct, c.takeProfitPct, c.stopLossPct, c.maxHoldMs,
      item.train.trades, item.train.netSol, item.validation.trades, item.validation.netSol,
      item.test.trades, item.test.netSol, item.test.profitFactor, item.test.maxDrawdownSol,
    ];
  });
  fs.writeFileSync(csvPath, [csvHeaders, ...csvRows].map((row) => row.map(csvEscape).join(',')).join('\n'));
  fs.writeFileSync(envPath, envBlock(output.recommended.candidate) + '\n');

  const q = output.dataQuality;
  const baselineRows = ['train', 'validation', 'test'].map((name) => {
    const m = metricRow(output.baseline[name]);
    return [name, m.trades, m.netSol, m.winRate, m.profitFactor, m.maxDdSol, m.avgHoldSec];
  });
  const topRows = output.topCandidates.map((item, index) => {
    const c = item.candidate;
    const test = metricRow(item.test);
    return [
      index + 1, c.entryMinVolumeUsd, c.entryMinRatio1m, c.entryMinTrades1m,
      c.entryMinBuyTrades5s, c.entryMinUniqueBuyers5s, c.entryMaxBuyerShare5s,
      c.flowExitRatio, c.flowExitMinHoldMs / 1000, item.validation.trades,
      round(item.validation.netSol, 4), test.trades, test.netSol, test.profitFactor,
    ];
  });
  const stressRows = output.costStress.map((item) => [
    item.costBps, item.metrics.trades, round(item.metrics.netSol, 4),
    round(item.metrics.profitFactor, 2), round(item.metrics.maxDrawdownSol, 4),
  ]);
  const foldRows = output.walkForward.map((item, index) => [
    index + 1, item.trades, round(item.netSol, 4), round(item.profitFactor, 2), round(item.maxDrawdownSol, 4),
  ]);
  const warnings = output.warnings.length ? output.warnings.map((x) => `- ${x}`).join('\n') : '- None';
  const verdict = output.robust
    ? 'The selected validation winner remained profitable on the untouched test set.'
    : 'No robust profitability claim can be made from this dataset. Treat the parameters as exploratory.';

  const markdown = `# Activity Flow optimization report

Generated: ${new Date().toISOString()}

## Verdict

${verdict}

${warnings}

## Data quality

- Range: ${formatDate(q.startTs)} to ${formatDate(q.endTs)} (${round(q.spanHours, 2)} hours)
- Rows: ${q.validRows} valid / ${q.rawRows} total
- Mints: ${q.mints}
- Invalid rows: ${q.invalidRows}
- Missing pool quote: ${q.missingPoolQuote}
- Duplicate signature+mint+side: ${q.duplicateSignatures}
- Gaps over 5 minutes: ${q.gapCount}; largest gap ${round(q.maxGapMs / 1000, 1)} seconds

## Method

- Chronological split: 60% train, 20% validation, 20% untouched test.
- Candidates: ${output.candidateCount}; deterministic seed ${args.seed}.
- Selection: train shortlist, then validation winner. Test metrics were not used to select the winner.
- Execution cost: ${output.options.costBps} bps per side plus ${output.options.priorityFeeSol} SOL per transaction.
- Position size: ${output.options.positionSol} SOL.

## Current baseline

${markdownTable(['split', 'trades', 'net SOL', 'win rate', 'profit factor', 'max DD SOL', 'avg hold sec'], baselineRows)}

## Validation-ranked candidates

${markdownTable(
    ['rank', 'vol USD', 'r1m', 'tx1m', 'buy tx5', 'buyers5', 'top buyer', 'exit ratio', 'exit hold s', 'val trades', 'val SOL', 'test trades', 'test SOL', 'test PF'],
    topRows,
  )}

## Recommended candidate

This is the validation winner, not the best-looking test result.

\`\`\`env
${envBlock(output.recommended.candidate)}
\`\`\`

Test bootstrap net SOL interval: p05=${round(output.bootstrap.p05, 4)}, p50=${round(output.bootstrap.p50, 4)}, p95=${round(output.bootstrap.p95, 4)}.

## Cost stress on untouched test

${markdownTable(['cost bps/side', 'trades', 'net SOL', 'profit factor', 'max DD SOL'], stressRows)}

## Chronological stability

${markdownTable(['fold', 'trades', 'net SOL', 'profit factor', 'max DD SOL'], foldRows)}

## Important limitations

- Backtest fills are modeled from observed swap prices; actual routing, failed transactions and MEV can be worse.
- Pool quote rows missing from history are rejected, matching the live liquidity guard.
- The optimizer models one position per mint and does not reproduce every asynchronous reconciliation delay.
- Do not deploy a recommendation until the test set and several chronological folds remain profitable after costs.
`;
  fs.writeFileSync(mdPath, markdown);
  return { mdPath, csvPath, jsonPath, envPath };
}

function stripTrades(result) {
  const { tradeRows, ...summary } = result;
  return summary;
}

function loadRows(dbPath, sinceTs) {
  let Database;
  try {
    Database = require('better-sqlite3');
  } catch (err) {
    throw new Error(`better-sqlite3 is required. Run npm install first. (${err.message})`);
  }
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  try {
    return db.prepare(`
      SELECT id, mint, symbol, signer, side, sol_volume AS solVolume, price,
             price_before AS priceBefore, price_change_pct AS priceChangePct,
             ts, signature, pool_quote_after AS poolQuoteAfter
      FROM swap_events
      WHERE ts >= ?
      ORDER BY mint, ts, id
    `).all(sinceTs);
  } finally {
    db.close();
  }
}

function optimize(preparedRows, runtime, args) {
  const { prepared, quality } = preparedRows;
  quality.spanHours = (quality.endTs - quality.startTs) / HOUR_MS;
  const splits = makeSplits(quality.startTs, quality.endTs);
  const options = {
    solPriceUsd: args.solPriceUsd ?? runtime.solPriceUsd,
    positionSol: args.positionSol ?? runtime.positionSol,
    costBps: args.costBps ?? runtime.costBps,
    priorityFeeSol: args.priorityFeeSol ?? runtime.priorityFeeSol,
    minTrades: args.minTrades,
  };
  const candidates = generateCandidates(runtime.baseline, args.iterations, args.seed);
  console.log(`Evaluating ${candidates.length} candidates on train data...`);
  const trainRank = candidates
    .map((candidate) => ({ candidate, train: evaluateCandidate(prepared, splits.train, candidate, options) }))
    .filter((item) => item.train.score > BAD_SCORE)
    .sort((a, b) => b.train.score - a.train.score);
  if (trainRank.length === 0) throw new Error('No candidate reached the minimum train trade count. Lower --min-trades or collect more data.');

  const shortlistSize = Math.min(trainRank.length, Math.max(args.top * 8, 50));
  console.log(`Evaluating ${shortlistSize} train survivors on validation data...`);
  const validationRank = trainRank.slice(0, shortlistSize)
    .map((item) => {
      const validation = evaluateCandidate(prepared, splits.validation, item.candidate, options);
      return { ...item, validation, selectionScore: scoreValidation(item.train, validation) };
    })
    .filter((item) => item.selectionScore > BAD_SCORE)
    .sort((a, b) => b.selectionScore - a.selectionScore);
  if (validationRank.length === 0) {
    throw new Error('No train survivor reached the minimum validation trade count. Lower --min-trades or collect more data.');
  }

  const top = validationRank.slice(0, args.top).map((item) => ({
    ...item,
    test: evaluateCandidate(prepared, splits.test, item.candidate, { ...options, minTrades: 1 }),
  }));
  const recommended = top[0];
  const baseline = {
    train: evaluateCandidate(prepared, splits.train, runtime.baseline, { ...options, minTrades: 1 }),
    validation: evaluateCandidate(prepared, splits.validation, runtime.baseline, { ...options, minTrades: 1 }),
    test: evaluateCandidate(prepared, splits.test, runtime.baseline, { ...options, minTrades: 1 }),
  };
  const costStress = [50, 100, 200].map((costBps) => ({
    costBps,
    metrics: evaluateCandidate(prepared, splits.test, recommended.candidate, {
      ...options,
      costBps,
      minTrades: 1,
    }),
  }));
  const walkForward = foldResults(
    prepared,
    quality.startTs,
    quality.endTs + 1,
    recommended.candidate,
    options,
  );
  const positiveFolds = walkForward.filter((result) => result.netSol > 0).length;
  const bootstrap = bootstrapNetSol(recommended.test.tradeRows, args.seed);
  const warnings = [];
  if (quality.spanHours < 72) warnings.push(`Only ${round(quality.spanHours, 1)} hours of data; 72+ hours is recommended.`);
  if (quality.validRows < 10_000) warnings.push(`Only ${quality.validRows} valid swaps; 10,000+ is recommended.`);
  if (recommended.test.trades < args.minTestTrades) {
    warnings.push(`Only ${recommended.test.trades} test trades; minimum robust target is ${args.minTestTrades}.`);
  }
  if (quality.missingPoolQuote / Math.max(quality.validRows, 1) > 0.20) {
    warnings.push('More than 20% of swaps have no pool quote and cannot pass the live liquidity guard.');
  }
  const robust = quality.spanHours >= 72 && quality.validRows >= 10_000 &&
    recommended.validation.netSol > 0 && recommended.test.netSol > 0 &&
    recommended.test.trades >= args.minTestTrades && positiveFolds >= 3 && bootstrap.p05 > 0;

  return {
    generatedAt: Date.now(),
    candidateCount: candidates.length,
    options,
    dataQuality: quality,
    splits,
    warnings,
    robust,
    baseline: {
      train: stripTrades(baseline.train),
      validation: stripTrades(baseline.validation),
      test: stripTrades(baseline.test),
    },
    topCandidates: top.map((item) => ({
      candidate: item.candidate,
      selectionScore: item.selectionScore,
      train: stripTrades(item.train),
      validation: stripTrades(item.validation),
      test: stripTrades(item.test),
    })),
    recommended: {
      candidate: recommended.candidate,
      train: stripTrades(recommended.train),
      validation: stripTrades(recommended.validation),
      test: stripTrades(recommended.test),
    },
    costStress: costStress.map((item) => ({ costBps: item.costBps, metrics: stripTrades(item.metrics) })),
    walkForward: walkForward.map(stripTrades),
    bootstrap,
  };
}

function selfTest() {
  const base = 1_700_000_000_000;
  const rows = [
    { id: 1, mint: 'HEALTHY', signer: 'S1', side: 'SELL', solVolume: 2, price: 100, priceBefore: 100, priceChangePct: 0, poolQuoteAfter: 100, ts: base },
    { id: 2, mint: 'HEALTHY', signer: 'B1', side: 'BUY', solVolume: 2, price: 101, priceBefore: 100, priceChangePct: 1, poolQuoteAfter: 100, ts: base + 500 },
    { id: 3, mint: 'HEALTHY', signer: 'B2', side: 'BUY', solVolume: 2, price: 102, priceBefore: 101, priceChangePct: 0.99, poolQuoteAfter: 100, ts: base + 1000 },
    { id: 4, mint: 'HEALTHY', signer: 'B3', side: 'BUY', solVolume: 2, price: 103, priceBefore: 102, priceChangePct: 0.98, poolQuoteAfter: 100, ts: base + 1500 },
    { id: 5, mint: 'HEALTHY', signer: 'B4', side: 'BUY', solVolume: 2, price: 104, priceBefore: 103, priceChangePct: 0.97, poolQuoteAfter: 100, ts: base + 2000 },
    { id: 6, mint: 'HEALTHY', signer: 'S2', side: 'SELL', solVolume: 10, price: 102, priceBefore: 104, priceChangePct: -1.92, poolQuoteAfter: 100, ts: base + 12_000 },
    { id: 7, mint: 'SPIKE', signer: 'S1', side: 'SELL', solVolume: 2, price: 100, priceBefore: 100, priceChangePct: 0, poolQuoteAfter: 100, ts: base },
    { id: 8, mint: 'SPIKE', signer: 'B1', side: 'BUY', solVolume: 8, price: 120, priceBefore: 100, priceChangePct: 20, poolQuoteAfter: 100, ts: base + 500 },
    { id: 9, mint: 'SPIKE', signer: 'B2', side: 'BUY', solVolume: 1, price: 121, priceBefore: 120, priceChangePct: 0.83, poolQuoteAfter: 100, ts: base + 1000 },
    { id: 10, mint: 'SPIKE', signer: 'B3', side: 'BUY', solVolume: 1, price: 122, priceBefore: 121, priceChangePct: 0.83, poolQuoteAfter: 100, ts: base + 1500 },
    { id: 11, mint: 'SPIKE', signer: 'B4', side: 'BUY', solVolume: 1, price: 123, priceBefore: 122, priceChangePct: 0.82, poolQuoteAfter: 100, ts: base + 2000 },
  ];
  const preparedRows = prepareRows(rows);
  const candidate = {
    ...FALLBACK.baseline,
    id: 'self-test',
    entryMinVolumeUsd: 5,
    entryMinRatio1m: 1.2,
    entryMinTrades1m: 5,
    flowExitMinVolumeSol: 5,
    flowExitMinHoldMs: 5_000,
    stabilizationMs: 0,
  };
  const split = { start: base, end: base + 30_000 };
  const result = evaluateCandidate(preparedRows.prepared, split, candidate, {
    solPriceUsd: 1,
    positionSol: 1,
    costBps: 0,
    priorityFeeSol: 0,
    minTrades: 1,
  });
  assert.strictEqual(result.trades, 1, 'healthy flow should enter and close once');
  assert.strictEqual(result.reasons.FLOW_REVERSAL_EXIT, 1, 'healthy flow should use flow reversal exit');
  assert.strictEqual(result.tradeRows[0].mint, 'HEALTHY', 'spike mint must be rejected');

  const repeated = [];
  let id = 100;
  for (let episode = 0; episode < 24; episode += 1) {
    const start = base + episode * HOUR_MS;
    const mint = `MINT_${episode}`;
    const episodeRows = [
      ['S1', 'SELL', 2, 100, 100, 0, 0],
      ['B1', 'BUY', 2, 101, 100, 1, 500],
      ['B2', 'BUY', 2, 102, 101, 0.99, 1000],
      ['B3', 'BUY', 2, 103, 102, 0.98, 1500],
      ['B4', 'BUY', 2, 104, 103, 0.97, 2000],
      ['S2', 'SELL', 10, 102, 104, -1.92, 12_000],
    ];
    for (const [signer, side, solVolume, price, before, impact, offset] of episodeRows) {
      repeated.push({
        id: id++, mint, signer, side, solVolume, price, priceBefore: before,
        priceChangePct: impact, poolQuoteAfter: 100, ts: start + offset,
      });
    }
  }
  const pipeline = optimize(
    prepareRows(repeated),
    { ...FALLBACK, baseline: candidate, solPriceUsd: 1, costBps: 0, priorityFeeSol: 0 },
    {
      iterations: 5, top: 3, minTrades: 1, minTestTrades: 1, seed: 42,
      solPriceUsd: 1, positionSol: 1, costBps: 0, priorityFeeSol: 0,
    },
  );
  assert.ok(pipeline.topCandidates.length >= 1, 'optimizer pipeline should rank candidates');
  assert.ok(pipeline.recommended.test.trades >= 1, 'optimizer should preserve an untouched test result');
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'activity-flow-optimizer-'));
  try {
    const files = writeReports(
      pipeline,
      { ...FALLBACK, reportsDir: tempDir },
      { reportsDir: tempDir, seed: 42 },
    );
    assert.ok(fs.existsSync(files.mdPath), 'markdown report should be written');
    assert.ok(fs.existsSync(files.csvPath), 'CSV report should be written');
    assert.ok(fs.existsSync(files.jsonPath), 'JSON report should be written');
    assert.ok(fs.existsSync(files.envPath), 'recommended env file should be written');
  } finally {
    if (tempDir.startsWith(os.tmpdir())) fs.rmSync(tempDir, { recursive: true, force: true });
  }
  console.log('optimize-activity-flow self-test: PASS');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    return;
  }
  if (args.selfTest) {
    selfTest();
    return;
  }
  const runtime = loadRuntime();
  const dbPath = path.resolve(args.dbPath || runtime.dbPath);
  const sinceTs = args.hours > 0 ? Date.now() - args.hours * HOUR_MS : 0;
  console.log(`Loading swap_events from ${dbPath}...`);
  const rows = loadRows(dbPath, sinceTs);
  if (rows.length === 0) throw new Error('No swap_events found in the requested time range.');
  const preparedRows = prepareRows(rows);
  if (preparedRows.quality.validRows === 0) throw new Error('No valid swap events after data-quality checks.');
  console.log(
    `Loaded ${preparedRows.quality.validRows} valid events across ${preparedRows.quality.mints} mints ` +
    `(${round((preparedRows.quality.endTs - preparedRows.quality.startTs) / HOUR_MS, 2)}h).`,
  );
  const output = optimize(preparedRows, runtime, args);
  const files = writeReports(output, runtime, args);
  console.table(output.topCandidates.map((item, index) => ({
    rank: index + 1,
    volUsd: item.candidate.entryMinVolumeUsd,
    ratio1m: item.candidate.entryMinRatio1m,
    tx1m: item.candidate.entryMinTrades1m,
    valTrades: item.validation.trades,
    valSol: round(item.validation.netSol, 4),
    testTrades: item.test.trades,
    testSol: round(item.test.netSol, 4),
    testPF: round(item.test.profitFactor, 2),
  })));
  console.log(output.robust
    ? 'Verdict: robust candidate found on the current evidence.'
    : 'Verdict: exploratory only; current evidence is not sufficient for a profitability claim.');
  console.log(`Report: ${files.mdPath}`);
  console.log(`Recommended env candidate: ${files.envPath}`);
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(`[optimizer] ${err.stack || err.message}`);
    process.exitCode = 1;
  }
}

module.exports = {
  computeWindows,
  evaluateCandidate,
  generateCandidates,
  makeSplits,
  optimize,
  passesEntry,
  prepareRows,
  selfTest,
};
