'use strict';

const fs = require('fs');
const path = require('path');

function openReadonlyDatabase(dbPath) {
  try {
    const Database = require('better-sqlite3');
    return new Database(dbPath, { readonly: true, fileMustExist: true });
  } catch (betterSqliteError) {
    try {
      const { DatabaseSync } = require('node:sqlite');
      const db = new DatabaseSync(dbPath, { readOnly: true });
      db.transaction = (fn) => (...args) => {
        db.exec('BEGIN');
        try {
          const result = fn(...args);
          db.exec('COMMIT');
          return result;
        } catch (err) {
          db.exec('ROLLBACK');
          throw err;
        }
      };
      return db;
    } catch (_) {
      throw betterSqliteError;
    }
  }
}

function parseArgs(argv) {
  const out = {};
  for (const arg of argv) {
    if (!arg.startsWith('--')) continue;
    const idx = arg.indexOf('=');
    if (idx === -1) out[arg.slice(2)] = true;
    else out[arg.slice(2, idx)] = arg.slice(idx + 1);
  }
  return out;
}

function parseTime(value, fallback) {
  if (value == null || value === '') return fallback;
  const number = Number(value);
  if (Number.isFinite(number) && number > 0) return number;
  const parsed = Date.parse(value);
  if (Number.isFinite(parsed)) return parsed;
  throw new Error(`Invalid time value: ${value}`);
}

function csvValue(value) {
  if (value == null) return '';
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value);
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

function writeCsv(filePath, rows) {
  if (!rows.length) {
    fs.writeFileSync(filePath, '', 'utf8');
    return;
  }
  const columns = [];
  const seen = new Set();
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      if (seen.has(key)) continue;
      seen.add(key);
      columns.push(key);
    }
  }
  const lines = [columns.map(csvValue).join(',')];
  for (const row of rows) {
    lines.push(columns.map((column) => csvValue(row[column])).join(','));
  }
  fs.writeFileSync(filePath, `${lines.join('\n')}\n`, 'utf8');
}

function parseJson(value) {
  try {
    return value ? JSON.parse(value) : {};
  } catch (_) {
    return {};
  }
}

function snakeCase(value) {
  return String(value)
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[^a-zA-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase();
}

function flattenObject(target, prefix, value) {
  if (value == null) {
    target[prefix] = null;
    return;
  }
  if (Array.isArray(value)) {
    target[prefix] = value.join('|');
    return;
  }
  if (typeof value !== 'object') {
    target[prefix] = value;
    return;
  }
  for (const [key, child] of Object.entries(value)) {
    flattenObject(target, `${prefix}_${snakeCase(key)}`, child);
  }
}

function flattenPositionRow(row) {
  const flat = { ...row };
  const entryMetrics = parseJson(row.entry_metrics_json);
  flattenObject(flat, 'entry_metric', entryMetrics);
  if (
    Number(row.entry_signal_price) > 0 &&
    Number(entryMetrics.executionPrice) > 0
  ) {
    flat.entry_execution_price_deviation_pct =
      ((Number(entryMetrics.executionPrice) - Number(row.entry_signal_price)) /
        Number(row.entry_signal_price)) * 100;
  }
  return flat;
}

function flattenResearchRow(row) {
  const metrics = parseJson(row.metrics_json);
  const flat = { ...row };
  for (const windowName of ['1s', '3s', '5s', '10s']) {
    const current = metrics.windows?.[windowName] || {};
    const previous = metrics.previousWindows?.[windowName] || {};
    const suffix = snakeCase(windowName);
    for (const [key, value] of Object.entries(current)) {
      flat[`${snakeCase(key)}_${suffix}`] = value;
    }
    for (const [key, value] of Object.entries(previous)) {
      flat[`previous_${snakeCase(key)}_${suffix}`] = value;
    }
  }
  flattenObject(flat, 'metric', metrics);
  flat.event_lag_ms =
    Number(row.received_at) > 0 && Number(row.ts) > 0
      ? Number(row.received_at) - Number(row.ts)
      : null;
  flat.market_data_age_ms =
    Number(row.received_at) > 0 && Number(metrics.market?.marketFetchedAt) > 0
      ? Number(row.received_at) - Number(metrics.market.marketFetchedAt)
      : null;
  flat.raw_effective_price_gap_pct =
    Number(row.raw_price) > 0 && Number(row.price) > 0
      ? ((Number(row.price) - Number(row.raw_price)) / Number(row.raw_price)) * 100
      : null;
  flat.signal_change_pct = metrics.signalChangePct;
  flat.pre_entry_vwap_change_pct = metrics.preEntryVwapChangePct;
  flat.price_below_signal = metrics.structure?.priceBelowSignal ? 1 : 0;
  flat.price_below_pre_entry_vwap =
    metrics.structure?.priceBelowPreEntryVwap ? 1 : 0;
  flat.net_flow_3s_negative = metrics.structure?.netFlow3sNegative ? 1 : 0;
  flat.buyer_count_declining = metrics.structure?.buyerCountDeclining ? 1 : 0;
  flat.conditional_tail_base =
    metrics.shadowCandidates?.conditionalTailBase ? 1 : 0;
  flat.loss_thresholds_crossed = Array.isArray(
    metrics.shadowCandidates?.lossThresholdsCrossed,
  )
    ? metrics.shadowCandidates.lossThresholdsCrossed.join('|')
    : '';
  flat.buy_sol_acceleration_3s = metrics.acceleration?.buySol3s;
  flat.trade_acceleration_3s = metrics.acceleration?.tradeCount3s;
  flat.buyer_acceleration_3s = metrics.acceleration?.uniqueBuyers3s;
  return flat;
}

function censorPositionAtCutoff(row, cutoffMs) {
  const closedAt = Number(row.closed_at);
  if (Number.isFinite(closedAt) && closedAt < cutoffMs) return row;

  return {
    ...row,
    closed_at: null,
    exit_price: null,
    exit_sol: null,
    pnl_sol: null,
    pnl_pct: null,
    sell_signature: null,
    exit_reason: null,
    exit_intent: null,
    peak_price: null,
    peak_ts: null,
    peak_pnl_pct: null,
    time_to_peak_ms: null,
    price_tick_count: null,
    pending_sell_signature: null,
    stuck_reason: null,
    status: 'open_at_cutoff',
  };
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const dbPath = path.resolve(args.db || './data/sniper.db');
  const until = parseTime(args.until, Date.now());
  const since = parseTime(args.since, until - 24 * 60 * 60 * 1000);
  const postMs = Math.max(0, Number(args['post-ms'] || 5 * 60 * 1000));
  if (since >= until) throw new Error('--since must be earlier than --until');

  const outDir = path.resolve(
    args.out || `./reports/position-research-${new Date(until).toISOString().replace(/[:.]/g, '-')}`,
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!fs.existsSync(dbPath)) throw new Error(`Database does not exist: ${dbPath}`);
  const db = openReadonlyDatabase(dbPath);
  const hasTable = (name) => !!db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`,
  ).get(name);
  if (!hasTable('position_research_events')) {
    throw new Error('position_research_events table is missing; deploy the telemetry build first');
  }

  const readSnapshot = db.transaction(() => {
    const positions = db.prepare(`
      SELECT * FROM positions
      WHERE opened_at >= ? AND opened_at < ?
      ORDER BY opened_at, position_id
    `).all(since, until).map((row) => censorPositionAtCutoff(row, until));

    const researchEvents = db.prepare(`
      SELECT r.*
      FROM position_research_events r
      JOIN positions p ON p.position_id = r.position_id
      WHERE p.opened_at >= ? AND p.opened_at < ?
        AND r.received_at < ?
      ORDER BY r.received_at, r.id
    `).all(since, until, until);

    const trades = db.prepare(`
      SELECT t.*
      FROM trades t
      JOIN positions p ON p.position_id = t.position_id
      WHERE p.opened_at >= ? AND p.opened_at < ?
        AND t.ts < ?
      ORDER BY t.ts, t.id
    `).all(since, until, until);

    const signals = db.prepare(`
      SELECT s.*
      FROM signals s
      WHERE s.ts >= ? AND s.ts < ?
      ORDER BY s.ts, s.id
    `).all(since - 10_000, until);

    const swaps = db.prepare(`
      SELECT s.*
      FROM swap_events s
      WHERE s.ts >= ? AND s.ts < ?
        AND EXISTS (
          SELECT 1 FROM positions p
          WHERE p.mint = s.mint
            AND p.opened_at >= ?
            AND p.opened_at < ?
            AND s.ts >= p.opened_at - 10000
            AND s.ts <= MIN(COALESCE(p.closed_at, ?), ?) + ?
        )
      ORDER BY s.ts, s.id
    `).all(since - 10_000, until + postMs, since, until, until, until, postMs);

    const postExitStats = hasTable('post_exit_stats')
      ? db.prepare(`
          SELECT x.*
          FROM post_exit_stats x
          JOIN positions p ON p.position_id = x.position_id
          WHERE p.opened_at >= ? AND p.opened_at < ?
            AND x.exit_ts < ?
            AND x.finalized_at < ?
          ORDER BY x.exit_ts
        `).all(since, until, until, until)
      : [];

    const tokens = hasTable('tokens')
      ? db.prepare(`
          SELECT t.*
          FROM tokens t
          WHERE EXISTS (
            SELECT 1 FROM positions p
            WHERE p.mint = t.mint
              AND p.opened_at >= ?
              AND p.opened_at < ?
          )
          ORDER BY t.mint
        `).all(since, until)
      : [];

    return { positions, researchEvents, trades, signals, swaps, postExitStats, tokens };
  });

  const data = readSnapshot();
  writeCsv(
    path.join(outDir, 'positions.csv'),
    data.positions.map(flattenPositionRow),
  );
  writeCsv(
    path.join(outDir, 'position-research-events.csv'),
    data.researchEvents.map(flattenResearchRow),
  );
  writeCsv(path.join(outDir, 'trades.csv'), data.trades);
  writeCsv(path.join(outDir, 'signals.csv'), data.signals);
  writeCsv(path.join(outDir, 'swaps.csv'), data.swaps);
  writeCsv(path.join(outDir, 'post-exit-stats.csv'), data.postExitStats);
  writeCsv(path.join(outDir, 'tokens.csv'), data.tokens);

  const countsByType = {};
  for (const event of data.researchEvents) {
    countsByType[event.event_type] = (countsByType[event.event_type] || 0) + 1;
  }
  const manifest = {
    schemaVersion: 1,
    generatedAt: Date.now(),
    generatedAtIso: new Date().toISOString(),
    dbPath,
    since,
    sinceIso: new Date(since).toISOString(),
    analysisCutoffMs: until,
    analysisCutoffIso: new Date(until).toISOString(),
    postExitWindowMs: postMs,
    counts: {
      positions: data.positions.length,
      closedPositions: data.positions.filter((row) => row.status === 'closed').length,
      openAtCutoff: data.positions.filter(
        (row) => row.status === 'open_at_cutoff',
      ).length,
      researchEvents: data.researchEvents.length,
      researchEventsByType: countsByType,
      swaps: data.swaps.length,
      trades: data.trades.length,
      signals: data.signals.length,
      tokens: data.tokens.length,
      postExitStats: data.postExitStats.length,
    },
    files: [
      'positions.csv',
      'position-research-events.csv',
      'trades.csv',
      'signals.csv',
      'swaps.csv',
      'post-exit-stats.csv',
      'tokens.csv',
      'data-dictionary.json',
    ],
  };
  const dataDictionary = {
    schemaVersion: 1,
    cutoffRule:
      'Rows and outcomes at or after manifest.analysisCutoffMs are excluded or censored.',
    files: {
      'positions.csv':
        'One row per entry. entry_metric_* columns flatten the exact entry signal snapshot.',
      'position-research-events.csv':
        'One row per trusted position-time event. Current and previous 1s/3s/5s/10s windows are flattened.',
      'swaps.csv':
        'Parsed trusted swaps from 10s before entry through the configured post-exit window, capped at the cutoff.',
      'trades.csv': 'Submitted BUY/SELL transactions linked to exported positions.',
      'signals.csv': 'Accepted and rejected signals near the analysis interval.',
      'post-exit-stats.csv': 'Completed post-exit follow-up summaries known before the cutoff.',
      'tokens.csv': 'Token registry metadata for exported mints.',
    },
    researchEventTypes: {
      POSITION_OPENED: 'A submitted BUY was registered as a position.',
      BUY_RECONCILED: 'Confirmed BUY with actual SOL, token amount, price and slot.',
      BUY_RECONCILED_FALLBACK:
        'BUY confirmation parsing failed but wallet balance proved ownership.',
      BUY_CHAIN_FAILED: 'Submitted BUY failed or did not land.',
      BUY_PARSE_FAILED: 'BUY landed but neither parsing nor wallet balance proved ownership.',
      BUY_RECONCILE_TIMEOUT: 'BUY remained unconfirmed with no wallet balance after 60s.',
      SWAP_METRICS: 'Rolling feature snapshot after a trusted swap.',
      EEI_CANDIDATE: 'First tick satisfying the early-entry invalidation candidate.',
      EEI_SHADOW_TRIGGER: 'Confirmed EEI shadow signal; no sell was submitted.',
      EEI_LIVE_TRIGGER: 'Confirmed EEI signal in live mode.',
      TRAILING_ARMED: 'Trailing exit became active.',
      FDV_STOP_TRIGGER: 'Realtime FDV crossed the configured exit threshold.',
      ACTUAL_EXIT_TRIGGER: 'A production exit condition requested a sell.',
      POSITION_CLOSED: 'Sell reconciliation completed; realized values are in metric_realized_*.',
    },
    timeColumns: {
      ts: 'Chain/event timestamp in milliseconds.',
      received_at: 'Local process receive timestamp in milliseconds.',
      event_lag_ms: 'received_at minus ts.',
      market_data_age_ms: 'received_at minus the matched realtime market snapshot timestamp.',
    },
  };
  fs.writeFileSync(
    path.join(outDir, 'manifest.json'),
    `${JSON.stringify(manifest, null, 2)}\n`,
    'utf8',
  );
  fs.writeFileSync(
    path.join(outDir, 'data-dictionary.json'),
    `${JSON.stringify(dataDictionary, null, 2)}\n`,
    'utf8',
  );
  db.close();
  console.log(JSON.stringify({ outDir, ...manifest.counts }, null, 2));
}

if (require.main === module) {
  try {
    main();
  } catch (err) {
    console.error(`[export-position-research] ${err.stack || err.message}`);
    process.exitCode = 1;
  }
}

module.exports = {
  censorPositionAtCutoff,
  flattenPositionRow,
  flattenResearchRow,
  main,
};
