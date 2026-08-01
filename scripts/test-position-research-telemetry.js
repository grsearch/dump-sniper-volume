'use strict';

const assert = require('assert');
const TradeLogger = require('../src/data/TradeLogger');

function createDatabase() {
  try {
    const Database = require('better-sqlite3');
    return new Database(':memory:');
  } catch (_) {
    const { DatabaseSync } = require('node:sqlite');
    const db = new DatabaseSync(':memory:');
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
  }
}

function run() {
  const db = createDatabase();
  const logger = new TradeLogger(db);
  const openedAt = 1_785_280_000_000;

  logger.openPosition({
    positionId: 'research-position-1',
    mint: 'ResearchMint111111111111111111111111111111',
    symbol: 'RESEARCH',
    openedAt,
    entrySol: 0.2,
    entryPrice: 0.000001,
    tokenAmount: 200_000,
    dryRun: false,
    buySignature: 'buy-signature',
    buySlot: 123,
    entryFdv: 50_000,
    entryPoolSol: 80,
    entryLiquidity: 12_000,
    entrySignalPrice: 0.00000098,
    preEntryVwap5s: 0.00000096,
    preEntryUniqueBuyers3s: 4,
    entryMetrics: {
      ageMs: 20_500,
      tradeCount5s: 6,
      uniqueBuyers5s: 4,
      netFlow1s: 0.12,
    },
  });

  logger.updateRunnerState('research-position-1', true, openedAt + 2_000);
  const restoredPosition = logger.getOpenPositions().find(
    (row) => row.position_id === 'research-position-1',
  );
  assert(restoredPosition, 'open position should be restorable');
  assert.strictEqual(restoredPosition.runner_armed, 1);
  assert.strictEqual(restoredPosition.runner_armed_at, openedAt + 2_000);

  logger.logSwapEvent({
    ts: openedAt + 1_000,
    receivedAt: openedAt + 1_080,
    mint: 'ResearchMint111111111111111111111111111111',
    symbol: 'RESEARCH',
    signer: 'market-buyer',
    side: 'BUY',
    solVolume: 0.3,
    price: 0.00000102,
    rawPrice: 0.00000101,
    poolAddress: 'pool-1',
    poolBaseAfter: 1_000_000,
    poolQuoteAfter: 82,
    baseDecimals: 6,
    virtualQuoteReserveSol: 18,
    effectiveQuoteReserveSol: 100,
    supplyUi: 1_000_000_000,
    fdvUsd: 77_010,
    liquidityUsd: 12_382,
    priceUsd: 0.00007701,
    marketFetchedAt: openedAt + 1_040,
    signature: 'swap-signature',
    slot: 124,
  });

  logger.logPositionResearchEvent({
    positionId: 'research-position-1',
    mint: 'ResearchMint111111111111111111111111111111',
    symbol: 'RESEARCH',
    eventType: 'SWAP_METRICS',
    ts: openedAt + 1_000,
    receivedAt: openedAt + 1_080,
    holdMs: 1_080,
    slot: 124,
    signature: 'swap-signature',
    side: 'BUY',
    signer: 'market-buyer',
    solVolume: 0.3,
    price: 0.00000102,
    entrySol: 0.2,
    entryPrice: 0.000001,
    signalPrice: 0.00000098,
    preEntryVwap5s: 0.00000096,
    tokenAmount: 200_000,
    marketPnlPct: 2,
    peakPnlPct: 2,
    drawdownPct: 0,
    trailingArmed: false,
    reconciled: true,
    fdvUsd: 77_010,
    liquidityUsd: 12_382,
    metrics: {
      schemaVersion: 1,
      windows: {
        '3s': {
          tradeCount: 4,
          buySol: 0.8,
          sellSol: 0.2,
          netFlowSol: 0.6,
          uniqueBuyers: 3,
        },
      },
      structure: {
        priceBelowSignal: false,
      },
    },
  });
  assert.strictEqual(logger.flushResearchEvents(), 1);

  const position = db.prepare(
    'SELECT * FROM positions WHERE position_id = ?',
  ).get('research-position-1');
  assert.strictEqual(position.entry_signal_price, 0.00000098);
  assert.strictEqual(position.pre_entry_unique_buyers_3s, 4);
  assert.strictEqual(JSON.parse(position.entry_metrics_json).tradeCount5s, 6);

  const swap = db.prepare(
    'SELECT * FROM swap_events WHERE signature = ?',
  ).get('swap-signature');
  assert.strictEqual(swap.received_at, openedAt + 1_080);
  assert.strictEqual(swap.raw_price, 0.00000101);
  assert.strictEqual(swap.fdv_usd, 77_010);
  assert.strictEqual(swap.liquidity_usd, 12_382);

  const research = db.prepare(
    'SELECT * FROM position_research_events WHERE position_id = ?',
  ).get('research-position-1');
  assert.strictEqual(research.event_type, 'SWAP_METRICS');
  assert.strictEqual(research.market_pnl_pct, 2);
  assert.strictEqual(JSON.parse(research.metrics_json).windows['3s'].tradeCount, 4);

  const positionColumns = new Set(
    db.prepare('PRAGMA table_info(positions)').all().map((row) => row.name),
  );
  for (const column of [
    'peak_price',
    'peak_ts',
    'peak_pnl_pct',
    'time_to_peak_ms',
    'price_tick_count',
    'pre_vol_5m_pct',
    'range_support',
    'runner_armed',
    'runner_armed_at',
  ]) {
    assert(positionColumns.has(column), `fresh schema is missing ${column}`);
  }

  logger.shutdown();
  db.close();
  console.log('position research telemetry tests passed');
}

run();
