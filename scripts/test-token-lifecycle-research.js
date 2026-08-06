'use strict';

const assert = require('assert');
const Module = require('module');
const { DatabaseSync } = require('node:sqlite');
const TradeLogger = require('../src/data/TradeLogger');
const originalLoad = Module._load;
Module._load = function loadWithDependencyStubs(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const MigrationHolderSnapshotCollector = require('../src/core/MigrationHolderSnapshotCollector');
Module._load = originalLoad;

function compatibleDatabase() {
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

async function run() {
  const db = compatibleDatabase();
  const logger = new TradeLogger(db);
  const mint = 'MintResearch111111111111111111111111111111';
  const migrationTime = Date.now() - 100;

  logger.logMigrationDetection({
    mint,
    migrationSignature: 'migration-holder-signature',
    migrationSlot: 123,
    migrationTime,
    detectedAt: migrationTime + 10,
  });
  logger.logMigrationDetection({
    mint,
    migrationSignature: 'later-duplicate-signature',
    migrationSlot: 124,
    migrationTime,
    detectedAt: migrationTime + 50,
    detectionPath: 'poll',
    detectionSlot: 125,
    poolAddress: 'pool-owner',
  });
  const detection = db.prepare('SELECT * FROM migration_detection_events WHERE mint = ?').get(mint);
  assert.strictEqual(
    db.prepare('SELECT COUNT(*) AS count FROM migration_detection_events').get().count,
    1,
  );
  assert.strictEqual(detection.migration_signature, 'migration-holder-signature');
  assert.strictEqual(detection.detected_at, migrationTime + 10);
  assert.strictEqual(detection.detection_path, 'poll');
  assert.strictEqual(detection.pool_address, 'pool-owner');

  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_ADDED:${mint}:1000`,
    mint,
    symbol: 'RESEARCH',
    eventType: 'TOKEN_ADDED',
    ts: 1000,
    addedAt: 1000,
    migrationTime: 900,
    fdvUsd: 25_000,
    liquidityUsd: 5_000,
  });
  logger.logTokenMarketSnapshot({
    mint,
    symbol: 'RESEARCH',
    ts: 2000,
    trigger: 'realtime',
    fdvUsd: 24_000,
    liquidityUsd: 4_800,
    priceSol: 0.000001,
  });
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_REMOVED:${mint}:1000`,
    mint,
    symbol: 'RESEARCH',
    eventType: 'TOKEN_REMOVED',
    ts: 3000,
    addedAt: 1000,
    reason: 'fdv_below_min',
  });
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_ADDED:${mint}:4000`,
    mint,
    symbol: 'RESEARCH',
    eventType: 'TOKEN_ADDED',
    ts: 4000,
    addedAt: 4000,
    migrationTime: 900,
  });
  assert.strictEqual(logger.getOpenTokenLifecycleSession(mint).added_at, 4000);

  let rpcCalls = 0;
  const collector = new MigrationHolderSnapshotCollector({
    tradeLogger: logger,
    enabled: true,
    pageSize: 100,
    maxPages: 1,
    retryDelaysMs: [0],
    rpcRequest: async (method, params) => {
      rpcCalls++;
      if (method === 'getTokenSupply') {
        assert(Array.isArray(params));
        return { value: { amount: '1000000000', decimals: 6 } };
      }
      if (method === 'getTokenAccounts') {
        assert.strictEqual(params.mint, mint);
        return {
          last_indexed_slot: 124,
          total: 5,
          token_accounts: [
            { address: 'pool-vault', owner: 'pool-owner', amount: '400000000' },
            { address: 'wallet-a-1', owner: 'wallet-a', amount: '100000000' },
            { address: 'wallet-a-2', owner: 'wallet-a', amount: '50000000' },
            { address: 'wallet-b-1', owner: 'wallet-b', amount: '60000000' },
            { address: 'wallet-c-1', owner: 'wallet-c', amount: '10000000' },
          ],
        };
      }
      if (method === 'getMultipleAccounts') {
        assert.deepStrictEqual(params[0], ['wallet-a', 'wallet-b', 'wallet-c']);
        return {
          value: params[0].map(() => ({
            owner: '11111111111111111111111111111111',
            executable: false,
          })),
        };
      }
      throw new Error(`unexpected RPC method ${method}`);
    },
  });

  const captureContext = {
    token: { mint, symbol: 'RESEARCH' },
    migration: {
      mint,
      signature: 'migration-holder-signature',
      slot: 123,
      migrationTime,
      poolBaseVault: 'pool-vault',
      poolAddress: 'pool-owner',
    },
  };
  await collector.capture(captureContext);
  assert.strictEqual(rpcCalls, 3);
  await collector.capture({
    ...captureContext,
    migration: {
      ...captureContext.migration,
      signature: 'duplicate-detection-signature',
    },
  });
  assert.strictEqual(rpcCalls, 3, 'a duplicate mint must not create another Holder snapshot');

  const lifecycle = db.prepare(
    'SELECT event_type, reason FROM token_lifecycle_events ORDER BY ts',
  ).all();
  assert.deepStrictEqual(
    lifecycle.map((row) => row.event_type),
    ['TOKEN_ADDED', 'TOKEN_REMOVED', 'TOKEN_ADDED'],
  );
  assert.strictEqual(lifecycle[1].reason, 'fdv_below_min');

  const market = db.prepare('SELECT * FROM token_market_snapshots').get();
  assert.strictEqual(market.fdv_usd, 24_000);
  assert.strictEqual(market.trigger, 'realtime');

  const holders = db.prepare('SELECT * FROM migration_holder_snapshots').get();
  assert.strictEqual(holders.holder_count, 3);
  assert.strictEqual(holders.token_account_count, 5);
  assert.strictEqual(holders.excluded_pool_amount, 400);
  assert.strictEqual(holders.top1_pct, 15);
  assert.strictEqual(holders.top5_pct, 22);
  assert.strictEqual(holders.largest_holder_owner, 'wallet-a');
  const details = JSON.parse(holders.holders_json);
  assert.strictEqual(details.top[0].tokenAccounts.length, 2);
  assert.strictEqual(details.top[0].ownerType, 'wallet');
  assert.strictEqual(details.ownerClassificationComplete, true);
  assert.strictEqual(details.lastIndexedSlot, 124);
  assert.strictEqual(details.migrationToIndexSlotDelta, 1);
  assert.strictEqual(
    db.prepare('SELECT COUNT(*) AS count FROM migration_holder_snapshots').get().count,
    1,
  );

  logger.shutdown();
  db.close();
  console.log('token lifecycle research tests passed');
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
