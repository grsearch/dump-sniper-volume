'use strict';

const assert = require('assert');
const { DatabaseSync } = require('node:sqlite');
const PositionManager = require('../src/core/PositionManager');
const TradeLogger = require('../src/data/TradeLogger');

(async () => {
  const manager = Object.create(PositionManager.prototype);
  const pos = {
    positionId: 'guard-position',
    mint: 'GuardMint111111111111111111111111111111111',
    symbol: 'GUARD',
  };
  manager.positions = new Map([[pos.positionId, pos]]);
  manager._sellAttemptsInFlight = new Set();
  let unlockedCalls = 0;
  let release;
  manager._attemptSellUnlocked = async () => {
    unlockedCalls++;
    await new Promise((resolve) => { release = resolve; });
    return true;
  };

  const first = manager._attemptSell(pos, 1);
  await new Promise((resolve) => setImmediate(resolve));
  const duplicate = await manager._attemptSell(pos, 1);
  assert.strictEqual(duplicate, false, 'a second unresolved sell must be suppressed');
  assert.strictEqual(unlockedCalls, 1, 'only one transaction may be built for a position');
  release();
  await first;
  assert.strictEqual(manager._sellAttemptsInFlight.size, 0, 'the guard must release after completion');

  const reconciler = Object.create(PositionManager.prototype);
  const deferred = [];
  let replacementAttempts = 0;
  let currentBlockHeight = 123455;
  reconciler.positions = new Map([[pos.positionId, pos]]);
  reconciler.tradeLogger = {
    getDuePendingRetries: () => [{
      position_id: pos.positionId,
      status: 'sell_confirming',
      last_retry_at: 0,
      pending_sell_signature: 'sell-signature',
      pending_sell_last_valid_block_height: 123456,
    }],
    deferSellConfirmation: (positionId, nextRetryAt) => deferred.push({ positionId, nextRetryAt }),
  };
  reconciler.executor = {
    confirmTx: async () => ({ confirmed: false, error: 'not_landed' }),
    getCurrentBlockHeight: async () => currentBlockHeight,
  };
  reconciler.priceTracker = { getPrice: () => 1 };
  reconciler._attemptSell = async () => { replacementAttempts++; };
  await reconciler._reconcileRetriesInner();
  assert.strictEqual(deferred.length, 1, 'an unexpired sell must be deferred');
  assert.strictEqual(replacementAttempts, 0, 'an unexpired sell must not be replaced');
  currentBlockHeight = 123457;
  await reconciler._reconcileRetriesInner();
  assert.strictEqual(replacementAttempts, 1, 'an expired sell may be replaced');

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
  const logger = new TradeLogger(db);
  db.prepare(`
    INSERT INTO positions (position_id, mint, opened_at, status)
    VALUES (?, ?, ?, 'open')
  `).run(pos.positionId, pos.mint, Date.now());
  const deadline = Date.now() + 30_000;
  logger.markSellPending(pos.positionId, 'sell-signature', 'TEST_EXIT', deadline, 123456);
  const stored = db.prepare(`
    SELECT status, next_retry_at, pending_sell_signature,
           pending_sell_last_valid_block_height
    FROM positions WHERE position_id = ?
  `).get(pos.positionId);
  assert.strictEqual(stored.status, 'sell_confirming');
  assert.strictEqual(stored.next_retry_at, deadline);
  assert.strictEqual(stored.pending_sell_signature, 'sell-signature');
  assert.strictEqual(stored.pending_sell_last_valid_block_height, 123456);
  assert.strictEqual(logger.getDuePendingRetries(deadline - 1).length, 0);
  assert.strictEqual(logger.getDuePendingRetries(deadline).length, 1);
  logger.deferSellConfirmation(pos.positionId, deadline + 5_000);
  assert.strictEqual(logger.getDuePendingRetries(deadline).length, 0);
  assert.strictEqual(logger.getDuePendingRetries(deadline + 5_000).length, 1);
  logger.shutdown();
  db.close();

  console.log('Sell confirmation guard tests: PASS');
  process.exit(0);
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
