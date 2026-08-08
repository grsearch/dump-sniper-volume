'use strict';

const assert = require('assert');
const { DatabaseSync } = require('node:sqlite');
const Executor = require('../src/core/Executor');
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

  const settlement = Object.create(PositionManager.prototype);
  const unsettledPos = {
    positionId: 'unsettled-position',
    mint: pos.mint,
    symbol: 'UNSETTLED',
    status: 'sell_confirming',
    exiting: true,
  };
  settlement.executor = {
    getWalletTokenBalanceSnapshot: async () => ({ rawAmount: 0n, uiAmount: 0 }),
  };
  let stuckReason = null;
  let closed = false;
  settlement.tradeLogger = {
    updateTradeConfirmation: () => {},
    markStuck: (_positionId, reason) => { stuckReason = reason; },
    closePosition: () => { closed = true; },
  };
  const handled = await settlement._settleConfirmedSell(
    unsettledPos,
    'unsettled-signature',
    {
      success: true,
      realTokenDelta: -100,
      realSolDelta: 0,
      externalWsolIncreases: [{ address: 'ExternalWsol', deltaSol: 0.2 }],
    },
    1,
    1,
  );
  assert.strictEqual(handled, true);
  assert.strictEqual(closed, false, 'missing proceeds must never synthesize a -100% close');
  assert.strictEqual(unsettledPos.status, 'stuck');
  assert.match(stuckReason, /without_wallet_quote/);
  assert.match(stuckReason, /ExternalWsol/);

  const executor = Object.create(Executor.prototype);
  executor.dryRun = false;
  executor.keypair = { publicKey: { toBase58: () => 'Wallet1111111111111111111111111111111111' } };
  executor.onlineSdk = {};
  executor.poolStateCache = { get: () => ({ pool: {} }) };
  executor.getWalletTokenBalanceSnapshot = async () => ({
    rawAmount: 98_345_398_488n,
    decimals: 6,
    uiAmount: 98_345.398488,
  });
  let quotedRaw = null;
  executor.pumpSdk = {
    sellBaseInput: async (_state, rawAmount) => {
      quotedRaw = rawAmount.toString();
      return {};
    },
  };
  executor._extractInstructions = () => [{}];
  executor._extractQuoteAmount = () => 400_000_000;
  executor._buildAndSignTx = async () => ({
    serialized: Buffer.alloc(65),
    feeInfo: { totalLamports: 5000, source: 'test' },
    lastValidBlockHeight: 1,
  });
  executor._submitTx = async () => {};
  const exactSell = await executor._sell({
    mint: 'So11111111111111111111111111111111111111112',
    symbol: 'EXACT',
    poolAddress: 'So11111111111111111111111111111111111111112',
    tokenAmount: 101_402.430108,
    sellAllAvailable: true,
    baseDecimals: 6,
    currentPrice: 0.000004,
  });
  assert.strictEqual(exactSell.success, true);
  assert.strictEqual(quotedRaw, '98345398488', 'sell quote must use the exact wallet raw balance');
  assert.strictEqual(exactSell.sellAmount, 98_345.398488);

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
  logger.logTrade({
    positionId: pos.positionId,
    mint: pos.mint,
    side: 'SELL',
    signature: 'sell-signature',
    success: false,
    error: 'pending_chain_confirmation',
    confirmationStatus: 'submitted',
  });
  let trade = db.prepare('SELECT * FROM trades WHERE signature = ?').get('sell-signature');
  assert.strictEqual(trade.success, 0, 'submission must not be reported as a successful sell');
  assert.strictEqual(trade.confirmation_status, 'submitted');
  logger.updateTradeConfirmation('sell-signature', {
    success: true,
    solAmount: 0.21,
    tokenAmount: 100,
    price: 0.0021,
  });
  trade = db.prepare('SELECT * FROM trades WHERE signature = ?').get('sell-signature');
  assert.strictEqual(trade.success, 1);
  assert.strictEqual(trade.confirmation_status, 'confirmed');
  assert.strictEqual(trade.sol_amount, 0.21);
  logger.updatePositionTokenAmount(pos.positionId, 98.5, 'balance corrected');
  const corrected = db.prepare(
    'SELECT token_amount, last_error FROM positions WHERE position_id = ?',
  ).get(pos.positionId);
  assert.strictEqual(corrected.token_amount, 98.5);
  assert.strictEqual(corrected.last_error, 'balance corrected');
  logger.markStuck(pos.positionId, 'settlement unresolved');
  const restoredStuck = logger.getOpenPositions().find(
    (row) => row.position_id === pos.positionId,
  );
  assert.strictEqual(restoredStuck.status, 'stuck', 'unresolved settlement must survive restart');
  assert.strictEqual(
    logger.getDuePendingRetries(Date.now() + 60_000).some(
      (row) => row.position_id === pos.positionId,
    ),
    false,
    'an unresolved settlement must not submit another sell automatically',
  );
  logger.shutdown();
  db.close();

  console.log('Sell confirmation guard tests: PASS');
  process.exit(0);
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
