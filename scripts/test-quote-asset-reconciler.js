'use strict';

const assert = require('assert');
const {
  nextScheduledAt,
  shouldAutoUnwrap,
} = require('../src/utils/quoteAssetSchedule');
const TradeLogger = require('../src/data/TradeLogger');

const CST_OFFSET = 8 * 60;
const hours = [0, 6, 12, 18];
const at0100Cst = Date.UTC(2026, 7, 1, 17, 0, 0);
assert.strictEqual(
  nextScheduledAt(at0100Cst, hours, CST_OFFSET),
  Date.UTC(2026, 7, 1, 22, 0, 0),
);
const at1800Cst = Date.UTC(2026, 7, 2, 10, 0, 0);
assert.strictEqual(
  nextScheduledAt(at1800Cst, hours, CST_OFFSET),
  Date.UTC(2026, 7, 2, 16, 0, 0),
);

assert.strictEqual(shouldAutoUnwrap({
  allowUnwrap: true,
  busy: false,
  amountLamports: 20_000_000n,
  minLamports: 10_000_000,
  accountCount: 1,
}), true);
assert.strictEqual(shouldAutoUnwrap({
  allowUnwrap: true,
  busy: true,
  amountLamports: 20_000_000n,
  minLamports: 10_000_000,
  accountCount: 1,
}), false);
assert.strictEqual(shouldAutoUnwrap({
  allowUnwrap: true,
  busy: false,
  amountLamports: 9_999_999n,
  minLamports: 10_000_000,
  accountCount: 1,
}), false);

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
const logger = new TradeLogger(db);
logger.logQuoteAssetMovement({
  ts: 1,
  signature: 'quote-movement-test',
  mint: 'Mint11111111111111111111111111111111111111',
  side: 'SELL',
  success: true,
  nativeSolDelta: -0.000005,
  walletWsolDelta: 0,
  jupiterEscrowWsolDelta: 0.2,
  quoteAssetDelta: 0.199995,
  preJupiterEscrowWsolSol: 0,
  postJupiterEscrowWsolSol: 0.2,
  feeLamports: 5000,
});
const movement = db.prepare(
  'SELECT * FROM quote_asset_movements WHERE signature = ?',
).get('quote-movement-test');
assert.strictEqual(movement.jupiter_escrow_wsol_delta, 0.2);
assert.strictEqual(movement.quote_asset_delta, 0.199995);

logger.logQuoteAssetReconciliation({
  ts: 2,
  reason: 'test',
  status: 'ok',
  nativeSol: 1,
  walletWsolSol: 0.1,
  walletWsolRentSol: 0.00203928,
  jupiterPendingWsolSol: 0.2,
  totalEquitySol: 1.3,
  walletWsolAccountCount: 1,
  action: 'inspect_only',
});
const reconciliation = logger.getLatestQuoteAssetReconciliation();
assert.strictEqual(reconciliation.total_equity_sol, 1.3);
assert.strictEqual(reconciliation.wallet_wsol_rent_sol, 0.00203928);
logger.shutdown();
db.close();

console.log('quote asset reconciler schedule tests passed');
