'use strict';

const assert = require('assert');
const {
  computeWalletQuoteAssetMovement,
} = require('../src/utils/quoteAssetAccounting');

const WALLET = 'Gu3si111111111111111111111111111111111tatVF';
const WSOL = 'So11111111111111111111111111111111111111112';
const ESCROW = 'DmrQLy5nVJNnRrP8RimSuW8GJxvcjByizcYVzcyFEJFZ';
const ESCROW_OWNER = 'FtgZ6iPt4PjyHVyWRRhsooGVwA2U2vfDrTwtiStdqrXS';
const WSOL_RENT = 2_039_280;

function tokenBalance(amount, accountIndex = 1, owner = WALLET) {
  return {
    accountIndex,
    mint: WSOL,
    owner,
    uiTokenAmount: { amount: String(amount), decimals: 9 },
  };
}

function tx({
  preNative,
  postNative,
  preWsol = 0,
  postWsol = 0,
  preEscrow = 0,
  postEscrow = 0,
}) {
  return {
    transaction: { message: { accountKeys: [WALLET, ESCROW] } },
    meta: {
      preBalances: [
        preNative,
        preWsol ? preWsol + WSOL_RENT : preEscrow ? preEscrow + WSOL_RENT : 0,
      ],
      postBalances: [
        postNative,
        postWsol ? postWsol + WSOL_RENT : postEscrow ? postEscrow + WSOL_RENT : 0,
      ],
      preTokenBalances: [
        ...(preWsol ? [tokenBalance(preWsol)] : []),
        ...(preEscrow ? [tokenBalance(preEscrow, 1, ESCROW_OWNER)] : []),
      ],
      postTokenBalances: [
        ...(postWsol ? [tokenBalance(postWsol)] : []),
        ...(postEscrow ? [tokenBalance(postEscrow, 1, ESCROW_OWNER)] : []),
      ],
    },
  };
}

function closeTo(actual, expected, label) {
  assert.ok(
    Math.abs(actual - expected) < 1e-12,
    `${label}: expected ${expected}, got ${actual}`,
  );
}

const wrapped = computeWalletQuoteAssetMovement(tx({
  preNative: 2_000_000_000,
  postNative: 997_955_720,
  postWsol: 1_000_000_000,
}), WALLET);
closeTo(wrapped.quoteDeltaSol, -0.000005, 'wrap only charges fee');

const soldIntoWsol = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 997_955_720,
  postWsol: 200_000_000,
}), WALLET);
closeTo(soldIntoWsol.quoteDeltaSol, 0.199995, 'sell into WSOL is income');

const unwrapped = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 2_002_034_280,
  preWsol: 1_000_000_000,
}), WALLET);
closeTo(unwrapped.quoteDeltaSol, -0.000005, 'unwrap only charges fee');

const soldIntoEscrow = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 999_995_000,
  postEscrow: 200_000_000,
}), WALLET, WSOL, [ESCROW]);
closeTo(soldIntoEscrow.quoteDeltaSol, 0.199995, 'sell into verified escrow is income');

const settledEscrow = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 1_199_995_000,
  preEscrow: 200_000_000,
}), WALLET, WSOL, [ESCROW]);
closeTo(settledEscrow.quoteDeltaSol, -0.000005, 'escrow settlement only charges fee');

console.log('quote asset accounting tests passed');
