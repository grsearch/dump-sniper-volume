'use strict';

const assert = require('assert');
const Module = require('module');
const {
  getVirtualQuoteReservesRaw,
  priceDetailsFromRawState,
  priceDetailsFromUiReserves,
  constantProductAfterBaseUi,
} = require('../src/utils/pumpSwapPricing');

function approx(actual, expected, tolerance = 1e-12) {
  assert(
    Math.abs(actual - expected) <= tolerance,
    `expected ${actual} to be within ${tolerance} of ${expected}`,
  );
}

const state = {
  poolBaseAmount: { toString: () => '100000000000000' }, // 100M tokens, 6 decimals
  poolQuoteAmount: { toString: () => '135800000000' },   // 135.8 SOL
  pool: {
    virtualQuoteReserves: { toString: () => '17900000000' }, // 17.9 SOL
  },
};

assert.strictEqual(getVirtualQuoteReservesRaw(state), 17_900_000_000);

const rawStatePricing = priceDetailsFromRawState(state, 6);
approx(rawStatePricing.rawPrice, 1.358e-6);
approx(rawStatePricing.effectivePrice, 1.537e-6);
approx(rawStatePricing.virtualQuoteUi, 17.9);
approx(rawStatePricing.effectiveQuoteUi, 153.7);
approx(
  ((rawStatePricing.effectivePrice - rawStatePricing.rawPrice) / rawStatePricing.rawPrice) * 100,
  13.181148748159055,
  1e-9,
);

const uiPricing = priceDetailsFromUiReserves(100_000_000, 135.8, state);
approx(uiPricing.effectivePrice, rawStatePricing.effectivePrice);

const simulatedSell = constantProductAfterBaseUi({
  baseBeforeUi: 100_000_000,
  baseAfterUi: 110_000_000,
  rawQuoteBeforeUi: 135.8,
  state,
});
assert(simulatedSell);
approx(simulatedSell.priceBefore, 1.537e-6);
approx(simulatedSell.effectiveQuoteAfterUi, 139.72727272727272, 1e-10);
approx(simulatedSell.rawQuoteAfterUi, 121.82727272727271, 1e-10);
approx(simulatedSell.quoteAmountUi, 13.972727272727269, 1e-10);

assert.strictEqual(
  priceDetailsFromRawState({
    poolBaseAmount: state.poolBaseAmount,
    poolQuoteAmount: state.poolQuoteAmount,
    pool: {},
  }, 6),
  null,
  'missing virtual reserve must not silently fall back to raw-vault pricing',
);

const zeroVirtualState = {
  ...state,
  pool: { virtualQuoteReserves: { toString: () => '0' } },
};
const zeroVirtualPricing = priceDetailsFromRawState(zeroVirtualState, 6);
approx(zeroVirtualPricing.effectivePrice, zeroVirtualPricing.rawPrice);

const monitor = { registerModule() {}, inc() {} };
const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === 'bs58') return { default: { encode: (value) => String(value) } };
  if (request === '../config') {
    return {
      config: {
        programs: { pumpAmm: 'PumpAmm111111111111111111111111111111111' },
        strategy: {},
      },
    };
  }
  if (request === '../monitor/HealthMonitor') return { getMonitor: () => monitor };
  return originalLoad.call(this, request, parent, isMain);
};
const DumpDetector = require('../src/core/DumpDetector');
Module._load = originalLoad;

const detector = Object.create(DumpDetector.prototype);
detector.poolStateCache = { get: () => state };
const baseMint = 'BaseMint111111111111111111111111111111111';
const wsolMint = 'So11111111111111111111111111111111111111112';
const tokenBalance = (accountIndex, mint, amount, decimals) => ({
  accountIndex,
  mint,
  uiTokenAmount: { amount: String(amount), decimals },
});
const parsed = detector._parseFullVault(
  {},
  {},
  [
    tokenBalance(1, baseMint, '100000000000000', 6),
    tokenBalance(2, wsolMint, '135800000000', 9),
  ],
  [
    tokenBalance(1, baseMint, '110000000000000', 6),
    tokenBalance(2, wsolMint, '121827272727', 9),
  ],
  1,
  2,
  baseMint,
  6,
  { pool_address: 'Pool111111111111111111111111111111111', symbol: 'TEST' },
  'BaseVault1111111111111111111111111111111',
  'QuoteVault111111111111111111111111111111',
  'signature',
  'signer',
  123,
  [],
);
assert(parsed);
assert.strictEqual(parsed.side, 'SELL');
approx(parsed.priceBefore, 1.537e-6);
approx(parsed.rawPriceBefore, 1.358e-6);
approx(parsed.virtualQuoteReserveSol, 17.9);
approx(parsed.priceAfter, 1.2702479338818182e-6, 1e-15);
approx(parsed.poolQuoteAfter, 121.827272727);

console.log('PumpSwap effective-reserve pricing tests: PASS');
