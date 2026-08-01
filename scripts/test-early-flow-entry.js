'use strict';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const EarlyFlowEntryTracker = require('../src/core/EarlyFlowEntryTracker');
Module._load = originalLoad;

const mint = 'EarlyFlowMint11111111111111111111111111111';
const poolAddress = 'EarlyFlowPool111111111111111111111111111';
const migrationTime = Date.now() - 15_000;
let marketPrice = 1;

const tokenRegistry = {
  getToken: () => ({
    mint,
    symbol: 'EARLY',
    migration_time: migrationTime,
    pool_address: poolAddress,
    is_active: 1,
  }),
};
const marketProvider = () => ({
  fdvUsd: 50_000,
  liquidityUsd: 12_000,
  priceSol: marketPrice,
  poolQuoteSol: 80,
  poolAddress,
  fetchedAt: Date.now(),
});

function swap(offsetMs, {
  price = 1,
  side = 'BUY',
  solVolume = 0.1,
  signer = `wallet-${offsetMs}`,
} = {}) {
  marketPrice = price;
  return {
    mint,
    symbol: 'EARLY',
    poolAddress,
    ts: migrationTime + offsetMs,
    slot: offsetMs,
    signature: `sig-${offsetMs}-${side}`,
    price,
    side,
    solVolume,
    signer,
    poolQuoteAfter: 80,
  };
}

function qualifyingTracker(overrides = {}) {
  return new EarlyFlowEntryTracker({
    tokenRegistry,
    marketProvider,
    minMigrationAgeMs: 15_000,
    maxMigrationAgeMs: 25_000,
    ...overrides,
  });
}

function feedQualifyingWindow(tracker, finalPrice = 1.02) {
  tracker.handleSwap(swap(5_000, { price: 1, side: 'SELL', solVolume: 0.05 }));
  tracker.handleSwap(swap(11_500, {
    price: 1.005,
    side: 'BUY',
    solVolume: 0.8,
    signer: 'buyer-a',
  }));
  tracker.handleSwap(swap(12_500, {
    price: 1.01,
    side: 'BUY',
    solVolume: 0.7,
    signer: 'buyer-b',
  }));
  tracker.handleSwap(swap(14_200, {
    price: 1.015,
    side: 'SELL',
    solVolume: 0.05,
    signer: 'seller-a',
  }));
  tracker.handleSwap(swap(15_000, {
    price: finalPrice,
    side: 'BUY',
    solVolume: 0.6,
    signer: 'buyer-c',
  }));
}

{
  const tracker = qualifyingTracker();
  const signals = [];
  tracker.on('earlyFlowSignal', (signal) => signals.push(signal));
  feedQualifyingWindow(tracker);
  assert.strictEqual(signals.length, 0, 'the qualifying swap must only arm the signal');
  tracker.handleSwap(swap(15_400, {
    price: 1.03,
    side: 'SELL',
    solVolume: 0.01,
    signer: 'seller-b',
  }));
  assert.strictEqual(signals.length, 1, 'the next trusted swap must execute the signal');
  assert.strictEqual(signals[0]._earlyFlow, true);
  assert.strictEqual(signals[0]._earlyFlowDetails.uniqueBuyers5s, 3);
  assert.strictEqual(signals[0]._earlyFlowDetails.tradeCount5s, 4);
  assert(signals[0]._earlyFlowDetails.buySol5s >= 2);
  assert.strictEqual(signals[0]._earlyFlowDetails.executionDelayMs, 400);
  assert.strictEqual(signals[0]._earlyFlowDetails.signalMigrationAgeMs, 15_000);
  assert(Number.isFinite(signals[0]._earlyFlowDetails.preEntryVwap5s));
  assert(signals[0]._earlyFlowDetails.preEntryVwap5s > 1);
  assert.strictEqual(signals[0]._earlyFlowDetails.preEntryUniqueBuyers3s, 2);
  assert.strictEqual(signals[0]._earlyFlowDetails.entryRiskScore, 0);
  assert.strictEqual(signals[0]._earlyFlowDetails.entryRiskBlocked, false);
}

{
  const tracker = qualifyingTracker();
  const signals = [];
  tracker.on('earlyFlowSignal', (signal) => signals.push(signal));
  feedQualifyingWindow(tracker);
  tracker.handleSwap(swap(15_500, { price: 1.18, side: 'BUY', signer: 'buyer-d' }));
  tracker.handleSwap(swap(16_000, { price: 1.10, side: 'SELL', signer: 'seller-b' }));
  assert.strictEqual(
    signals.length,
    1,
    'an over-cap execution must be skipped while an executable trade is still allowed within 3s',
  );
  assert.strictEqual(signals[0]._earlyFlowDetails.executionDelayMs, 1_000);
}

{
  const tracker = qualifyingTracker();
  const signals = [];
  tracker.on('earlyFlowSignal', (signal) => signals.push(signal));
  tracker.handleSwap(swap(5_000, { price: 1, side: 'SELL', solVolume: 0.01 }));
  tracker.handleSwap(swap(11_500, {
    price: 1,
    side: 'BUY',
    solVolume: 1.6,
    signer: 'buyer-a',
  }));
  tracker.handleSwap(swap(12_500, {
    price: 1,
    side: 'BUY',
    solVolume: 0.2,
    signer: 'buyer-b',
  }));
  tracker.handleSwap(swap(14_000, {
    price: 1,
    side: 'SELL',
    solVolume: 0.01,
    signer: 'seller-a',
  }));
  tracker.handleSwap(swap(15_000, {
    price: 1,
    side: 'BUY',
    solVolume: 0.2,
    signer: 'buyer-c',
  }));
  tracker.handleSwap(swap(15_500, { price: 1, side: 'BUY', signer: 'buyer-d' }));
  assert.strictEqual(signals.length, 0, 'a single buy above 70% must not arm');
}

{
  const tracker = qualifyingTracker();
  const signals = [];
  tracker.on('earlyFlowSignal', (signal) => signals.push(signal));
  tracker.handleSwap(swap(11_500, {
    price: 1,
    side: 'BUY',
    solVolume: 0.6,
    signer: 'buyer-a',
  }));
  tracker.handleSwap(swap(12_500, {
    price: 1.01,
    side: 'BUY',
    solVolume: 0.6,
    signer: 'buyer-b',
  }));
  tracker.handleSwap(swap(14_200, {
    price: 1.01,
    side: 'SELL',
    solVolume: 0.05,
    signer: 'seller-a',
  }));
  tracker.handleSwap(swap(15_000, {
    price: 1.02,
    side: 'BUY',
    solVolume: 0.6,
    signer: 'buyer-c',
  }));
  tracker.handleSwap(swap(15_400, { price: 1.02, side: 'SELL', solVolume: 0.05 }));
  assert.strictEqual(signals.length, 0, 'less than 2 SOL of real 5s buys must not arm');
}

{
  const tracker = qualifyingTracker();
  const signals = [];
  tracker.on('earlyFlowSignal', (signal) => signals.push(signal));
  feedQualifyingWindow(tracker);
  tracker.handleSwap(swap(18_100, { price: 1.02, side: 'BUY', signer: 'buyer-d' }));
  assert.strictEqual(signals.length, 0, 'the execution window must expire after 3 seconds');
}

console.log('Early-flow entry tracker tests: PASS');
process.exit(0);
