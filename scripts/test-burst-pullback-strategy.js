'use strict';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const BurstPullbackTracker = require('../src/core/OrderFlowTracker');
Module._load = originalLoad;

function event(mint, ts, side, solVolume, price, signer) {
  return {
    mint,
    symbol: 'TEST',
    signer,
    side,
    solVolume,
    price,
    priceBefore: price,
    ts,
    slot: 1,
    signature: signer + ':' + ts,
    poolAddress: 'Pool1111111111111111111111111111111111',
    poolQuoteAfter: 50,
  };
}

function run() {
  const mint = 'BurstMint1111111111111111111111111111111';
  const confirmAt = Date.now();
  const tracker = new BurstPullbackTracker({
    maxSignalAgeMs: 10_000,
    cooldownMs: 300_000,
  });
  const state = tracker._stateOf(mint);

  const baseline = [
    event(mint, confirmAt - 15_000, 'BUY', 1, 99, 'OLD_A'),
    event(mint, confirmAt - 13_000, 'SELL', 1, 100, 'OLD_B'),
  ];
  const burst = [
    event(mint, confirmAt - 9_500, 'BUY', 2, 101, 'BURST_A'),
    event(mint, confirmAt - 8_500, 'SELL', 1, 102, 'BURST_S1'),
    event(mint, confirmAt - 7_500, 'BUY', 2, 103, 'BURST_B'),
    event(mint, confirmAt - 6_500, 'SELL', 1, 104, 'BURST_S2'),
  ];
  state.events.push(...baseline, ...burst);
  tracker._detectFirstBurst(state, burst[burst.length - 1]);

  assert(state.burst, '3x volume and 2x TPS must arm the first burst');
  assert.strictEqual(state.burst.preBurstPrice, 100);
  assert.strictEqual(state.burst.volumeMultiple, 3);
  assert.strictEqual(state.burst.tpsMultiple, 2);

  const signals = [];
  tracker.on('burstPullbackSignal', (signal) => signals.push(signal));
  tracker.handleSwap(event(mint, confirmAt - 4_000, 'BUY', 4, 106, 'NEW_A'));
  tracker.handleSwap(event(mint, confirmAt - 2_500, 'BUY', 3, 104, 'NEW_B'));
  assert.strictEqual(signals.length, 0, 'pullback below 2% must not buy');
  tracker.handleSwap(event(mint, confirmAt - 1_000, 'SELL', 0.5, 103, 'NEW_S'));

  assert.strictEqual(signals.length, 1, 'all confirmation conditions must emit one signal');
  const signal = signals[0];
  assert.strictEqual(signal._burstPullback, true);
  assert(signal._burst.peakRisePct >= 5);
  assert(signal._burst.pullbackPct >= 2 && signal._burst.pullbackPct <= 8);
  assert(signal._burst.netFlow5sSol > 0);
  assert(signal._burst.current5SellSol < signal._burst.previous5SellSol);
  assert(signal._burst.buyerAcceleration >= 1.5);
  assert(signal._burst.currentNewBuyers > signal._burst.previousNewBuyers);
  assert((tracker.cooldowns.get(mint) || 0) > Date.now() + 299_000);

  const quietTracker = new BurstPullbackTracker();
  const quietState = quietTracker._stateOf(mint);
  quietState.events.push(...baseline, ...burst);
  quietState.lastEquivalentBurstAt = burst[burst.length - 1].ts - 10_000;
  quietTracker._detectFirstBurst(quietState, burst[burst.length - 1]);
  assert.strictEqual(quietState.burst, null, 'same-level burst inside 30s must not arm');

  const emptyBaselineTracker = new BurstPullbackTracker();
  const emptyState = emptyBaselineTracker._stateOf(mint);
  emptyState.events.push(...burst);
  emptyBaselineTracker._detectFirstBurst(emptyState, burst[burst.length - 1]);
  assert.strictEqual(emptyState.burst, null, 'zero-baseline expansion must be rejected');

  const buyerTracker = new BurstPullbackTracker();
  const buyerState = buyerTracker._stateOf(mint);
  const buyerBurst = {
    detectedAt: 10_000,
    buyersKnownAtBurst: new Set(['OLD_BUYER']),
  };
  buyerState.events.push(
    event(mint, 12_000, 'BUY', 1, 101, 'NEW_ONCE'),
    event(mint, 22_000, 'BUY', 1, 102, 'NEW_ONCE'),
    event(mint, 23_000, 'BUY', 1, 103, 'NEW_TWO'),
    event(mint, 24_000, 'BUY', 1, 104, 'OLD_BUYER'),
  );
  assert.strictEqual(
    buyerTracker._newBuyerCount(buyerState, buyerBurst, 10_000, 20_000),
    1,
  );
  assert.strictEqual(
    buyerTracker._newBuyerCount(buyerState, buyerBurst, 20_000, 30_000),
    1,
    'repeat buyers and buyers known before the burst must not count as new',
  );

  console.log('First-burst pullback strategy tests: PASS');
}

run();
process.exit(0);
