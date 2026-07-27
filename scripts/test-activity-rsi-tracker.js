'use strict';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const ActivityRsiTracker = require('../src/core/ActivityRsiTracker');
Module._load = originalLoad;

function swap(mint, ts, solVolume, price = 1, signer = `wallet:${ts}`) {
  return {
    mint,
    symbol: 'TEST',
    side: 'BUY',
    signer,
    solVolume,
    price,
    priceBefore: price,
    ts,
    slot: 1,
    signature: `sig:${ts}`,
    poolAddress: 'Pool1111111111111111111111111111111111',
    poolQuoteAfter: 50,
  };
}

function makeTracker(opts = {}) {
  let rsi = 29;
  const tracker = new ActivityRsiTracker({
    enabled: true,
    rsiCalculator: {
      snapshot: () => ({ rsi5s: rsi, bucketCount5s: 8 }),
    },
    solPriceUsd: 1,
    minVolumeUsd: 100,
    minUniqueBuyers1m: 1,
    maxSignalAgeMs: 0,
    ...opts,
  });
  return { tracker, setRsi: (value) => { rsi = value; } };
}

function run() {
  const mint = 'ActivityMint11111111111111111111111111111';
  const now = Date.now();

  {
    const { tracker, setRsi } = makeTracker();
    const signals = [];
    tracker.on('activityRsiSignal', (signal) => signals.push(signal));
    tracker.handleSwap(swap(mint, now - 30_000, 60));
    setRsi(31);
    tracker.handleSwap(swap(mint, now, 41));
    assert.strictEqual(signals.length, 1, 'volume > 100 and RSI 29->31 must buy');
    assert.strictEqual(signals[0]._activityRsi, true);
    assert.strictEqual(signals[0]._activity.volumeUsd, 101);
    assert.strictEqual(signals[0]._activity.previousRsi5s, 29);
    assert.strictEqual(signals[0]._activity.currentRsi5s, 31);
  }

  {
    const { tracker, setRsi } = makeTracker();
    const signals = [];
    tracker.on('activityRsiSignal', (signal) => signals.push(signal));
    tracker.handleSwap(swap(mint, now - 30_000, 60));
    setRsi(31);
    tracker.handleSwap(swap(mint, now, 40));
    assert.strictEqual(signals.length, 0, 'volume exactly at threshold must not buy');
  }

  {
    const { tracker, setRsi } = makeTracker();
    const signals = [];
    tracker.on('activityRsiSignal', (signal) => signals.push(signal));
    tracker.handleSwap(swap(mint, now - 30_000, 100));
    setRsi(30);
    tracker.handleSwap(swap(mint, now, 1));
    assert.strictEqual(signals.length, 0, 'RSI exactly 30 is not an upward cross');
  }

  {
    const { tracker, setRsi } = makeTracker({ minUniqueBuyers1m: 3 });
    const signals = [];
    tracker.on('activityRsiSignal', (signal) => signals.push(signal));
    tracker.handleSwap(swap(mint, now - 20_000, 40, 1, 'wallet-a'));
    tracker.handleSwap(swap(mint, now - 10_000, 40, 1, 'wallet-a'));
    setRsi(31);
    tracker.handleSwap(swap(mint, now, 40, 1, 'wallet-b'));
    assert.strictEqual(signals.length, 0, 'duplicate buyers must count once');
  }

  {
    const { tracker, setRsi } = makeTracker({ minUniqueBuyers1m: 3 });
    const signals = [];
    tracker.on('activityRsiSignal', (signal) => signals.push(signal));
    tracker.handleSwap(swap(mint, now - 20_000, 40, 1, 'wallet-a'));
    tracker.handleSwap(swap(mint, now - 10_000, 40, 1, 'wallet-b'));
    setRsi(31);
    tracker.handleSwap(swap(mint, now, 40, 1, 'wallet-c'));
    assert.strictEqual(signals.length, 1, 'three independent BUY wallets must pass');
    assert.strictEqual(signals[0]._activity.uniqueBuyers1m, 3);
  }

  console.log('Activity/RSI tracker tests: PASS');
  process.exit(0);
}

run();
