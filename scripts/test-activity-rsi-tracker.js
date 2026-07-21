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

function swap(mint, ts, solVolume, price = 1) {
  return {
    mint,
    symbol: 'TEST',
    side: 'BUY',
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
    rsiCalculator: {
      snapshot: () => ({ rsi5s: rsi, bucketCount5s: 8 }),
    },
    solPriceUsd: 1,
    minVolumeUsd: 100,
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

  console.log('Activity/RSI tracker tests: PASS');
  process.exit(0);
}

run();
