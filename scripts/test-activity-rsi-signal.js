'use strict';

const assert = require('assert');
const EventEmitter = require('events');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const SignalEngine = require('../src/core/SignalEngine');
Module._load = originalLoad;

function makeEngine(openForMint = 0) {
  const engine = Object.create(SignalEngine.prototype);
  EventEmitter.call(engine);
  engine.lastTriggerTs = new Map();
  engine.ourSignatures = new Set();
  engine.inflightBuys = new Set();
  engine._exitCooldowns = new Map();
  engine.positionManager = {
    openPositionCount: () => openForMint,
    openPositionCountByMint: () => openForMint,
    hasOpenPosition: () => openForMint > 0,
  };
  engine.loggedSignals = [];
  engine.tradeLogger = { logSignal: (row) => engine.loggedSignals.push(row) };
  return engine;
}

function signal(mint, overrides = {}) {
  return {
    mint,
    symbol: 'TEST',
    signature: 'activity-rsi:test',
    ts: Date.now(),
    slot: 123,
    priceAfter: 1.03,
    _activityRsi: true,
    _activity: {
      volumeSol: 140,
      volumeUsd: 10_500,
      previousRsi5s: 29,
      currentRsi5s: 31,
      ...overrides,
    },
  };
}

async function run() {
  const mint = 'SignalMint111111111111111111111111111111';
  const engine = makeEngine();
  let order = null;
  engine.on('buyOrder', (value) => { order = value; });
  await engine.handleActivityRsiSignal(signal(mint));
  await new Promise((resolve) => setImmediate(resolve));
  assert(order, 'valid activity/RSI signal must emit a buy order');
  assert(order.reason.includes('volume1m=$10500'));
  assert(order.reason.includes('rsi5s=29.0->31.0'));
  assert.strictEqual(engine.loggedSignals[0].kind, 'ACTIVITY_RSI');

  const blocked = makeEngine(1);
  let blockedOrder = null;
  blocked.on('buyOrder', (value) => { blockedOrder = value; });
  await blocked.handleActivityRsiSignal(signal(mint));
  assert.strictEqual(blockedOrder, null, 'existing position must block add-on');

  const lowVolume = makeEngine();
  let lowVolumeOrder = null;
  lowVolume.on('buyOrder', (value) => { lowVolumeOrder = value; });
  await lowVolume.handleActivityRsiSignal(signal(mint, { volumeUsd: 10_000 }));
  assert.strictEqual(lowVolumeOrder, null, 'exactly $10,000 must be rejected');

  const noCross = makeEngine();
  let noCrossOrder = null;
  noCross.on('buyOrder', (value) => { noCrossOrder = value; });
  await noCross.handleActivityRsiSignal(signal(mint, { previousRsi5s: 31, currentRsi5s: 32 }));
  assert.strictEqual(noCrossOrder, null, 'RSI already above 30 is not a fresh cross');

  console.log('Activity/RSI signal engine tests: PASS');
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
