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

function makeEngine(openForMint = 0, rsi5s = 55, bucketCount5s = 8) {
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
  engine.rsiCalculator = {
    snapshot: () => ({ rsi5s, bucketCount5s }),
  };
  engine.loggedSignals = [];
  engine.tradeLogger = {
    logSignal: (row) => engine.loggedSignals.push(row),
  };
  return engine;
}

function signal(mint) {
  return {
    mint,
    symbol: 'TEST',
    sellSol: 0.5,
    priceImpactPct: 3,
    signature: 'burst:test',
    ts: Date.now(),
    slot: 123,
    priceAfter: 1.03,
    _burstPullback: true,
    _burst: {
      volumeMultiple: 3.2,
      tpsMultiple: 2.1,
      peakRisePct: 6,
      pullbackPct: 3,
      netFlow5sSol: 6.5,
      buyerAcceleration: 1.75,
      previousNewBuyers: 1,
      currentNewBuyers: 3,
    },
  };
}

async function run() {
  const mint = 'SignalMint111111111111111111111111111111';
  const engine = makeEngine();
  let order = null;
  engine.on('buyOrder', (value) => { order = value; });
  await engine.handleDumpSignal(signal(mint));
  await new Promise((resolve) => setImmediate(resolve));

  assert(order, 'dedicated signal must emit a buy order');
  assert(order.reason.startsWith('burst_pullback:'));
  assert(order.reason.includes('rsi5s=55.0'));
  assert.strictEqual(order.sizeSol > 0, true);
  assert.strictEqual(engine.inflightBuys.has(mint), true);
  assert.strictEqual(engine.loggedSignals[0].kind, 'BURST_PULLBACK');

  const blocked = makeEngine(1);
  let blockedOrder = null;
  blocked.on('buyOrder', (value) => { blockedOrder = value; });
  await blocked.handleDumpSignal(signal(mint));
  assert.strictEqual(blockedOrder, null, 'existing position must block add-on');
  assert.strictEqual(blocked.loggedSignals[0].accepted, false);
  assert(blocked.loggedSignals[0].rejectReason.includes('add-on disabled'));

  const atLimit = makeEngine(0, 70, 8);
  let atLimitOrder = null;
  atLimit.on('buyOrder', (value) => { atLimitOrder = value; });
  await atLimit.handleDumpSignal(signal(mint));
  assert.strictEqual(atLimitOrder, null, 'RSI exactly at 70 must be rejected');
  assert(atLimit.loggedSignals[0].rejectReason.includes('RSI_5S_HIGH'));

  const insufficient = makeEngine(0, null, 8);
  let insufficientOrder = null;
  insufficient.on('buyOrder', (value) => { insufficientOrder = value; });
  await insufficient.handleDumpSignal(signal(mint));
  assert.strictEqual(insufficientOrder, null, 'insufficient RSI history must be rejected');
  assert(insufficient.loggedSignals[0].rejectReason.includes('RSI_5S_UNAVAILABLE'));

  console.log('Dedicated burst signal engine tests: PASS');
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
