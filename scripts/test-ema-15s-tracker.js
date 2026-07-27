'use strict';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const Ema15sTracker = require('../src/core/Ema15sTracker');
Module._load = originalLoad;

const tracker = new Ema15sTracker({
  fastPeriod: 2,
  slowPeriod: 3,
  barMs: 1_000,
  resetGapMs: 5_000,
  executionDelayMs: 500,
});
const crosses = [];
tracker.on('downCross', (cross) => crosses.push(cross));

function tick(ts, price) {
  tracker.handlePriceTick({
    mint: 'EmaMint111111111111111111111111111111111',
    ts,
    price,
    slot: ts,
    signature: `sig-${ts}`,
  });
}

tick(100, 1);
tick(1_100, 2);
tick(2_100, 3);
tick(3_100, 1);
tick(4_100, 1);
assert.strictEqual(crosses.length, 0, 'EMA cross must wait 500ms after the closed bar');
tick(4_499, 1);
assert.strictEqual(crosses.length, 0);
tick(4_500, 1);
assert.strictEqual(crosses.length, 1);
assert.strictEqual(crosses[0].crossAt, 4_000);
assert.strictEqual(crosses[0].dueAt, 4_500);
assert(crosses[0].emaFast < crosses[0].emaSlow);

const staleTracker = new Ema15sTracker({
  fastPeriod: 2,
  slowPeriod: 3,
  barMs: 1_000,
  resetGapMs: 5_000,
  executionDelayMs: 500,
});
const staleCrosses = [];
staleTracker.on('downCross', (cross) => staleCrosses.push(cross));
for (const [ts, price] of [[100, 1], [1_100, 2], [2_100, 3], [3_100, 1], [4_100, 1]]) {
  staleTracker.handlePriceTick({
    mint: 'StaleEmaMint11111111111111111111111111111',
    ts,
    price,
  });
}
staleTracker.handlePriceTick({
  mint: 'StaleEmaMint11111111111111111111111111111',
  ts: 10_100,
  price: 1,
});
assert.strictEqual(staleCrosses.length, 0, 'a cross must be discarded after a gap above 5 minutes');

console.log('Closed 15-second EMA tracker tests: PASS');
process.exit(0);
