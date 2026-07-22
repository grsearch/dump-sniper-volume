'use strict';

const assert = require('assert');
const Module = require('module');

const monitor = {
  registerModule() {},
  inc() {},
  beat() {},
};

const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  if (request === '../monitor/HealthMonitor') return { getMonitor: () => monitor };
  return originalLoad.call(this, request, parent, isMain);
};
const PriceTracker = require('../src/core/PriceTracker');
Module._load = originalLoad;

const tracker = new PriceTracker();
const mint = 'TestMint111111111111111111111111111111111';
const updates = [];
tracker.on('update', (event) => updates.push(event));

assert.strictEqual(tracker.update(mint, 1, 1000, 'pool', {
  slot: 200,
  signature: 'slot-200',
  source: 'chain_swap',
  rawPrice: 0.88,
  virtualQuoteReserveSol: 12.5,
  effectiveQuoteReserveSol: 112.5,
}), true);
assert.strictEqual(tracker.get(mint).slot, 200);
assert.strictEqual(tracker.get(mint).rawPrice, 0.88);
assert.strictEqual(tracker.get(mint).virtualQuoteReserveSol, 12.5);
assert.strictEqual(updates[0].effectiveQuoteReserveSol, 112.5);

assert.strictEqual(tracker.update(mint, 0.9, 1100, 'pool', {
  slot: 199,
  signature: 'slot-199',
  source: 'chain_swap',
}), false);
assert.strictEqual(tracker.getPrice(mint), 1);
assert.strictEqual(updates.length, 1);

assert.strictEqual(tracker.update(mint, 0.95, 1200, 'pool', {
  source: 'pool_poll_rpc',
  marketSource: 'rpc',
  snapshotRequestedAt: 1150,
  snapshotFetchedAt: 1190,
}), true);
assert.strictEqual(tracker.getPrice(mint), 0.95);
assert.strictEqual(tracker.get(mint).slot, 200, 'unsequenced RPC updates preserve the slot floor');
assert.strictEqual(tracker.get(mint).marketSource, 'rpc');
assert.strictEqual(tracker.get(mint).snapshotRequestedAt, 1150);
assert.strictEqual(tracker.get(mint).snapshotFetchedAt, 1190);

assert.strictEqual(tracker.update(mint, 0.8, 1300, 'pool', {
  slot: 199,
  signature: 'late-slot-199',
  source: 'chain_swap',
}), false);
assert.strictEqual(tracker.getPrice(mint), 0.95);

assert.strictEqual(tracker.update(mint, 0.9, 1400, 'pool', {
  slot: 201,
  signature: 'slot-201',
  source: 'chain_swap',
}), true);
assert.strictEqual(tracker.getPrice(mint), 0.9);
assert.strictEqual(tracker.get(mint).slot, 201);

console.log('Price slot ordering tests: PASS');
