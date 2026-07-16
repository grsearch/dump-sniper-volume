'use strict';

process.env.RSI_1M_EXIT_ENABLED = 'true';
process.env.RSI_1M_EXIT_THRESHOLD = '80';
process.env.ACTIVITY_FLOW_RSI_1M_MIN_BARS = '8';

const assert = require('assert');
const Module = require('module');

// This policy test does not need dotenv; stub it so the test also runs in a
// dependency-light checkout used by CI/static validation.
const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const PositionManager = require('../src/core/PositionManager');
Module._load = originalLoad;

function position(id, mint, overrides = {}) {
  return {
    positionId: id,
    mint,
    symbol: 'TEST',
    reconciled: true,
    dryRun: false,
    stabilizing: false,
    trailingArmed: false,
    exiting: false,
    status: 'open',
    ...overrides,
  };
}

function managerWith(...positions) {
  const manager = Object.create(PositionManager.prototype);
  manager.positions = new Map();
  manager.byMint = new Map();
  manager._exitCalls = [];
  manager._exit = function mockExit(pos, price, reason) {
    if (pos.exiting) return;
    pos.exiting = true;
    pos.exitReason = reason;
    this._exitCalls.push({ id: pos.positionId, price, reason });
  };

  for (const pos of positions) {
    manager.positions.set(pos.positionId, pos);
    if (!manager.byMint.has(pos.mint)) manager.byMint.set(pos.mint, new Set());
    manager.byMint.get(pos.mint).add(pos.positionId);
  }
  return manager;
}

function rsiSnapshot(live, overrides = {}) {
  return {
    rsi1mLive: live,
    rsi1mClosed: 75,
    rsi1mClosedBars: 8,
    ...overrides,
  };
}

function run() {
  const mint = 'TestMint111111111111111111111111111111111';

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    assert.strictEqual(manager.handleRsiForExit(mint, 1, rsiSnapshot(80)), false);
    assert.strictEqual(manager._exitCalls.length, 0, 'RSI must be strictly greater than 80');
  }

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    assert.strictEqual(manager.handleRsiForExit(mint, 1, rsiSnapshot(80.1)), true);
    assert.deepStrictEqual(manager._exitCalls.map((x) => x.id), ['p1', 'p2']);
    assert(manager._exitCalls.every((x) => x.reason === 'RSI_1M_EXIT'));
  }

  {
    const manager = managerWith(
      position('p1', mint),
      position('p2', mint, { trailingArmed: true }),
    );
    assert.strictEqual(manager.handleRsiForExit(mint, 1, rsiSnapshot(95)), false);
    assert.strictEqual(manager._exitCalls.length, 0, 'armed trailing must suppress RSI for the whole mint');
  }

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    assert.strictEqual(
      manager.handleRsiForExit(mint, 1, rsiSnapshot(95, { rsi1mClosedBars: 7 })),
      false,
    );
    assert.strictEqual(manager._exitCalls.length, 0, 'RSI needs the configured completed-bar minimum');
  }

  {
    const first = position('p1', mint);
    const second = position('p2', mint);
    const manager = managerWith(first, second);
    manager._exitForCondition(second, 0.8, 'TRAILING_STOP');
    assert.deepStrictEqual(manager._exitCalls.map((x) => x.id), ['p1', 'p2']);
    assert(manager._exitCalls.every((x) => x.reason === 'TRAILING_STOP'));
  }

  {
    const first = position('p1', mint, {
      exiting: true,
      openedAt: 1,
      entryPrice: 1,
    });
    const manager = managerWith(first);
    manager.priceTracker = { getPrice: () => 0.7 };
    assert.strictEqual(manager.canAddOn(mint).reason, 'group_exit_in_progress');
  }

  console.log('Position exit policy tests: PASS');
}

run();
process.exit(0);
