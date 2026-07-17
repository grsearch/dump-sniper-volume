'use strict';

process.env.RSI_1M_EXIT_ENABLED = 'true';
process.env.RSI_1M_EXIT_THRESHOLD = '80';
process.env.ACTIVITY_FLOW_RSI_1M_MIN_BARS = '8';
process.env.FIXED_STOP_LOSS_PCT = '-30';

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
const { config } = require('../src/config');
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
  manager._rsiExitSkipLogAt = new Map();
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
  assert.strictEqual(config.strategy.rebuyCooldownMs, 300_000, 'default post-sale cooldown must be 5 minutes');
  assert.strictEqual(config.strategy.fixedStopLossPct, -30);

  {
    const now = Date.now();
    const first = position('p1', mint, {
      entryPrice: 1,
      highWaterMark: 1,
      openedAt: now,
      reconciledAt: now,
      stabilizing: true,
      _stabilizeSamples: [],
    });
    const second = position('p2', mint, { entryPrice: 1, highWaterMark: 1 });
    const manager = managerWith(first, second);
    manager._checkExit('p1', 0.7);
    assert.deepStrictEqual(manager._exitCalls.map((x) => x.id), ['p1', 'p2']);
    assert(manager._exitCalls.every((x) => x.reason === 'FIXED_STOP_LOSS'));
  }

  {
    const now = Date.now();
    const first = position('p1', mint, {
      entryPrice: 1,
      highWaterMark: 1,
      openedAt: now,
      reconciledAt: now,
      stabilizing: true,
      _stabilizeSamples: [],
    });
    const manager = managerWith(first);
    manager._checkExit('p1', 0.7001);
    assert.strictEqual(manager._exitCalls.length, 0, 'fixed stop must not trigger above -30%');
  }

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
    assert.strictEqual(manager._rsiExitSkipLogAt.get(mint).reason, 'trailing_armed');
  }

  {
    const manager = managerWith(position('p1', mint, { stabilizing: true }));
    assert.strictEqual(manager.handleRsiForExit(mint, 1, rsiSnapshot(95)), false);
    assert.strictEqual(manager._rsiExitSkipLogAt.get(mint).reason, 'stabilizing');
  }

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    assert.strictEqual(
      manager.handleRsiForExit(mint, 1, rsiSnapshot(95, { rsi1mClosedBars: 7 })),
      false,
    );
    assert.strictEqual(manager._exitCalls.length, 0, 'RSI needs the configured completed-bar minimum');
    assert.strictEqual(manager._rsiExitSkipLogAt.get(mint).reason, 'insufficient_bars');
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
