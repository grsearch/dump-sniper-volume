'use strict';

process.env.BURST_EXIT_TAKE_PROFIT_PCT = '20';
process.env.BURST_EXIT_STOP_LOSS_PCT = '-10';
process.env.BURST_EXIT_MAX_HOLD_MS = '120000';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const PositionManager = require('../src/core/PositionManager');
const { config } = require('../src/config');
Module._load = originalLoad;

function position(id, mint, overrides = {}) {
  const now = Date.now();
  return {
    positionId: id,
    mint,
    symbol: 'TEST',
    entryPrice: 1,
    highWaterMark: 1,
    openedAt: now,
    reconciledAt: now,
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
  manager._tickCount = 0;
  manager.priceTracker = { getPrice: () => 1, forceSet() {} };
  manager.executor = null;
  manager.tokenRegistry = null;
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

function run() {
  const mint = 'TestMint111111111111111111111111111111111';
  assert.strictEqual(config.strategy.dedicatedExitOnly, true);
  assert.strictEqual(config.strategy.takeProfitPct, 20);
  assert.strictEqual(config.strategy.fixedStopLossPct, -10);
  assert.strictEqual(config.strategy.maxHoldMs, 120_000);
  assert.strictEqual(config.strategy.rsi1mExitEnabled, false);
  assert.strictEqual(config.strategy.flowReversalExitEnabled, false);
  assert.strictEqual(config.strategy.trailingActivatePct, 0);
  assert.strictEqual(config.strategy.trailingDrawdownPct, 0);

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    manager._checkExit('p1', 0.9);
    assert(Number.isFinite(manager.positions.get('p1')._exitTriggeredAt));
    assert.deepStrictEqual(manager._exitCalls.map((item) => item.id), ['p1', 'p2']);
    assert(manager._exitCalls.every((item) => item.reason === 'FIXED_STOP_LOSS'));
  }

  {
    const manager = managerWith(position('p1', mint));
    manager._checkExit('p1', 0.9001);
    assert.strictEqual(manager._exitCalls.length, 0, 'stop must not trigger above -10%');
  }

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    manager._checkExit('p1', 1.2);
    assert.deepStrictEqual(manager._exitCalls.map((item) => item.id), ['p1', 'p2']);
    assert(manager._exitCalls.every((item) => item.reason === 'TAKE_PROFIT'));
  }

  {
    const manager = managerWith(position('p1', mint));
    manager._checkExit('p1', 1.1999);
    assert.strictEqual(manager._exitCalls.length, 0, 'take profit must not trigger below +20%');
  }

  {
    const manager = managerWith(position('p1', mint));
    assert.strictEqual(
      manager.handleRsiForExit(mint, 1.3, { rsi1mLive: 99, rsi1mClosedBars: 20 }),
      false,
    );
    assert.strictEqual(manager._exitCalls.length, 0, 'RSI exit must remain disabled');
  }

  {
    const manager = managerWith(position('p1', mint, { openedAt: Date.now() - 120_001 }));
    manager.priceTracker = { getPrice: () => 1.05, forceSet() {} };
    manager._tick();
    assert.strictEqual(manager._exitCalls.length, 1);
    assert.strictEqual(manager._exitCalls[0].reason, 'MAX_HOLD');
  }

  console.log('Dedicated position exit policy tests: PASS');
}

run();
process.exit(0);
