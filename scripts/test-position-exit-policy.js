'use strict';

process.env.ACTIVITY_RSI_TRAILING_ACTIVATE_PCT = '20';
process.env.ACTIVITY_RSI_TRAILING_DRAWDOWN_PCT = '10';
process.env.ACTIVITY_RSI_STOP_LOSS_PCT = '-20';
process.env.ACTIVITY_RSI_EXIT_DOWN_CROSS = '70';
process.env.ACTIVITY_RSI_EXIT_OVERBOUGHT = '80';

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
    highWaterMarkTs: now,
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
  manager._lastRsi5sByMint = new Map();
  manager._pendingRsi5sExit = new Map();
  manager._exitCalls = [];
  manager._tickCount = 0;
  manager.priceTracker = { getPrice: () => 1, forceSet() {} };
  manager.executor = null;
  manager.tokenRegistry = null;
  manager.tradeLogger = null;
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

function rsi(value) {
  return { rsi5s: value, bucketCount5s: 8 };
}

function run() {
  const mint = 'TestMint111111111111111111111111111111111';
  assert.strictEqual(config.strategy.dedicatedExitOnly, true);
  assert.strictEqual(config.strategy.takeProfitPct, 0);
  assert.strictEqual(config.strategy.fixedStopLossPct, -20);
  assert.strictEqual(config.strategy.maxHoldMs, 0);
  assert.strictEqual(config.strategy.flowReversalExitEnabled, false);
  assert.strictEqual(config.strategy.trailingActivatePct, 20);
  assert.strictEqual(config.strategy.trailingDrawdownPct, 10);
  assert.strictEqual(config.strategy.rsi5sExitDownCross, 70);
  assert.strictEqual(config.strategy.rsi5sExitOverbought, 80);

  {
    const manager = managerWith();
    const price = manager._priceFromState({
      poolBaseAmount: { toString: () => '100000000000000' },
      poolQuoteAmount: { toString: () => '135800000000' },
      pool: { virtualQuoteReserves: { toString: () => '17900000000' } },
    }, 6);
    assert(Math.abs(price - 1.537e-6) < 1e-15, 'position polling must include virtual reserves');
  }

  {
    const manager = managerWith(position('p1', mint));
    manager._checkExit('p1', 0.801);
    assert.strictEqual(manager._exitCalls.length, 0, '-19.9% must not trigger the fixed stop');
    manager._checkExit('p1', 0.8);
    assert.strictEqual(manager._exitCalls[0].reason, 'FIXED_STOP_LOSS');
  }

  {
    const manager = managerWith(position('p1', mint, { buySlot: 200 }));
    manager._checkExit('p1', 0.8, {
      slot: 199,
      signature: 'older-signature',
      source: 'chain_swap',
    });
    manager._checkExit('p1', 0.8, {
      slot: 200,
      signature: 'same-slot-signature',
      source: 'chain_swap',
    });
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'prices from before or within the BUY landing slot must not trigger a stop',
    );
    assert.strictEqual(manager.positions.get('p1').tickCount, undefined);
    manager._checkExit('p1', 0.8, {
      slot: 201,
      signature: 'post-buy-signature',
      source: 'chain_swap',
    });
    assert.strictEqual(manager._exitCalls[0].reason, 'FIXED_STOP_LOSS');
  }

  {
    const manager = managerWith(position('p1', mint, {
      buySlot: 200,
      reconciledAt: 2_000,
    }));
    manager._checkExit('p1', 0.8, {
      slot: 0,
      source: 'pool_poll_rpc_cache',
      marketSource: 'rpc',
      snapshotRequestedAt: 1_999,
      snapshotFetchedAt: 2_001,
    });
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'an RPC request started before BUY reconciliation must not trigger a stop',
    );
    assert.strictEqual(manager.positions.get('p1').tickCount, undefined);
  }

  {
    const manager = managerWith(position('p1', mint, {
      buySlot: 200,
      reconciledAt: 2_000,
    }));
    manager._checkExit('p1', 0.8, {
      slot: 0,
      source: 'pool_poll_rpc',
      marketSource: 'rpc',
      snapshotRequestedAt: 2_001,
      snapshotFetchedAt: 2_002,
    });
    assert.strictEqual(
      manager._exitCalls[0].reason,
      'FIXED_STOP_LOSS',
      'a fresh RPC pool quote must remain eligible for immediate stop loss',
    );
  }

  {
    const manager = managerWith(position('p1', mint));
    manager._checkExit('p1', 1.2);
    assert.strictEqual(manager.positions.get('p1').trailingArmed, true, '+20% must arm trailing');
    assert.strictEqual(manager._exitCalls.length, 0);
    manager._checkExit('p1', 1.08);
    assert.strictEqual(manager._exitCalls[0].reason, 'TRAILING_STOP');
  }

  {
    const manager = managerWith(position('p1', mint), position('p2', mint));
    const requested = manager.forceExitAllByMint(mint, 'TOKEN_AGE_EXPIRED');
    assert.strictEqual(requested, 2);
    assert.strictEqual(manager._exitCalls.length, 2);
    assert(manager._exitCalls.every((call) => call.reason === 'TOKEN_AGE_EXPIRED'));
  }

  {
    const manager = managerWith(position('p1', mint));
    manager.handleRsi5sForExit(mint, 1.1, rsi(69));
    manager.handleRsi5sForExit(mint, 1.1, rsi(71));
    assert.strictEqual(manager._exitCalls.length, 0, 'upward cross of 70 must not sell');
    manager.handleRsi5sForExit(mint, 1.1, rsi(69));
    assert.strictEqual(manager._exitCalls[0].reason, 'RSI_5S_DOWN_CROSS_70');
  }

  {
    const manager = managerWith(position('p1', mint));
    manager.handleRsi5sForExit(mint, 1.1, rsi(80));
    assert.strictEqual(manager._exitCalls.length, 0, 'RSI exactly 80 must not sell');
    manager.handleRsi5sForExit(mint, 1.1, rsi(80.1));
    assert.strictEqual(manager._exitCalls[0].reason, 'RSI_5S_OVERBOUGHT');
  }

  {
    const manager = managerWith(position('p1', mint, {
      trailingArmed: true,
      highWaterMark: 1.3,
      _armedHwm: 1.3,
    }));
    manager._pendingRsi5sExit.set(mint, 'RSI_5S_OVERBOUGHT');
    manager.handleRsi5sForExit(mint, 1.2, rsi(71));
    manager.handleRsi5sForExit(mint, 1.2, rsi(69));
    manager.handleRsi5sForExit(mint, 1.2, rsi(80.1));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'armed trailing must suppress both RSI exit rules',
    );
    assert.strictEqual(
      manager._pendingRsi5sExit.has(mint),
      false,
      'arming trailing must clear a pending RSI exit',
    );
  }

  {
    const manager = managerWith(position('p1', mint, { openedAt: Date.now() - 999_999 }));
    manager.priceTracker = { getPrice: () => 1.05, forceSet() {} };
    manager._tick();
    assert.strictEqual(manager._exitCalls.length, 0, 'timeout exit must be disabled');
  }

  console.log('Dedicated activity/RSI exit policy tests: PASS');
  process.exit(0);
}

run();
