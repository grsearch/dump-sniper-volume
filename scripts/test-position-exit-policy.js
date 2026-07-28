'use strict';

process.env.EARLY_FLOW_TRAILING_ACTIVATE_PCT = '40';
process.env.EARLY_FLOW_TRAILING_DRAWDOWN_PCT = '10';
process.env.EARLY_FLOW_FDV_EXIT_USD = '10000';
process.env.EARLY_WRONG_EXIT_ENABLED = 'true';
process.env.EARLY_WRONG_EXIT_MIN_HOLD_MS = '3000';
process.env.EARLY_WRONG_EXIT_MAX_HOLD_MS = '15000';
process.env.EARLY_WRONG_EXIT_MAX_PEAK_PNL_PCT = '3';
process.env.EARLY_WRONG_EXIT_PRICE_BREAK_PCT = '-3';
process.env.EARLY_WRONG_EXIT_FLOW_WINDOW_MS = '3000';
process.env.EARLY_WRONG_EXIT_SELL_BUY_RATIO = '1.5';
process.env.EARLY_WRONG_EXIT_MAX_UNIQUE_BUYERS = '1';
process.env.EARLY_WRONG_EXIT_CONFIRM_MS = '500';
process.env.EARLY_WRONG_EXIT_CONFIRM_TRADES = '2';

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
    entrySignalPrice: 1,
    preEntryVwap5s: 1,
    preEntryUniqueBuyers3s: 3,
    buySignature: 'our-buy-signature',
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
  manager._flowExitEvents = new Map();
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

function exitSwap(mint, receivedAt, overrides = {}) {
  return {
    mint,
    side: 'SELL',
    price: 0.95,
    solVolume: 0.2,
    signer: `seller-${receivedAt}`,
    signature: `sell-${receivedAt}`,
    ts: receivedAt,
    receivedAt,
    ...overrides,
  };
}

function rsi(value) {
  return { rsi5s: value, bucketCount5s: 8 };
}

function run() {
  const mint = 'TestMint111111111111111111111111111111111';
  assert.strictEqual(config.strategy.dedicatedExitOnly, true);
  assert.strictEqual(config.strategy.takeProfitPct, 0);
  assert.strictEqual(config.strategy.fixedStopLossPct, 0);
  assert.strictEqual(config.strategy.maxHoldMs, 0);
  assert.strictEqual(config.strategy.flowReversalExitEnabled, false);
  assert.strictEqual(config.strategy.trailingActivatePct, 40);
  assert.strictEqual(config.strategy.trailingDrawdownPct, 10);
  assert.strictEqual(config.strategy.emaExitEnabled, true);
  assert.strictEqual(config.strategy.fdvExitUsd, 10_000);
  assert.strictEqual(config.strategy.rsi5sExitEnabled, false);
  assert.strictEqual(config.strategy.earlyWrongExitEnabled, true);
  assert.strictEqual(config.strategy.earlyWrongExitMinHoldMs, 3_000);
  assert.strictEqual(config.strategy.earlyWrongExitMaxHoldMs, 15_000);

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
    manager._checkExit('p1', 0.5);
    assert.strictEqual(manager._exitCalls.length, 0, 'fixed stop must remain disabled');
  }

  {
    const manager = managerWith(position('p1', mint));
    manager._checkExit('p1', 1.4);
    assert.strictEqual(manager.positions.get('p1').trailingArmed, true, '+40% must arm trailing');
    assert.strictEqual(manager._exitCalls.length, 0);
    manager._checkExit('p1', 1.26);
    assert.strictEqual(manager._exitCalls[0].reason, 'TRAILING_STOP');
  }

  {
    const manager = managerWith(position('p1', mint));
    manager.handleFdvForExit({ mint, price: 0.8, fdvUsd: 10_000 });
    assert.strictEqual(manager._exitCalls.length, 0, 'FDV exactly $10,000 must not exit');
    manager.handleFdvForExit({ mint, price: 0.8, fdvUsd: 9_999 });
    assert.strictEqual(manager._exitCalls[0].reason, 'FDV_STOP');
  }

  {
    const manager = managerWith(position('p1', mint));
    manager.handleEmaDownCross({
      mint,
      price: 1.05,
      ts: Date.now(),
      emaFast: 1.01,
      emaSlow: 1.02,
    });
    assert.strictEqual(manager._exitCalls[0].reason, 'EMA9_CROSS_BELOW_EMA20');
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
    manager.handleRsi5sForExit(mint, 1.1, rsi(69));
    manager.handleRsi5sForExit(mint, 1.1, rsi(80));
    manager.handleRsi5sForExit(mint, 1.1, rsi(80.1));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      '5-second RSI down-cross and overbought exits must both remain disabled',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: now - 5_000,
    }));
    manager.handleSwapForExit(exitSwap(mint, now));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'one invalidation trade must only arm confirmation',
    );
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(manager._exitCalls.length, 1);
    assert.strictEqual(manager._exitCalls[0].reason, 'EARLY_ENTRY_INVALIDATED');
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: now - 5_000,
      highWaterMark: 1.04,
    }));
    manager.handleSwapForExit(exitSwap(mint, now));
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'a position that already gained at least 3% must not be classified as never-strengthened',
    );
  }

  {
    const now = Date.now();
    const pos = position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: null,
      reconciled: false,
      highWaterMark: 1,
    });
    const manager = managerWith(pos);
    manager.handleSwapForExit(exitSwap(mint, now - 1_000, {
      side: 'BUY',
      price: 1.04,
      signer: 'market-buyer',
      signature: 'market-buy-before-reconcile',
    }));
    pos.reconciled = true;
    pos.reconciledAt = now - 500;
    manager.handleSwapForExit(exitSwap(mint, now));
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'a pre-reconciliation market peak must still protect a position that strengthened',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: now - 5_000,
      trailingArmed: true,
    }));
    manager.handleSwapForExit(exitSwap(mint, now));
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'trailing must own the exit after it has armed',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: now - 5_000,
    }));
    manager.handleSwapForExit(exitSwap(mint, now - 100, {
      side: 'BUY',
      price: 0.95,
      solVolume: 0.5,
      signer: 'our-wallet',
      signature: 'our-buy-signature',
    }));
    manager.handleSwapForExit(exitSwap(mint, now, { solVolume: 0.2 }));
    manager.handleSwapForExit(exitSwap(mint, now + 600, {
      price: 0.94,
      solVolume: 0.2,
    }));
    assert.strictEqual(
      manager._exitCalls[0].reason,
      'EARLY_ENTRY_INVALIDATED',
      'the bot buy must be excluded from post-entry flow',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 16_000,
      reconciledAt: now - 16_000,
    }));
    manager.handleSwapForExit(exitSwap(mint, now));
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'entry invalidation must stop evaluating after 15 seconds',
    );
  }

  {
    const manager = managerWith(position('p1', mint, { openedAt: Date.now() - 999_999 }));
    manager.priceTracker = { getPrice: () => 1.05, forceSet() {} };
    manager._tick();
    assert.strictEqual(manager._exitCalls.length, 0, 'timeout exit must be disabled');
  }

  console.log('Dedicated early-flow exit policy tests: PASS');
  process.exit(0);
}

run();
