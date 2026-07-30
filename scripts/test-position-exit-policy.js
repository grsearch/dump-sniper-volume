'use strict';

process.env.EARLY_FLOW_TRAILING_ACTIVATE_PCT = '9';
process.env.EARLY_FLOW_TRAILING_DRAWDOWN_PCT = '5';
process.env.EARLY_FLOW_FDV_EXIT_USD = '10000';
process.env.EARLY_FLOW_TAIL_STOP_ENABLED = 'true';
process.env.EARLY_FLOW_TAIL_STOP_PNL_PCT = '-30';
process.env.EARLY_FLOW_TAIL_STOP_CONFIRM_MS = '500';
process.env.EARLY_FLOW_TAIL_STOP_CONFIRM_TRADES = '2';
process.env.ADDON_SHADOW_ENABLED = 'true';
process.env.EARLY_WRONG_EXIT_ENABLED = 'true';
process.env.EARLY_WRONG_EXIT_MODE = 'live';
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
  manager._addonShadowsByMint = new Map();
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
  assert.strictEqual(config.strategy.trailingActivatePct, 9);
  assert.strictEqual(config.strategy.trailingDrawdownPct, 5);
  assert.strictEqual(config.strategy.tailStopPnlPct, -30);
  assert.strictEqual(config.strategy.tailStopConfirmMs, 500);
  assert.strictEqual(config.strategy.tailStopConfirmTrades, 2);
  assert.strictEqual(config.strategy.emaExitEnabled, true);
  assert.strictEqual(config.strategy.fdvExitUsd, 10_000);
  assert.strictEqual(config.strategy.rsi5sExitEnabled, false);
  assert.strictEqual(config.strategy.earlyWrongExitEnabled, true);
  assert.strictEqual(config.strategy.earlyWrongExitMode, 'live');
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
    manager._checkExit('p1', 1.09);
    assert.strictEqual(manager.positions.get('p1').trailingArmed, true, '+9% must arm trailing');
    assert.strictEqual(manager._exitCalls.length, 0);
    manager._checkExit('p1', 1.03);
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
    const first = position('p1', mint);
    const addon = position('p2', mint, {
      entryPrice: 0.8,
      highWaterMark: 0.8,
      isAddOn: true,
    });
    const manager = managerWith(first, addon);
    manager._exitForCondition(first, 1.03, 'TRAILING_STOP');
    assert.deepStrictEqual(
      manager._exitCalls.map((call) => call.id),
      ['p1'],
      'a normal exit must only sell the position whose own condition fired',
    );
    assert.strictEqual(addon.exiting, false);
  }

  {
    const now = Date.now();
    const manager = managerWith(
      position('p1', mint, { openedAt: now - 5_000 }),
      position('p2', mint, {
        openedAt: now - 5_000,
        entryPrice: 0.9,
        highWaterMark: 0.9,
        isAddOn: true,
      }),
    );
    manager.handleSwapForExit(exitSwap(mint, now, {
      price: 0.69,
      signature: 'tail-sig-1',
    }));
    assert.strictEqual(manager._exitCalls.length, 0);
    manager.handleSwapForExit(exitSwap(mint, now + 600, {
      price: 0.68,
      signature: 'tail-sig-2',
    }));
    assert.strictEqual(manager._exitCalls.length, 2);
    assert(
      manager._exitCalls.every((call) => call.reason === 'CONFIRMED_TAIL_STOP'),
      'confirmed tail protection must sell every position for the mint',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      trailingArmed: true,
    }));
    manager.handleSwapForExit(exitSwap(mint, now, {
      price: 0.6,
      signature: 'armed-tail-1',
    }));
    manager.handleSwapForExit(exitSwap(mint, now + 600, {
      price: 0.5,
      signature: 'armed-tail-2',
    }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'tail protection must stay disabled after this position arms trailing',
    );
  }

  {
    const now = Date.now();
    const manager = managerWith(position('p1', mint, { openedAt: now - 5_000 }));
    manager.handleSwapForExit(exitSwap(mint, now, {
      price: 0.69,
      signature: 'same-tail-signature',
    }));
    manager.handleSwapForExit(exitSwap(mint, now + 600, {
      price: 0.68,
      signature: 'same-tail-signature',
    }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'a duplicate transaction signature must not confirm the tail stop',
    );
    manager.handleSwapForExit(exitSwap(mint, now + 1_200, {
      price: 0.67,
      signature: 'different-tail-signature',
    }));
    assert.strictEqual(manager._exitCalls[0].reason, 'CONFIRMED_TAIL_STOP');
  }

  {
    const now = Date.now();
    const pos = position('p1', mint, { openedAt: now - 5_000 });
    const manager = managerWith(pos);
    manager._maybeConfirmedTailStop(
      pos,
      exitSwap(mint, now, { price: 0.69, signature: 'reset-tail-1' }),
      {},
    );
    manager._maybeConfirmedTailStop(
      pos,
      exitSwap(mint, now + 300, { price: 0.71, signature: 'reset-recovery' }),
      {},
    );
    manager._maybeConfirmedTailStop(
      pos,
      exitSwap(mint, now + 600, { price: 0.69, signature: 'reset-tail-2' }),
      {},
    );
    manager._maybeConfirmedTailStop(
      pos,
      exitSwap(mint, now + 1_000, { price: 0.68, signature: 'reset-tail-3' }),
      {},
    );
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'a recovery above -30% must restart the 500ms confirmation clock',
    );
    manager._maybeConfirmedTailStop(
      pos,
      exitSwap(mint, now + 1_200, { price: 0.67, signature: 'reset-tail-4' }),
      {},
    );
    assert.strictEqual(manager._exitCalls[0].reason, 'CONFIRMED_TAIL_STOP');
  }

  {
    const now = Date.now();
    const pos = position('p1', mint, { openedAt: now - 20_000 });
    const manager = managerWith(pos);
    manager._researchEvents = [];
    manager.tradeLogger = {
      logPositionResearchEvent(event) {
        manager._researchEvents.push(event);
      },
    };
    const metrics = {
      holdMs: 20_000,
      marketPnlPct: -17,
      windows: {
        '3s': {
          netFlowSol: 1,
          buySellRatio: 2,
          uniqueBuyers: 3,
          tradeCount: 8,
        },
      },
      acceleration: { buySol3s: 4 },
    };
    manager._maybeAddonShadowSignal(
      pos,
      exitSwap(mint, now, { price: 0.79, signature: 'shadow-low' }),
      metrics,
    );
    manager._maybeAddonShadowSignal(
      pos,
      exitSwap(mint, now + 100, { price: 0.83, signature: 'shadow-rebound' }),
      metrics,
    );
    assert(
      manager._researchEvents.some((event) => event.eventType === 'ADDON_SHADOW_SIGNAL'),
      'a qualifying recovery must create a research-only add-on signal',
    );
    assert.strictEqual(manager._exitCalls.length, 0, 'shadow add-on must not submit a trade');
    manager._updateAddonShadows(
      mint,
      exitSwap(mint, now + 1_000, { price: 1.25, signature: 'shadow-peak' }),
    );
    manager._updateAddonShadows(
      mint,
      exitSwap(mint, now + 2_000, { price: 1.11, signature: 'shadow-drawdown' }),
    );
    assert(
      manager._researchEvents.some((event) => event.eventType === 'ADDON_SHADOW_ARMED'),
      'the virtual add-on must arm its own trailing exit',
    );
    assert(
      manager._researchEvents.some((event) => event.eventType === 'ADDON_SHADOW_EXIT'),
      'the virtual add-on must close independently after its own drawdown',
    );
    assert.strictEqual(manager._exitCalls.length, 0);
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
    const previousMode = config.strategy.earlyWrongExitMode;
    config.strategy.earlyWrongExitMode = 'shadow';
    const now = Date.now();
    const manager = managerWith(position('p1', mint, {
      openedAt: now - 5_000,
      reconciledAt: now - 5_000,
    }));
    manager._researchEvents = [];
    manager.tradeLogger = {
      logPositionResearchEvent(event) {
        manager._researchEvents.push(event);
      },
    };
    manager.handleSwapForExit(exitSwap(mint, now));
    manager.handleSwapForExit(exitSwap(mint, now + 600, { price: 0.94 }));
    assert.strictEqual(
      manager._exitCalls.length,
      0,
      'shadow invalidation must never submit a sell',
    );
    assert(
      manager._researchEvents.some((event) => event.eventType === 'EEI_SHADOW_TRIGGER'),
      'shadow invalidation must persist its confirmed trigger',
    );
    config.strategy.earlyWrongExitMode = previousMode;
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
