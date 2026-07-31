'use strict';

const assert = require('assert');
const Module = require('module');

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  return originalLoad.call(this, request, parent, isMain);
};
const SignalEngine = require('../src/core/SignalEngine');
const { config } = require('../src/config');
Module._load = originalLoad;

const mint = 'EarlyFlowSignalMint111111111111111111111111';

function engine() {
  const instance = new SignalEngine({
    positionManager: {
      openPositionCount: () => 0,
      openPositionCountByMint: () => 0,
      hasOpenPosition: () => false,
    },
    tradeLogger: { logSignal() {} },
  });
  instance.loggedRejects = [];
  instance._logReject = function logReject(_signal, reason) {
    this.loggedRejects.push(reason);
  };
  return instance;
}

function signal(overrides = {}) {
  const now = Date.now();
  const details = {
    signalPrice: 1,
    executionPrice: 1.1,
    executionDelayMs: 200,
    signalMigrationAgeMs: 18_000,
    fdvUsd: 50_000,
    priceChangePct: 2,
    netFlow1sSol: 0.1,
    uniqueBuyers5s: 8,
    tradeCount5s: 10,
    buySol5s: 4,
    largestBuyShare5s: 0.3,
    ...overrides,
  };
  return {
    mint,
    symbol: 'EARLY',
    signature: `sig-${Math.random()}`,
    ts: now,
    slot: 123,
    priceAfter: details.signalPrice,
    _earlyFlow: true,
    _earlyFlowDetails: details,
  };
}

(async () => {
  assert.strictEqual(config.earlyFlow.minMigrationAgeMs, 15_000);
  assert.strictEqual(config.earlyFlow.maxMigrationAgeMs, 25_000);
  assert.strictEqual(config.earlyFlow.minBuySol5s, 2);
  assert.strictEqual(config.earlyFlow.riskEnabled, true);
  assert.strictEqual(config.earlyFlow.riskRejectScore, 4);
  assert.strictEqual(config.strategy.positionSizeSol, 0.2);

  const accepted = engine();
  const orders = [];
  accepted.on('buyOrder', (order) => orders.push(order));
  await accepted.handleEarlyFlowSignal(signal());
  assert.strictEqual(orders.length, 1);
  assert.strictEqual(orders[0].sizeSol, 0.2);
  assert.strictEqual(orders[0].entryFdvSource, 'chain_realtime_signal');
  assert.strictEqual(orders[0]._earlyFlowDetails.entryRiskScore, 0);

  const missingBuyers = engine();
  await missingBuyers.handleEarlyFlowSignal(signal({ uniqueBuyers5s: NaN }));
  assert.strictEqual(missingBuyers.inflightBuys.size, 0);
  assert(missingBuyers.loggedRejects[0].startsWith('UNIQUE_BUYERS_5S_LOW'));

  const lowBuyVolume = engine();
  await lowBuyVolume.handleEarlyFlowSignal(signal({ buySol5s: 1.999 }));
  assert.strictEqual(lowBuyVolume.inflightBuys.size, 0);
  assert(lowBuyVolume.loggedRejects[0].startsWith('BUY_VOLUME_5S_LOW'));

  const late = engine();
  await late.handleEarlyFlowSignal(signal({ executionDelayMs: 3_001 }));
  assert.strictEqual(late.inflightBuys.size, 0);
  assert(late.loggedRejects[0].startsWith('EXECUTION_WINDOW_MISSED'));

  const expensive = engine();
  await expensive.handleEarlyFlowSignal(signal({ executionPrice: 1.150001 }));
  assert.strictEqual(expensive.inflightBuys.size, 0);
  assert(expensive.loggedRejects[0].startsWith('EXECUTION_PRICE_HIGH'));

  const risky = engine();
  await risky.handleEarlyFlowSignal(signal({
    executionDelayMs: 600,
    signalMigrationAgeMs: 22_000,
    fdvUsd: 20_000,
    priceChangePct: -3,
    uniqueBuyers5s: 4,
    buySol5s: 2.2,
    largestBuyShare5s: 0.5,
  }));
  assert.strictEqual(risky.inflightBuys.size, 0);
  assert(risky.loggedRejects[0].startsWith('ENTRY_RISK_SCORE_HIGH'));

  console.log('Early-flow SignalEngine tests: PASS');
  process.exit(0);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
