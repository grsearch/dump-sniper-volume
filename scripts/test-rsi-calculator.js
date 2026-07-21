'use strict';

const assert = require('assert');
const RsiCalculator = require('../src/core/RsiCalculator');

function approx(actual, expected, tolerance = 1e-9) {
  assert.ok(
    Math.abs(actual - expected) <= tolerance,
    `expected ${actual} to be within ${tolerance} of ${expected}`,
  );
}

function referenceRsi(closes, period) {
  let gain = 0;
  let loss = 0;
  for (let index = 1; index <= period; index += 1) {
    const delta = closes[index] - closes[index - 1];
    gain += Math.max(delta, 0);
    loss += Math.max(-delta, 0);
  }
  let avgGain = gain / period;
  let avgLoss = loss / period;
  for (let index = period + 1; index < closes.length; index += 1) {
    const delta = closes[index] - closes[index - 1];
    avgGain = (avgGain * (period - 1) + Math.max(delta, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-delta, 0)) / period;
  }
  if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
  return 100 - 100 / (1 + avgGain / avgLoss);
}

function run() {
  const calculator = new RsiCalculator({ period60: 7 });
  const mint = 'rsi-test-mint';
  const minute = 60_000;
  const closes = [44, 44.15, 43.9, 44.35, 44.1, 44.8, 45.0];

  closes.forEach((price, index) => {
    calculator.feedTick(mint, price, index * minute + 50_000);
  });
  assert.strictEqual(calculator.snapshot(mint).rsi1m, null, 'RSI(7) needs eight 1-minute closes');

  calculator.feedTrade(mint, 44.6, 100, 'buy', 7 * minute + 10_000, 50);
  let snapshot = calculator.snapshot(mint);
  approx(snapshot.rsi1m, 62.5);
  approx(snapshot.rsi1mLive, 62.5);
  assert.strictEqual(snapshot.rsi1mClosed, null, 'closed RSI needs eight completed 1-minute closes');
  assert.strictEqual(snapshot.rsi1mClosedBars, 7);
  assert.strictEqual(snapshot.bucketCount1m, 8);

  // The live candle's latest price is its close. Its high-volume first trade must not turn RSI into VWAP RSI.
  calculator.feedTrade(mint, 45.6, 1, 'buy', 7 * minute + 50_000, 50);
  snapshot = calculator.snapshot(mint);
  approx(snapshot.rsi1m, 80.76923076923077);
  approx(snapshot.rsi1mLive, 80.76923076923077);
  assert.strictEqual(snapshot.rsi1mClosed, null);
  assert.strictEqual(snapshot.bucketCount1m, 8);

  // Once the minute closes, preserve its RSI separately while the new live bar changes.
  calculator.feedTick(mint, 45.3, 8 * minute + 10_000);
  snapshot = calculator.snapshot(mint);
  approx(snapshot.rsi1mClosed, referenceRsi([...closes, 45.6], 7));
  approx(snapshot.rsi1mLive, referenceRsi([...closes, 45.6, 45.3], 7));
  assert.strictEqual(snapshot.rsi1mClosedBars, 8);
  assert.strictEqual(snapshot.rsi1mLiveClose, 45.3);
  assert.strictEqual(snapshot.rsi1mLastClosedClose, 45.6);

  const sparse = new RsiCalculator({ period60: 7, maxBuckets: 120 });
  sparse.feedTick('sparse', 10, 0);
  sparse.feedTick('sparse', 11, 10 * 24 * 60 * minute);
  assert.strictEqual(sparse.snapshot('sparse').bucketCount1m, 120, 'large gaps must stay memory-bounded');

  const longRun = new RsiCalculator({ period60: 7, maxBuckets: 120 });
  const longCloses = Array.from({ length: 300 }, (_, index) =>
    100 + Math.sin(index / 7) * 4 + index * 0.01);
  longCloses.forEach((price, index) => longRun.feedTick('long', price, index * minute + 50_000));
  const longSnapshot = longRun.snapshot('long');
  approx(longSnapshot.rsi1mLive, referenceRsi(longCloses, 7));
  approx(longSnapshot.rsi1mClosed, referenceRsi(longCloses.slice(0, -1), 7));

  const scaleReset = new RsiCalculator({ period60: 7, priceScaleResetRatio: 100 });
  const beforeMigration = [1, 1.02, 1.01, 1.04, 1.03, 1.05, 1.06, 1.08];
  beforeMigration.forEach((price, index) =>
    scaleReset.feedTick('scale-reset', price, index * minute + 50_000));
  assert(scaleReset.snapshot('scale-reset').rsi1mLive != null);

  const afterMigration = [
    1.10e-6, 1.12e-6, 1.11e-6, 1.14e-6,
    1.13e-6, 1.16e-6, 1.18e-6, 1.17e-6,
  ];
  scaleReset.feedTick('scale-reset', afterMigration[0], 8 * minute + 50_000);
  assert.strictEqual(
    scaleReset.snapshot('scale-reset'),
    null,
    'a migration-scale price discontinuity must discard contaminated RSI history',
  );
  afterMigration.slice(1).forEach((price, index) =>
    scaleReset.feedTick('scale-reset', price, (index + 9) * minute + 50_000));
  const resetSnapshot = scaleReset.snapshot('scale-reset');
  approx(resetSnapshot.rsi1mLive, referenceRsi(afterMigration, 7));
  assert.strictEqual(resetSnapshot.rsi1mClosedBars, 7);

  const belowThreshold = new RsiCalculator({ period60: 7, priceScaleResetRatio: 100 });
  beforeMigration.forEach((price, index) =>
    belowThreshold.feedTick('below-threshold', price, index * minute + 50_000));
  belowThreshold.feedTick('below-threshold', 106, 8 * minute + 50_000);
  assert.strictEqual(
    belowThreshold.snapshot('below-threshold').rsi1mClosedBars,
    8,
    'price changes below the configured ratio must not reset RSI history',
  );

  // TradingView RSI uses each candle's close, not its volume-weighted price.
  const fiveSecond = new RsiCalculator({ period5: 7 });
  const fiveSecondCloses = [44, 44.15, 43.9, 44.35, 44.1, 44.8, 45.0];
  fiveSecondCloses.forEach((price, index) => {
    fiveSecond.feedTrade('five-second', price, 1, 'buy', index * 5_000 + 1_000, 50);
  });
  fiveSecond.feedTrade('five-second', 44.6, 100, 'sell', 7 * 5_000 + 1_000, 50);
  fiveSecond.feedTrade('five-second', 45.6, 1, 'buy', 7 * 5_000 + 4_000, 50);
  const fiveSecondSnapshot = fiveSecond.snapshot('five-second');
  approx(fiveSecondSnapshot.rsi5s, referenceRsi([...fiveSecondCloses, 45.6], 7));
  assert.strictEqual(fiveSecondSnapshot.bucketCount5s, 8);

  console.log('RsiCalculator TradingView 1m/5s RSI self-test: PASS');
}

run();
