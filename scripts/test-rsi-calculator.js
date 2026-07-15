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
  assert.strictEqual(snapshot.bucketCount1m, 8);

  // The live candle's latest price is its close. Its high-volume first trade must not turn RSI into VWAP RSI.
  calculator.feedTrade(mint, 45.6, 1, 'buy', 7 * minute + 50_000, 50);
  snapshot = calculator.snapshot(mint);
  approx(snapshot.rsi1m, 80.76923076923077);
  assert.strictEqual(snapshot.bucketCount1m, 8);

  const sparse = new RsiCalculator({ period60: 7, maxBuckets: 120 });
  sparse.feedTick('sparse', 10, 0);
  sparse.feedTick('sparse', 11, 10 * 24 * 60 * minute);
  assert.strictEqual(sparse.snapshot('sparse').bucketCount1m, 120, 'large gaps must stay memory-bounded');

  const longRun = new RsiCalculator({ period60: 7, maxBuckets: 120 });
  const longCloses = Array.from({ length: 300 }, (_, index) =>
    100 + Math.sin(index / 7) * 4 + index * 0.01);
  longCloses.forEach((price, index) => longRun.feedTick('long', price, index * minute + 50_000));
  approx(longRun.snapshot('long').rsi1m, referenceRsi(longCloses, 7));

  console.log('RsiCalculator TradingView 1m RSI self-test: PASS');
}

run();
