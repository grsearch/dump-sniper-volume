'use strict';

const assert = require('assert');
const {
  evaluateBuyExecutionGuard,
  calculateMinBaseAmountOut,
} = require('../src/utils/buyExecutionGuard');

function close(actual, expected, tolerance = 1e-9) {
  assert(Math.abs(actual - expected) <= tolerance, `${actual} != ${expected}`);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1.151,
    maxDeviationPct: 15,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, false);
  assert.strictEqual(result.reason, 'expected_price_above_signal_cap');
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1.01,
    maxDeviationPct: 15,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, true);
  close(result.effectiveSlippagePct, ((1.15 / 1.01) - 1) * 100);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1,
    maxDeviationPct: 15,
    configuredSlippagePct: 2,
  });
  assert.strictEqual(result.allowed, true);
  assert.strictEqual(result.effectiveSlippagePct, 2);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 0.9,
    maxDeviationPct: 15,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, true);
  close(result.effectiveSlippagePct, ((1.15 / 0.9) - 1) * 100);
}

assert.strictEqual(calculateMinBaseAmountOut(1_000n, 15), 870n);
assert.strictEqual(calculateMinBaseAmountOut(1_000n, 2.1), 980n);
assert.strictEqual(calculateMinBaseAmountOut(1_000n, 0), 1_000n);

console.log('Buy execution price guard tests: PASS');
