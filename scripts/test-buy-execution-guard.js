'use strict';

const assert = require('assert');
const { evaluateBuyExecutionGuard } = require('../src/utils/buyExecutionGuard');

function close(actual, expected, tolerance = 1e-9) {
  assert(Math.abs(actual - expected) <= tolerance, `${actual} != ${expected}`);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1.051,
    maxDeviationPct: 5,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, false);
  assert.strictEqual(result.reason, 'expected_price_above_signal_cap');
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1.03,
    maxDeviationPct: 5,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, true);
  close(result.effectiveSlippagePct, ((1.05 / 1.03) - 1) * 100);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 1,
    maxDeviationPct: 5,
    configuredSlippagePct: 2,
  });
  assert.strictEqual(result.allowed, true);
  assert.strictEqual(result.effectiveSlippagePct, 2);
}

{
  const result = evaluateBuyExecutionGuard({
    signalPrice: 1,
    expectedPrice: 0.9,
    maxDeviationPct: 5,
    configuredSlippagePct: 50,
  });
  assert.strictEqual(result.allowed, true);
  close(result.effectiveSlippagePct, ((1.05 / 0.9) - 1) * 100);
}

console.log('Buy execution price guard tests: PASS');
