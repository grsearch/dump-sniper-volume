'use strict';

function finitePositive(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function evaluateBuyExecutionGuard({
  signalPrice,
  expectedPrice,
  maxDeviationPct,
  configuredSlippagePct,
}) {
  const signal = finitePositive(signalPrice);
  const expected = finitePositive(expectedPrice);
  const maxDeviation = Number(maxDeviationPct);
  const configuredSlippage = Math.max(0, Number(configuredSlippagePct) || 0);

  if (!signal || !expected) {
    return {
      allowed: false,
      reason: 'invalid_signal_or_expected_price',
      signalPrice: signal,
      expectedPrice: expected,
      maxPrice: null,
      deviationPct: null,
      effectiveSlippagePct: 0,
    };
  }

  if (!Number.isFinite(maxDeviation) || maxDeviation <= 0) {
    return {
      allowed: true,
      reason: 'guard_disabled',
      signalPrice: signal,
      expectedPrice: expected,
      maxPrice: null,
      deviationPct: ((expected / signal) - 1) * 100,
      effectiveSlippagePct: configuredSlippage,
    };
  }

  const maxPrice = signal * (1 + maxDeviation / 100);
  const deviationPct = ((expected / signal) - 1) * 100;
  if (expected > maxPrice * (1 + 1e-12)) {
    return {
      allowed: false,
      reason: 'expected_price_above_signal_cap',
      signalPrice: signal,
      expectedPrice: expected,
      maxPrice,
      deviationPct,
      effectiveSlippagePct: 0,
    };
  }

  const remainingToCapPct = Math.max(0, ((maxPrice / expected) - 1) * 100);
  return {
    allowed: true,
    reason: 'within_signal_cap',
    signalPrice: signal,
    expectedPrice: expected,
    maxPrice,
    deviationPct,
    remainingToCapPct,
    effectiveSlippagePct: Math.min(configuredSlippage, remainingToCapPct),
  };
}

module.exports = { evaluateBuyExecutionGuard };
