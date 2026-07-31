'use strict';

function finite(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function evaluateEarlyFlowEntryRisk(details = {}, strategy = {}) {
  const reasons = [];
  const add = (condition, reason) => {
    if (condition) reasons.push(reason);
  };

  add(
    finite(details.uniqueBuyers5s) < finite(strategy.riskMinUniqueBuyers5s, 6),
    'buyers5s_low',
  );
  add(
    finite(details.buySol5s) < finite(strategy.riskMinBuySol5s, 3),
    'buy_sol5s_low',
  );
  add(
    finite(details.priceChangePct) < finite(strategy.riskMinPriceChangePct, -2),
    'price_change10s_weak',
  );
  add(
    finite(details.largestBuyShare5s, Infinity) >
      finite(strategy.riskMaxLargestBuyShare, 0.45),
    'largest_buy_concentrated',
  );
  add(
    finite(details.executionDelayMs, Infinity) >
      finite(strategy.riskMaxExecutionDelayMs, 400),
    'execution_slow',
  );
  add(
    finite(details.fdvUsd) < finite(strategy.riskMinFdvUsd, 25_000),
    'fdv_low',
  );
  add(
    finite(details.signalMigrationAgeMs, Infinity) >
      finite(strategy.riskMaxMigrationAgeMs, 20_000),
    'migration_age_late',
  );

  const score = reasons.length;
  const rejectScore = Math.max(1, finite(strategy.riskRejectScore, 4));
  const enabled = strategy.riskEnabled !== false;
  return {
    enabled,
    score,
    rejectScore,
    blocked: enabled && score >= rejectScore,
    reasons,
  };
}

module.exports = {
  evaluateEarlyFlowEntryRisk,
};
