'use strict';

function nextScheduledAt(nowMs, hours, timezoneOffsetMinutes = 480) {
  const offsetMs = timezoneOffsetMinutes * 60_000;
  const local = new Date(nowMs + offsetMs);
  const y = local.getUTCFullYear();
  const m = local.getUTCMonth();
  const d = local.getUTCDate();
  const sortedHours = [...new Set(hours)].sort((a, b) => a - b);
  if (sortedHours.length === 0) return nowMs + 6 * 60 * 60_000;

  for (const hour of sortedHours) {
    const candidate = Date.UTC(y, m, d, hour, 0, 0, 0) - offsetMs;
    if (candidate > nowMs + 1_000) return candidate;
  }
  return Date.UTC(y, m, d + 1, sortedHours[0], 0, 0, 0) - offsetMs;
}

function shouldAutoUnwrap({ allowUnwrap, busy, amountLamports, minLamports, accountCount }) {
  return !!allowUnwrap &&
    !busy &&
    BigInt(amountLamports || 0) >= BigInt(minLamports || 0) &&
    Number(accountCount || 0) > 0;
}

module.exports = { nextScheduledAt, shouldAutoUnwrap };
