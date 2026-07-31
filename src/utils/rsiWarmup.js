'use strict';

function warmupRsiFromDb(db, rsiCalculator, warmupStartMs) {
  if (!db || !rsiCalculator) return 0;

  const statement = db.prepare(`
    SELECT mint, ts, price
    FROM swap_events
    WHERE ts > ? AND price > 0
    ORDER BY ts ASC, id ASC
  `);
  const rows = statement.iterate(warmupStartMs);
  let fed = 0;
  for (const row of rows) {
    rsiCalculator.feedTick(row.mint, Number(row.price), Number(row.ts));
    fed++;
  }
  return fed;
}

module.exports = { warmupRsiFromDb };
