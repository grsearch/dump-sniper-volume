'use strict';

const assert = require('assert');
const MigrationRugTelemetry = require('../src/core/MigrationRugTelemetry');

(async () => {
  const rows = [];
  let auditOptions = null;
  const migrationTime = Date.now() - 20_000;
  const mint = 'RugTelemetryMint111111111111111111111111111';
  const telemetry = new MigrationRugTelemetry({
    tradeLogger: {
      logMigrationRiskSnapshot(row) { rows.push(row); },
    },
    scanner: {
      async audit(_migration, options) {
        auditOptions = options;
        return {
          allowed: false,
          reasonCode: 'large_transfer',
          matches: [
            {
              type: 'large_transfer',
              slot: 99,
              blockTimeMs: migrationTime - 1_000,
              supplyPct: 12,
            },
            {
              type: 'same_tx_buy_migrate',
              slot: 100,
              blockTimeMs: migrationTime,
            },
          ],
          summary: { startSlot: 70, detectionSlot: 125 },
        };
      },
    },
    settings: {
      rugTelemetryEnabled: true,
      rugTelemetryWindowMs: 10_000,
      rugTelemetryPreSlots: 32,
      rugTelemetryMaxEvents: 100,
    },
  });
  const migration = {
    mint,
    signature: 'migration-signature',
    slot: 100,
    migrationTime,
  };
  telemetry.observeMigration(migration);
  telemetry.markAccepted({
    token: { mint, symbol: 'RUGDATA' },
    migration,
    screening: { market: { fdv: 35_000, liquidity: 5_000 } },
  });
  const addSwap = (offset, side, solVolume, signer, price, poolQuoteAfter) => {
    telemetry.handleSwap({
      mint,
      ts: migrationTime + offset,
      side,
      solVolume,
      signer,
      signature: `swap-${offset}`,
      price,
      poolQuoteAfter,
      fdvUsd: price * 1_000_000_000 * 75.5,
    });
  };
  addSwap(-2_000, 'BUY', 0.5, 'buyer-pre', 0.000001, 80);
  addSwap(500, 'BUY', 2, 'buyer-a', 0.0000011, 82);
  addSwap(1_500, 'SELL', 3, 'seller-a', 0.0000008, 79);
  addSwap(4_000, 'BUY', 1, 'buyer-b', 0.0000009, 80);

  const row = await telemetry.finalizeNow(mint);
  assert.strictEqual(rows.length, 1, 'one observe-only snapshot must be persisted');
  assert.strictEqual(row.mint, mint);
  assert.strictEqual(row.swapEventCount, 4);
  assert.strictEqual(row.buyCount, 2, 'top-level flow summarizes post-migration trades');
  assert.strictEqual(row.sellCount, 1);
  assert.strictEqual(row.largeTransferCount, 1);
  assert.strictEqual(row.sameTxBuyCount, 1);
  assert.strictEqual(row.metrics.observeOnly, true);
  assert.strictEqual(row.metrics.windows.pre10s.buyCount, 1);
  assert.strictEqual(row.metrics.windows.post5s.uniqueBuyers, 2);
  assert.strictEqual(row.metrics.chain.maxLargeTransferSupplyPct, 12);
  assert.strictEqual(auditOptions.force, true, 'disabled admission audit must be reusable for telemetry');
  assert.strictEqual(auditOptions.refreshDetectionSlot, true);
  assert.strictEqual(auditOptions.startTimeMs, migrationTime - 10_000);
  assert.strictEqual(auditOptions.endTimeMs, migrationTime + 10_000);
  assert.strictEqual(telemetry.states.size, 0);
  telemetry.stop();

  console.log('Migration RUG telemetry tests: PASS');
  process.exit(0);
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exitCode = 1;
});
