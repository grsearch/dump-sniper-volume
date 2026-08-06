'use strict';

const assert = require('assert');
const MigrationRugTelemetry = require('../src/core/MigrationRugTelemetry');

(async () => {
  const rows = [];
  let auditOptions = null;
  let auditCalls = 0;
  const migrationTime = Date.now() - 20_000;
  const mint = 'RugTelemetryMint111111111111111111111111111';
  const telemetry = new MigrationRugTelemetry({
    tradeLogger: {
      logMigrationRiskSnapshot(row) { rows.push(row); },
      getSwapEventsForMintInRange() {
        return [
          {
            ts: migrationTime + 1_500,
            side: 'SELL',
            signer: 'seller-a',
            signature: 'swap-1500',
            sol_volume: 3,
            price: 0.0000008,
            pool_quote_after: 79,
          },
          {
            ts: migrationTime + 2_500,
            side: 'SELL',
            signer: 'seller-db',
            signature: 'swap-db-2500',
            sol_volume: 0.5,
            price: 0.00000082,
            pool_quote_after: 78.5,
          },
        ];
      },
    },
    scanner: {
      async audit(_migration, options) {
        auditOptions = options;
        auditCalls++;
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
          activityEvents: [
            {
              ts: migrationTime,
              side: 'BUY',
              signer: 'buyer-a',
              signature: 'swap-500',
              solVolume: 2,
              price: 0.0000011,
              source: 'chain_replay',
            },
            {
              ts: migrationTime - 8_000,
              side: 'SELL',
              signer: 'seller-chain',
              signature: 'swap-chain-pre',
              solVolume: 0.2,
              price: 0.00000105,
              source: 'chain_replay',
            },
          ],
          summary: {
            startSlot: 70,
            detectionSlot: 125,
            firstScannedBlockTimeMs: migrationTime - (auditCalls === 1 ? 5_000 : 10_000),
            lastScannedBlockTimeMs: migrationTime + (auditCalls === 1 ? 5_000 : 10_000),
          },
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
  assert.ok(
    telemetry.states.get(mint).finalizeTimer,
    'every detected migration must schedule a risk snapshot before screening',
  );
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
  telemetry.markAccepted({
    token: { mint, symbol: 'RUGDATA' },
    migration,
    screening: { market: { fdv: 35_000, liquidity: 5_000 } },
  });
  telemetry.observeMigration({ ...migration, detectionPath: 'poll-duplicate' });
  assert.strictEqual(telemetry.states.get(mint).accepted, true);
  assert.strictEqual(telemetry.states.get(mint).events.length, 1);
  addSwap(500, 'BUY', 2, 'buyer-a', 0.0000011, 82);
  addSwap(1_500, 'SELL', 3, 'seller-a', 0.0000008, 79);
  addSwap(4_000, 'BUY', 1, 'buyer-b', 0.0000009, 80);

  const row = await telemetry.finalizeNow(mint);
  assert.strictEqual(rows.length, 1, 'one observe-only snapshot must be persisted');
  assert.strictEqual(row.mint, mint);
  assert.strictEqual(row.swapEventCount, 6);
  assert.strictEqual(row.buyCount, 2, 'top-level flow summarizes post-migration trades');
  assert.strictEqual(row.sellCount, 2);
  assert.strictEqual(row.largeTransferCount, 1);
  assert.strictEqual(row.sameTxBuyCount, 1);
  assert.strictEqual(row.metrics.observeOnly, true);
  assert.strictEqual(row.metrics.windows.pre10s.buyCount, 1);
  assert.strictEqual(row.metrics.windows.pre10s.sellCount, 1);
  assert.strictEqual(row.metrics.windows.post5s.uniqueBuyers, 2);
  assert.strictEqual(row.metrics.chain.maxLargeTransferSupplyPct, 12);
  assert.strictEqual(row.metrics.coverage.complete, true);
  assert.strictEqual(row.metrics.coverage.liveEventCount, 4);
  assert.strictEqual(row.metrics.coverage.persistedEventCount, 1);
  assert.strictEqual(row.metrics.coverage.replayEventCount, 1);
  assert.strictEqual(row.auditIncomplete, 0);
  assert.strictEqual(auditOptions.force, true, 'disabled admission audit must be reusable for telemetry');
  assert.strictEqual(auditOptions.refreshDetectionSlot, true);
  assert.strictEqual(auditCalls, 2, 'an incomplete time boundary must trigger one wider scan');
  assert.strictEqual(auditOptions.preSlots, 64);
  assert.strictEqual(auditOptions.postSlots, 64);
  assert.strictEqual(auditOptions.startTimeMs, migrationTime - 10_000);
  assert.strictEqual(auditOptions.endTimeMs, migrationTime + 10_000);
  assert.strictEqual(telemetry.states.size, 0);
  telemetry.stop();

  const rejectedRows = [];
  const rejectedMint = 'RejectedMigrationMint111111111111111111111111';
  const rejectedTelemetry = new MigrationRugTelemetry({
    tradeLogger: {
      logMigrationRiskSnapshot(row) { rejectedRows.push(row); },
      getSwapEventsForMintInRange() { return []; },
    },
    scanner: {
      async audit() {
        return {
          allowed: true,
          matches: [],
          activityEvents: [],
          summary: {
            firstScannedBlockTimeMs: migrationTime - 10_000,
            lastScannedBlockTimeMs: migrationTime + 10_000,
          },
        };
      },
    },
    settings: {
      rugTelemetryEnabled: true,
      rugTelemetryWindowMs: 10_000,
      rugTelemetryPreSlots: 64,
    },
  });
  rejectedTelemetry.observeMigration({
    mint: rejectedMint,
    signature: 'rejected-migration-signature',
    slot: 200,
    migrationTime,
  });
  const rejectedRow = await rejectedTelemetry.finalizeNow(rejectedMint);
  assert.strictEqual(rejectedRows.length, 1, 'screening rejection must not suppress telemetry');
  assert.strictEqual(rejectedRow.metrics.screening.accepted, false);
  assert.strictEqual(rejectedRow.metrics.coverage.complete, true);
  rejectedTelemetry.stop();

  console.log('Migration RUG telemetry tests: PASS');
  process.exit(0);
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exitCode = 1;
});
