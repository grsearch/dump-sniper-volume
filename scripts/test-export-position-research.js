'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const TradeLogger = require('../src/data/TradeLogger');
const { main: exportResearch } = require('./export-position-research');

function compatibleDatabase(filePath) {
  const db = new DatabaseSync(filePath);
  db.transaction = (fn) => (...args) => {
    db.exec('BEGIN');
    try {
      const result = fn(...args);
      db.exec('COMMIT');
      return result;
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  };
  return db;
}

function parseCsv(filePath) {
  const text = fs.readFileSync(filePath, 'utf8').trim();
  const records = [];
  let record = [];
  let field = '';
  let quoted = false;
  for (let index = 0; index < text.length; index++) {
    const char = text[index];
    if (quoted) {
      if (char === '"' && text[index + 1] === '"') {
        field += '"';
        index++;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ',') {
      record.push(field);
      field = '';
    } else if (char === '\n') {
      record.push(field.replace(/\r$/, ''));
      records.push(record);
      record = [];
      field = '';
    } else {
      field += char;
    }
  }
  record.push(field.replace(/\r$/, ''));
  records.push(record);
  const [headers, ...rows] = records;
  return rows.map((values) => Object.fromEntries(
    headers.map((key, index) => [key, values[index] || '']),
  ));
}

function run() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dump-sniper-research-'));
  const resolvedRoot = path.resolve(root);
  if (!resolvedRoot.startsWith(path.resolve(os.tmpdir()))) {
    throw new Error(`Refusing to use unexpected test directory: ${resolvedRoot}`);
  }
  const dbPath = path.join(root, 'sniper.db');
  const outDir = path.join(root, 'export');
  const envPath = path.join(root, '.env');
  fs.writeFileSync(envPath, [
    'HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=super-secret-key',
    'BIRDEYE_API_KEY=super-secret-birdeye',
    'WALLET_PRIVATE_KEY_BS58=super-private-wallet',
    'JITO_AUTH_KEYPAIR=super-secret-jito-keypair',
    'HELIUS_LASERSTREAM_TOKEN=super-secret-laser-token',
    'ALERT_WEBHOOK_URL=https://alerts.example/send/secret-webhook',
    'PUBLIC_SETTING=keep-me',
    '',
  ].join('\n'));
  const db = compatibleDatabase(dbPath);
  const logger = new TradeLogger(db);
  const since = 1_785_280_000_000;
  const cutoff = since + 60_000;

  logger.openPosition({
    positionId: 'closed-before-cutoff',
    mint: 'MintClosed11111111111111111111111111111111',
    symbol: 'CLOSED',
    openedAt: since + 1_000,
    entrySol: 0.2,
    entryPrice: 1,
    tokenAmount: 0.2,
  });
  logger.closePosition('closed-before-cutoff', {
    closedAt: cutoff - 1_000,
    exitPrice: 1.2,
    exitSol: 0.24,
    pnlSol: 0.04,
    pnlPct: 20,
    exitReason: 'TRAILING_STOP',
    peakPnlPct: 30,
    peakPrice: 1.3,
    peakTs: cutoff - 2_000,
    timeToPeakMs: 20_000,
    priceTickCount: 10,
  });

  logger.openPosition({
    positionId: 'closed-after-cutoff',
    mint: 'MintOpen111111111111111111111111111111111',
    symbol: 'OPEN',
    openedAt: since + 2_000,
    entrySol: 0.2,
    entryPrice: 1,
    tokenAmount: 0.2,
  });
  logger.closePosition('closed-after-cutoff', {
    closedAt: cutoff + 10_000,
    exitPrice: 0.5,
    exitSol: 0.1,
    pnlSol: -0.1,
    pnlPct: -50,
    exitReason: 'FDV_STOP',
    peakPnlPct: 5,
    peakPrice: 1.05,
    peakTs: cutoff + 2_000,
    timeToPeakMs: 60_000,
    priceTickCount: 30,
  });

  logger.logPositionResearchEvent({
    positionId: 'closed-after-cutoff',
    mint: 'MintOpen111111111111111111111111111111111',
    eventType: 'SWAP_METRICS',
    ts: cutoff - 500,
    receivedAt: cutoff - 400,
    price: 0.9,
    metrics: {
      windows: {
        '3s': {
          tradeCount: 3,
          netFlowSol: -0.2,
        },
      },
    },
  });
  logger.logPositionResearchEvent({
    positionId: 'closed-after-cutoff',
    mint: 'MintOpen111111111111111111111111111111111',
    eventType: 'POSITION_CLOSED',
    ts: cutoff + 10_000,
    receivedAt: cutoff + 10_000,
    price: 0.5,
    metrics: { pnlPct: -50 },
  });
  logger.logMigrationRiskSnapshot({
    mint: 'MigrationMint11111111111111111111111111111',
    symbol: 'MIGRATION',
    migrationSignature: 'migration-signature',
    migrationSlot: 123,
    migrationTime: since + 500,
    capturedAt: since + 10_500,
    windowBeforeMs: 10_000,
    windowAfterMs: 10_000,
    swapEventCount: 8,
    buyCount: 5,
    sellCount: 3,
    buySol: 4,
    sellSol: 2,
    netFlowSol: 2,
    uniqueBuyers: 4,
    uniqueSellers: 2,
    largestBuyShare: 0.4,
    priceReturnPct: -12,
    peakReturnPct: 8,
    troughReturnPct: -20,
    maxDrawdownPct: -25,
    poolQuoteChangePct: -10,
    mintToCount: 1,
    largeTransferCount: 2,
    sameTxBuyCount: 1,
    metrics: {
      observeOnly: true,
      windows: { post10s: { maxSingleTradeDropPct: -15 } },
    },
  });
  const monitoredMint = 'MonitoredOnly11111111111111111111111111111';
  const monitoredAddedAt = since + 5_000;
  logger.logMigrationDetection({
    mint: monitoredMint,
    migrationSignature: 'monitored-migration-signature',
    migrationSlot: 455,
    migrationTime: since + 4_000,
    detectedAt: since + 4_100,
    detectionPath: 'websocket',
    detectionSlot: 456,
    poolAddress: 'monitored-pool',
  });
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_ADDED:${monitoredMint}:${monitoredAddedAt}`,
    mint: monitoredMint,
    symbol: 'WATCHED',
    eventType: 'TOKEN_ADDED',
    ts: monitoredAddedAt,
    addedAt: monitoredAddedAt,
    migrationTime: since + 4_000,
    source: 'pump_graduation',
  });
  logger.logTokenMarketSnapshot({
    mint: monitoredMint,
    symbol: 'WATCHED',
    ts: since + 6_000,
    trigger: 'realtime',
    source: 'chain_pool_realtime',
    fdvUsd: 30_000,
    liquidityUsd: 6_000,
    priceSol: 0.000001,
    addedAt: monitoredAddedAt,
  });
  logger.logSwapEvent({
    mint: monitoredMint,
    symbol: 'WATCHED',
    ts: since + 7_000,
    receivedAt: since + 7_010,
    signature: 'monitored-swap-signature',
    slot: 456,
    side: 'BUY',
    signer: 'buyer-wallet',
    solVolume: 1,
    price: 0.0000011,
  });
  logger.logMigrationHolderSnapshot({
    mint: monitoredMint,
    symbol: 'WATCHED',
    migrationSignature: 'monitored-migration-signature',
    migrationSlot: 455,
    migrationTime: since + 4_000,
    capturedAt: since + 4_500,
    captureDelayMs: 500,
    source: 'helius_das_getTokenAccounts',
    isComplete: true,
    holderCount: 4,
    tokenAccountCount: 5,
    supplyUi: 1_000_000_000,
    top1Pct: 4,
    holders: { top: [{ owner: 'holder-a', pctSupply: 4, ownerType: 'wallet' }] },
  });
  logger.logMigrationHolderSnapshot({
    mint: monitoredMint,
    symbol: 'WATCHED',
    migrationSignature: 'duplicate-holder-signature',
    migrationSlot: 455,
    migrationTime: since + 4_000,
    capturedAt: since + 4_600,
    captureDelayMs: 600,
    source: 'retry',
    isComplete: false,
    error: 'synthetic retry',
  });
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_REMOVED:${monitoredMint}:${monitoredAddedAt}`,
    mint: monitoredMint,
    symbol: 'WATCHED',
    eventType: 'TOKEN_REMOVED',
    ts: since + 8_000,
    addedAt: monitoredAddedAt,
    reason: 'fdv_below_min',
  });
  logger.logSwapEvent({
    mint: monitoredMint,
    symbol: 'WATCHED',
    ts: since + 9_000,
    receivedAt: since + 9_010,
    signature: 'post-removal-swap-signature',
    slot: 457,
    side: 'SELL',
    signer: 'seller-wallet',
    solVolume: 1,
    price: 0.0000008,
  });
  const cutoffMint = 'CutoffSession111111111111111111111111111111';
  const cutoffAddedAt = since + 20_000;
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_ADDED:${cutoffMint}:${cutoffAddedAt}`,
    mint: cutoffMint,
    symbol: 'CUTOFF',
    eventType: 'TOKEN_ADDED',
    ts: cutoffAddedAt,
    addedAt: cutoffAddedAt,
    migrationTime: since + 19_000,
    source: 'pump_graduation',
  });
  logger.logTokenLifecycleEvent({
    eventKey: `TOKEN_REMOVED:${cutoffMint}:${cutoffAddedAt}`,
    mint: cutoffMint,
    symbol: 'CUTOFF',
    eventType: 'TOKEN_REMOVED',
    ts: cutoff + 1_000,
    addedAt: cutoffAddedAt,
    reason: 'fdv_below_min',
  });
  logger.shutdown();
  db.close();

  exportResearch([
    `--db=${dbPath}`,
    `--since=${since}`,
    `--until=${cutoff}`,
    `--out=${outDir}`,
    `--env=${envPath}`,
  ]);

  const manifest = JSON.parse(
    fs.readFileSync(path.join(outDir, 'manifest.json'), 'utf8'),
  );
  assert.strictEqual(manifest.analysisCutoffMs, cutoff);
  assert.strictEqual(manifest.counts.positions, 2);
  assert.strictEqual(manifest.counts.closedPositions, 1);
  assert.strictEqual(manifest.counts.openAtCutoff, 1);
  assert.strictEqual(manifest.counts.researchEvents, 1);
  assert.strictEqual(manifest.counts.migrationRiskSnapshots, 1);
  assert.strictEqual(manifest.counts.monitoredSessions, 2);
  assert.strictEqual(manifest.counts.lifecycleEvents, 3);
  assert.strictEqual(manifest.counts.monitoredMarketSnapshots, 1);
  assert.strictEqual(manifest.counts.monitoredSwaps, 1);
  assert.strictEqual(manifest.counts.migrationHolderSnapshots, 1);
  assert.strictEqual(manifest.counts.migrationDetections, 1);
  assert.strictEqual(manifest.migrationCoverage.detectedUniqueMints, 1);
  assert.strictEqual(manifest.migrationCoverage.holderCoveredDetectedMints, 1);
  assert.strictEqual(manifest.migrationCoverage.holderCoverageRatio, 1);
  assert.strictEqual(manifest.migrationCoverage.acceptedDetectedMints, 1);
  assert.strictEqual(manifest.migrationCoverage.riskCoveredAcceptedMints, 0);
  assert.strictEqual(manifest.migrationCoverage.riskCoverageRatio, 0);

  const positions = parseCsv(path.join(outDir, 'positions.csv'));
  const censored = positions.find((row) => row.position_id === 'closed-after-cutoff');
  assert(censored);
  assert.strictEqual(censored.status, 'open_at_cutoff');
  assert.strictEqual(censored.closed_at, '');
  assert.strictEqual(censored.pnl_sol, '');
  assert.strictEqual(censored.exit_reason, '');
  assert.strictEqual(censored.peak_price, '');

  const research = parseCsv(path.join(outDir, 'position-research-events.csv'));
  assert.strictEqual(research.length, 1);
  assert.strictEqual(research[0].event_type, 'SWAP_METRICS');
  assert.strictEqual(research[0].trade_count_3s, '3');

  const migrationRisk = parseCsv(path.join(outDir, 'migration-risk-snapshots.csv'));
  assert.strictEqual(migrationRisk.length, 1);
  assert.strictEqual(migrationRisk[0].mint, 'MigrationMint11111111111111111111111111111');
  assert.strictEqual(migrationRisk[0].large_transfer_count, '2');
  assert.strictEqual(migrationRisk[0].metric_observe_only, 'true');

  const sessions = parseCsv(path.join(outDir, 'monitored-sessions.csv'));
  assert.strictEqual(sessions.length, 2);
  const monitoredSession = sessions.find((row) => row.mint === monitoredMint);
  assert.strictEqual(monitoredSession.removal_reason, 'fdv_below_min');
  const cutoffSession = sessions.find((row) => row.mint === cutoffMint);
  assert(cutoffSession);
  assert.strictEqual(cutoffSession.removed_at, '');
  assert.strictEqual(cutoffSession.removal_reason, '');

  const lifecycle = parseCsv(path.join(outDir, 'token-lifecycle-events.csv'));
  assert.strictEqual(lifecycle.length, 3);
  assert(lifecycle.every((row) => Number(row.ts) < cutoff));

  const monitoredSwaps = parseCsv(path.join(outDir, 'monitored-swaps.csv'));
  assert.strictEqual(monitoredSwaps.length, 1);
  assert.strictEqual(monitoredSwaps[0].signature, 'monitored-swap-signature');

  const holderSnapshots = parseCsv(
    path.join(outDir, 'migration-holder-snapshots.csv'),
  );
  assert.strictEqual(holderSnapshots.length, 1);
  assert.strictEqual(holderSnapshots[0].top1_pct, '4');
  assert.strictEqual(holderSnapshots[0].holder_top_1_owner, 'holder-a');
  assert.strictEqual(holderSnapshots[0].holder_top_1_owner_type, 'wallet');

  const detections = parseCsv(path.join(outDir, 'migration-detections.csv'));
  assert.strictEqual(detections.length, 1);
  assert.strictEqual(detections[0].mint, monitoredMint);

  const effectiveConfig = fs.readFileSync(
    path.join(outDir, 'effective-config.txt'),
    'utf8',
  );
  assert(!effectiveConfig.includes('super-secret-key'));
  assert(!effectiveConfig.includes('super-secret-birdeye'));
  assert(!effectiveConfig.includes('super-private-wallet'));
  assert(!effectiveConfig.includes('super-secret-jito-keypair'));
  assert(!effectiveConfig.includes('super-secret-laser-token'));
  assert(!effectiveConfig.includes('secret-webhook'));
  assert(effectiveConfig.includes('REDACTED'));
  assert(effectiveConfig.includes('PUBLIC_SETTING=keep-me'));

  fs.rmSync(resolvedRoot, { recursive: true, force: true });
  console.log('position research export tests passed');
}

run();
