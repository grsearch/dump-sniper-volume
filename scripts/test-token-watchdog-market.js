'use strict';

const assert = require('assert');
const Module = require('module');
const {
  normalizeUnixMs,
  extractMigrationInfo,
} = require('../src/utils/migrationTime');

assert.strictEqual(normalizeUnixMs(1_700_000_000), 1_700_000_000_000);
assert.strictEqual(normalizeUnixMs(1_700_000_000_123), 1_700_000_000_123);
assert.strictEqual(normalizeUnixMs('2026-07-16T10:00:00Z'), 1_784_196_000_000);
assert.deepStrictEqual(
  extractMigrationInfo({
    migration_time: 1_700_000_000,
    migration_slot: 123,
    migration_signature: 'sig',
  }),
  {
    migrationTime: 1_700_000_000_000,
    migrationTimeSource: 'webhook_payload',
    migrationSlot: 123,
    migrationSignature: 'sig',
  },
);

const originalLoad = Module._load;
Module._load = function loadWithDependencyStubs(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  if (request === 'axios') return { get: async () => ({ data: [] }), post: async () => ({ data: {} }) };
  return originalLoad.call(this, request, parent, isMain);
};
const {
  selectDexScreenerPair,
  normalizeDexScreenerPair,
} = require('../src/utils/tokenMeta');
const TokenWatchdog = require('../src/core/TokenWatchdog');
Module._load = originalLoad;

const mint = 'M'.repeat(32);
const preferredPool = 'P'.repeat(32);
const pairs = [
  {
    chainId: 'solana',
    pairAddress: 'Q'.repeat(32),
    baseToken: { address: mint, symbol: 'TEST', name: 'Test' },
    liquidity: { usd: 50_000 },
    fdv: 40_000,
    priceUsd: '0.00004',
  },
  {
    chainId: 'solana',
    pairAddress: preferredPool,
    baseToken: { address: mint, symbol: 'TEST', name: 'Test' },
    liquidity: { usd: 4_000 },
    fdv: 2_000,
    priceUsd: '0.000002',
    volume: { h24: 12_345 },
    pairCreatedAt: 1_700_000_000_000,
    dexId: 'pumpswap',
  },
];
assert.strictEqual(
  selectDexScreenerPair(pairs, mint, preferredPool).pairAddress,
  preferredPool,
  'the registry pool must win over another higher-liquidity pair',
);
assert.strictEqual(
  selectDexScreenerPair(pairs, mint).pairAddress,
  'Q'.repeat(32),
  'without a registry pool, the deepest pair should be selected',
);
assert.deepStrictEqual(
  {
    ...normalizeDexScreenerPair(pairs[1]),
    fetchedAt: 0,
  },
  {
    symbol: 'TEST',
    name: 'Test',
    fdv: 2_000,
    marketCap: null,
    liquidity: 4_000,
    price: 0.000002,
    priceChange24h: null,
    volume24h: 12_345,
    pairAddress: preferredPool,
    pairCreatedAt: 1_700_000_000_000,
    dexId: 'pumpswap',
    marketComplete: true,
    marketSource: 'dexscreener',
    fetchedAt: 0,
  },
);
const incompletePair = {
  chainId: 'solana',
  pairAddress: preferredPool,
  baseToken: { address: mint, symbol: 'TEST', name: 'Test' },
  liquidity: { usd: 4_000 },
  pairCreatedAt: 1_700_000_000_000,
  dexId: 'pumpswap',
};
assert.strictEqual(
  normalizeDexScreenerPair(incompletePair, mint).marketComplete,
  false,
  'pair creation metadata must survive even when FDV is not available yet',
);

(async () => {
  const now = Date.now();
  const token = {
    mint,
    symbol: 'TEST',
    source: 'pump_graduation',
    pool_address: preferredPool,
    fdv: 40_000,
    liquidity: 20_000,
    added_at: now - 60_000,
    market_updated_at: null,
    migration_time: null,
    is_active: 1,
  };
  let removed = false;
  const registry = {
    listActive: () => (removed ? [] : [token]),
    updateMarket: (_mint, market) => {
      Object.assign(token, {
        fdv: market.fdv,
        liquidity: market.liquidity,
        price: market.price,
        market_updated_at: market.fetchedAt,
        market_source: market.marketSource,
        meta_json: JSON.stringify(market),
      });
      return token;
    },
    recordMigration: (_mint, info) => {
      token.migration_time = info.migrationTime;
      token.migration_time_source = info.migrationTimeSource;
      return token;
    },
    removeToken: () => { removed = true; },
  };

  const previousCheckInterval = process.env.WATCHDOG_CHECK_INTERVAL_MS;
  process.env.WATCHDOG_CHECK_INTERVAL_MS = '900000';
  const watchdog = new TokenWatchdog({
    tokenRegistry: registry,
    positionManager: { hasOpenPosition: () => false },
    tradeLogger: null,
    fetchMarkets: async () => new Map([[
      mint,
      {
        fdv: null,
        liquidity: 4_000,
        pairCreatedAt: now - 10 * 60_000,
        marketComplete: false,
        marketSource: 'dexscreener',
        fetchedAt: now,
      },
    ]]),
    fetchMarket: async () => ({
      fdv: 2_000,
      liquidity: 4_000,
      price: 0.000002,
      volume24h: 50_000,
      marketSource: 'birdeye',
      fetchedAt: now,
    }),
  });
  if (previousCheckInterval == null) delete process.env.WATCHDOG_CHECK_INTERVAL_MS;
  else process.env.WATCHDOG_CHECK_INTERVAL_MS = previousCheckInterval;
  assert.strictEqual(
    watchdog.checkIntervalMs,
    60_000,
    'legacy 15-minute watchdog configuration must be clamped to one minute',
  );
  watchdog.minFdVUsd = 15_000;
  watchdog.minLiquidityUsd = 3_000;
  watchdog.minVolume24hUsd = 0;
  watchdog.noBuyRemoveMs = 0;

  await watchdog._check();
  assert.strictEqual(token.market_source, 'birdeye');
  assert.strictEqual(token.migration_time, now - 10 * 60_000);
  assert.strictEqual(token.migration_time_source, 'dexscreener_pairCreatedAt');
  assert.strictEqual(removed, true, 'fresh FDV below the threshold must remove the token');

  const staleToken = {
    ...token,
    migration_time: now - 10 * 60_000,
    fdv: 2_000,
    liquidity: 1_000,
    market_updated_at: now - 10 * 60_000,
  };
  let staleRemoved = false;
  const staleRegistry = {
    listActive: () => (staleRemoved ? [] : [staleToken]),
    updateMarket: () => staleToken,
    recordMigration: () => staleToken,
    removeToken: () => { staleRemoved = true; },
  };
  const staleWatchdog = new TokenWatchdog({
    tokenRegistry: staleRegistry,
    positionManager: { hasOpenPosition: () => false },
    tradeLogger: null,
    fetchMarkets: async () => new Map(),
    fetchMarket: async () => {
      const err = new Error('rate limited');
      err.response = { status: 429 };
      throw err;
    },
  });
  staleWatchdog.minFdVUsd = 15_000;
  staleWatchdog.minLiquidityUsd = 3_000;
  staleWatchdog.minVolume24hUsd = 0;
  staleWatchdog.noBuyRemoveMs = 0;
  staleWatchdog.maxTokenAgeMs = 0;
  staleWatchdog.maxWatchDurationMs = 0;

  await staleWatchdog._check();
  assert.strictEqual(
    staleRemoved,
    false,
    'stale FDV/LP must not remove a token when both market providers fail',
  );

  console.log('Token market refresh and migration AGE tests passed');
  process.exit(0);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
