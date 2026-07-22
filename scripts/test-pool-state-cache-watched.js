'use strict';

const assert = require('assert');
const Module = require('module');

const monitor = {
  registerModule() {},
  inc() {},
  set() {},
  beat() {},
  recordError() {},
};

const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === '@solana/web3.js') return { PublicKey: class PublicKey {} };
  if (request === 'bn.js') return class BN {};
  if (request === '../monitor/HealthMonitor') return { getMonitor: () => monitor };
  return originalLoad.call(this, request, parent, isMain);
};

const PoolStateCache = require('../src/core/PoolStateCache');
Module._load = originalLoad;

async function main() {
  let watched = [{ mint: 'mint-1', poolAddress: 'pool-1' }];
  let registryUnavailable = false;
  const fetched = [];

  const cache = new PoolStateCache({
    onlineSdk: {},
    user: {},
    getMintList: () => {
      if (registryUnavailable) throw new Error('registry unavailable');
      return watched;
    },
  });
  cache.watchedRefreshMs = 60_000;
  cache.watchedBatchSize = 1;
  cache._fetchPoolState = async (poolAddress) => {
    fetched.push(poolAddress);
    return { poolAddress };
  };

  await cache._refreshAll();
  assert(cache.get('pool-1'), 'watched pool should be prewarmed without a hot mint');
  assert.deepStrictEqual(fetched, ['pool-1']);

  await cache._refreshAll();
  assert.deepStrictEqual(fetched, ['pool-1'], 'fresh watched pool should not be fetched again');

  cache.cache.set('orphan-pool', { state: {}, fetchedAt: Date.now() });
  await cache._refreshAll();
  assert.strictEqual(cache.get('orphan-pool'), null, 'inactive pool should be evicted');

  cache.cache.set('preserved-pool', { state: {}, fetchedAt: Date.now() });
  registryUnavailable = true;
  await cache._refreshAll();
  assert(cache.get('preserved-pool'), 'registry errors must not wipe the cache');

  registryUnavailable = false;
  watched = [];
  await cache._refreshAll();
  assert.strictEqual(cache.cache.size, 0, 'empty valid registry snapshot should evict inactive pools');

  cache.hotMints.set('hot-mint', {
    poolAddress: 'hot-pool',
    isPosition: false,
  });
  await cache._refreshAll();
  assert(cache.get('hot-pool'), 'hot pool should still use the fast refresh tier');

  console.log('PoolStateCache watched-tier tests passed');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
