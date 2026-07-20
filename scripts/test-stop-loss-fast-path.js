'use strict';

const assert = require('assert');
const Module = require('module');

class FakeBN {
  constructor(value) {
    this.value = String(value);
  }

  toString() {
    return this.value;
  }
}

const monitor = {
  registerModule() {},
  inc() {},
  set() {},
  recordError() {},
};

const originalLoad = Module._load;
Module._load = function loadWithDotenvStub(request, parent, isMain) {
  if (request === 'dotenv') return { config() {} };
  if (request === 'bn.js') return FakeBN;
  if (request === 'bs58') return { default: { decode() {}, encode() {} } };
  if (request === '@solana/web3.js') {
    class PublicKey {}
    return {
      Connection: class {},
      Keypair: class {},
      PublicKey,
      VersionedTransaction: class {},
      TransactionMessage: class {},
      ComputeBudgetProgram: {},
      SystemProgram: {},
    };
  }
  if (request === '@solana/spl-token') {
    return {
      getAssociatedTokenAddressSync() {},
      createAssociatedTokenAccountIdempotentInstruction() {},
      TOKEN_PROGRAM_ID: {},
      ASSOCIATED_TOKEN_PROGRAM_ID: {},
    };
  }
  if (request === '@allenhark/slipstream') throw new Error('not installed in unit test');
  if (request === '../monitor/HealthMonitor') return { getMonitor: () => monitor };
  return originalLoad.call(this, request, parent, isMain);
};
const PoolStateCache = require('../src/core/PoolStateCache');
const Executor = require('../src/core/Executor');
Module._load = originalLoad;

function delayedResult(ms, value) {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms));
}

async function run() {
  const poolAddress = 'Pool111111111111111111111111111111111';
  const cache = Object.create(PoolStateCache.prototype);
  cache.cache = new Map([[poolAddress, {
    state: {
      poolBaseAmount: { toString: () => '1000000' },
      poolQuoteAmount: { toString: () => '1000000000' },
    },
    fetchedAt: Date.now() - 400,
  }]]);

  assert.strictEqual(cache.applySwapBalances(poolAddress, {
    poolBaseAfter: 123.456789,
    poolQuoteAfter: 42.25,
    baseDecimals: 6,
    slot: 200,
  }), true);
  const updated = cache.cache.get(poolAddress);
  assert.strictEqual(updated.state.poolBaseAmount.toString(), '123456789');
  assert.strictEqual(updated.state.poolQuoteAmount.toString(), '42250000000');
  assert.strictEqual(updated.marketSlot, 200);

  assert.strictEqual(cache.applySwapBalances(poolAddress, {
    poolBaseAfter: 999,
    poolQuoteAfter: 999,
    baseDecimals: 6,
    slot: 199,
  }), false, 'an older slot must not overwrite the stop-loss quote state');
  assert.strictEqual(updated.state.poolQuoteAmount.toString(), '42250000000');

  const calls = [];
  const executor = Object.create(Executor.prototype);
  executor.stakedRpc = {
    rpcEndpoint: 'https://staked.example',
    sendRawTransaction: (serialized, options) => {
      calls.push({ channel: 'staked', serialized, options });
      return delayedResult(30, 'staked-signature');
    },
  };
  executor.rpc = {
    rpcEndpoint: 'https://regular.example',
    sendRawTransaction: (serialized, options) => {
      calls.push({ channel: 'regular', serialized, options });
      return delayedResult(5, 'regular-signature');
    },
  };

  const payload = Buffer.from([1, 2, 3]);
  const signature = await executor._submitSellRace(payload);
  assert.strictEqual(signature, 'regular-signature');
  assert.strictEqual(calls.length, 2);
  assert(calls.every((call) => call.serialized === payload));
  assert(calls.every((call) => call.options.skipPreflight === true));
  assert(calls.every((call) => call.options.maxRetries === 2));

  console.log('Stop-loss fast-path tests: PASS');
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
