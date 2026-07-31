'use strict';

const assert = require('assert');
const { warmupRsiFromDb } = require('../src/utils/rsiWarmup');

const fed = [];
const db = {
  prepare() {
    return {
      all() {
        throw new Error('warmup must not materialize the result set');
      },
      *iterate(sinceMs) {
        assert.strictEqual(sinceMs, 123);
        yield { mint: 'a', ts: 10, price: 1.5 };
        yield { mint: 'b', ts: 20, price: 2.5 };
      },
    };
  },
};
const calculator = {
  feedTick(mint, price, ts) {
    fed.push({ mint, price, ts });
  },
};

assert.strictEqual(warmupRsiFromDb(db, calculator, 123), 2);
assert.deepStrictEqual(fed, [
  { mint: 'a', price: 1.5, ts: 10 },
  { mint: 'b', price: 2.5, ts: 20 },
]);
console.log('RSI warmup streaming test passed');
