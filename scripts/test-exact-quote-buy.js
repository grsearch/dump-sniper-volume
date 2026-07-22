'use strict';

const assert = require('assert');
const BN = require('bn.js');
const {
  OFFLINE_PUMP_AMM_PROGRAM,
  PUMP_AMM_PROGRAM_ID,
} = require('@pump-fun/pump-swap-sdk');

assert(OFFLINE_PUMP_AMM_PROGRAM, 'PumpSwap instruction coder missing');
assert(PUMP_AMM_PROGRAM_ID, 'PumpSwap program id missing');

const spendableQuoteIn = new BN(200_000_000);
const minBaseAmountOut = new BN('123456789');
const data = OFFLINE_PUMP_AMM_PROGRAM.coder.instruction.encode(
  'buyExactQuoteIn',
  {
    spendableQuoteIn,
    minBaseAmountOut,
    trackVolume: { 0: true },
  },
);

assert.deepStrictEqual(
  Array.from(data.subarray(0, 8)),
  [198, 46, 21, 82, 180, 217, 232, 112],
);
assert.strictEqual(data.readBigUInt64LE(8), 200_000_000n);
assert.strictEqual(data.readBigUInt64LE(16), 123_456_789n);

console.log('PumpSwap exact-quote buy instruction test: PASS');
