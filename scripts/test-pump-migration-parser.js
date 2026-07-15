'use strict';

const assert = require('assert');
const {
  PUMP_PROGRAM_ID,
  PUMP_AMM_PROGRAM_ID,
  MIGRATE_DISCRIMINATOR,
  decodeBase58,
  parsePumpMigrationTransaction,
} = require('../src/utils/pumpMigrationParser');

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function encodeBase58(buffer) {
  let number = BigInt(`0x${buffer.toString('hex') || '0'}`);
  let encoded = '';
  while (number > 0n) {
    encoded = BASE58_ALPHABET[Number(number % 58n)] + encoded;
    number /= 58n;
  }
  for (const byte of buffer) {
    if (byte !== 0) break;
    encoded = `1${encoded}`;
  }
  return encoded || '1';
}

function key(char) {
  return char.repeat(32);
}

function migrationAccounts() {
  const accounts = Array.from({ length: 24 }, (_, index) => key(String.fromCharCode(65 + index)));
  accounts[2] = key('M');
  accounts[8] = PUMP_AMM_PROGRAM_ID;
  accounts[9] = key('P');
  accounts[17] = key('V');
  accounts[18] = key('W');
  return accounts;
}

function parsedTransaction(overrides = {}) {
  const instruction = {
    programId: PUMP_PROGRAM_ID,
    accounts: migrationAccounts(),
    data: encodeBase58(MIGRATE_DISCRIMINATOR),
    ...(overrides.instruction || {}),
  };
  return {
    slot: 123456,
    blockTime: 1_700_000_000,
    meta: { err: null, innerInstructions: [] },
    transaction: { message: { accountKeys: [], instructions: [instruction] } },
    ...overrides.transaction,
  };
}

const parsed = parsePumpMigrationTransaction(parsedTransaction(), {
  signature: 'test-signature',
  detectionPath: 'test',
});
assert(parsed, 'official migrate instruction should be detected');
assert.strictEqual(parsed.mint, key('M'));
assert.strictEqual(parsed.poolAddress, key('P'));
assert.strictEqual(parsed.poolBaseVault, key('V'));
assert.strictEqual(parsed.poolQuoteVault, key('W'));
assert.strictEqual(parsed.slot, 123456);
assert.strictEqual(parsed.migrationTime, 1_700_000_000_000);
assert.strictEqual(parsed.migrationTimeSource, 'blockTime');

const wrongData = Buffer.from(MIGRATE_DISCRIMINATOR);
wrongData[0] ^= 0xff;
assert.strictEqual(parsePumpMigrationTransaction(parsedTransaction({
  instruction: { data: encodeBase58(wrongData) },
})), null, 'a generic Pump instruction must not be accepted');

const wrongAmmAccounts = migrationAccounts();
wrongAmmAccounts[8] = key('Q');
assert.strictEqual(parsePumpMigrationTransaction(parsedTransaction({
  instruction: { accounts: wrongAmmAccounts },
})), null, 'migrate must target the official PumpSwap program');

const compiledAccounts = migrationAccounts();
const accountKeys = [PUMP_PROGRAM_ID, ...compiledAccounts];
const compiled = parsedTransaction({
  instruction: {
    programId: undefined,
    programIdIndex: 0,
    accounts: compiledAccounts.map((_, index) => index + 1),
  },
  transaction: {
    transaction: {
      message: {
        accountKeys,
        instructions: [{
          programIdIndex: 0,
          accounts: compiledAccounts.map((_, index) => index + 1),
          data: encodeBase58(MIGRATE_DISCRIMINATOR),
        }],
      },
    },
  },
});
assert(parsePumpMigrationTransaction(compiled), 'compiled account indexes should be supported');

assert.deepStrictEqual(decodeBase58('1'), Buffer.from([0]));
assert.deepStrictEqual(decodeBase58('2'), Buffer.from([1]));

console.log('Pump migration parser tests passed');
