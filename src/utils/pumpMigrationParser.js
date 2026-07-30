'use strict';

const PUMP_PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const PUMP_AMM_PROGRAM_ID = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const SYSTEM_PROGRAM_ID = '11111111111111111111111111111111';
const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const MIGRATE_DISCRIMINATOR = Buffer.from([155, 234, 231, 146, 236, 158, 162, 30]);
const MIGRATE_V2_DISCRIMINATOR = Buffer.from([187, 203, 18, 31, 206, 237, 254, 41]);

const MIGRATION_LAYOUTS = [
  {
    version: 'v1',
    discriminator: MIGRATE_DISCRIMINATOR,
    mintIndex: 2,
    userIndex: 5,
    pumpAmmIndex: 8,
    poolIndex: 9,
    poolAuthorityIndex: 10,
    baseVaultIndex: 17,
    quoteVaultIndex: 18,
    migrationTokenAccountIndexes: [4, 11, 17],
  },
  {
    version: 'v2',
    discriminator: MIGRATE_V2_DISCRIMINATOR,
    mintIndex: 2,
    userIndex: 7,
    pumpAmmIndex: 9,
    poolIndex: 10,
    poolAuthorityIndex: 11,
    baseVaultIndex: 17,
    quoteVaultIndex: 18,
    migrationTokenAccountIndexes: [5, 12, 17],
  },
];

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function decodeBase58(value) {
  if (typeof value !== 'string' || value.length === 0) return Buffer.alloc(0);

  let number = 0n;
  for (const char of value) {
    const digit = BASE58_ALPHABET.indexOf(char);
    if (digit < 0) throw new Error('invalid base58 instruction data');
    number = number * 58n + BigInt(digit);
  }

  let body = Buffer.alloc(0);
  if (number > 0n) {
    let hex = number.toString(16);
    if (hex.length % 2 !== 0) hex = `0${hex}`;
    body = Buffer.from(hex, 'hex');
  }

  let leadingZeros = 0;
  while (leadingZeros < value.length && value[leadingZeros] === '1') leadingZeros++;
  return Buffer.concat([Buffer.alloc(leadingZeros), body]);
}

function decodeInstructionData(data) {
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof Uint8Array) return Buffer.from(data);
  if (Array.isArray(data) && typeof data[0] === 'string') {
    return Buffer.from(data[0], data[1] === 'base64' ? 'base64' : 'utf8');
  }
  if (data && typeof data === 'object' && typeof data.data === 'string') {
    return Buffer.from(data.data, data.encoding === 'base64' ? 'base64' : 'utf8');
  }
  if (typeof data === 'string') return decodeBase58(data);
  return Buffer.alloc(0);
}

function publicKeyString(value) {
  if (typeof value === 'string') return value;
  if (!value) return null;
  if (typeof value.pubkey === 'string') return value.pubkey;
  if (value.pubkey && typeof value.pubkey.toBase58 === 'function') return value.pubkey.toBase58();
  if (typeof value.toBase58 === 'function') return value.toBase58();
  const rendered = value.toString?.();
  return rendered && rendered !== '[object Object]' ? rendered : null;
}

function resolveAccountKeys(transactionResult) {
  const message = transactionResult?.transaction?.message || {};
  const meta = transactionResult?.meta || {};
  const rawKeys = message.staticAccountKeys || message.accountKeys || [];
  const keys = rawKeys.map(publicKeyString);

  const accountKeysAlreadyParsed = rawKeys.some(
    (key) => key && typeof key === 'object' && Object.prototype.hasOwnProperty.call(key, 'source'),
  );
  if (!message.staticAccountKeys && accountKeysAlreadyParsed) return keys;

  const loaded = meta.loadedAddresses || {};
  return keys.concat(
    (loaded.writable || []).map(publicKeyString),
    (loaded.readonly || loaded.readOnly || []).map(publicKeyString),
  );
}

function resolveKey(value, accountKeys) {
  if (Number.isInteger(value)) return accountKeys[value] || null;
  return publicKeyString(value);
}

function collectInstructions(transactionResult) {
  const outer = transactionResult?.transaction?.message?.instructions || [];
  const inner = transactionResult?.meta?.innerInstructions || [];
  return outer.concat(inner.flatMap((group) => group?.instructions || []));
}

function isLikelyPublicKey(value) {
  return typeof value === 'string' && value.length >= 32 && value.length <= 44 &&
    [...value].every((char) => BASE58_ALPHABET.includes(char));
}

function isLikelyVaultAddress(value) {
  return isLikelyPublicKey(value) &&
    value !== SYSTEM_PROGRAM_ID &&
    value !== TOKEN_PROGRAM_ID &&
    value !== TOKEN_2022_PROGRAM_ID &&
    value !== PUMP_PROGRAM_ID &&
    value !== PUMP_AMM_PROGRAM_ID;
}

/**
 * Parse a confirmed Pump migrate transaction using the official instruction layout.
 * Log messages are deliberately ignored here: they are only a transport-side prefilter.
 */
function parsePumpMigrationTransaction(transactionResult, opts = {}) {
  if (!transactionResult || transactionResult.meta?.err) return null;

  const accountKeys = resolveAccountKeys(transactionResult);
  for (const instruction of collectInstructions(transactionResult)) {
    const programId = resolveKey(instruction.programId ?? instruction.programIdIndex, accountKeys);
    if (programId !== PUMP_PROGRAM_ID) continue;

    let data;
    try {
      data = decodeInstructionData(instruction.data);
    } catch (_) {
      continue;
    }
    const layout = MIGRATION_LAYOUTS.find(({ discriminator }) =>
      data.length >= discriminator.length &&
      data.subarray(0, discriminator.length).equals(discriminator));
    if (!layout) continue;

    const accounts = (instruction.accounts || []).map((account) => resolveKey(account, accountKeys));
    const mint = accounts[layout.mintIndex];
    const pumpAmm = accounts[layout.pumpAmmIndex];
    const poolAddress = accounts[layout.poolIndex];
    if (!isLikelyPublicKey(mint) || pumpAmm !== PUMP_AMM_PROGRAM_ID || !isLikelyPublicKey(poolAddress)) {
      continue;
    }

    const blockTime = Number(transactionResult.blockTime);
    return {
      mint,
      migrationUser: isLikelyPublicKey(accounts[layout.userIndex])
        ? accounts[layout.userIndex]
        : null,
      poolAddress,
      migrationPoolAuthority: isLikelyPublicKey(accounts[layout.poolAuthorityIndex])
        ? accounts[layout.poolAuthorityIndex]
        : null,
      poolBaseVault: isLikelyVaultAddress(accounts[layout.baseVaultIndex])
        ? accounts[layout.baseVaultIndex]
        : null,
      poolQuoteVault: isLikelyVaultAddress(accounts[layout.quoteVaultIndex])
        ? accounts[layout.quoteVaultIndex]
        : null,
      migrationTokenAccounts: layout.migrationTokenAccountIndexes
        .map((index) => accounts[index])
        .filter(isLikelyVaultAddress),
      migrationVersion: layout.version,
      signature: opts.signature || null,
      slot: Number(transactionResult.slot) || null,
      migrationTime: Number.isFinite(blockTime) && blockTime > 0
        ? Math.trunc(blockTime * 1000)
        : Date.now(),
      migrationTimeSource: Number.isFinite(blockTime) && blockTime > 0 ? 'blockTime' : 'observedAt',
      detectionPath: opts.detectionPath || 'unknown',
    };
  }

  return null;
}

module.exports = {
  PUMP_PROGRAM_ID,
  PUMP_AMM_PROGRAM_ID,
  MIGRATE_DISCRIMINATOR,
  MIGRATE_V2_DISCRIMINATOR,
  decodeBase58,
  decodeInstructionData,
  isLikelyVaultAddress,
  parsePumpMigrationTransaction,
};

