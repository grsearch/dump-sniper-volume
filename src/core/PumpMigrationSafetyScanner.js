'use strict';

const crypto = require('crypto');
const {
  PUMP_PROGRAM_ID,
  PUMP_AMM_PROGRAM_ID,
  decodeInstructionData,
} = require('../utils/pumpMigrationParser');

const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const BUY_INSTRUCTION_NAMES = [
  'buy',
  'buy_exact_quote_in',
  'buy_exact_quote_in_v2',
  'buy_exact_sol_in',
];

function finiteNumber(value) {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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
  const parsedKeys = rawKeys.some(
    (key) => key && typeof key === 'object' && Object.prototype.hasOwnProperty.call(key, 'source'),
  );
  if (!message.staticAccountKeys && parsedKeys) return keys;

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

function anchorDiscriminator(name) {
  return crypto.createHash('sha256').update(`global:${name}`).digest().subarray(0, 8);
}

const BUY_DISCRIMINATORS = BUY_INSTRUCTION_NAMES.map(anchorDiscriminator);

function instructionProgramId(instruction, accountKeys) {
  return resolveKey(instruction?.programId ?? instruction?.programIdIndex, accountKeys);
}

function instructionAccounts(instruction, accountKeys) {
  return (instruction?.accounts || []).map((account) => resolveKey(account, accountKeys));
}

function instructionData(instruction) {
  try {
    return decodeInstructionData(instruction?.data);
  } catch (_) {
    return Buffer.alloc(0);
  }
}

function readU64Le(data, offset = 0) {
  if (!Buffer.isBuffer(data) || data.length < offset + 8) return null;
  let value = 0n;
  for (let index = 7; index >= 0; index--) {
    value = (value << 8n) + BigInt(data[offset + index]);
  }
  return value;
}

function rawToUiAmount(rawAmount, decimals) {
  if (rawAmount == null || !Number.isInteger(decimals) || decimals < 0) return null;
  const raw = typeof rawAmount === 'bigint' ? rawAmount : BigInt(String(rawAmount));
  const divisor = 10 ** decimals;
  const amount = Number(raw);
  return Number.isFinite(amount) && Number.isFinite(divisor) && divisor > 0
    ? amount / divisor
    : null;
}

function transactionSignature(transactionResult) {
  return transactionResult?.transaction?.signatures?.[0] || null;
}

function tokenAccountMetadata(transactionResult, accountKeys) {
  const result = new Map();
  const rows = [
    ...(transactionResult?.meta?.preTokenBalances || []),
    ...(transactionResult?.meta?.postTokenBalances || []),
  ];
  for (const row of rows) {
    const account = accountKeys[row?.accountIndex];
    if (!account || !row?.mint) continue;
    result.set(account, {
      mint: row.mint,
      owner: row.owner || null,
      decimals: Number.isInteger(row?.uiTokenAmount?.decimals)
        ? row.uiTokenAmount.decimals
        : null,
    });
  }
  return result;
}

function tokenBalanceUi(row) {
  const tokenAmount = row?.uiTokenAmount;
  const direct = finiteNumber(tokenAmount?.uiAmountString) ??
    finiteNumber(tokenAmount?.uiAmount);
  if (direct != null) return direct;
  const decimals = Number(tokenAmount?.decimals);
  return rawToUiAmount(tokenAmount?.amount, decimals);
}

function targetMintBalanceDeltas(transactionResult, accountKeys, mint) {
  const balances = new Map();
  const applyRows = (rows, side) => {
    for (const row of rows || []) {
      if (row?.mint !== mint) continue;
      const account = accountKeys[row.accountIndex];
      const amountUi = tokenBalanceUi(row);
      if (!account || amountUi == null) continue;
      const current = balances.get(account) || {
        account,
        owner: null,
        decimals: null,
        preUi: 0,
        postUi: 0,
      };
      current.owner = row.owner || current.owner;
      current.decimals = Number.isInteger(row?.uiTokenAmount?.decimals)
        ? row.uiTokenAmount.decimals
        : current.decimals;
      current[side] = amountUi;
      balances.set(account, current);
    }
  };

  applyRows(transactionResult?.meta?.preTokenBalances, 'preUi');
  applyRows(transactionResult?.meta?.postTokenBalances, 'postUi');
  return [...balances.values()].map((row) => ({
    ...row,
    deltaUi: row.postUi - row.preUi,
  }));
}

function parsedAmount(info, fallbackDecimals) {
  const tokenAmount = info?.tokenAmount;
  const decimals = Number.isInteger(tokenAmount?.decimals)
    ? tokenAmount.decimals
    : fallbackDecimals;
  const raw = tokenAmount?.amount ?? info?.amount;
  if (raw == null) return { rawAmount: null, amountUi: null, decimals };
  return {
    rawAmount: String(raw),
    amountUi: rawToUiAmount(raw, decimals),
    decimals,
  };
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(Math.max(1, concurrency), Math.max(1, items.length)) },
    async () => {
      while (nextIndex < items.length) {
        const index = nextIndex++;
        results[index] = await mapper(items[index], index);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

class PumpMigrationSafetyScanner {
  constructor(opts = {}) {
    this.rpcRequest = opts.rpcRequest;
    this.settings = opts.settings || {};
    this.blockCache = new Map();
  }

  async audit(migration) {
    if (!this.settings.auditEnabled) {
      return { allowed: true, skipped: true, reason: 'disabled' };
    }
    if (!this.rpcRequest) throw new Error('PumpMigrationSafetyScanner requires rpcRequest');
    if (!migration?.mint || !Number.isFinite(Number(migration?.slot))) {
      return this._incomplete('migration metadata is incomplete', migration);
    }

    const migrationSlot = Math.trunc(Number(migration.slot));
    let observedDetectionSlot = finiteNumber(migration.detectionSlot);
    if (observedDetectionSlot == null) {
      try {
        observedDetectionSlot = finiteNumber(await this._rpcWithRetries('getSlot', [
          { commitment: 'confirmed' },
        ]));
      } catch (err) {
        return this._incomplete(`detection slot unavailable: ${err.message}`, migration);
      }
    }
    if (observedDetectionSlot == null) {
      return this._incomplete('detection slot unavailable', migration);
    }
    const detectionSlot = Math.max(migrationSlot, Math.trunc(observedDetectionSlot));
    const preSlots = Math.max(0, Math.trunc(finiteNumber(this.settings.auditPreSlots) || 10));
    const startSlot = Math.max(0, migrationSlot - preSlots);
    const range = { startSlot, migrationSlot, detectionSlot };

    try {
      const supply = await this._fetchSupply(migration.mint);
      const producedSlots = await this._rpcWithRetries('getBlocks', [
        startSlot,
        detectionSlot,
        { commitment: 'confirmed' },
      ]);
      if (!Array.isArray(producedSlots)) {
        return this._incomplete('getBlocks returned no slot list', migration, range);
      }

      const scannedSlots = await mapWithConcurrency(
        producedSlots,
        Math.max(1, Math.trunc(finiteNumber(this.settings.auditRpcConcurrency) || 6)),
        async (slot) => {
          const block = await this._fetchBlock(slot);
          const slotMatches = [];
          let transactionsScanned = 0;
          for (const row of block.transactions || []) {
            transactionsScanned++;
            slotMatches.push(...this._inspectTransaction(row, {
              slot,
              migration,
              supply,
            }));
          }
          return {
            transactionsScanned,
            matches: slotMatches,
          };
        },
      );

      const matches = scannedSlots.flatMap((row) => row.matches);
      const transactionsScanned = scannedSlots.reduce(
        (sum, row) => sum + row.transactionsScanned,
        0,
      );

      const priority = {
        mint_to: 0,
        same_tx_buy_migrate: 1,
        large_transfer: 2,
      };
      matches.sort((left, right) =>
        (priority[left.type] ?? 99) - (priority[right.type] ?? 99) ||
        left.slot - right.slot);

      const summary = {
        ...range,
        producedSlots: producedSlots.length,
        blocksScanned: scannedSlots.length,
        transactionsScanned,
        supplyUi: supply.supplyUi,
        decimals: supply.decimals,
      };
      if (matches.length === 0) {
        return { allowed: true, summary, matches: [] };
      }

      const primary = matches[0];
      return {
        allowed: false,
        reasonCode: primary.type,
        message: this._messageFor(primary),
        evidence: primary,
        matches: matches.slice(0, 20),
        summary,
      };
    } catch (err) {
      return this._incomplete(err.message, migration, range);
    }
  }

  async _fetchSupply(mint) {
    const result = await this._rpcWithRetries('getTokenSupply', [
      mint,
      { commitment: 'confirmed' },
    ]);
    const value = result?.value;
    const decimals = Number(value?.decimals);
    const rawAmount = value?.amount;
    const supplyUi = finiteNumber(value?.uiAmountString) ??
      rawToUiAmount(rawAmount, decimals);
    if (!Number.isInteger(decimals) || decimals < 0 || !(supplyUi > 0)) {
      throw new Error('token supply unavailable');
    }
    return {
      decimals,
      rawAmount: rawAmount != null ? String(rawAmount) : null,
      supplyUi,
    };
  }

  async _fetchBlock(slot) {
    const now = Date.now();
    const cached = this.blockCache.get(slot);
    if (cached && cached.expiresAt > now) return cached.promise;

    const promise = this._rpcWithRetries('getBlock', [
      slot,
      {
        commitment: 'confirmed',
        encoding: 'jsonParsed',
        transactionDetails: 'full',
        rewards: false,
        maxSupportedTransactionVersion: 0,
      },
    ]).then((block) => {
      if (!block) throw new Error(`getBlock returned null for produced slot ${slot}`);
      return block;
    });
    this.blockCache.set(slot, {
      expiresAt: now + Math.max(1_000, finiteNumber(this.settings.auditBlockCacheMs) || 120_000),
      promise,
    });
    const maxCacheSlots = Math.max(
      1,
      Math.trunc(finiteNumber(this.settings.auditBlockCacheMaxSlots) || 32),
    );
    while (this.blockCache.size > maxCacheSlots) {
      const oldestSlot = this.blockCache.keys().next().value;
      this.blockCache.delete(oldestSlot);
    }
    try {
      return await promise;
    } catch (err) {
      this.blockCache.delete(slot);
      throw err;
    }
  }

  async _rpcWithRetries(method, params) {
    const retries = Math.max(1, Math.trunc(finiteNumber(this.settings.auditRpcRetries) || 3));
    let lastError = null;
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        return await this.rpcRequest(method, params);
      } catch (err) {
        lastError = err;
        if (attempt < retries) {
          await new Promise((resolve) => setTimeout(resolve, 150 * attempt));
        }
      }
    }
    throw lastError || new Error(`${method} failed`);
  }

  _inspectTransaction(transactionResult, context) {
    if (!transactionResult || transactionResult.meta?.err) return [];
    const { migration, slot, supply } = context;
    const accountKeys = resolveAccountKeys(transactionResult);
    const tokenAccounts = tokenAccountMetadata(transactionResult, accountKeys);
    const balanceDeltas = targetMintBalanceDeltas(
      transactionResult,
      accountKeys,
      migration.mint,
    );
    const signature = transactionSignature(transactionResult);
    const migrationTx = signature === migration.signature;
    const matches = [];
    const transfers = [];
    let hasExplicitBuy = false;

    for (const instruction of collectInstructions(transactionResult)) {
      const programId = instructionProgramId(instruction, accountKeys);
      const accounts = instructionAccounts(instruction, accountKeys);
      const data = instructionData(instruction);
      const parsed = instruction?.parsed;
      const parsedType = String(parsed?.type || '').toLowerCase();
      const info = parsed?.info || {};

      if (
        (programId === PUMP_PROGRAM_ID || programId === PUMP_AMM_PROGRAM_ID) &&
        accounts.includes(migration.mint) &&
        BUY_DISCRIMINATORS.some((discriminator) =>
          data.length >= discriminator.length &&
          data.subarray(0, discriminator.length).equals(discriminator))
      ) {
        hasExplicitBuy = true;
      }

      if (programId !== TOKEN_PROGRAM_ID && programId !== TOKEN_2022_PROGRAM_ID) continue;

      if (parsedType === 'mintto' || parsedType === 'minttochecked') {
        if (info.mint === migration.mint) {
          matches.push({
            type: 'mint_to',
            slot,
            signature,
            mint: migration.mint,
            destination: info.account || info.destination || null,
            amountRaw: info.amount ?? info.tokenAmount?.amount ?? null,
          });
        }
        continue;
      }

      const rawInstructionType = data.length > 0 ? data[0] : null;
      if (rawInstructionType === 7 || rawInstructionType === 14) {
        if (accounts[0] === migration.mint) {
          matches.push({
            type: 'mint_to',
            slot,
            signature,
            mint: migration.mint,
            destination: accounts[1] || null,
            amountRaw: readU64Le(data, 1)?.toString() || null,
          });
        }
        continue;
      }

      let transfer = null;
      if (parsedType === 'transfer' || parsedType === 'transferchecked') {
        const source = info.source || null;
        const destination = info.destination || null;
        const sourceMeta = tokenAccounts.get(source);
        const destinationMeta = tokenAccounts.get(destination);
        const mint = info.mint || sourceMeta?.mint || destinationMeta?.mint || null;
        const amount = parsedAmount(
          info,
          sourceMeta?.decimals ?? destinationMeta?.decimals ?? supply.decimals,
        );
        transfer = {
          mint,
          source,
          destination,
          sourceOwner: sourceMeta?.owner || null,
          destinationOwner: destinationMeta?.owner || null,
          ...amount,
        };
      } else if (rawInstructionType === 3 || rawInstructionType === 12) {
        const checked = rawInstructionType === 12;
        const source = accounts[0] || null;
        const destination = accounts[checked ? 2 : 1] || null;
        const sourceMeta = tokenAccounts.get(source);
        const destinationMeta = tokenAccounts.get(destination);
        const mint = checked
          ? accounts[1]
          : sourceMeta?.mint || destinationMeta?.mint || null;
        const decimals = checked && data.length > 9
          ? data[9]
          : sourceMeta?.decimals ?? destinationMeta?.decimals ?? supply.decimals;
        const rawAmount = readU64Le(data, 1);
        transfer = {
          mint,
          source,
          destination,
          sourceOwner: sourceMeta?.owner || null,
          destinationOwner: destinationMeta?.owner || null,
          rawAmount: rawAmount?.toString() || null,
          amountUi: rawToUiAmount(rawAmount, decimals),
          decimals,
        };
      }

      if (transfer?.mint === migration.mint) transfers.push(transfer);
    }

    const externalPositiveDeltas = balanceDeltas.filter((delta) =>
      delta.deltaUi > 0 &&
      !this._isMigrationInfrastructureBalance(delta, migrationTx, migration));

    if (migrationTx && (hasExplicitBuy || externalPositiveDeltas.length > 0)) {
      const userTransfer = transfers.find((transfer) =>
        !this._isMigrationInfrastructureTransfer(transfer, migrationTx, migration));
      const userDelta = externalPositiveDeltas[0];
      matches.push({
        type: 'same_tx_buy_migrate',
        slot,
        signature,
        mint: migration.mint,
        source: userTransfer?.source || null,
        destination: userTransfer?.destination || userDelta?.account || null,
        destinationOwner: userTransfer?.destinationOwner || userDelta?.owner || null,
        amountUi: userTransfer?.amountUi ?? userDelta?.deltaUi ?? null,
        detection: hasExplicitBuy ? 'pump_buy_instruction' : 'token_balance_delta',
      });
    }

    const transferCandidates = transfers.map((transfer) => ({
      ...transfer,
      detection: 'token_instruction',
    }));
    for (const delta of externalPositiveDeltas) {
      const alreadyRepresented = transfers.some((transfer) => {
        if (transfer.destination !== delta.account) return false;
        const transferAmount = finiteNumber(transfer.amountUi);
        if (transferAmount == null) return false;
        const tolerance = Math.max(0.000001, Math.abs(delta.deltaUi) * 1e-9);
        return Math.abs(transferAmount - delta.deltaUi) <= tolerance;
      });
      if (alreadyRepresented) continue;
      transferCandidates.push({
        mint: migration.mint,
        source: null,
        destination: delta.account,
        sourceOwner: null,
        destinationOwner: delta.owner,
        rawAmount: null,
        amountUi: delta.deltaUi,
        decimals: delta.decimals,
        detection: 'token_balance_delta',
      });
    }

    for (const transfer of transferCandidates) {
      if (this._isMigrationInfrastructureTransfer(transfer, migrationTx, migration)) continue;
      const amountUi = finiteNumber(transfer.amountUi);
      if (!(amountUi >= 0)) continue;
      const supplyPct = supply.supplyUi > 0 ? (amountUi / supply.supplyUi) * 100 : null;
      const absoluteThreshold = finiteNumber(this.settings.auditLargeTransferTokens) || 100_000_000;
      const supplyThreshold = finiteNumber(this.settings.auditLargeTransferSupplyPct) || 5;
      if (amountUi <= absoluteThreshold && !(supplyPct > supplyThreshold)) continue;

      matches.push({
        type: 'large_transfer',
        slot,
        signature,
        mint: migration.mint,
        source: transfer.source,
        destination: transfer.destination,
        sourceOwner: transfer.sourceOwner,
        destinationOwner: transfer.destinationOwner,
        amountUi,
        supplyPct,
        absoluteThreshold,
        supplyThreshold,
        detection: transfer.detection,
      });
    }

    return matches;
  }

  _migrationInfrastructure(migration, migrationTx) {
    const accounts = new Set([
      ...(migration.migrationTokenAccounts || []),
      migration.poolBaseVault,
      migration.poolAddress,
      migration.migrationPoolAuthority,
    ].filter(Boolean));
    const owners = new Set([
      migration.poolAddress,
      migration.migrationPoolAuthority,
    ].filter(Boolean));
    if (migrationTx && migration.migrationUser) {
      accounts.add(migration.migrationUser);
      owners.add(migration.migrationUser);
    }
    return { accounts, owners };
  }

  _isMigrationInfrastructureBalance(balance, migrationTx, migration) {
    const infrastructure = this._migrationInfrastructure(migration, migrationTx);
    return infrastructure.accounts.has(balance.account) ||
      infrastructure.owners.has(balance.owner);
  }

  _isMigrationInfrastructureTransfer(transfer, migrationTx, migration) {
    if (!migrationTx) return false;
    const infrastructure = this._migrationInfrastructure(migration, migrationTx);
    const sourceIsInfrastructure = infrastructure.accounts.has(transfer.source) ||
      infrastructure.owners.has(transfer.sourceOwner);
    const destinationIsInfrastructure = infrastructure.accounts.has(transfer.destination) ||
      infrastructure.owners.has(transfer.destinationOwner);
    return sourceIsInfrastructure && destinationIsInfrastructure;
  }

  _incomplete(message, migration, range = null) {
    const result = {
      allowed: this.settings.auditFailClosed === false,
      reasonCode: 'audit_incomplete',
      message: `migration safety audit incomplete: ${message}`,
      evidence: {
        type: 'audit_incomplete',
        mint: migration?.mint || null,
        signature: migration?.signature || null,
        detail: message,
      },
      summary: range,
    };
    return result;
  }

  _messageFor(evidence) {
    if (evidence.type === 'mint_to') {
      return `MintTo found at slot ${evidence.slot} (${evidence.signature || 'unknown signature'})`;
    }
    if (evidence.type === 'same_tx_buy_migrate') {
      return `Buy and Migrate found in the same transaction ${evidence.signature || ''}`.trim();
    }
    if (evidence.type === 'large_transfer') {
      return `large transfer ${Math.round(evidence.amountUi).toLocaleString('en-US')} token ` +
        `(${evidence.supplyPct.toFixed(2)}% supply) at slot ${evidence.slot}`;
    }
    return evidence.type;
  }
}

module.exports = {
  PumpMigrationSafetyScanner,
  anchorDiscriminator,
  resolveAccountKeys,
};
