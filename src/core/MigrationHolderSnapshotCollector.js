'use strict';

const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const {
  PUMP_PROGRAM_ID,
  PUMP_AMM_PROGRAM_ID,
} = require('../utils/pumpMigrationParser');

const monitor = getMonitor();
const MODULE = 'MigrationHolderSnapshot';
const SYSTEM_PROGRAM_ID = '11111111111111111111111111111111';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function finiteNumber(value) {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function positiveNumber(value) {
  const parsed = finiteNumber(value);
  return parsed != null && parsed > 0 ? parsed : null;
}

function normalizeAmount(value, decimals) {
  const directUi = positiveNumber(
    value?.uiAmountString ?? value?.ui_amount_string ?? value?.uiAmount ?? value?.ui_amount,
  );
  if (directUi != null) return directUi;

  const raw = positiveNumber(value?.amount ?? value);
  if (raw == null) return 0;
  const scale = 10 ** Math.max(0, Number(decimals) || 0);
  return Number.isFinite(scale) && scale > 0 ? raw / scale : raw;
}

class MigrationHolderSnapshotCollector {
  constructor(opts = {}) {
    this.tradeLogger = opts.tradeLogger || null;
    this.rpcUrl = opts.rpcUrl || config.helius.rpcUrl;
    this.enabled = opts.enabled ??
      ((process.env.MIGRATION_HOLDER_SNAPSHOT_ENABLED ?? 'true').toLowerCase() === 'true');
    this.pageSize = Math.min(
      1000,
      Math.max(100, Number(opts.pageSize) ||
        parseInt(process.env.MIGRATION_HOLDER_PAGE_SIZE || '1000', 10)),
    );
    this.maxPages = Math.min(
      20,
      Math.max(1, Number(opts.maxPages) ||
        parseInt(process.env.MIGRATION_HOLDER_MAX_PAGES || '5', 10)),
    );
    this.retryDelaysMs = Array.isArray(opts.retryDelaysMs)
      ? opts.retryDelaysMs
      : [0, 1500, 4000, 8000];
    this.rpcRequest = opts.rpcRequest || this._rpcRequest.bind(this);
    this.inFlight = new Map();
    this.completed = new Set();

    monitor.registerModule(MODULE, {
      staleMs: 30 * 60_000,
      label: 'Migration Holder Snapshot',
    });
  }

  capture(context = {}) {
    const migration = context.migration || {};
    const key = migration.mint || context.token?.mint;
    if (!this.enabled || !this.tradeLogger || !migration.mint || !key) return Promise.resolve(null);
    const persisted = this.tradeLogger.getMigrationHolderSnapshotStatusByMint?.(key);
    if (this.completed.has(key) || Number(persisted?.is_complete) === 1) {
      return Promise.resolve(null);
    }
    if (this.inFlight.has(key)) return this.inFlight.get(key);

    const task = this._captureWithRetries(context)
      .catch((err) => {
        this._persistFailure(context, err);
        monitor.recordError(MODULE, err, { mint: migration.mint });
        return null;
      })
      .finally(() => this.inFlight.delete(key));
    this.inFlight.set(key, task);
    return task;
  }

  async _captureWithRetries(context) {
    let lastError = null;
    for (let attempt = 0; attempt < this.retryDelaysMs.length; attempt++) {
      const delayMs = Math.max(0, Number(this.retryDelaysMs[attempt]) || 0);
      if (delayMs > 0) await sleep(delayMs);
      try {
        const snapshot = await this._fetchSnapshot(context);
        this.tradeLogger.logMigrationHolderSnapshot(snapshot);
        if (snapshot.isComplete) this.completed.add(snapshot.mint);
        monitor.beat(MODULE, `captured:${snapshot.holderCount ?? 0}`);
        monitor.inc(`${MODULE}.captured`, 1, MODULE);
        console.log(
          `[MigrationHolder] ${snapshot.symbol || snapshot.mint.slice(0, 8)} ` +
            `holders=${snapshot.holderCount ?? '?'} top1=${snapshot.top1Pct?.toFixed(2) ?? '?'}% ` +
            `delay=${snapshot.captureDelayMs}ms complete=${snapshot.isComplete ? 'yes' : 'no'}`,
        );
        return snapshot;
      } catch (err) {
        lastError = err;
        monitor.inc(`${MODULE}.retry`, 1, MODULE);
      }
    }
    throw lastError || new Error('holder snapshot unavailable');
  }

  async _fetchSnapshot({ token = {}, migration = {} } = {}) {
    const mint = migration.mint || token.mint;
    const supplyResult = await this.rpcRequest('getTokenSupply', [mint, { commitment: 'confirmed' }]);
    const supplyValue = supplyResult?.value || supplyResult || {};
    const decimals = finiteNumber(supplyValue.decimals) ?? finiteNumber(token.decimals) ?? 6;
    const supplyUi = positiveNumber(supplyValue.uiAmountString ?? supplyValue.uiAmount) ||
      normalizeAmount(supplyValue, decimals);
    if (!(supplyUi > 0)) throw new Error('token supply unavailable');

    let accountResult;
    let source = 'helius_das_getTokenAccounts';
    try {
      accountResult = await this._fetchDasTokenAccounts(mint, decimals);
    } catch (err) {
      source = 'solana_getTokenLargestAccounts_fallback';
      accountResult = await this._fetchLargestAccounts(mint, decimals);
      accountResult.error = `DAS unavailable: ${err.message}`;
    }

    const migrationSlot = finiteNumber(migration.slot);
    const lastIndexedSlot = finiteNumber(accountResult.lastIndexedSlot);
    if (
      source === 'helius_das_getTokenAccounts' &&
      migrationSlot != null &&
      lastIndexedSlot != null &&
      lastIndexedSlot < migrationSlot
    ) {
      throw new Error(
        `holder index behind migration by ${migrationSlot - lastIndexedSlot} slot(s)`,
      );
    }

    if (!accountResult.accounts.length) throw new Error('holder accounts unavailable');
    const excludedTokenAccounts = new Set([
      migration.poolBaseVault,
      migration.poolQuoteVault,
      ...(migration.migrationTokenAccounts || []),
    ].filter(Boolean));
    const excludedOwners = new Set([
      migration.poolAddress,
      migration.migrationPoolAuthority,
    ].filter(Boolean));

    const byOwner = new Map();
    let excludedPoolAmount = 0;
    for (const account of accountResult.accounts) {
      const amount = positiveNumber(account.amountUi) || 0;
      if (!(amount > 0)) continue;
      const excluded = excludedTokenAccounts.has(account.address) || excludedOwners.has(account.owner);
      if (excluded) {
        excludedPoolAmount += amount;
        continue;
      }
      const owner = account.owner || `unresolved:${account.address}`;
      const current = byOwner.get(owner) || { owner, amountUi: 0, tokenAccounts: [] };
      current.amountUi += amount;
      current.tokenAccounts.push(account.address);
      byOwner.set(owner, current);
    }

    const holders = [...byOwner.values()]
      .sort((a, b) => b.amountUi - a.amountUi)
      .map((holder) => ({
        ...holder,
        pctSupply: supplyUi > 0 ? (holder.amountUi / supplyUi) * 100 : null,
      }));
    const ownerClassifications = await this._classifyOwners(holders.slice(0, 20), migration);
    for (const holder of holders) {
      const classification = ownerClassifications.get(holder.owner);
      holder.ownerType = classification?.ownerType || 'not_classified';
      holder.ownerProgram = classification?.ownerProgram || null;
      holder.ownerExecutable = classification?.ownerExecutable ?? null;
      holder.exclusionReason = null;
    }
    const sumPct = (count) => holders.slice(0, count)
      .reduce((sum, holder) => sum + (Number(holder.pctSupply) || 0), 0);
    const capturedAt = Date.now();
    const migrationTime = finiteNumber(migration.migrationTime) || capturedAt;

    return {
      mint,
      symbol: token.symbol || null,
      migrationSignature: migration.signature || `${mint}:${migration.slot || 0}`,
      migrationSlot: migration.slot || null,
      migrationTime,
      capturedAt,
      captureDelayMs: Math.max(0, capturedAt - migrationTime),
      source,
      isComplete: accountResult.isComplete,
      pageCount: accountResult.pageCount,
      holderCount: holders.length,
      tokenAccountCount: accountResult.accounts.length,
      supplyUi,
      excludedPoolAmount,
      top1Pct: sumPct(1),
      top5Pct: sumPct(5),
      top10Pct: sumPct(10),
      top20Pct: sumPct(20),
      largestHolderOwner: holders[0]?.owner || null,
      largestHolderPct: holders[0]?.pctSupply ?? null,
      holders: {
        top: holders.slice(0, 20),
        excludedTokenAccounts: [...excludedTokenAccounts],
        excludedOwners: [...excludedOwners],
        rawAccountTotal: accountResult.total,
        lastIndexedSlot,
        ownerClassificationComplete: holders.slice(0, 20).every(
          (holder) => holder.ownerType !== 'not_classified',
        ),
        migrationToIndexSlotDelta:
          migrationSlot != null && lastIndexedSlot != null
            ? lastIndexedSlot - migrationSlot
            : null,
        note: accountResult.error || null,
      },
      error: accountResult.error || null,
    };
  }

  async _fetchDasTokenAccounts(mint, decimals) {
    const accounts = [];
    let total = null;
    let pageCount = 0;
    let isComplete = false;
    let lastIndexedSlot = null;
    for (let page = 1; page <= this.maxPages; page++) {
      const result = await this.rpcRequest('getTokenAccounts', {
        mint,
        limit: this.pageSize,
        page,
      });
      const rows = result?.token_accounts || result?.tokenAccounts || result?.items || [];
      if (!Array.isArray(rows)) throw new Error('invalid getTokenAccounts response');
      pageCount = page;
      total = finiteNumber(result?.total) ?? total;
      lastIndexedSlot = finiteNumber(result?.last_indexed_slot) ?? lastIndexedSlot;
      for (const row of rows) {
        accounts.push({
          address: row.address || row.token_account || row.tokenAccount || null,
          owner: row.owner || null,
          amountUi: normalizeAmount(row, decimals),
        });
      }
      if (rows.length < this.pageSize || (total != null && accounts.length >= total)) {
        isComplete = true;
        break;
      }
    }
    return { accounts, total, pageCount, isComplete, lastIndexedSlot };
  }

  async _fetchLargestAccounts(mint, decimals) {
    const largest = await this.rpcRequest('getTokenLargestAccounts', [
      mint,
      { commitment: 'confirmed' },
    ]);
    const rows = largest?.value || largest || [];
    if (!Array.isArray(rows) || rows.length === 0) {
      return { accounts: [], total: null, pageCount: 1, isComplete: false };
    }
    const addresses = rows.map((row) => row.address).filter(Boolean);
    const multiple = addresses.length > 0
      ? await this.rpcRequest('getMultipleAccounts', [addresses, {
          encoding: 'jsonParsed',
          commitment: 'confirmed',
        }])
      : null;
    const values = multiple?.value || [];
    return {
      accounts: rows.map((row, index) => ({
        address: row.address || null,
        owner: values[index]?.data?.parsed?.info?.owner || null,
        amountUi: normalizeAmount(row, decimals),
      })),
      total: null,
      pageCount: 1,
      isComplete: false,
      lastIndexedSlot: finiteNumber(largest?.context?.slot),
    };
  }

  async _classifyOwners(holders, migration) {
    const owners = [...new Set(holders.map((holder) => holder.owner).filter(Boolean))];
    const result = new Map();
    if (owners.length === 0) return result;
    let accounts = [];
    try {
      const response = await this.rpcRequest('getMultipleAccounts', [owners, {
        encoding: 'jsonParsed',
        commitment: 'confirmed',
      }]);
      accounts = response?.value || [];
    } catch (_) {
      return result;
    }
    const migrationOwners = new Set([
      migration.migrationUser,
      migration.developer,
      migration.devAddress,
    ].filter(Boolean));
    const pumpPrograms = new Set([PUMP_PROGRAM_ID, PUMP_AMM_PROGRAM_ID]);
    owners.forEach((owner, index) => {
      const account = accounts[index] || null;
      const ownerProgram = account?.owner || null;
      let ownerType = 'unresolved_or_pda';
      if (migrationOwners.has(owner)) ownerType = 'migration_user';
      else if (account?.executable) ownerType = 'executable_program';
      else if (ownerProgram === SYSTEM_PROGRAM_ID) ownerType = 'wallet';
      else if (pumpPrograms.has(ownerProgram)) ownerType = 'pump_program_account';
      else if (ownerProgram) ownerType = 'program_owned_account';
      result.set(owner, {
        ownerType,
        ownerProgram,
        ownerExecutable: account == null ? null : !!account.executable,
      });
    });
    return result;
  }

  async _rpcRequest(method, params) {
    if (!this.rpcUrl) throw new Error('HELIUS_RPC_URL unavailable');
    const axios = require('axios');
    const { data } = await axios.post(this.rpcUrl, {
      jsonrpc: '2.0',
      id: `${MODULE}-${Date.now()}`,
      method,
      params,
    }, { timeout: 10_000 });
    if (data?.error) throw new Error(`${method}: ${JSON.stringify(data.error)}`);
    return data?.result;
  }

  _persistFailure({ token = {}, migration = {} }, err) {
    const capturedAt = Date.now();
    this.tradeLogger?.logMigrationHolderSnapshot({
      mint: migration.mint || token.mint,
      symbol: token.symbol || null,
      migrationSignature: migration.signature || `${migration.mint || token.mint}:${migration.slot || 0}`,
      migrationSlot: migration.slot || null,
      migrationTime: migration.migrationTime || capturedAt,
      capturedAt,
      captureDelayMs: Math.max(0, capturedAt - (migration.migrationTime || capturedAt)),
      source: 'unavailable',
      isComplete: false,
      holders: null,
      error: err?.message || String(err),
    });
  }
}

module.exports = MigrationHolderSnapshotCollector;
