'use strict';

const EventEmitter = require('events');
const axios = require('axios');
const WebSocket = require('ws');
const { config } = require('../config');
const {
  fetchTokenAssetFromHelius,
  fetchTokenMarketFromBirdeye,
} = require('../utils/tokenMeta');
const { parsePumpMigrationTransaction } = require('../utils/pumpMigrationParser');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
const MODULE = 'PumpGraduationDiscovery';
const DEDUP_TTL_MS = 6 * 60 * 60_000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function finiteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toMilliseconds(unixTime) {
  const value = finiteNumber(unixTime);
  if (!value || value <= 0) return null;
  return value > 10_000_000_000 ? Math.trunc(value) : Math.trunc(value * 1000);
}

function buildWebSocketUrl(explicitUrl, rpcUrl, apiKey) {
  const source = explicitUrl || rpcUrl ||
    (apiKey ? `https://mainnet.helius-rpc.com/?api-key=${apiKey}` : null);
  if (!source) return null;
  const url = new URL(source);
  if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol === 'http:') url.protocol = 'ws:';
  return url.toString();
}

class PumpGraduationDiscovery extends EventEmitter {
  constructor(opts = {}) {
    super();
    this.tokenRegistry = opts.tokenRegistry;
    this.onBeforeAdd = opts.onBeforeAdd || null;
    this.onMigrationDetected = opts.onMigrationDetected || null;
    this.onTokenAdded = opts.onTokenAdded || null;
    this.settings = { ...config.pumpDiscovery, ...(opts.settings || {}) };
    this.rpcUrl = opts.rpcUrl || config.helius.rpcUrl;
    this.wsUrl = buildWebSocketUrl(
      this.settings.wsUrl,
      this.rpcUrl,
      config.helius.apiKey,
    );
    this.migrationWallet = opts.migrationWallet || config.programs.pumpMigrationWallet;
    this.fetchMarket = opts.fetchMarket || fetchTokenMarketFromBirdeye;
    this.fetchAsset = opts.fetchAsset || fetchTokenAssetFromHelius;
    this.rpcRequest = opts.rpcRequest || this._rpcRequest.bind(this);

    this.running = false;
    this.ws = null;
    this.wsSubscriptionId = null;
    this.reconnectTimer = null;
    this.pollTimer = null;
    this.pingTimer = null;
    this.cleanupTimer = null;
    this.polling = false;
    this.startupCutoffMs = null;
    this.reconnectAttempt = 0;
    this.rpcId = 1;
    this.seenSignatures = new Map();
    this.processingSignatures = new Set();
    this.seenMints = new Map();
    this.candidateQueue = [];
    this.queuedMints = new Set();
    this.activeChecks = 0;
  }

  start() {
    if (!this.settings.enabled) {
      console.log('[PumpDiscovery] disabled');
      return;
    }
    if (this.running) return;
    if (!this.tokenRegistry) throw new Error('PumpGraduationDiscovery requires tokenRegistry');
    if (!this.rpcUrl || !this.wsUrl) throw new Error('PumpGraduationDiscovery requires HELIUS_RPC_URL');

    this.running = true;
    this.startupCutoffMs = Date.now() - Math.max(0, this.settings.startupLookbackSec) * 1000;
    monitor.registerModule(MODULE, {
      staleMs: Math.max(30_000, this.settings.pollIntervalMs * 4),
      label: 'Pump.fun Graduation Discovery',
    });
    console.log(
      `[PumpDiscovery] enabled: FDV $${this.settings.minFdvUsd}-${this.settings.maxFdvUsd}, ` +
        `liquidity >= $${this.settings.minLiquidityUsd}, market-only screening`,
    );

    this._connectWebSocket();
    this._poll().catch((err) => this._recordError(err, 'initial_poll'));
    this.pollTimer = setInterval(() => {
      this._poll().catch((err) => this._recordError(err, 'poll'));
    }, Math.max(1000, this.settings.pollIntervalMs));
    this.cleanupTimer = setInterval(() => this._cleanupDedup(), 10 * 60_000);
  }

  stop() {
    this.running = false;
    clearTimeout(this.reconnectTimer);
    clearInterval(this.pollTimer);
    clearInterval(this.pingTimer);
    clearInterval(this.cleanupTimer);
    this.reconnectTimer = null;
    this.pollTimer = null;
    this.pingTimer = null;
    this.cleanupTimer = null;
    if (this.ws) {
      try { this.ws.removeAllListeners(); } catch (_) {}
      try { this.ws.close(); } catch (_) {}
      this.ws = null;
    }
  }

  async _rpcRequest(method, params) {
    const { data } = await axios.post(this.rpcUrl, {
      jsonrpc: '2.0',
      id: this.rpcId++,
      method,
      params,
    }, { timeout: 10_000 });
    if (data?.error) throw new Error(`${method}: ${JSON.stringify(data.error)}`);
    return data?.result ?? null;
  }

  _connectWebSocket() {
    if (!this.running) return;
    clearTimeout(this.reconnectTimer);

    const ws = new WebSocket(this.wsUrl, { handshakeTimeout: 10_000 });
    this.ws = ws;
    ws.on('open', () => {
      this.reconnectAttempt = 0;
      monitor.beat(MODULE, 'websocket:connected');
      ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'logsSubscribe',
        params: [
          // The official migrate instruction always includes withdraw_authority.
          // This avoids streaming every ordinary Pump bonding-curve trade.
          { mentions: [this.migrationWallet] },
          { commitment: 'confirmed' },
        ],
      }));
      clearInterval(this.pingTimer);
      this.pingTimer = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          try { ws.ping(); } catch (_) {}
        }
      }, 20_000);
    });
    ws.on('message', (raw) => this._handleWebSocketMessage(raw));
    ws.on('error', (err) => {
      this._recordError(err, 'websocket');
      try { ws.terminate(); } catch (_) {}
    });
    ws.on('close', () => {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
      if (this.ws === ws) this.ws = null;
      this._scheduleReconnect();
    });
  }

  _scheduleReconnect() {
    if (!this.running || this.reconnectTimer) return;
    const delay = Math.min(30_000, 1000 * (2 ** Math.min(this.reconnectAttempt++, 5)));
    monitor.inc(`${MODULE}.wsReconnects`, 1, MODULE);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this._connectWebSocket();
    }, delay);
  }

  _handleWebSocketMessage(raw) {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }
    if (message.id === 1 && message.error) {
      this._recordError(new Error(`logsSubscribe: ${JSON.stringify(message.error)}`), 'subscribe');
      try { this.ws?.close(); } catch (_) {}
      return;
    }
    if (message.id === 1 && message.result != null) {
      this.wsSubscriptionId = message.result;
      monitor.beat(MODULE, `websocket:subscribed:${message.result}`);
      return;
    }

    const value = message?.params?.result?.value;
    if (!value?.signature || value.err || !this._hasMigrationHint(value.logs)) return;
    monitor.inc(`${MODULE}.migrationHints`, 1, MODULE);
    this._processSignature(value.signature, 'websocket').catch((err) => {
      this._recordError(err, 'websocket_signature');
    });
  }

  _hasMigrationHint(logs) {
    if (!Array.isArray(logs)) return false;
    return logs.some((line) =>
      /Instruction:\s*Migrate(?:V2)?\b/i.test(line) ||
      /MigrateFunds|CreatePool|InitializePool/i.test(line),
    );
  }

  async _poll() {
    if (!this.running || this.polling) return;
    this.polling = true;
    try {
      const rows = await this.rpcRequest('getSignaturesForAddress', [
        this.migrationWallet,
        { limit: Math.min(1000, Math.max(1, this.settings.pollLimit)) },
      ]);
      if (!Array.isArray(rows)) throw new Error('getSignaturesForAddress returned no rows');

      monitor.beat(MODULE, `poll:${rows.length}`);
      for (const row of rows.slice().reverse()) {
        if (!row?.signature || this.seenSignatures.has(row.signature)) continue;
        const blockTimeMs = toMilliseconds(row.blockTime);
        if (blockTimeMs && blockTimeMs < this.startupCutoffMs) {
          this._markSignatureSeen(row.signature);
          continue;
        }
        await this._processSignature(row.signature, 'poll');
      }
    } finally {
      this.polling = false;
    }
  }

  async _processSignature(signature, detectionPath) {
    if (!signature || this.seenSignatures.has(signature) || this.processingSignatures.has(signature)) return;
    this.processingSignatures.add(signature);
    try {
      const transaction = await this._fetchTransaction(signature);
      if (!transaction) return;

      const migration = parsePumpMigrationTransaction(transaction, { signature, detectionPath });
      this._markSignatureSeen(signature);
      if (!migration) return;

      if (migration.migrationTimeSource !== 'blockTime' && migration.slot) {
        try {
          const blockTime = finiteNumber(await this.rpcRequest('getBlockTime', [migration.slot]));
          if (blockTime && blockTime > 0) {
            migration.migrationTime = Math.trunc(blockTime * 1000);
            migration.migrationTimeSource = 'getBlockTime';
          }
        } catch (_) {
          // observedAt remains a last-resort timestamp.
        }
      }

      monitor.inc(`${MODULE}.migrationsConfirmed`, 1, MODULE);
      this._enqueueCandidate(migration);
    } finally {
      this.processingSignatures.delete(signature);
    }
  }

  async _fetchTransaction(signature) {
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        const transaction = await this.rpcRequest('getTransaction', [signature, {
          encoding: 'jsonParsed',
          commitment: 'confirmed',
          maxSupportedTransactionVersion: 0,
        }]);
        if (transaction) return transaction;
      } catch (err) {
        if (attempt === 3) throw err;
      }
      await sleep(250 * (attempt + 1));
    }
    return null;
  }

  _enqueueCandidate(migration) {
    if (this.onMigrationDetected) {
      try {
        this.onMigrationDetected(migration);
      } catch (err) {
        console.warn(`[PumpDiscovery] migration callback failed: ${err.message}`);
      }
    }
    if (this.seenMints.has(migration.mint) || this.queuedMints.has(migration.mint)) return;
    this.queuedMints.add(migration.mint);
    this.candidateQueue.push(migration);
    this._drainQueue();
  }

  _drainQueue() {
    const maxConcurrent = Math.max(1, this.settings.maxConcurrentChecks);
    while (this.running && this.activeChecks < maxConcurrent && this.candidateQueue.length > 0) {
      const migration = this.candidateQueue.shift();
      this.activeChecks++;
      this._screenAndAdd(migration)
        .catch((err) => this._recordError(err, 'candidate'))
        .finally(() => {
          this.activeChecks--;
          this.queuedMints.delete(migration.mint);
          this.seenMints.set(migration.mint, Date.now());
          this._drainQueue();
        });
    }
  }

  async _screenAndAdd(migration) {
    const existing = this.tokenRegistry.getToken(migration.mint);
    if (existing?.is_active) {
      this.tokenRegistry.recordMigration(migration.mint, migration);
      if (migration.poolAddress) this.tokenRegistry.setPoolInfo(migration.mint, migration);
      monitor.inc(`${MODULE}.alreadyWatched`, 1, MODULE);
      console.log(
        `[PumpDiscovery] confirmed existing ${existing.symbol || migration.mint.slice(0, 8)} ` +
          `migration at slot ${migration.slot}`,
      );
      return;
    }

    if (this.settings.marketInitialDelayMs > 0) await sleep(this.settings.marketInitialDelayMs);
    const screening = await this._fetchScreeningData(migration.mint);
    const rejection = this._getRejection(screening);
    if (rejection) {
      monitor.inc(`${MODULE}.rejected`, 1, MODULE);
      monitor.inc(`${MODULE}.rejected.${rejection.code}`, 1, MODULE);
      console.log(`[PumpDiscovery] reject ${migration.mint.slice(0, 8)}..: ${rejection.message}`);
      this.emit('rejected', { migration, screening, rejection });
      return;
    }

    let asset = {};
    try {
      asset = await this.fetchAsset(migration.mint);
    } catch (err) {
      console.warn(`[PumpDiscovery] metadata unavailable for ${migration.mint.slice(0, 8)}..: ${err.message}`);
    }

    const evicted = this.onBeforeAdd ? await this.onBeforeAdd(migration.mint) : [];
    const token = await this.tokenRegistry.addToken(migration.mint, {
      source: 'pump_graduation',
      meta: { ...asset, ...screening.market, fetchedAt: Date.now() },
      fetchCreationTime: false,
      poolAddress: migration.poolAddress,
      poolBaseVault: migration.poolBaseVault,
      poolQuoteVault: migration.poolQuoteVault,
      migrationTime: migration.migrationTime,
      migrationTimeSource: migration.migrationTimeSource,
      migrationSlot: migration.slot,
      migrationSignature: migration.signature,
    });

    monitor.inc(`${MODULE}.tokensAdded`, 1, MODULE);
    console.log(
      `[PumpDiscovery] added ${token?.symbol || migration.mint.slice(0, 8)} ` +
        `FDV=$${Math.round(screening.market.fdv)} LP=$${Math.round(screening.market.liquidity)} ` +
        `slot=${migration.slot} via=${migration.detectionPath}`,
    );
    const event = { token, migration, screening, evicted: evicted || [] };
    if (this.onTokenAdded) await this.onTokenAdded(event);
    this.emit('tokenAdded', event);
  }

  async _fetchScreeningData(mint) {
    let lastMarketError = null;
    const attempts = Math.max(1, this.settings.marketRetries);

    for (let attempt = 1; attempt <= attempts; attempt++) {
      let market = null;
      try {
        market = await this.fetchMarket(mint);
        lastMarketError = null;
      } catch (err) {
        lastMarketError = err;
      }

      const hasMarket = finiteNumber(market?.fdv) > 0 && finiteNumber(market?.liquidity) > 0;
      if (hasMarket) return { market };
      if (attempt < attempts) await sleep(Math.max(250, this.settings.marketRetryMs));
    }

    const details = lastMarketError?.message || '';
    throw new Error(`screening data unavailable for ${mint}${details ? `: ${details}` : ''}`);
  }

  _getRejection({ market }) {
    const fdv = finiteNumber(market?.fdv);
    const liquidity = finiteNumber(market?.liquidity);
    if (fdv == null) return { code: 'fdv_missing', message: 'FDV unavailable' };
    if (fdv < this.settings.minFdvUsd) {
      return { code: 'fdv_low', message: `FDV $${Math.round(fdv)} < $${this.settings.minFdvUsd}` };
    }
    if (this.settings.maxFdvUsd > 0 && fdv > this.settings.maxFdvUsd) {
      return { code: 'fdv_high', message: `FDV $${Math.round(fdv)} > $${this.settings.maxFdvUsd}` };
    }
    if (liquidity == null || liquidity < this.settings.minLiquidityUsd) {
      return {
        code: 'liquidity_low',
        message: `liquidity $${Math.round(liquidity || 0)} < $${this.settings.minLiquidityUsd}`,
      };
    }
    return null;
  }

  _markSignatureSeen(signature) {
    this.seenSignatures.set(signature, Date.now());
  }

  _cleanupDedup() {
    const cutoff = Date.now() - DEDUP_TTL_MS;
    for (const [signature, ts] of this.seenSignatures) {
      if (ts < cutoff) this.seenSignatures.delete(signature);
    }
    for (const [mint, ts] of this.seenMints) {
      if (ts < cutoff) this.seenMints.delete(mint);
    }
  }

  _recordError(err, phase) {
    monitor.recordError(MODULE, err, { phase });
    monitor.inc(`${MODULE}.failures`, 1, MODULE);
    console.warn(`[PumpDiscovery] ${phase} failed: ${err.message}`);
  }
}

module.exports = PumpGraduationDiscovery;
