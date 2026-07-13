'use strict';

/**
 * TokenRegistry
 * =============
 * SQLite-backed persistence for the set of mints we are watching for dump signals.
 *
 * NOTE: this module is RECONSTRUCTED from call sites since it was missing from
 * the v3.17.13 handoff zip. Behavior is best-effort to match what the rest of
 * the codebase expects:
 *   - tokens table holds one row per mint
 *   - Pool info (pool_address / pool_base_vault / pool_quote_vault) optionally
 *     filled in later by PoolFinder
 *   - addToken() fetches symbol/decimals/FDV from Helius DAS + Birdeye via
 *     tokenMeta helper
 *   - tokenRegistry.db is exposed so TradeLogger can share the same DB handle
 *   - .cache is an in-memory Map for hot lookups; .stmts is the prepared-statement
 *     map TokenWatchdog pokes directly to refresh FDV
 */

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');
const { PublicKey } = require('@solana/web3.js');
const { config } = require('../config');
const { fetchTokenFullInfo, fetchTokenCreationTime } = require('../utils/tokenMeta');

class TokenRegistry {
  constructor(dbPath = config.storage.dbPath) {
    // Ensure parent directory exists
    const dir = path.dirname(dbPath);
    if (dir && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    this._initSchema();
    this._prepareStatements();

    // Hot in-memory cache: mint → row
    this.cache = new Map();
    this._reloadCache();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tokens (
        mint TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        decimals INTEGER DEFAULT 6,
        fdv REAL,
        market_cap REAL,
        liquidity REAL,
        price REAL,
        pool_address TEXT,
        pool_base_vault TEXT,
        pool_quote_vault TEXT,
        is_active INTEGER NOT NULL DEFAULT 1,
        source TEXT,
        added_at INTEGER,
        updated_at INTEGER,
        meta_json TEXT,
        creation_time INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_tokens_active ON tokens (is_active);
    `);
  }

  _prepareStatements() {
    this.stmts = {
      insert: this.db.prepare(`
        INSERT INTO tokens
          (mint, symbol, name, decimals, fdv, market_cap, liquidity, price,
           pool_address, pool_base_vault, pool_quote_vault,
           is_active, source, added_at, updated_at, meta_json, creation_time)
        VALUES (@mint, @symbol, @name, @decimals, @fdv, @market_cap, @liquidity, @price,
                @pool_address, @pool_base_vault, @pool_quote_vault,
                1, @source, @added_at, @updated_at, @meta_json, @creation_time)
        ON CONFLICT(mint) DO UPDATE SET
          symbol = COALESCE(excluded.symbol, tokens.symbol),
          name = COALESCE(excluded.name, tokens.name),
          decimals = COALESCE(excluded.decimals, tokens.decimals),
          fdv = COALESCE(excluded.fdv, tokens.fdv),
          market_cap = COALESCE(excluded.market_cap, tokens.market_cap),
          liquidity = COALESCE(excluded.liquidity, tokens.liquidity),
          price = COALESCE(excluded.price, tokens.price),
          is_active = 1,
          updated_at = excluded.updated_at,
          meta_json = COALESCE(excluded.meta_json, tokens.meta_json),
          creation_time = COALESCE(excluded.creation_time, tokens.creation_time)
      `),

      // Used by TokenWatchdog._check via tokenRegistry.stmts.update.run(...)
      // Note: TokenWatchdog passes positional args, NOT named, so the placeholders
      // here MUST stay positional (?) in the SAME order it expects.
      update: this.db.prepare(`
        UPDATE tokens SET
          symbol = ?,
          name = ?,
          decimals = ?,
          fdv = ?,
          market_cap = ?,
          liquidity = ?,
          price = ?,
          updated_at = ?,
          meta_json = ?
        WHERE mint = ?
      `),

      get: this.db.prepare('SELECT * FROM tokens WHERE mint = ?'),
      getActive: this.db.prepare('SELECT * FROM tokens WHERE is_active = 1 ORDER BY added_at DESC'),
      getAll: this.db.prepare('SELECT * FROM tokens ORDER BY is_active DESC, added_at DESC'),
      remove: this.db.prepare('UPDATE tokens SET is_active = 0, updated_at = ? WHERE mint = ?'),
      hardRemove: this.db.prepare('DELETE FROM tokens WHERE mint = ?'),
      setPool: this.db.prepare(`
        UPDATE tokens SET
          pool_address = ?,
          pool_base_vault = ?,
          pool_quote_vault = ?,
          updated_at = ?
        WHERE mint = ?
      `),

      // v3.35: 移除 creation_time 超过 maxAgeMs 的活跃代币
      removeStaleByAge: this.db.prepare(`
        UPDATE tokens SET is_active = 0, updated_at = ?
        WHERE is_active = 1 AND creation_time IS NOT NULL AND creation_time < ?
      `),

      // v3.17.20: 查询某个 pool_address 是否已被另一个活跃 mint 占用（防签名串，图8）
      //   返回占用它的 mint（排除自己），用于拒绝重复 pool_address 写入。
      getPoolOwner: this.db.prepare(`
        SELECT mint, symbol FROM tokens
        WHERE pool_address = ? AND mint != ? AND is_active = 1
        LIMIT 1
      `),
    };
  }

  _reloadCache() {
    this.cache.clear();
    const rows = this.stmts.getActive.all();
    for (const row of rows) this.cache.set(row.mint, row);
  }

  /** Static validator used by server.js endpoints */
  static validateMint(mint) {
    if (typeof mint !== 'string' || mint.length < 32 || mint.length > 44) {
      throw new Error(`invalid mint string: ${mint}`);
    }
    try {
      // throws if not valid base58 pubkey
      new PublicKey(mint);
    } catch (err) {
      throw new Error(`invalid mint pubkey: ${mint}`);
    }
    return true;
  }

  /**
   * Add (or re-activate) a token. Fetches metadata via tokenMeta helper.
   * @param {string} mint
   * @param {{symbol?:string, source?:string}} opts
   * @returns {Promise<object>} the token row
   */
  async addToken(mint, opts = {}) {
    TokenRegistry.validateMint(mint);

    // Try to enrich with full meta; tolerate failures so user can still add a token
    let meta = null;
    try {
      meta = await fetchTokenFullInfo(mint);
    } catch (err) {
      console.warn(`[TokenRegistry] meta fetch failed for ${mint}: ${err.message}`);
    }

    const now = Date.now();
    const row = {
      mint,
      symbol: opts.symbol || meta?.symbol || null,
      name: meta?.name || null,
      decimals: meta?.decimals ?? 6,
      fdv: meta?.fdv ?? null,
      market_cap: meta?.marketCap ?? null,
      liquidity: meta?.liquidity ?? null,
      price: meta?.price ?? null,
      pool_address: null,
      pool_base_vault: null,
      pool_quote_vault: null,
      source: opts.source || 'manual',
      added_at: now,
      updated_at: now,
      meta_json: meta ? JSON.stringify({ ...meta, _birdeyeError: undefined }) : null,
      creation_time: null,  // filled below
    };

    // v3.19: 获取代币创建时间（Birdeye token_security）
    try {
      const creationInfo = await fetchTokenCreationTime(mint);
      if (creationInfo?.creationTime) {
        row.creation_time = creationInfo.creationTime * 1000; // unix seconds → ms
      }
    } catch (err) {
      // non-critical, skip
    }

    // Preserve existing pool info on re-add
    const existing = this.stmts.get.get(mint);
    if (existing) {
      row.pool_address = existing.pool_address || null;
      row.pool_base_vault = existing.pool_base_vault || null;
      row.pool_quote_vault = existing.pool_quote_vault || null;
      // keep original added_at
      row.added_at = existing.added_at || now;
    }

    this.stmts.insert.run(row);
    const fresh = this.stmts.get.get(mint);
    if (fresh) this.cache.set(mint, fresh);
    return fresh;
  }

  /**
   * Set pool info after PoolFinder resolves it
   * @param {string} mint
   * @param {{poolAddress:string, poolBaseVault?:string, poolQuoteVault?:string}} info
   *
   * v3.17.20: 防签名串(图8) — 拒绝把一个已经被另一个活跃 mint 占用的 pool_address
   *   写到当前 mint 上。同一个 pool_address 对应同一个 base_mint，两个不同代币
   *   共享同一个池子地址必然是数据错误，会导致 BUY 用错 mint 构造交易、签名对不上。
   */
  setPoolInfo(mint, info) {
    if (!info || !info.poolAddress) return;

    const conflict = this.stmts.getPoolOwner.get(info.poolAddress, mint);
    if (conflict) {
      console.error(
        `[TokenRegistry] 🚫 pool_address conflict: ${info.poolAddress.slice(0, 8)}.. ` +
          `already owned by mint ${conflict.mint.slice(0, 8)}.. (${conflict.symbol || '?'}); ` +
          `refusing to assign it to ${mint.slice(0, 8)}.. — likely a PoolFinder mis-resolve.`,
      );
      return;
    }

    this.stmts.setPool.run(
      info.poolAddress,
      info.poolBaseVault || null,
      info.poolQuoteVault || null,
      Date.now(),
      mint,
    );
    const fresh = this.stmts.get.get(mint);
    if (fresh) this.cache.set(mint, fresh);
  }

  /**
   * Soft-remove: mark inactive (so dump signals stop firing) but keep history
   */
  removeToken(mint) {
    this.stmts.remove.run(Date.now(), mint);
    this.cache.delete(mint);
  }

  getToken(mint) {
    return this.cache.get(mint) || this.stmts.get.get(mint) || null;
  }

  isActive(mint) {
    const row = this.cache.get(mint);
    return !!(row && row.is_active);
  }

  listActive() {
    return Array.from(this.cache.values());
  }

  listAll() {
    return this.stmts.getAll.all();
  }

  /**
   * v3.35: 移除创建时间超过 maxAgeMs 的活跃代币。
   * 返回被移除的数量。
   */
  removeStaleByAge(maxAgeMs = 86400000) {
    const cutoff = Date.now() - maxAgeMs;
    const info = this.stmts.removeStaleByAge.run(Date.now(), cutoff);
    const removed = info.changes;
    if (removed > 0) {
      // 清除缓存中对应的条目
      for (const [mint, token] of this.cache) {
        if (token.creation_time && token.creation_time < cutoff) {
          this.cache.delete(mint);
        }
      }
      console.log(`[TokenRegistry] 🕐 Removed ${removed} tokens older than ${Math.round(maxAgeMs / 3600000)}h`);
    }
    return removed;
  }

  getActiveMintSet() {
    return new Set(this.cache.keys());
  }
}

module.exports = TokenRegistry;
