'use strict';

/**
 * PoolStateCache (v3.17.22 — 合并 RPC 调用 + 缓存全局配置)
 * =====================
 * 后台预取 pool state，砸盘瞬间 BUY 直接读内存。
 *
 * v3.17.22 改造（credits 砍 ~67%）：
 *   - 第一刀：缓存 GLOBAL_CONFIG_PDA + FEE_CONFIG_PDA（启动查一次，永不再查）
 *   - 第二刀：合并 getMultipleAccountsInfo 调用
 *     旧版 swapSolanaState = 3 次 getMultipleAccounts（3+4+2=9 账户, 3 RPC calls）
 *     新版 = 2 次 getMultipleAccounts（1+7 账户, 2 RPC calls）
 *     第1次：poolKey（1 账户，拿到 baseMint/quoteMint 等偏移量）
 *     第2次：baseMint + quoteMint + poolBase + poolQuote + userBase + userQuote（6 账户）
 *     3 次 → 2 次，getMultipleAccounts RPS 从 30 降到 20
 *   - 第三刀：分级刷新频率
 *     持仓币 500ms（止损不能慢）
 *     信号币 2000ms（只是预加载，不需要那么快）
 *
 * hotMints 生命周期：
 *   1. dumpSignal 触发 → addHot(mint, isPosition=false)
 *   2. 买入持仓（registerOpen）→ addHot(mint, isPosition=true) 或 markPosition(mint)
 *   3. 平仓 → removeHot(mint)（有延迟，等 trailing 窗口关闭）
 *
 * 暴露 API：
 *   - start()
 *   - stop()
 *   - get(poolAddress) → state | null
 *   - getAge(poolAddress) → ms 或 null
 *   - applySwapBalances(poolAddress, balances) → boolean
 *   - addHot(mint, poolAddress, isPosition)
 *   - markPosition(mint)
 *   - removeHot(mint)
 *   - refreshOne(poolAddress)
 */

const { PublicKey } = require('@solana/web3.js');
const BN = require('bn.js');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
monitor.registerModule('PoolStateCache', { staleMs: 30_000, label: 'Pool State Cache' });

class PoolStateCache {
  /**
   * @param {object} opts
   * @param {object} opts.onlineSdk - 已初始化的 OnlinePumpAmmSdk
   * @param {PublicKey} opts.user - 钱包公钥（swapSolanaState 需要）
   * @param {function} opts.getMintList - 返回 [{mint, poolAddress}] 的函数
   * @param {number} [opts.refreshIntervalMs=1000]
   */
  constructor({ onlineSdk, user, getMintList, refreshIntervalMs }) {
    this.onlineSdk = onlineSdk;
    this.user = user;
    this.getMintList = getMintList;

    // 第三刀：分级刷新间隔
    // 持仓币高频（默认 500ms），信号币低频（默认 2000ms）
    this.positionRefreshMs = parseInt(
      process.env.POOL_STATE_POSITION_REFRESH_MS || '500', 10,
    );
    this.signalRefreshMs = parseInt(
      process.env.POOL_STATE_SIGNAL_REFRESH_MS || '2000', 10,
    );
    // 兼容旧 env 变量
    if (process.env.POOL_STATE_REFRESH_MS) {
      this.positionRefreshMs = parseInt(process.env.POOL_STATE_REFRESH_MS, 10);
    }

    // tick 间隔（默认 200ms）
    this._tickIntervalMs = parseInt(process.env.POOL_STATE_TICK_MS || '200', 10);
    this.watchedRefreshMs = Math.max(
      this._tickIntervalMs,
      parseInt(process.env.POOL_STATE_WATCHED_REFRESH_MS || '60000', 10),
    );
    this.watchedBatchSize = Math.max(
      0,
      parseInt(process.env.POOL_STATE_WATCHED_BATCH_SIZE || '0', 10),
    );

    this.cache = new Map();   // poolAddress(string) → { state, fetchedAt }
    this.timer = null;
    this._refreshing = false;

    // hotMints — 只有这些币才高频轮询
    // key: mint, value: { poolAddress, addedAt, isPosition }
    this.hotMints = new Map();

    // v3.32: 已迁移/死亡 pool 集合（IncorrectProgramId 等原因）
    this.deadPools = new Set();  // poolAddress string

    // 第一刀：全局配置缓存（启动后查一次，永不再查）
    this._globalConfig = null;
    this._feeConfig = null;
    this._globalConfigFetched = false;

    // 第三刀：分级 cursor
    this._positionCursor = 0;
    this._signalCursor = 0;
    this._watchedCursor = 0;
  }

  /**
   * 启动时预取全局配置（GLOBAL_CONFIG_PDA + FEE_CONFIG_PDA）
   * 这俩几乎永远不变，查一次就够了
   */
  async _prefetchGlobalConfig() {
    if (this._globalConfigFetched) return;
    try {
      const { PUMP_AMM_SDK, GLOBAL_CONFIG_PDA, PUMP_AMM_FEE_CONFIG_PDA } = require('@pump-fun/pump-swap-sdk');
      const connection = this.onlineSdk.connection;
      const [globalConfigAccountInfo, feeConfigAccountInfo] = await connection.getMultipleAccountsInfo([
        GLOBAL_CONFIG_PDA,
        PUMP_AMM_FEE_CONFIG_PDA,
      ]);
      if (globalConfigAccountInfo) {
        this._globalConfig = PUMP_AMM_SDK.decodeGlobalConfig(globalConfigAccountInfo);
      }
      if (feeConfigAccountInfo) {
        this._feeConfig = PUMP_AMM_SDK.decodeFeeConfig(feeConfigAccountInfo);
      }
      this._globalConfigFetched = true;
      console.log(
        `[PoolStateCache] 🏗️ global config cached (globalConfig=${!!this._globalConfig}, feeConfig=${!!this._feeConfig})`,
      );
    } catch (err) {
      console.warn(`[PoolStateCache] ⚠️ global config prefetch failed: ${err.message}`);
      // 失败不阻断，后续 refresh 会 fallback 到 swapSolanaState
    }
  }

  /**
   * v3.17.22: 加入热集合，同时立即刷新一次补足价格
   * @param {string} mint
   * @param {string} poolAddress
   * @param {boolean} [isPosition=false] - true=持仓币(500ms刷新), false=信号币(2000ms刷新)
   */
  addHot(mint, poolAddress, isPosition = false) {
    if (!mint || !poolAddress) return;
    const already = this.hotMints.has(mint);
    const prevInfo = already ? this.hotMints.get(mint) : null;
    // v3.17.27: isPosition 升级时也打印日志（之前 already=true 时静默跳过，导致"持仓没进 hotMints"的误判）
    const isUpgrade = already && isPosition && prevInfo && !prevInfo.isPosition;
    this.hotMints.set(mint, { poolAddress, addedAt: Date.now(), isPosition });
    monitor.set('PoolStateCache.hotMintsSize', this.hotMints.size, 'PoolStateCache');
    if (!already || isUpgrade) {
      const label = isPosition ? '💰' : '🔥';
      const upgradeTag = isUpgrade ? ' (upgraded from signal)' : '';
      console.log(`[PoolStateCache] ${label} addHot: ${mint.slice(0, 6)}.. (hot=${this.hotMints.size}, pos=${isPosition})${upgradeTag}`);
    }
    // 立即刷新一次，确保 BUY 路径能命中 cache（仅首次或升级时）
    if (!already || isUpgrade) {
      this.refreshOne(poolAddress).catch(() => {});
    }
  }

  /**
   * v3.17.22: 标记为持仓币（提高刷新频率）
   */
  markPosition(mint) {
    const info = this.hotMints.get(mint);
    if (info && !info.isPosition) {
      info.isPosition = true;
      console.log(`[PoolStateCache] 💰→pos: ${mint.slice(0, 6)}.. (upgraded to position refresh)`);
    }
  }

  /**
   * 从热集合移除
   * @param {string} mint
   */
  removeHot(mint) {
    if (!mint) return;
    const deleted = this.hotMints.delete(mint);
    if (deleted) {
      monitor.set('PoolStateCache.hotMintsSize', this.hotMints.size, 'PoolStateCache');
      console.log(`[PoolStateCache] ❄️ removeHot: ${mint.slice(0, 6)}.. (hot=${this.hotMints.size})`);
    }
  }

  // v3.32: 标记 pool 为已死亡（迁移到 Raydium 等），不再刷新/买入
  markDead(poolAddress) {
    if (!poolAddress) return;
    this.deadPools.add(poolAddress);
    // 同时从缓存中删除，避免读到过期数据
    this.cache.delete(poolAddress);
    console.log(`[PoolStateCache] 🪦 markDead: ${poolAddress.slice(0, 8)}.. (deadPools=${this.deadPools.size})`);
  }

  // v3.32: 检查 pool 是否已标记死亡
  isDead(poolAddress) {
    return poolAddress && this.deadPools.has(poolAddress);
  }

  async start() {
    if (this.timer) return;
    if (!this.onlineSdk || !this.user) {
      console.warn('[PoolStateCache] not started: missing onlineSdk or user');
      return;
    }

    // 第一刀：预取全局配置
    await this._prefetchGlobalConfig();

    // Prewarm one bounded watched-pool batch before live transactions arrive.
    await this._refreshAll();

    // 滚动刷新只遍历 hotMints
    this.timer = setInterval(() => {
      this._refreshAll().catch((err) => {
        monitor.recordError('PoolStateCache', err, { phase: 'periodic_refresh' });
      });
    }, this._tickIntervalMs);
    console.log(
      `[PoolStateCache] started (pos=${this.positionRefreshMs}ms, signal=${this.signalRefreshMs}ms, watched=${this.watchedRefreshMs}ms, tick=${this._tickIntervalMs}ms, globalConfigCached=${this._globalConfigFetched})`,
    );
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.cache.clear();
    this.hotMints.clear();
  }

  /**
   * 同步取缓存。返回最近一次 state 或 null。
   * @param {string} poolAddress
   * @returns {object | null}
   */
  get(poolAddress) {
    if (!poolAddress) return null;
    const entry = this.cache.get(poolAddress);
    if (!entry) return null;
    const age = Date.now() - entry.fetchedAt;
    monitor.set('PoolStateCache.lastReadAgeMs', age, 'PoolStateCache');
    return entry.state;
  }

  getAge(poolAddress) {
    const entry = this.cache.get(poolAddress);
    return entry ? Date.now() - entry.fetchedAt : null;
  }

  getMarketSlot(poolAddress) {
    const entry = this.cache.get(poolAddress);
    return entry ? Number(entry.marketSlot) || 0 : 0;
  }

  getMarketMeta(poolAddress) {
    const entry = this.cache.get(poolAddress);
    if (!entry) return null;
    return {
      slot: Number(entry.marketSlot) || 0,
      fetchedAt: Number(entry.fetchedAt) || 0,
      requestedAt: Number(entry.requestedAt) || 0,
      source: entry.marketSource || 'unknown',
    };
  }

  advanceMarketSlot(poolAddress, slot) {
    const numericSlot = Number(slot) || 0;
    const entry = this.cache.get(poolAddress);
    if (!entry || numericSlot <= 0) return false;
    entry.marketSlot = Math.max(Number(entry.marketSlot) || 0, numericSlot);
    return true;
  }

  /**
   * Apply the post-swap vault balances already present in the parsed transaction.
   * This keeps an emergency SELL quote on the same pool state as the price tick
   * that triggered it, without waiting for the 500ms background RPC refresh.
   */
  applySwapBalances(poolAddress, {
    poolBaseAfter,
    poolQuoteAfter,
    baseDecimals = 6,
    quoteDecimals = 9,
    slot = 0,
  } = {}) {
    if (!poolAddress) return false;
    const entry = this.cache.get(poolAddress);
    if (!entry?.state) return false;

    const base = Number(poolBaseAfter);
    const quote = Number(poolQuoteAfter);
    const baseScale = 10 ** Number(baseDecimals);
    const quoteScale = 10 ** Number(quoteDecimals);
    const baseRaw = Math.round(base * baseScale);
    const quoteRaw = Math.round(quote * quoteScale);
    if (
      !Number.isFinite(base) || base <= 0 ||
      !Number.isFinite(quote) || quote <= 0 ||
      !Number.isSafeInteger(baseRaw) || baseRaw <= 0 ||
      !Number.isSafeInteger(quoteRaw) || quoteRaw <= 0
    ) return false;

    const numericSlot = Number(slot) || 0;
    if (numericSlot > 0 && entry.marketSlot > numericSlot) return false;

    entry.state.poolBaseAmount = new BN(String(baseRaw));
    entry.state.poolQuoteAmount = new BN(String(quoteRaw));
    const now = Date.now();
    entry.fetchedAt = now;
    entry.requestedAt = now;
    entry.marketSource = 'chain_swap';
    if (numericSlot > 0) entry.marketSlot = numericSlot;
    monitor.inc('PoolStateCache.swapBalanceApplied', 1, 'PoolStateCache');
    return true;
  }

  /**
   * 单点刷新（信号触发 / addHot / BUY 前使用）。
   * 缓存年龄不超过 maxAgeMs 时复用，否则同步读取最新池状态。
   */
  async refreshOne(poolAddress, maxAgeMs = 500) {
    if (!this.onlineSdk || !this.user || !poolAddress) return null;
    const cached = this.cache.get(poolAddress);
    const allowedAgeMs = Number.isFinite(Number(maxAgeMs))
      ? Math.max(0, Number(maxAgeMs))
      : 500;
    if (cached && Date.now() - cached.fetchedAt <= allowedAgeMs) return cached.state;
    try {
      const requestedAt = Date.now();
      const state = await this._fetchPoolState(poolAddress);
      if (state) {
        this.cache.set(poolAddress, {
          state,
          fetchedAt: Date.now(),
          requestedAt,
          marketSlot: Number(cached?.marketSlot) || 0,
          marketSource: 'rpc',
        });
        monitor.inc('PoolStateCache.refreshOneOk', 1, 'PoolStateCache');
      }
      return state || null;
    } catch (err) {
      monitor.inc('PoolStateCache.refreshOneFail', 1, 'PoolStateCache');
      return null;
    }
  }

  /**
   * v3.17.22: 核心 — 合并 RPC 调用，自建 swapSolanaState 等价对象
   *
   * 旧版 swapSolanaState = 3 次 getMultipleAccountsInfo：
   *   1. [GLOBAL_CONFIG_PDA, FEE_CONFIG_PDA, poolKey]           → 3 账户
   *   2. [baseMint, quoteMint, poolBaseToken, poolQuoteToken]   → 4 账户
   *   3. [userBaseTokenAccount, userQuoteTokenAccount]           → 2 账户
   *   共 3 RPC calls
   *
   * 新版 = 2 次 getMultipleAccountsInfo：
   *   1. [poolKey]                                                → 1 账户（拿到 baseMint/quoteMint 等）
   *   2. [baseMint, quoteMint, poolBaseToken, poolQuoteToken,     → 6 账户
   *      userBaseTokenAccount, userQuoteTokenAccount]
   *   + globalConfig/feeConfig 从内存缓存取
   *   共 2 RPC calls（-33%）
   *
   * 为什么不能合并成 1 次？因为 userBaseTokenAccount 和 userQuoteTokenAccount
   * 需要 baseMintInfo.owner（tokenProgram）来派生，而 tokenProgram 在第2次查询才能拿到。
   *
   * 实际做法：先用 cached pool 数据派生 user accounts（大部分 Pump 币都是 TOKEN_PROGRAM_ID），
   * 如果 pool 缓存里有 tokenProgram 信息，就能 1 次 RPC 搞定。
   * 否则 fallback 到 2 次。
   */
  async _fetchPoolState(poolAddressStr) {
    const {
      PUMP_AMM_SDK,
      GLOBAL_CONFIG_PDA,
      PUMP_AMM_FEE_CONFIG_PDA,
    } = require('@pump-fun/pump-swap-sdk');
    const { MintLayout, AccountLayout, getAssociatedTokenAddressSync, TOKEN_PROGRAM_ID } = require('@solana/spl-token');

    const connection = this.onlineSdk.connection;
    const poolKey = new PublicKey(poolAddressStr);
    const user = this.user;

    // 全局配置未缓存则 fallback 到原始 swapSolanaState
    if (!this._globalConfigFetched || !this._globalConfig) {
      return await this.onlineSdk.swapSolanaState(poolKey, user);
    }

    // 尝试 1 次 RPC 路径：如果 cache 里有这个 pool 的旧 state，
    // 可以从中拿到 baseMint/quoteMint/baseTokenProgram/quoteTokenProgram 来派生 user accounts
    const cachedEntry = this.cache.get(poolAddressStr);
    const cachedState = cachedEntry?.state;

    if (cachedSt