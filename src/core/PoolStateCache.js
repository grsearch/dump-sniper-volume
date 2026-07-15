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

    // 滚动刷新只遍历 hotMints
    this.timer = setInterval(() => {
      this._refreshAll().catch((err) => {
        monitor.recordError('PoolStateCache', err, { phase: 'periodic_refresh' });
      });
    }, this._tickIntervalMs);
    console.log(
      `[PoolStateCache] started (pos=${this.positionRefreshMs}ms, signal=${this.signalRefreshMs}ms, tick=${this._tickIntervalMs}ms, globalConfigCached=${this._globalConfigFetched})`,
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

  /**
   * 单点刷新（dumpSignal 触发 / addHot 时使用）
   * 不阻塞调用方；后台异步刷新。如果该 pool 0.5s 内已经刷过则跳过。
   */
  async refreshOne(poolAddress) {
    if (!this.onlineSdk || !this.user || !poolAddress) return null;
    const cached = this.cache.get(poolAddress);
    if (cached && Date.now() - cached.fetchedAt < 500) return cached.state;
    try {
      const state = await this._fetchPoolState(poolAddress);
      if (state) {
        this.cache.set(poolAddress, { state, fetchedAt: Date.now() });
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

    if (cachedState?.baseTokenProgram && cachedState?.quoteTokenProgram) {
      // ✅ 快速路径：1 次 getMultipleAccountsInfo（7 账户）
      try {
        const { baseMint, quoteMint, poolBaseTokenAccount, poolQuoteTokenAccount } = cachedState.pool;
        const baseTokenProgram = cachedState.baseTokenProgram;
        const quoteTokenProgram = cachedState.quoteTokenProgram;

        const userBaseTokenAccount = getAssociatedTokenAddressSync(
          baseMint, user, true, baseTokenProgram,
        );
        const userQuoteTokenAccount = getAssociatedTokenAddressSync(
          quoteMint, user, true, quoteTokenProgram,
        );

        const [
          poolAccountInfo,
          baseMintAccountInfo,
          quoteMintAccountInfo,
          poolBaseAccountInfo,
          poolQuoteAccountInfo,
          userBaseAccountInfo,
          userQuoteAccountInfo,
        ] = await connection.getMultipleAccountsInfo([
          poolKey,
          baseMint,
          quoteMint,
          poolBaseTokenAccount,
          poolQuoteTokenAccount,
          userBaseTokenAccount,
          userQuoteTokenAccount,
        ]);

        if (!poolAccountInfo) return null;

        // v3.32: 检查pool owner是否还是Pump AMM程序（已迁移到Raydium的pool owner会变）
        const PUMP_AMM_PROGRAM_ID = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
        if (poolAccountInfo.owner && poolAccountInfo.owner.toBase58() !== PUMP_AMM_PROGRAM_ID) {
          this.markDead(poolAddressStr);
          console.warn(`[PoolStateCache] 🪦 Pool migrated (owner=${poolAccountInfo.owner.toBase58().slice(0,8)}.. ≠ pAMMBay): ${poolAddressStr.slice(0,8)}..`);
          return null;
        }

        const pool = PUMP_AMM_SDK.decodePool(poolAccountInfo);
        if (!baseMintAccountInfo || !quoteMintAccountInfo || !poolBaseAccountInfo || !poolQuoteAccountInfo) return null;

        // 校验：如果 tokenProgram 变了（极罕见），需要下次走慢路径
        if (!baseMintAccountInfo.owner.equals(baseTokenProgram) ||
            !quoteMintAccountInfo.owner.equals(quoteTokenProgram)) {
          // tokenProgram 变了，走慢路径
          return await this._fetchPoolStateSlow(poolAddressStr, poolKey);
        }

        const decodedBaseMint = MintLayout.decode(baseMintAccountInfo.data);
        const decodedPoolBase = AccountLayout.decode(poolBaseAccountInfo.data);
        const decodedPoolQuote = AccountLayout.decode(poolQuoteAccountInfo.data);

        return {
          globalConfig: this._globalConfig,
          feeConfig: this._feeConfig,
          poolKey,
          poolAccountInfo,
          pool,
          poolBaseAmount: new BN(decodedPoolBase.amount.toString()),
          poolQuoteAmount: new BN(decodedPoolQuote.amount.toString()),
          baseTokenProgram,
          quoteTokenProgram,
          baseMint,
          baseMintAccount: decodedBaseMint,
          user,
          userBaseTokenAccount,
          userQuoteTokenAccount,
          userBaseAccountInfo,
          userQuoteAccountInfo,
        };
      } catch (err) {
        // 快速路径失败，fallback 到慢路径
        monitor.inc('PoolStateCache.fastPathFail', 1, 'PoolStateCache');
      }
    }

    // 慢路径（首次 / cache miss）：2 次 getMultipleAccountsInfo
    return await this._fetchPoolStateSlow(poolAddressStr, poolKey);
  }

  /**
   * 慢路径：2 次 getMultipleAccountsInfo（首次 / cache miss）
   * 第1次：[poolKey, baseMint, quoteMint, poolBaseToken, poolQuoteToken] → 5 账户
   *   poolKey 用来 decode pool 拿地址，其余 4 个同时查
   *   baseMint.owner → tokenProgram，用来派生 user ATA
   * 第2次：[userBaseTokenAccount, userQuoteTokenAccount] → 2 账户
   *
   * 2 次 vs 旧版 3 次，省 1 次 RPC（-33%）
   * 稳定后首次 refresh 走慢路径，后续全部走快速路径（1 次 RPC）
   */
  async _fetchPoolStateSlow(poolAddressStr, poolKey) {
    const { PUMP_AMM_SDK } = require('@pump-fun/pump-swap-sdk');
    const { MintLayout, AccountLayout, getAssociatedTokenAddressSync, TOKEN_PROGRAM_ID } = require('@solana/spl-token');
    const connection = this.onlineSdk.connection;
    const user = this.user;

    // 优化：先查 pool 拿到 mint 地址，然后和 mint 一起查
    // 但 pool 里的 baseMint/quoteMint/poolBaseToken/poolQuoteToken 都是 PublicKey
    // 我们不知道 baseMint 等地址，所以必须先查 pool
    //
    // 然而！如果我们之前缓存过这个 pool 的 state，可以直接用旧 pool 数据
    // 如果没有旧缓存，就必须先查 pool 再查其余 —— 但我们可以把 pool + 猜测的 tokenProgram 一起查

    // 第1次：5 账户（pool + 4 个子账户）
    // 注意：pool 账户里有 baseMint/quoteMint/poolBaseToken/poolQuoteToken
    // 我们不能在不查 pool 的情况下知道这些地址
    // 所以必须分两步：先查 pool，再查其余

    // 实际方案：第1次只查 pool（1 账户），拿到地址后第2次查 6 账户
    const [poolAccountInfo] = await connection.getMultipleAccountsInfo([poolKey]);
    if (!poolAccountInfo) return null;

    // v3.32: 检查pool owner是否还是Pump AMM程序
    const PUMP_AMM_PROGRAM_ID = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
    if (poolAccountInfo.owner && poolAccountInfo.owner.toBase58() !== PUMP_AMM_PROGRAM_ID) {
      this.markDead(poolAddressStr);
      console.warn(`[PoolStateCache] 🪦 Pool migrated (owner=${poolAccountInfo.owner.toBase58().slice(0,8)}.. ≠ pAMMBay): ${poolAddressStr.slice(0,8)}..`);
      return null;
    }

    const pool = PUMP_AMM_SDK.decodePool(poolAccountInfo);
    const { baseMint, quoteMint, poolBaseTokenAccount, poolQuoteTokenAccount } = pool;

    // 第2次：4 账户（baseMint, quoteMint, poolBaseToken, poolQuoteToken）
    // 拿到 baseTokenProgram 后再派生 user ATA
    const [
      baseMintAccountInfo,
      quoteMintAccountInfo,
      poolBaseAccountInfo,
      poolQuoteAccountInfo,
    ] = await connection.getMultipleAccountsInfo([
      baseMint,
      quoteMint,
      poolBaseTokenAccount,
      poolQuoteTokenAccount,
    ]);

    if (!baseMintAccountInfo || !quoteMintAccountInfo || !poolBaseAccountInfo || !poolQuoteAccountInfo) return null;

    const decodedBaseMint = MintLayout.decode(baseMintAccountInfo.data);
    const baseTokenProgram = baseMintAccountInfo.owner;
    const quoteTokenProgram = quoteMintAccountInfo.owner;

    const decodedPoolBase = AccountLayout.decode(poolBaseAccountInfo.data);
    const decodedPoolQuote = AccountLayout.decode(poolQuoteAccountInfo.data);

    // 派生 user token accounts
    const userBaseTokenAccount = getAssociatedTokenAddressSync(
      baseMint, user, true, baseTokenProgram,
    );
    const userQuoteTokenAccount = getAssociatedTokenAddressSync(
      quoteMint, user, true, quoteTokenProgram,
    );

    // 第3次：2 账户（user token accounts）
    // 这第3次在慢路径里无法避免，因为需要 tokenProgram 才能派生 ATA
    // 但慢路径只在首次 refresh 时走，后续全走快速路径（1 次 RPC）
    const [userBaseAccountInfo, userQuoteAccountInfo] = await connection.getMultipleAccountsInfo([
      userBaseTokenAccount,
      userQuoteTokenAccount,
    ]);

    return {
      globalConfig: this._globalConfig,
      feeConfig: this._feeConfig,
      poolKey,
      poolAccountInfo,
      pool,
      poolBaseAmount: new BN(decodedPoolBase.amount.toString()),
      poolQuoteAmount: new BN(decodedPoolQuote.amount.toString()),
      baseTokenProgram,
      quoteTokenProgram,
      baseMint,
      baseMintAccount: decodedBaseMint,
      user,
      userBaseTokenAccount,
      userQuoteTokenAccount,
      userBaseAccountInfo,
      userQuoteAccountInfo,
    };
  }

  /**
   * v3.17.22: 分级刷新 — 只刷 hotMints，持仓币和信号币不同频率
   */
  async _refreshAll() {
    if (this._refreshing) return;
    this._refreshing = true;
    try {
      const targets = [];
      for (const [mint, info] of this.hotMints) {
        targets.push({ mint, poolAddress: info.poolAddress, isPosition: info.isPosition });
      }

      if (targets.length === 0) {
        // 无热币：只做清理
        let removed = 0;
        for (const addr of this.cache.keys()) {
          this.cache.delete(addr);
          removed += 1;
        }
        if (removed > 0) {
          monitor.inc('PoolStateCache.evicted', removed, 'PoolStateCache');
        }
        monitor.beat('PoolStateCache', 'idle:0');
        monitor.set('PoolStateCache.cacheSize', this.cache.size, 'PoolStateCache');
        monitor.set('PoolStateCache.hotMintsSize', 0, 'PoolStateCache');
        return;
      }

      // 清理 cache 中已不在 hotMints 的 entry
      const hotAddresses = new Set(targets.map((t) => t.poolAddress));
      let removed = 0;
      for (const addr of this.cache.keys()) {
        if (!hotAddresses.has(addr)) {
          this.cache.delete(addr);
          removed += 1;
        }
      }
      if (removed > 0) {
        monitor.inc('PoolStateCache.evicted', removed, 'PoolStateCache');
        console.log(`[PoolStateCache] evicted ${removed} stale entries (not in hotMints)`);
      }

      // 第三刀：分级刷新
      const positionTargets = targets.filter(t => t.isPosition);
      const signalTargets = targets.filter(t => !t.isPosition);

      const positionBatchSize = positionTargets.length > 0
        ? Math.max(1, Math.ceil(positionTargets.length / (this.positionRefreshMs / this._tickIntervalMs)))
        : 0;
      const signalBatchSize = signalTargets.length > 0
        ? Math.max(1, Math.ceil(signalTargets.length / (this.signalRefreshMs / this._tickIntervalMs)))
        : 0;

      const slice = [];

      if (positionTargets.length > 0) {
        for (let i = 0; i < positionBatchSize; i++) {
          slice.push(positionTargets[this._positionCursor % positionTargets.length]);
          this._positionCursor++;
        }
      }

      if (signalTargets.length > 0) {
        for (let i = 0; i < signalBatchSize; i++) {
          slice.push(signalTargets[this._signalCursor % signalTargets.length]);
          this._signalCursor++;
        }
      }

      if (slice.length === 0) {
        monitor.beat('PoolStateCache', 'idle:0');
        return;
      }

      monitor.beat('PoolStateCache', `refresh:${slice.length}/pos:${positionTargets.length}/sig:${signalTargets.length}`);
      const t0 = Date.now();

      let okCount = 0;
      let failCount = 0;
      for (const t of slice) {
        try {
          const state = await this._fetchPoolState(t.poolAddress);
          if (state) {
            this.cache.set(t.poolAddress, { state, fetchedAt: Date.now() });
            okCount++;
          } else {
            failCount++;
          }
        } catch (err) {
          failCount++;
          if (err.message && err.message.includes('429')) {
            await new Promise((r) => setTimeout(r, 200));
          }
        }
      }

      const elapsed = Date.now() - t0;
      monitor.set('PoolStateCache.lastRefreshMs', elapsed, 'PoolStateCache');
      monitor.set('PoolStateCache.cacheSize', this.cache.size, 'PoolStateCache');
      monitor.set('PoolStateCache.hotMintsSize', this.hotMints.size, 'PoolStateCache');
      monitor.inc('PoolStateCache.refreshOk', okCount, 'PoolStateCache');
      if (failCount > 0) monitor.inc('PoolStateCache.refreshFail', failCount, 'PoolStateCache');
    } finally {
      this._refreshing = false;
    }
  }
}

module.exports = PoolStateCache;
