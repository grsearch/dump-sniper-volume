'use strict';

/**
 * TokenWatchdog (v3.17.13)
 * =========================
 * 定时巡检任务：
 *   1) 监控超时：代币被收录后超过 MAX_WATCH_DURATION_MS，自动移除（有仓先卖再移除）
 *   2) FDV 下限：监控期间 FDV < MIN_FDV_USD，自动移除（数据来自 Birdeye，USD 计价）
 *   3) LP 下限：监控期间池子 SOL 余额 < MIN_LP_SOL，自动移除（数据来自链上 PoolStateCache，SOL 计价）
 *
 * ⚠️ 单位注意：
 *    - FDV 来自 Birdeye，USD 计价
 *    - LP 来自链上池子 quote vault 余额，SOL 计价（比 Birdeye 的 liquidity 字段更准确）
 *    - Pump 新币在 Birdeye 的 liquidity 数据经常为 0 或极低，不可靠
 */

const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { fetchTokenMarketOnly, fetchTokenFullInfo } = require('../utils/tokenMeta');

const monitor = getMonitor();
monitor.registerModule('TokenWatchdog', { staleMs: 600_000, label: 'Token Watchdog' });

class TokenWatchdog {
  /**
   * @param {object} opts
   * @param {import('../data/TokenRegistry')} opts.tokenRegistry
   * @param {import('./PositionManager')} opts.positionManager
   * @param {import('./PoolStateCache')} [opts.poolStateCache] - 用于读取链上池子 SOL 余额
   * @param {function} opts.onTokenRemoved - 代币被移除后的回调
   */
  constructor({ tokenRegistry, positionManager, poolStateCache, tradeLogger, onTokenRemoved }) {
    this.tokenRegistry = tokenRegistry;
    this.positionManager = positionManager;
    this.poolStateCache = poolStateCache;
    this.onTokenRemoved = onTokenRemoved;

    // v3.17.20: 默认值改从 config.strategy 读（config 已设默认 FDV=$20000, LP=5000 SOL,
    //   maxWatch=0）。这样不在 .env 显式设置时也能默认开启 FDV/LP 巡检。
    //   显式 env 变量仍可覆盖（env 优先）。
    this.maxWatchDurationMs = process.env.MAX_WATCH_DURATION_MS != null
      ? parseInt(process.env.MAX_WATCH_DURATION_MS, 10)
      : config.strategy.maxWatchDurationMs;
    this.minFdVUsd = process.env.MIN_FDV_USD != null
      ? parseFloat(process.env.MIN_FDV_USD)
      : config.strategy.minFdVUsd;
    // v3.17.20: FDV 上限 — FDV > 此值移除监控（大盘币不是我们的目标）
    this.maxFdVUsd = process.env.MAX_FDV_USD != null
      ? parseFloat(process.env.MAX_FDV_USD)
      : (config.strategy.maxFdVUsd || 0);
    this.minLpSol = process.env.MIN_LP_SOL != null
      ? parseFloat(process.env.MIN_LP_SOL)
      : config.strategy.minLpSol;

    // v3.29: 代币年龄过滤 — creation_time 超过此值的代币自动移除监控（减少 watchlist 压力）
    this.maxTokenAgeMs = parseInt(process.env.MAX_TOKEN_AGE_MS || '86400000', 10); // 默认 24h

    this._pendingExitMints = new Set();
    this._checkInterval = null;

    // v3.17.13: Birdeye 熔断 — 连续失败后跳过调用，避免 _check 卡死
    this._birdeyeConsecutiveFails = 0;
    this._birdeyeCircuitOpen = false;
    this._birdeyeCircuitOpenUntil = 0;

    // v3.17.41: 24h 过滤
    this.minVolume24hUsd = parseFloat(process.env.MIN_VOLUME_24H_USD || '20000');
    this.noBuyRemoveMs = parseInt(process.env.NO_BUY_REMOVE_MS || '86400000', 10); // 默认 24h
    this.tradeLogger = tradeLogger || null; // v3.17.41: 从参数注入

    const features = [];
    if (this.maxWatchDurationMs > 0) features.push(`maxWatch=${this.maxWatchDurationMs / 60000}min`);
    if (this.minFdVUsd > 0) features.push(`minFDV=$${this.minFdVUsd}`);
    if (this.maxFdVUsd > 0) features.push(`maxFDV=$${this.maxFdVUsd}`);
    if (this.minLpSol > 0) features.push(`minLP=${this.minLpSol} SOL`);
    if (this.minVolume24hUsd > 0) features.push(`minVol24h=$${this.minVolume24hUsd}`);
    if (this.noBuyRemoveMs > 0) features.push(`noBuyRemove=${this.noBuyRemoveMs / 3600000}h`);
    if (this.maxTokenAgeMs > 0) features.push(`maxAge=${this.maxTokenAgeMs / 3600000}h`);
    if (features.length > 0) {
      console.log(`[TokenWatchdog] enabled: ${features.join(', ')}`);
    }
  }

  start() {
    if (this.maxWatchDurationMs <= 0 && this.minFdVUsd <= 0 && this.maxFdVUsd <= 0 && this.minLpSol <= 0 && this.minVolume24hUsd <= 0 && this.noBuyRemoveMs <= 0 && this.maxTokenAgeMs <= 0) return;

    this._checkInterval = setInterval(() => {
      this._check().catch((err) => {
        monitor.recordError('TokenWatchdog', err, { phase: 'check' });
        console.error(`[TokenWatchdog] check error: ${err.message}`);
      });
    }, parseInt(process.env.WATCHDOG_CHECK_INTERVAL_MS || '15000', 10));

    // 首次检查延迟 10 秒
    setTimeout(() => {
      this._check().catch((err) => {
        console.error(`[TokenWatchdog] initial check error: ${err.message}`);
      });
    }, 10_000);
  }

  stop() {
    if (this._checkInterval) {
      clearInterval(this._checkInterval);
      this._checkInterval = null;
    }
  }

  /**
   * 从 PoolStateCache 读取池子实际 SOL 余额
   */
  _getPoolQuoteSol(token) {
    if (!this.poolStateCache || !token.pool_address) return null;
    const cached = this.poolStateCache.get(token.pool_address);
    if (!cached || !cached.state) return null;
    const state = cached.state;
    // Pump AMM SDK swapSolanaState 返回 poolQuoteAmount (lamports BN)
    const lamports = state.poolQuoteAmount;
    if (lamports == null) return null;
    // BN or number
    const val = typeof lamports === 'object' && lamports.toNumber ? lamports.toNumber() : Number(lamports);
    return val / 1e9; // lamports → SOL
  }

  async _check() {
    monitor.beat('TokenWatchdog', 'check');
    const now = Date.now();
    const activeTokens = this.tokenRegistry.listActive();
    let removed = 0;
    let fdvRefreshed = 0;

    for (const token of activeTokens) {
      const reasons = [];

      // 1) 监控超时
      if (this.maxWatchDurationMs > 0 && token.added_at) {
        const watchAge = now - token.added_at;
        if (watchAge >= this.maxWatchDurationMs) {
          reasons.push(`watch_timeout(${Math.round(watchAge / 60000)}min >= ${this.maxWatchDurationMs / 60000}min)`);
        }
      }

      // 2) LP 下限（链上 PoolStateCache，秒级，不依赖 Birdeye）
      //    优先用链上数据，比 Birdeye 快得多且更准确
      const poolSol = this._getPoolQuoteSol(token);
      let latestLp = token.liquidity;
      if (poolSol != null) latestLp = poolSol;

      if (this.minLpSol > 0) {
        if (poolSol != null && poolSol < this.minLpSol) {
          reasons.push(`lp_too_low(${poolSol.toFixed(2)} SOL < ${this.minLpSol} SOL)`);
        }
      }

      // 3) FDV 下限（Birdeye，USD）— 只对有仓位的代币查 FDV，减少 API 调用
      let fdv = token.fdv;
      let birdeyeFailed = false;
      const hasPosition = this.positionManager.hasOpenPosition(token.mint);
      // v3.22: 分层FDV检查频率省birdeye API
      // 有仓位: 15min（FDV异常可能需要提前退出）
      // 无仓位: 1h（只需维持监控状态，很多代币几天才1次交易机会）
      const fdvCooldownMs = hasPosition ? 900_000 : 3_600_000;
      const needFdVCheck = this.minFdVUsd > 0 && (
        (fdv == null || fdv === 0 || (now - (token.updated_at || 0)) > fdvCooldownMs)
      );

      const birdeyeAvailable = !this._birdeyeCircuitOpen || Date.now() >= this._birdeyeCircuitOpenUntil;

      if (needFdVCheck && birdeyeAvailable) {
        try {
          const freshInfo = await fetchTokenMarketOnly(token.mint);
          if (freshInfo._birdeyeError) {
            const bErr = freshInfo._birdeyeError;
            const status = bErr.response?.status;
            if (status === 404) {
              birdeyeFailed = true;
              fdv = 0;
              console.warn(
                `[TokenWatchdog] Birdeye 404 for ${token.symbol || token.mint.slice(0,8)} — token not found, treating as FDV=0`,
              );
            } else if (status === 502 || status === 503) {
              this._birdeyeConsecutiveFails++;
              if (this._birdeyeConsecutiveFails >= 3) {
                this._birdeyeCircuitOpen = true;
                this._birdeyeCircuitOpenUntil = Date.now() + 60_000; // 熔断 1 分钟
                console.warn(
                  `[TokenWatchdog] Birdeye circuit breaker OPEN — ${this._birdeyeConsecutiveFails} consecutive failures, skipping for 1min`,
                );
              }
            }
          } else {
            this._birdeyeConsecutiveFails = 0;
            this._birdeyeCircuitOpen = false;
            if (freshInfo.fdv != null) fdv = freshInfo.fdv;
            if (freshInfo.liquidity != null && freshInfo.liquidity > 0) latestLp = freshInfo.liquidity;
            // 更新缓存
            this.tokenRegistry.stmts.update.run(
              freshInfo.symbol || token.symbol,
              freshInfo.name || token.name,
              freshInfo.decimals ?? token.decimals,
              fdv,
              freshInfo.marketCap ?? token.market_cap,
              latestLp,
              freshInfo.price ?? token.price,
              Date.now(),
              JSON.stringify(freshInfo),
              token.mint,
            );
            const refreshed = this.tokenRegistry.stmts.get.get(token.mint);
            if (refreshed) this.tokenRegistry.cache.set(token.mint, refreshed);
            fdvRefreshed++;
          }
        } catch (err) {
          this._birdeyeConsecutiveFails++;
          if (this._birdeyeConsecutiveFails >= 3 && !this._birdeyeCircuitOpen) {
            this._birdeyeCircuitOpen = true;
            this._birdeyeCircuitOpenUntil = Date.now() + 60_000;
          }
        }
        await new Promise((r) => setTimeout(r, 100)); // 节流 100ms
      }

      // FDV 下限检查
      if (this.minFdVUsd > 0 && birdeyeFailed && fdv === 0) {
        reasons.push(`fdv_too_low($0 < $${this.minFdVUsd}), birdeye_404`);
      } else if (this.minFdVUsd > 0 && fdv != null && fdv > 0 && fdv < this.minFdVUsd) {
        reasons.push(`fdv_too_low($${Math.round(fdv)} < $${this.minFdVUsd})`);
      }

      // v3.17.20: FDV 上限检查 — 大盘币不是 snipe 目标，移除监控
      if (this.maxFdVUsd > 0 && fdv != null && fdv > 0 && fdv > this.maxFdVUsd) {
        reasons.push(`fdv_too_high($${Math.round(fdv)} > $${this.maxFdVUsd})`);
      }

      // v3.17.41: 24h 交易量下限 — 流动性太差的币不值得监控
      if (this.minVolume24hUsd > 0) {
        // 从 Birdeye meta_json 取 volume24h（Birdeye 刷新时已存入）
        let vol24h = 0;
        try {
          const meta = token.meta_json ? JSON.parse(token.meta_json) : {};
          vol24h = meta.volume24h || 0;
        } catch (_) {}
        // 只在 added_at 超过 10 分钟的代币上检查（新币可能还没 volume 数据）
        const tokenAge = now - (token.added_at || 0);
        if (tokenAge > 600_000 && vol24h < this.minVolume24hUsd) {
          reasons.push(`vol24h_too_low($${Math.round(vol24h)} < $${this.minVolume24hUsd})`);
        }
      }

      // v3.17.41: 24h 无买入 → 移除（监控了但不交易=浪费资源）
      if (this.noBuyRemoveMs > 0 && this.tradeLogger) {
        const tokenAge = now - (token.added_at || 0);
        if (tokenAge > this.noBuyRemoveMs) {
          try {
            const sinceMs = now - this.noBuyRemoveMs;
            const recentBuys = this.tradeLogger.countRecentBuysByMint(token.mint, sinceMs);
            if (recentBuys === 0) {
              reasons.push(`no_buy_24h (last ${this.noBuyRemoveMs / 3600000}h no position)`);
            }
          } catch (_) {}
        }
      }

      // v3.32d: 代币年龄过滤 — 用 creation_time（币创建时间）
      //   入场处也用 creation_time (MAX_MINT_AGE_HOURS)，保持一致
      //   creation_time 为 null 时跳过（还没回填，不阻塞）
      if (this.maxTokenAgeMs > 0 && token.creation_time && token.creation_time > 0) {
        const tokenAge = now - token.creation_time;
        if (tokenAge >= this.maxTokenAgeMs) {
          reasons.push(`token_too_old(${Math.round(tokenAge / 3600000)}h >= ${this.maxTokenAgeMs / 3600000}h)`);
        }
      }

      if (reasons.length === 0) continue;

      const symbol = token.symbol || token.mint.slice(0, 8);
      const reasonStr = reasons.join(', ');

      // v3.20: WATCHDOG_EXIT 已禁用 — FDV跌破阈值不再强制卖出
      // 竞对数据证明低FDV币也能弹回赚钱，watchdog卖出反而亏更多
      // FDV < MIN_FDV_USD 时只移除监控（不再交易新信号），不卖出已有持仓
      const hasOpenPos = this.positionManager.hasOpenPosition(token.mint);
      if (hasOpenPos) {
        // v3.20: 有持仓 → 只移除监控，不触发卖出
        // 让 EMERGENCY_STOP / trailing / timeout 自然处理退出
        console.log(
          `[TokenWatchdog] ⏸  ${symbol} ${reasonStr} — has open position, skip exit (v3.20: watchdog exit disabled)`
        );
        // 继续执行 removeToken（停止监控新信号），但跳过卖出
        // 注意：不设置 _pendingExitMints，因为没有触发卖出
      }

      console.log(`[TokenWatchdog] 🗑️ REMOVE ${symbol}: ${reasonStr}${hasOpenPos ? ' (position exit triggered)' : ''}`);
      this.tokenRegistry.removeToken(token.mint);
      this._pendingExitMints.delete(token.mint);
      removed++;

      if (this.onTokenRemoved) this.onTokenRemoved();
      monitor.inc('TokenWatchdog.tokensRemoved', 1, 'TokenWatchdog');
    }

    if (removed > 0 || fdvRefreshed > 0) {
      console.log(`[TokenWatchdog] check done: removed=${removed}, fdvRefreshed=${fdvRefreshed}`);
    }
  }
}

module.exports = TokenWatchdog;
