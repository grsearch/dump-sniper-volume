'use strict';

/**
 * TokenWatchdog (v3.17.13)
 * =========================
 * 定时巡检任务：
 *   1) 监控超时：代币被收录后超过 MAX_WATCH_DURATION_MS，自动移除（有仓先卖再移除）
 *   2) FDV 下限：监控期间 FDV < MIN_FDV_USD，自动移除（数据来自 Birdeye，USD 计价）
 *   3) LP 下限：监控期间 Birdeye 流动性 < MIN_LIQUIDITY_USD，自动移除（USD 计价）
 *
 * ⚠️ 单位注意：
 *    - FDV 来自 Birdeye，USD 计价
 *    - LP 来自 Birdeye liquidity，与 PumpDiscovery 使用同一美元阈值
 */

const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { fetchTokenMarketOnly } = require('../utils/tokenMeta');

const monitor = getMonitor();
monitor.registerModule('TokenWatchdog', { staleMs: 300_000, label: 'Token Watchdog' });

class TokenWatchdog {
  /**
   * @param {object} opts
   * @param {import('../data/TokenRegistry')} opts.tokenRegistry
   * @param {import('./PositionManager')} opts.positionManager
   * @param {function} opts.onTokenRemoved - 代币被移除后的回调
   */
  constructor({ tokenRegistry, positionManager, tradeLogger, onTokenRemoved }) {
    this.tokenRegistry = tokenRegistry;
    this.positionManager = positionManager;
    this.onTokenRemoved = onTokenRemoved;

    // Defaults come from config.strategy so FDV/LP checks stay enabled without explicit env values.
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
    this.minLiquidityUsd = process.env.MIN_LIQUIDITY_USD != null
      ? parseFloat(process.env.MIN_LIQUIDITY_USD)
      : config.strategy.minLiquidityUsd;

    // Monitoring-list age limit. Open positions are retained until they close.
    this.maxTokenAgeMs = parseInt(process.env.MAX_TOKEN_AGE_MS || '14400000', 10);

    this._pendingExitMints = new Set();
    this._checkInterval = null;
    this._checking = false;
    this._lastFdvCheckAt = new Map();
    this.checkIntervalMs = Math.max(
      10_000,
      parseInt(process.env.WATCHDOG_CHECK_INTERVAL_MS || '60000', 10),
    );

    // v3.17.13: Birdeye 熔断 — 连续失败后跳过调用，避免 _check 卡死
    this._birdeyeConsecutiveFails = 0;
    this._birdeyeCircuitOpen = false;
    this._birdeyeCircuitOpenUntil = 0;

    // v3.17.41: 24h 过滤
    this.minVolume24hUsd = parseFloat(process.env.MIN_VOLUME_24H_USD || '20000');
    this.noBuyRemoveMs = parseInt(process.env.NO_BUY_REMOVE_MS || '86400000', 10); // 默认 24h
    this.tradeLogger = tradeLogger || null; // v3.17.41: 从参数注入

    const features = [`checkEvery=${this.checkIntervalMs / 60_000}min`];
    if (this.maxWatchDurationMs > 0) features.push(`maxWatch=${this.maxWatchDurationMs / 60000}min`);
    if (this.minFdVUsd > 0) features.push(`minFDV=$${this.minFdVUsd}`);
    if (this.maxFdVUsd > 0) features.push(`maxFDV=$${this.maxFdVUsd}`);
    if (this.minLiquidityUsd > 0) features.push(`minLiquidity=$${this.minLiquidityUsd}`);
    if (this.minVolume24hUsd > 0) features.push(`minVol24h=$${this.minVolume24hUsd}`);
    if (this.noBuyRemoveMs > 0) features.push(`noBuyRemove=${this.noBuyRemoveMs / 3600000}h`);
    if (this.maxTokenAgeMs > 0) features.push(`maxAge=${this.maxTokenAgeMs / 3600000}h`);
    if (features.length > 0) {
      console.log(`[TokenWatchdog] enabled: ${features.join(', ')}`);
    }
  }

  start() {
    if (this.maxWatchDurationMs <= 0 && this.minFdVUsd <= 0 && this.maxFdVUsd <= 0 && this.minLiquidityUsd <= 0 && this.minVolume24hUsd <= 0 && this.noBuyRemoveMs <= 0 && this.maxTokenAgeMs <= 0) return;

    this._checkInterval = setInterval(() => this._runCheck(), this.checkIntervalMs);

    // 首次检查延迟 10 秒
    setTimeout(() => {
      this._runCheck();
    }, 10_000);
  }

  async _runCheck() {
    if (this._checking) {
      monitor.beat('TokenWatchdog', 'check:overlap_skipped');
      monitor.inc('TokenWatchdog.overlapSkipped', 1, 'TokenWatchdog');
      return;
    }
    this._checking = true;
    try {
      await this._check();
    } catch (err) {
      monitor.recordError('TokenWatchdog', err, { phase: 'check' });
      console.error(`[TokenWatchdog] check error: ${err.message}`);
    } finally {
      this._checking = false;
    }
  }

  stop() {
    if (this._checkInterval) {
      clearInterval(this._checkInterval);
      this._checkInterval = null;
    }
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

      let liquidityUsd = token.liquidity;

      // 3) FDV 下限（Birdeye，USD）— 只对有仓位的代币查 FDV，减少 API 调用
      let fdv = token.fdv;
      let birdeyeFailed = false;
      // FDV uses its own timestamp because token.updated_at is also changed by
      // pool and metadata writes. Both positioned and unpositioned tokens are
      // refreshed at the configured one-minute watchdog cadence.
      const lastFdvCheckAt = this._lastFdvCheckAt.get(token.mint) || 0;
      const marketFilterEnabled = this.minFdVUsd > 0 || this.maxFdVUsd > 0 || this.minLiquidityUsd > 0;
      const needMarketCheck = marketFilterEnabled && now - lastFdvCheckAt >= this.checkIntervalMs;

      const birdeyeAvailable = !this._birdeyeCircuitOpen || Date.now() >= this._birdeyeCircuitOpenUntil;

      if (needMarketCheck && birdeyeAvailable) {
        this._lastFdvCheckAt.set(token.mint, now);
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
            if (freshInfo.liquidity != null) liquidityUsd = freshInfo.liquidity;
            // 更新缓存
            this.tokenRegistry.stmts.update.run(
              freshInfo.symbol || token.symbol,
              freshInfo.name || token.name,
              freshInfo.decimals ?? token.decimals,
              fdv,
              freshInfo.marketCap ?? token.market_cap,
              liquidityUsd,
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

      // Birdeye liquidity is USD-denominated and refreshed with FDV every minute.
      const normalizedLiquidityUsd = Number(liquidityUsd);
      if (
        this.minLiquidityUsd > 0 &&
        liquidityUsd != null &&
        Number.isFinite(normalizedLiquidityUsd) &&
        normalizedLiquidityUsd < this.minLiquidityUsd
      ) {
        reasons.push(
          `liquidity_too_low($${Math.round(normalizedLiquidityUsd)} < $${this.minLiquidityUsd})`,
        );
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

      // WATCHDOG_EXIT is disabled. If this mint has an open position, keep it
      // subscribed so price-driven take-profit and trailing exits can still fire.
      // Tokens with no open position may be removed from monitoring normally.
      const hasOpenPos = this.positionManager.hasOpenPosition(token.mint);
      if (hasOpenPos) {
        console.log(
          `[TokenWatchdog] ⏸ KEEP ${symbol}: ${reasonStr} — has open position, keep monitoring until exit`
        );
        monitor.inc('TokenWatchdog.tokensRetainedForPosition', 1, 'TokenWatchdog');
        continue;
      }

      console.log(`[TokenWatchdog] 🗑️ REMOVE ${symbol}: ${reasonStr}`);
      this.tokenRegistry.removeToken(token.mint);
      this._pendingExitMints.delete(token.mint);
      this._lastFdvCheckAt.delete(token.mint);
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
