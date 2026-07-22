'use strict';

const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const {
  fetchTokenMarketOnly,
  fetchTokenMarketsFromDexScreener,
} = require('../utils/tokenMeta');
const { normalizeUnixMs } = require('../utils/migrationTime');

const monitor = getMonitor();
monitor.registerModule('TokenWatchdog', { staleMs: 300_000, label: 'Token Watchdog' });

class TokenWatchdog {
  constructor({
    tokenRegistry,
    positionManager,
    poolStateCache,
    tradeLogger,
    onTokenRemoved,
    fetchMarkets,
    fetchMarket,
    maxTokenAgeMs,
  }) {
    this.tokenRegistry = tokenRegistry;
    this.positionManager = positionManager;
    this.poolStateCache = poolStateCache || null;
    this.tradeLogger = tradeLogger || null;
    this.onTokenRemoved = onTokenRemoved;
    this.fetchMarkets = fetchMarkets || fetchTokenMarketsFromDexScreener;
    this.fetchMarket = fetchMarket || fetchTokenMarketOnly;

    this.maxWatchDurationMs = process.env.MAX_WATCH_DURATION_MS != null
      ? parseInt(process.env.MAX_WATCH_DURATION_MS, 10)
      : config.strategy.maxWatchDurationMs;
    this.minFdVUsd = process.env.MIN_FDV_USD != null
      ? parseFloat(process.env.MIN_FDV_USD)
      : config.strategy.minFdVUsd;
    this.maxFdVUsd = process.env.MAX_FDV_USD != null
      ? parseFloat(process.env.MAX_FDV_USD)
      : (config.strategy.maxFdVUsd || 0);
    this.minLiquidityUsd = process.env.MIN_LIQUIDITY_USD != null
      ? parseFloat(process.env.MIN_LIQUIDITY_USD)
      : config.strategy.minLiquidityUsd;
    this.minVolume24hUsd = parseFloat(process.env.MIN_VOLUME_24H_USD || '5000');
    this.noBuyRemoveMs = parseInt(process.env.NO_BUY_REMOVE_MS || '86400000', 10);
    this.maxTokenAgeMs = maxTokenAgeMs ?? config.activityRsi.watchlistMaxAgeMs;
    if (
      process.env.MAX_TOKEN_AGE_MS != null &&
      parseInt(process.env.MAX_TOKEN_AGE_MS, 10) !== this.maxTokenAgeMs
    ) {
      console.warn(
        '[TokenWatchdog] MAX_TOKEN_AGE_MS is legacy and ignored; ' +
          'use BURST_WATCHLIST_MAX_AGE_MS (default 1500000ms)',
      );
    }

    const configuredCheckIntervalMs = Math.max(
      10_000,
      parseInt(process.env.WATCHDOG_CHECK_INTERVAL_MS || '60000', 10),
    );
    this.checkIntervalMs = Math.min(60_000, configuredCheckIntervalMs);
    if (configuredCheckIntervalMs > this.checkIntervalMs) {
      console.warn(
        `[TokenWatchdog] WATCHDOG_CHECK_INTERVAL_MS=${configuredCheckIntervalMs} is obsolete; ` +
        'clamped to 60000ms',
      );
    }
    this.marketStaleMs = Math.max(
      this.checkIntervalMs * 3,
      parseInt(process.env.WATCHDOG_MARKET_STALE_MS || '180000', 10),
    );
    this.marketBatchSize = Math.min(
      30,
      Math.max(1, parseInt(process.env.WATCHDOG_MARKET_BATCH_SIZE || '30', 10)),
    );
    this.marketBatchDelayMs = Math.max(
      200,
      parseInt(process.env.WATCHDOG_MARKET_BATCH_DELAY_MS || '250', 10),
    );
    this.marketFallbackMaxPerCycle = Math.max(
      0,
      parseInt(process.env.WATCHDOG_MARKET_FALLBACK_MAX_PER_CYCLE || '10', 10),
    );
    this.ageCheckIntervalMs = Math.max(
      250,
      parseInt(process.env.WATCHDOG_AGE_CHECK_INTERVAL_MS || '1000', 10),
    );
    this.realtimeMarketPersistMs = Math.max(
      250,
      parseInt(process.env.WATCHDOG_REALTIME_MARKET_PERSIST_MS || '1000', 10),
    );
    this.solPriceUsd = Number(config.activityRsi.solPriceUsd) || 0;

    this._pendingExitMints = new Set();
    this._checkInterval = null;
    this._ageCheckInterval = null;
    this._checking = false;
    this._lastMarketAttemptAt = new Map();
    this._lastProviderMarketAt = new Map();
    this._lastRealtimePersistAt = new Map();
    this._lastRealtimeKeepLogAt = new Map();

    const features = [
      `checkEvery=${this.checkIntervalMs / 60_000}min`,
      `market=dexscreener(batch ${this.marketBatchSize})+birdeye fallback`,
    ];
    if (this.poolStateCache && this.solPriceUsd > 0) {
      features.push(`realtimeFDV/LP=${this.realtimeMarketPersistMs}ms`);
    }
    if (this.maxWatchDurationMs > 0) features.push(`maxWatch=${this.maxWatchDurationMs / 60000}min`);
    if (this.minFdVUsd > 0) features.push(`minFDV=$${this.minFdVUsd}`);
    if (this.maxFdVUsd > 0) features.push(`maxFDV=$${this.maxFdVUsd}`);
    if (this.minLiquidityUsd > 0) features.push(`minLiquidity=$${this.minLiquidityUsd}`);
    if (this.minVolume24hUsd > 0) features.push(`minVol24h=$${this.minVolume24hUsd}`);
    if (this.noBuyRemoveMs > 0) features.push(`noBuyRemove=${this.noBuyRemoveMs / 3600000}h`);
    if (this.maxTokenAgeMs > 0) {
      features.push(`maxAge=${this.maxTokenAgeMs / 60000}min`);
      features.push(`ageCheck=${this.ageCheckIntervalMs / 1000}s`);
    }
    console.log(`[TokenWatchdog] enabled: ${features.join(', ')}`);
  }

  start() {
    if (
      this.maxWatchDurationMs <= 0 &&
      this.minFdVUsd <= 0 &&
      this.maxFdVUsd <= 0 &&
      this.minLiquidityUsd <= 0 &&
      this.minVolume24hUsd <= 0 &&
      this.noBuyRemoveMs <= 0 &&
      this.maxTokenAgeMs <= 0
    ) {
      return;
    }

    this._checkInterval = setInterval(() => this._runCheck(), this.checkIntervalMs);
    setTimeout(() => this._runCheck(), 10_000);
    if (this.maxTokenAgeMs > 0) {
      this._ageCheckInterval = setInterval(() => this._runAgeCheck(), this.ageCheckIntervalMs);
      setTimeout(() => this._runAgeCheck(), Math.min(250, this.ageCheckIntervalMs));
    }
  }

  stop() {
    if (this._checkInterval) {
      clearInterval(this._checkInterval);
      this._checkInterval = null;
    }
    if (this._ageCheckInterval) {
      clearInterval(this._ageCheckInterval);
      this._ageCheckInterval = null;
    }
    this._lastProviderMarketAt.clear();
    this._lastRealtimePersistAt.clear();
    this._lastRealtimeKeepLogAt.clear();
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

  _runAgeCheck() {
    try {
      this._checkAges();
    } catch (err) {
      monitor.recordError('TokenWatchdog', err, { phase: 'age_check' });
      console.error(`[TokenWatchdog] age check error: ${err.message}`);
    }
  }

  _getMigrationAgeMs(token, now = Date.now()) {
    const migrationTime = Number(token?.migration_time);
    if (!Number.isFinite(migrationTime) || migrationTime <= 0) return null;
    return now - migrationTime;
  }

  _removeToken(token, reasonStr) {
    if (!token?.mint) return false;
    const symbol = token.symbol || token.mint.slice(0, 8);
    console.log(`[TokenWatchdog] REMOVE ${symbol}: ${reasonStr}`);
    this.tokenRegistry.removeToken(token.mint);
    this._pendingExitMints.delete(token.mint);
    this._lastMarketAttemptAt.delete(token.mint);
    this._lastProviderMarketAt.delete(token.mint);
    this._lastRealtimePersistAt.delete(token.mint);
    this._lastRealtimeKeepLogAt.delete(token.mint);
    if (this.onTokenRemoved) this.onTokenRemoved();
    monitor.inc('TokenWatchdog.tokensRemoved', 1, 'TokenWatchdog');
    return true;
  }

  _checkAges(now = Date.now()) {
    if (this.maxTokenAgeMs <= 0) return { removed: 0, exitsRequested: 0 };

    let removed = 0;
    let exitsRequested = 0;
    for (const token of this.tokenRegistry.listActive()) {
      const migrationAge = this._getMigrationAgeMs(token, now);
      if (migrationAge == null || migrationAge <= this.maxTokenAgeMs) continue;

      const symbol = token.symbol || token.mint.slice(0, 8);
      const reason = `migration_too_old(${Math.ceil(migrationAge / 1000)}s > ` +
        `${Math.floor(this.maxTokenAgeMs / 1000)}s)`;
      if (this.positionManager.hasOpenPosition(token.mint)) {
        if (!this._pendingExitMints.has(token.mint)) {
          const requested = this.positionManager.forceExitAllByMint(
            token.mint,
            'TOKEN_AGE_EXPIRED',
          );
          this._pendingExitMints.add(token.mint);
          exitsRequested++;
          console.warn(
            `[TokenWatchdog] AGE_EXIT ${symbol}: ${reason} - ` +
              `requested=${requested}, keep monitoring until sell confirms`,
          );
          monitor.inc('TokenWatchdog.ageExitRequested', 1, 'TokenWatchdog');
        }
        continue;
      }

      if (this._removeToken(token, reason)) removed++;
    }
    return { removed, exitsRequested };
  }

  handlePositionClosed(position) {
    const mint = position?.mint;
    if (!mint || !this._pendingExitMints.has(mint)) return false;
    if (this.positionManager.hasOpenPosition(mint)) return false;

    const token = this.tokenRegistry.getToken?.(mint);
    const migrationAge = this._getMigrationAgeMs(token);
    if (
      !token ||
      this.maxTokenAgeMs <= 0 ||
      migrationAge == null ||
      migrationAge <= this.maxTokenAgeMs
    ) {
      this._pendingExitMints.delete(mint);
      return false;
    }

    return this._removeToken(
      token,
      `migration_too_old(${Math.ceil(migrationAge / 1000)}s > ` +
        `${Math.floor(this.maxTokenAgeMs / 1000)}s), position closed`,
    );
  }

  _number(value) {
    if (value == null) return null;
    try {
      const number = Number(
        typeof value === 'object' && typeof value.toString === 'function'
          ? value.toString()
          : value,
      );
      return Number.isFinite(number) ? number : null;
    } catch (_) {
      return null;
    }
  }

  _realtimeMarketReasons(fdvUsd, liquidityUsd) {
    const reasons = [];
    if (this.minFdVUsd > 0 && fdvUsd < this.minFdVUsd) {
      reasons.push(`fdv_too_low($${Math.round(fdvUsd)} < $${this.minFdVUsd})`);
    }
    if (this.maxFdVUsd > 0 && fdvUsd > this.maxFdVUsd) {
      reasons.push(`fdv_too_high($${Math.round(fdvUsd)} > $${this.maxFdVUsd})`);
    }
    if (this.minLiquidityUsd > 0 && liquidityUsd < this.minLiquidityUsd) {
      reasons.push(
        `liquidity_too_low($${Math.round(liquidityUsd)} < $${this.minLiquidityUsd})`,
      );
    }
    return reasons;
  }

  handleRealtimePoolTick({
    mint,
    price,
    poolAddress,
    poolQuoteAfter,
    baseDecimals,
  } = {}) {
    if (!mint || !this.poolStateCache || this.solPriceUsd <= 0) return null;

    const token = this.tokenRegistry.getToken?.(mint);
    if (!token || Number(token.is_active) !== 1) return null;

    const resolvedPoolAddress = poolAddress || token.pool_address;
    if (!resolvedPoolAddress) return null;
    const state = this.poolStateCache.get(resolvedPoolAddress);
    if (!state?.baseMintAccount || !state.poolQuoteAmount) return null;

    const priceSol = this._number(price);
    const supplyRaw = this._number(state.baseMintAccount.supply);
    const decimals = this._number(state.baseMintAccount.decimals) ??
      this._number(baseDecimals) ??
      this._number(token.decimals) ??
      6;
    const supplyScale = 10 ** decimals;
    const supplyUi = supplyRaw != null && supplyRaw > 0 && Number.isFinite(supplyScale)
      ? supplyRaw / supplyScale
      : null;

    let quoteSol = this._number(poolQuoteAfter);
    if (!(quoteSol > 0)) {
      const quoteRaw = this._number(state.poolQuoteAmount);
      quoteSol = quoteRaw != null ? quoteRaw / 1e9 : null;
    }
    if (!(priceSol > 0) || !(supplyUi > 0) || !(quoteSol > 0)) return null;

    const fdvUsd = priceSol * supplyUi * this.solPriceUsd;
    const liquidityUsd = quoteSol * 2 * this.solPriceUsd;
    const priceUsd = priceSol * this.solPriceUsd;
    if (
      !Number.isFinite(fdvUsd) || fdvUsd <= 0 ||
      !Number.isFinite(liquidityUsd) || liquidityUsd <= 0 ||
      !Number.isFinite(priceUsd) || priceUsd <= 0
    ) return null;

    const now = Date.now();
    const reasons = this._realtimeMarketReasons(fdvUsd, liquidityUsd);
    const lastPersistAt = this._lastRealtimePersistAt.get(mint) || 0;
    let currentToken = token;
    if (reasons.length > 0 || now - lastPersistAt >= this.realtimeMarketPersistMs) {
      currentToken = this.tokenRegistry.updateMarket(mint, {
        fdv: fdvUsd,
        liquidity: liquidityUsd,
        price: priceUsd,
        priceSol,
        supply: supplyUi,
        poolQuoteSol: quoteSol,
        marketSource: 'chain_pool_realtime',
        fetchedAt: now,
      }) || token;
      this._lastRealtimePersistAt.set(mint, now);
    }

    monitor.inc('TokenWatchdog.realtimeMarketTicks', 1, 'TokenWatchdog');
    if (reasons.length === 0) {
      return { removed: false, fdvUsd, liquidityUsd, priceUsd };
    }

    const reasonStr = `${reasons.join(', ')} (realtime)`;
    if (this.positionManager.hasOpenPosition(mint)) {
      const lastLogAt = this._lastRealtimeKeepLogAt.get(mint) || 0;
      if (now - lastLogAt >= 10_000) {
        console.log(
          `[TokenWatchdog] KEEP ${currentToken.symbol || mint.slice(0, 8)}: ` +
            `${reasonStr} - has open position, keep monitoring until exit`,
        );
        this._lastRealtimeKeepLogAt.set(mint, now);
      }
      monitor.inc('TokenWatchdog.realtimeRetainedForPosition', 1, 'TokenWatchdog');
      return { removed: false, retainedForPosition: true, fdvUsd, liquidityUsd, priceUsd };
    }

    const removed = this._removeToken(currentToken, reasonStr);
    if (removed) monitor.inc('TokenWatchdog.realtimeRemoved', 1, 'TokenWatchdog');
    return { removed, fdvUsd, liquidityUsd, priceUsd };
  }

  _isMarketFresh(token, now = Date.now()) {
    const updatedAt = Number(token?.market_updated_at);
    return Number.isFinite(updatedAt) && updatedAt > 0 && now - updatedAt <= this.marketStaleMs;
  }

  _backfillMissingMigration(token, market) {
    const hasExactMigrationTime = token?.migration_time &&
      token?.migration_time_source !== 'pump_graduation_added_at_fallback';
    if (hasExactMigrationTime) return;
    const migrationTime = normalizeUnixMs(market?.pairCreatedAt);
    if (!migrationTime) return;
    this.tokenRegistry.recordMigration(token.mint, {
      migrationTime,
      migrationTimeSource: 'dexscreener_pairCreatedAt',
    });
    console.log(
      `[TokenWatchdog] AGE backfilled for ${token.symbol || token.mint.slice(0, 8)} ` +
      'from DEX pair creation time',
    );
  }

  async _refreshMarkets(tokens, now = Date.now()) {
    const marketFilterEnabled = (
      this.minFdVUsd > 0 ||
      this.maxFdVUsd > 0 ||
      this.minLiquidityUsd > 0 ||
      this.minVolume24hUsd > 0
    );
    if (!marketFilterEnabled) return { refreshed: 0, failed: 0 };

    const due = tokens.filter((token) => {
      const lastSuccess = this._lastProviderMarketAt.get(token.mint) || (
        token.market_source === 'chain_pool_realtime'
          ? 0
          : Number(token.market_updated_at) || 0
      );
      const lastAttempt = this._lastMarketAttemptAt.get(token.mint) || 0;
      return (
        now - lastSuccess >= this.checkIntervalMs &&
        now - lastAttempt >= Math.min(this.checkIntervalMs, 10_000)
      );
    });
    if (due.length === 0) return { refreshed: 0, failed: 0 };

    let refreshed = 0;
    let failed = 0;
    const fallback = [];

    for (let offset = 0; offset < due.length; offset += this.marketBatchSize) {
      const batch = due.slice(offset, offset + this.marketBatchSize);
      for (const token of batch) this._lastMarketAttemptAt.set(token.mint, now);

      let markets = new Map();
      try {
        markets = await this.fetchMarkets(batch.map((token) => ({
          mint: token.mint,
          poolAddress: token.pool_address,
        })));
      } catch (err) {
        monitor.inc('TokenWatchdog.dexScreenerBatchFail', 1, 'TokenWatchdog');
        console.warn(
          `[TokenWatchdog] DEX Screener batch refresh failed (${batch.length} tokens): ${err.message}`,
        );
      }

      for (const token of batch) {
        const market = markets.get(token.mint);
        if (market) this._backfillMissingMigration(token, market);
        const marketComplete = (
          Number(market?.fdv) > 0 &&
          Number(market?.liquidity) > 0
        );
        if (!marketComplete) {
          fallback.push(token);
          continue;
        }
        this.tokenRegistry.updateMarket(token.mint, market);
        this._lastProviderMarketAt.set(token.mint, Number(market.fetchedAt) || now);
        refreshed++;
      }

      monitor.beat('TokenWatchdog', `market:${refreshed}/${due.length}`);
      if (offset + this.marketBatchSize < due.length) {
        await new Promise((resolve) => setTimeout(resolve, this.marketBatchDelayMs));
      }
    }

    const fallbackTokens = fallback.slice(0, this.marketFallbackMaxPerCycle);
    for (const token of fallbackTokens) {
      try {
        const market = await this.fetchMarket(token.mint);
        if (market?._birdeyeError) throw market._birdeyeError;
        const fallbackFdv = Number(market?.fdv ?? market?.marketCap);
        const fallbackLiquidity = Number(market?.liquidity);
        const hasMarket = (
          Number.isFinite(fallbackFdv) &&
          fallbackFdv > 0 &&
          Number.isFinite(fallbackLiquidity) &&
          fallbackLiquidity > 0
        );
        if (!hasMarket) throw new Error('Birdeye returned no market fields');
        this.tokenRegistry.updateMarket(token.mint, {
          ...market,
          fdv: fallbackFdv,
          marketSource: market.marketSource || 'birdeye',
        });
        this._lastProviderMarketAt.set(token.mint, Number(market.fetchedAt) || now);
        refreshed++;
      } catch (err) {
        failed++;
        monitor.inc('TokenWatchdog.marketFallbackFail', 1, 'TokenWatchdog');
        const status = err?.response?.status;
        console.warn(
          `[TokenWatchdog] market refresh failed for ` +
          `${token.symbol || token.mint.slice(0, 8)}${status ? ` HTTP ${status}` : ''}: ${err.message}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, this.marketBatchDelayMs));
    }

    if (fallback.length > fallbackTokens.length) {
      failed += fallback.length - fallbackTokens.length;
      console.warn(
        `[TokenWatchdog] ${fallback.length - fallbackTokens.length} market fallback(s) deferred ` +
        'to protect provider rate limits',
      );
    }

    return { refreshed, failed };
  }

  async _check() {
    monitor.beat('TokenWatchdog', 'check');
    const now = Date.now();
    const marketStats = await this._refreshMarkets(this.tokenRegistry.listActive(), now);
    const activeTokens = this.tokenRegistry.listActive();
    let removed = 0;

    for (const token of activeTokens) {
      const reasons = [];

      if (this.maxWatchDurationMs > 0 && token.added_at) {
        const watchAge = now - token.added_at;
        if (watchAge >= this.maxWatchDurationMs) {
          reasons.push(
            `watch_timeout(${Math.round(watchAge / 60000)}min >= ` +
            `${this.maxWatchDurationMs / 60000}min)`,
          );
        }
      }

      const marketFresh = this._isMarketFresh(token, now);
      const fdv = Number(token.fdv);
      const liquidityUsd = Number(token.liquidity);

      if (
        marketFresh &&
        this.minFdVUsd > 0 &&
        Number.isFinite(fdv) &&
        fdv >= 0 &&
        fdv < this.minFdVUsd
      ) {
        reasons.push(`fdv_too_low($${Math.round(fdv)} < $${this.minFdVUsd})`);
      }
      if (
        marketFresh &&
        this.maxFdVUsd > 0 &&
        Number.isFinite(fdv) &&
        fdv > this.maxFdVUsd
      ) {
        reasons.push(`fdv_too_high($${Math.round(fdv)} > $${this.maxFdVUsd})`);
      }
      if (
        marketFresh &&
        this.minLiquidityUsd > 0 &&
        Number.isFinite(liquidityUsd) &&
        liquidityUsd < this.minLiquidityUsd
      ) {
        reasons.push(
          `liquidity_too_low($${Math.round(liquidityUsd)} < $${this.minLiquidityUsd})`,
        );
      }

      if (marketFresh && this.minVolume24hUsd > 0) {
        let vol24h = 0;
        try {
          const meta = token.meta_json ? JSON.parse(token.meta_json) : {};
          vol24h = Number(meta.volume24h) || 0;
        } catch (_) {}
        const tokenWatchAge = now - (token.added_at || 0);
        if (tokenWatchAge > 600_000 && vol24h < this.minVolume24hUsd) {
          reasons.push(`vol24h_too_low($${Math.round(vol24h)} < $${this.minVolume24hUsd})`);
        }
      }

      if (this.noBuyRemoveMs > 0 && this.tradeLogger) {
        const tokenWatchAge = now - (token.added_at || 0);
        if (tokenWatchAge > this.noBuyRemoveMs) {
          try {
            const sinceMs = now - this.noBuyRemoveMs;
            const recentBuys = this.tradeLogger.countRecentBuysByMint(token.mint, sinceMs);
            if (recentBuys === 0) {
              reasons.push(`no_buy_24h (last ${this.noBuyRemoveMs / 3600000}h no position)`);
            }
          } catch (_) {}
        }
      }

      if (reasons.length === 0) continue;

      const symbol = token.symbol || token.mint.slice(0, 8);
      const reasonStr = reasons.join(', ');
      const hasOpenPos = this.positionManager.hasOpenPosition(token.mint);
      if (hasOpenPos) {
        console.log(
          `[TokenWatchdog] KEEP ${symbol}: ${reasonStr} - ` +
          'has open position, keep monitoring until exit',
        );
        monitor.inc('TokenWatchdog.tokensRetainedForPosition', 1, 'TokenWatchdog');
        continue;
      }

      if (this._removeToken(token, reasonStr)) removed++;
    }

    if (removed > 0 || marketStats.refreshed > 0 || marketStats.failed > 0) {
      console.log(
        `[TokenWatchdog] check done: removed=${removed}, ` +
        `marketRefreshed=${marketStats.refreshed}, marketFailed=${marketStats.failed}`,
      );
    }
  }
}

module.exports = TokenWatchdog;
