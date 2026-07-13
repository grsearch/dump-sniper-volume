'use strict';

const crypto = require('crypto');
const { config, validateConfig } = require('./config');
const TokenRegistry = require('./data/TokenRegistry');
const TradeLogger = require('./data/TradeLogger');
const TickStream = require('./core/TickStream');
const DumpDetector = require('./core/DumpDetector');
const PriceTracker = require('./core/PriceTracker');
const SignalEngine = require('./core/SignalEngine');
const Executor = require('./core/Executor');
const PositionManager = require('./core/PositionManager');
const PostExitTracker = require('./core/PostExitTracker');
const DailyReport = require('./reports/DailyReport');
const Server = require('./server/server');
const PoolFinder = require('./utils/poolFinder');
const { getMonitor } = require('./monitor/HealthMonitor');
const AlertChecker = require('./monitor/AlertChecker');
const TokenWatchdog = require('./core/TokenWatchdog');
const CompetitorTracker = require('./core/CompetitorTracker');
const OrderFlowTracker = require('./core/OrderFlowTracker');

const monitor = getMonitor();

async function main() {
  console.log('================================================');
  console.log('?? Dump Sniper V3.17.20 starting...');
  console.log(`Mode: ${config.DRY_RUN ? 'DRY_RUN' : '??  LIVE TRADING ??'}`);
  console.log(`Position: ${config.strategy.positionSizeSol} SOL`);
  console.log(`TP: +${config.strategy.takeProfitPct}% (immediate, no confirm)`);
  console.log(`Trailing: arm at +${config.strategy.trailingActivatePct}% / drawdown ${config.strategy.trailingDrawdownPct}% (priority: TP > trailing)`);
  console.log(
    `Entry: ORDER_FLOW reversal ` +
      `(sell>=${config.orderFlow.minSellSol} SOL/${config.orderFlow.windowMs}ms, ` +
      `drop ${config.orderFlow.minDropPct}-${config.orderFlow.maxDropPct}%, ` +
      `buy/sell>=${config.orderFlow.minBuySellRatio}, rebound ${config.orderFlow.minReboundPct}-${config.orderFlow.maxReboundPct}%)`,
  );
  console.log(`Legacy dumpSignal: ${config.orderFlow.replaceDumpSignal ? 'suppressed' : 'allowed fallback'}`);
  console.log(`Watchdog: FDV>=$${config.strategy.minFdVUsd}, LP>=${config.strategy.minLpSol} SOL (15s check)`);
  console.log(`Emergency stop: ${config.strategy.emergencyStopLossPct}%`);
  console.log(`Max hold: ${config.strategy.maxHoldMs > 0 ? config.strategy.maxHoldMs + 'ms' : 'disabled'}`);
  console.log(`Add-on: disabled (one position per mint)`);
  console.log(`Executor: Pump AMM SDK direct (no Jupiter)`);
  console.log('================================================');

  const errors = validateConfig();
  if (errors.length) {
    console.error('Config errors:');
    errors.forEach((e) => console.error('  - ' + e));
    if (errors.some((e) => e.includes('LaserStream') || e.includes('HELIUS_API_KEY'))) {
      console.error('Critical config missing. Exiting.');
      process.exit(1);
    }
  }

  // ============ ??? ============
  const tokenRegistry = new TokenRegistry();
  const tradeLogger = new TradeLogger(tokenRegistry.db);

  // ============ ???? ============
  const priceTracker = new PriceTracker();
  const dumpDetector = new DumpDetector(tokenRegistry);
  const executor = new Executor();

  // v3.5: PoolStateCache - ??????????? Pump pool state
  // BUY ?????? swapSolanaState?80-150ms RPC?????? 0ms
  // v3.15: ? executor.cacheSdk????????? RPC????? stakedRpc ??
  if (!config.DRY_RUN && executor.cacheSdk && executor.keypair) {
    const PoolStateCache = require('./core/PoolStateCache');
    const poolStateCache = new PoolStateCache({
      onlineSdk: executor.cacheSdk,  // v3.15: ? cacheSdk ??? onlineSdk
      user: executor.keypair.publicKey,
      getMintList: () => {
        return tokenRegistry.listActive()
          .filter((t) => t.pool_address)
          .map((t) => ({ mint: t.mint, poolAddress: t.pool_address }));
      },
    });
    executor.setPoolStateCache(poolStateCache);
    dumpDetector.setPoolStateCache(poolStateCache);
    poolStateCache.start();
  }

  // v3.17.31: ??? 5 ??????(??,??????)
  const postExitTracker = new PostExitTracker(priceTracker, tradeLogger, {
    windowMs: parseInt(process.env.POST_EXIT_WINDOW_MS || '300000', 10),
  });
  setInterval(() => {
    const stats = postExitTracker.getStats();
    monitor.set('PostExitTracker.activeTracking', stats.activeTracking, 'PostExitTracker');
    monitor.set('PostExitTracker.activeMints', stats.activeMints, 'PostExitTracker');
  }, 30_000);

  const positionManager = new PositionManager({
    tradeLogger,
    executor,
    priceTracker,
    tokenRegistry,
    postExitTracker,
  });
  // v3.17.7: tickStream ???? signalEngine ???signalEngine ???? latestSlot getter?
  const tickStream = new TickStream();
  // v3.17.11: PositionManager ?? tickStream.latestSlot ??? SLOT_EXIT
  positionManager.tickStream = tickStream;
  // v3.17.12: DumpDetector ?? sig ??????SS vs LS?
  dumpDetector._tickStream = tickStream;
  // v3.17.17: SS pre-warm ?? tokenRegistry ? base_vault ? mint ??
  tickStream.setTokenRegistry(tokenRegistry);

  // v3.17.17 (revised v2): RsiCalculator
  //   RSI_FILTER ????:
  //     off    ? ??,?? RSI ??(????????)
  //     peak   ? ????,??? 5s RSI > 92 ????????
  //     slope  ? ?"????"?? ?? ? sniper ??,???
  const RsiCalculator = require('./core/RsiCalculator');
  const rsiMode = process.env.RSI_FILTER || 'off';
  const rsiCalculator = (rsiMode === 'peak' || rsiMode === 'slope' || rsiMode === 'off')
    ? new RsiCalculator()
    : null;
  // v3.17.30: ?? RSI_FILTER=off ??? RsiCalculator (RECENT_PUMP ? buckets ??)
  // ??????? RSI ????? null
  if (rsiCalculator) {
    console.log(`[main] RSI filter enabled, mode=${rsiMode}`);
    if (rsiMode === 'slope') {
      console.warn('[main] ??  RSI_FILTER=slope conflicts with sniper strategy. Consider RSI_FILTER=peak or off.');
    }
    setInterval(() => rsiCalculator.cleanup(), 60_000);

    // v3.17.42: ?price_samples??RSI ? ??????30s???
    //   ???5min?samples??RsiCalculator??????4min?RSI????
    try {
      const warmupStart = Date.now() - 600000; // 5min?
      const warmupRows = tradeLogger.db.prepare(`
        SELECT mint, ts, price FROM price_samples 
        WHERE ts > ? ORDER BY ts ASC
      `).all(warmupStart);
      let fed = 0;
      for (const r of warmupRows) {
        rsiCalculator.feedTick(r.mint, r.price, r.ts);
        fed++;
      }
      console.log(`[main] RSI warmup: fed ${fed} price_samples from last 5min`);
    } catch (e) {
      console.warn('[main] RSI warmup failed:', e.message);
    }
  }

  // ============ EMA Service?EMA ??????? ============
      // EMA watch removed

  // ============ Competitor Tracker?????????? ============
  //   v3.17.32: ?? DailyReport ??????? competitorTracker
  //   ????????????????????? round-trip ????/??/?????
  //   ???? DumpDetector ? swapParsed ?????? RPC???? BUY ???
  //   ???? .env COMPETITOR_WALLETS????????????????????
  const competitorWallets = (process.env.COMPETITOR_WALLETS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const defaultCompetitors = [
    'BSHdFzWq6BfXpTx49LcCuvF4FVZakEZTibkKgjBcJqLD',
    '3fZftz6m8d37X5pBhnF4rHhgrG5hW8rsCKgdhtuPBf6u',
  ];
  const competitorTracker = new CompetitorTracker({
    db: tokenRegistry.db,
    addresses: competitorWallets.length > 0 ? competitorWallets : defaultCompetitors,
    dumpDetector,                              // ????????????????
    poolStateCache: executor.poolStateCache || null, // ?????? SOL ???
    fetchTokenInfo: async (mint) => {          // ??????FDV/???/24h????????
      try {
        const { fetchTokenFullInfo } = require('./utils/tokenMeta');
        const info = await fetchTokenFullInfo(mint);
        return { fdv: info.fdv, liquidity: info.liquidity, holders: info.holders ?? null, volume24h: info.volume24h };
      } catch (_) { return null; }
    },
    enrichEntry: (process.env.COMPETITOR_ENRICH ?? 'true').toLowerCase() === 'true',
    // ???????????"?????"???????? COMPETITOR_FOLLOW_SELL=true ????
    followSell: (process.env.COMPETITOR_FOLLOW_SELL ?? 'false').toLowerCase() === 'true',
    followSellMinWinRate: parseFloat(process.env.COMPETITOR_FOLLOW_SELL_MIN_WINRATE || '60'),
    followSellMinClosed: parseInt(process.env.COMPETITOR_FOLLOW_SELL_MIN_CLOSED || '10', 10),
  });
  const orderFlowTracker = new OrderFlowTracker();
  console.log(
    `[main] OrderFlow ${orderFlowTracker.enabled ? 'enabled' : 'disabled'}: ` +
      `window=${orderFlowTracker.windowMs}ms confirm=${orderFlowTracker.confirmWindowMs}ms ` +
      `minSell=${orderFlowTracker.minSellSol}SOL minDrop=${orderFlowTracker.minDropPct}% ` +
      `buy/sell>=${orderFlowTracker.minBuySellRatio} rebound=${orderFlowTracker.minReboundPct}-${orderFlowTracker.maxReboundPct}% ` +
      `replaceDump=${orderFlowTracker.replaceDumpSignal}`,
  );
  dumpDetector.on("swapParsed", (swap) => {
    try { competitorTracker.handleSwap(swap); } catch (_) { /* prevent CT errors from breaking DumpDetector */ }
    try { orderFlowTracker.handleSwap(swap); } catch (err) {
      console.warn(`[OrderFlow] handleSwap failed: ${err.message}`);
    }
  });

  // ============ ?? ============
  const dailyReport = new DailyReport({ tradeLogger, tokenRegistry, competitorTracker });
  dailyReport.start();

  // ?????? ? ??????? followSell=false????????
  //   ???????????????????????? TP/trailing????? eligible=true
  //   ???? + ????????? COMPETITOR_FOLLOW_SELL=true ?????
  competitorTracker.on('competitorSell', (sig) => {
    if (!sig.eligible) return; // ???? ? ???????/???? ? ???????
    const pids = positionManager.byMint.get(sig.mint);
    if (!pids || pids.size === 0) return; // ????????
    console.log(
      `[main] ?? FOLLOW_SELL ${sig.symbol || sig.mint.slice(0, 6)}: competitor ${sig.wallet.slice(0, 6)}.. ` +
        `(winRate ${sig.walletWinRatePct.toFixed(0)}%, n=${sig.walletClosedCount}) sold ? exiting our positions`,
    );
    for (const pid of pids) {
      const pos = positionManager.positions.get(pid);
      if (pos && !pos.exiting) {
        const px = positionManager.priceTracker.getPrice(sig.mint) || pos.entryPrice;
        positionManager._exit(pos, px, 'COMPETITOR_FOLLOW_SELL');
      }
    }
  });

  // ============ Signal Engine ============
  const signalEngine = new SignalEngine({
    tradeLogger,
    positionManager,
    tickStream,
    dumpDetector,
    rsiCalculator,  // v3.17.17: ?? null,SignalEngine ????? RSI ??
    poolStateCache: executor.poolStateCache || null,  // v3.17.21: ????? addHot
    tokenRegistry,  // v3.26: ???? ? ? token age ??????
  });
  // v3.17.41: PositionManager blacklist needs signalEngine reference
  positionManager.signalEngine = signalEngine;
  orderFlowTracker.on('flowReversalSignal', (signal) => {
    Promise.resolve(signalEngine.handleDumpSignal(signal)).catch((err) => {
      console.error(`[OrderFlow] SignalEngine error: ${err.message}`);
    });
  });

  // ============ ??? ============
  const server = new Server({
    tokenRegistry,
    tradeLogger,
    positionManager,
    signalEngine,
    dailyReport,
    competitorTracker,
    onTokenListChanged: () => {
      const mints = tokenRegistry.listActive().map((t) => t.mint);
      tickStream.updateSubscription(mints);
      // v2: ?? EMA ????
    },
    onTokenAdded: async (token) => {
      // ???? ? ????? pool ??
      if (config.autoFillPoolsOnStart) {
        fillPoolForToken(tokenRegistry, token.mint).catch((err) => {
          console.warn(`[onTokenAdded] fillPool failed for ${token.symbol || token.mint.slice(0,8)}: ${err.message}`);
        });
      }
      // v2: ???? EMA ??
    },
  });

  // ============ ????????? ============
  const restored = positionManager.restoreFromDb();
  if (restored.length > 0) {
    console.log(`[main] restored ${restored.length} open position(s) from db`);
    monitor.inc('main.restoredPositions', restored.length, 'main');
  }

  // ============ Token Watchdog????? + FDV/LP ????? ============
  const tokenWatchdog = new TokenWatchdog({
    tokenRegistry,
    positionManager,
    poolStateCache: executor.poolStateCache || null,
    tradeLogger: tradeLogger, // v3.17.41: 24h no-buy filter
    onTokenRemoved: () => {
      const mints = tokenRegistry.listActive().map((t) => t.mint);
      tickStream.updateSubscription(mints);
      // v2: ?? EMA ????
    },
  });
  tokenWatchdog.start();

  // Competitor stats periodic logging + cleanup (tracker created earlier, before Server)
  setInterval(() => competitorTracker.cleanupExpiredLots(), 10 * 60_000);
  setInterval(() => {
    const stats = competitorTracker.getAllStats();
    for (const s of stats) {
      if (s.buyCount === 0 && s.sellCount === 0) continue;
      console.log(
        `[CompetitorTracker] ?? ${s.wallet.slice(0, 8)}..${s.label ? ` (${s.label})` : ''}: ` +
          `${s.closedCount} round-trips, win ${s.winRatePct.toFixed(0)}%, ` +
          `totalPnL=${s.totalPnlSol >= 0 ? '+' : ''}${s.totalPnlSol.toFixed(3)} SOL, ` +
          `avgPnL=${s.avgPnlPct.toFixed(1)}%, avgHold=${(s.avgHoldMs / 1000).toFixed(0)}s, ` +
          `openLots=${s.openLots}`,
      );
      // ?????????????? MIN_SELL_SOL / MIN_PRICE_IMPACT_PCT?
      const e = competitorTracker.getEntryStats(s.wallet);
      if (e && e.n > 0) {
        const f = (x) => (x == null ? '?' : x.toFixed(1));
        console.log(
          `[CompetitorTracker] ?? entry(n=${e.n}): trigger sell ${f(e.triggerSellSol.min)}/${f(e.triggerSellSol.avg)}/${f(e.triggerSellSol.max)} SOL (min/avg/max), ` +
            `impact ${f(e.triggerImpactPct.min)}/${f(e.triggerImpactPct.avg)}/${f(e.triggerImpactPct.max)}%, ` +
            `poolLP avg ${f(e.poolLpSol.avg)} SOL, FDV avg $${e.fdvUsd.avg ? Math.round(e.fdvUsd.avg) : '?'}, ` +
            `holders avg ${e.avgHolders ? Math.round(e.avgHolders) : '?'}` +
            ` | our thresholds: MIN_SELL=${config.strategy.minSellSol} MIN_IMPACT=${config.strategy.minPriceImpactPct}%`,
        );
      }
    }
  }, 3600_000);

  // ============ v3.35: ??????24h??? ============
  // ???24?????????????????is_active=0?
  const TOKEN_MAX_AGE_MS = parseInt(process.env.TOKEN_MAX_AGE_MS || '86400000', 10); // ??24h
  setInterval(() => {
    const removed = tokenRegistry.removeStaleByAge(TOKEN_MAX_AGE_MS);
    if (removed > 0) {
      // ?? TickStream ? PoolStateCache ??
      if (tickStream && tickStream.watchedMints) {
        const activeMints = tokenRegistry.listActive().map(t => t.mint);
        // TickStream ???? tick ?????
      }
    }
  }, 300_000); // ?5??????

  // ============ ???? pool ???? 60 ?????? ============
  // ?? onTokenAdded ? PoolFinder ?????????? pool
  setInterval(() => {
    const missing = tokenRegistry.listActive().filter(t => !t.pool_address);
    if (missing.length === 0) return;
    console.log(`[pool-refill] ${missing.length} token(s) missing pool info, filling...`);
    for (const t of missing) {
      fillPoolForToken(tokenRegistry, t.mint).then(() => {
        const fresh = tokenRegistry.getToken(t.mint);
        if (fresh?.pool_address) {
          console.log(`[pool-refill] ${t.symbol || t.mint.slice(0,8)} pool filled`);
        }
      }).catch(() => {});
    }
  }, 60_000);

  // ============ ???? / ?? ============
  const alertChecker = new AlertChecker({
    monitor,
    tickStream,
    executor,
    positionManager,
    tokenRegistry,
    config,
  });
  alertChecker.start();

  monitor.on('alert', (alert) => {
    console.error(`[ALERT] [${alert.severity.toUpperCase()}] ${alert.name}: ${alert.message}`);
    server.broadcast({ type: 'alert', alert });
  });
  monitor.on('alertCleared', (alert) => {
    console.log(`[ALERT] cleared: ${alert.name}`);
    server.broadcast({ type: 'alertCleared', alert });
  });

  // ============ ???? ============

  tickStream.on('transaction', (tx) => dumpDetector.handleTransaction(tx));

  // ============ v3.17.23: VaultBalanceWatcher ============
  // ????? vault ??????????? Jupiter ??????
  if (!config.DRY_RUN && executor.rpc) {
    const VaultBalanceWatcher = require('./core/VaultBalanceWatcher');
    const vaultWatcher = new VaultBalanceWatcher({
      connection: executor.rpc,
      tokenRegistry,
    });
    vaultWatcher.on('vaultSell', (info) => {
      // v3.17.23: VaultWatcher ????????????
      // ????????VaultWatcher ? impact ????????????????
      // ???????????????????? impact ???
      // ?? priceTick ?? + PoolStateCache ?? + ????
      monitor.inc('VaultWatcher.vaultSellDetected', 1, 'VaultWatcher');

      // ??? PriceTracker
      if (info.priceAfter > 0) {
        priceTracker.update(info.mint, info.priceAfter, info.ts, info.poolAddress);
      }

      // ?? PoolStateCache
      if (executor.poolStateCache && info.poolAddress) {
        executor.poolStateCache.refreshOne(info.poolAddress).catch(() => {});
        // ???? hotMints ???????????
        if (!executor.poolStateCache.hotMints.has(info.mint)) {
          executor.poolStateCache.addHot(info.mint, info.poolAddress, false); // isPosition=false ? ?????
        }
      }
    });
    vaultWatcher.start();
    vaultWatcher.setTickStream(tickStream);
    // token ????? watch list
    tokenRegistry.on?.('changed', () => vaultWatcher.markDirty());
  }

  // ?????????????????????????????????????????????????????????????????
  // v3.17.17: SS Pre-warm ???
  // ?????????????????????????????????????????????????????????????????
  // ShredStream ? LaserStream ? 50-200ms (?? ssLeadCounters ????)?
  // SS ??? sell instruction ????? pool state RPC refresh,
  // ? LaserStream ??? tx ?? BUY ?,Executor ? cache ???? hit,
  // ?? 80-150ms ? RPC ?? ? BUY ?? 1 ? slot ???
  //
  // ??:
  //   - ??? buyOrder,? refresh (SS ?? tx ? meta,?????? sellSol/impact)
  //   - dedup 1s ?? pool ??? refresh (1 ? pool 1s ??????????)
  // ?????????????????????????????????????????????????????????????????
  const _prewarmDedup = new Map(); // poolAddress ? lastRefreshTs
  const PREWARM_DEDUP_MS = parseInt(process.env.SS_PREWARM_DEDUP_MS || '1000', 10);

  tickStream.on('prewarmSignal', (signal) => {
    if (!executor.poolStateCache || !signal.poolAddress) return;
    const now = Date.now();
    const last = _prewarmDedup.get(signal.poolAddress) || 0;
    if (now - last < PREWARM_DEDUP_MS) return;
    _prewarmDedup.set(signal.poolAddress, now);

    // ?? refresh,??? SS loop
    executor.poolStateCache.refreshOne(signal.poolAddress).then(() => {
      monitor.inc('main.prewarmHit', 1, 'main');
    }).catch(() => {
      // ???? (cache miss/RPC ????,?? 5s ?????)
      monitor.inc('main.prewarmFail', 1, 'main');
    });

    if (process.env.SS_PREWARM_DEBUG === 'true') {
      console.log(
        `[main] ?? SS pre-warm ? refresh pool ${signal.poolAddress.slice(0, 6)}.. ` +
        `(${signal.symbol || signal.mint.slice(0, 6)}, min_quote=${signal.minQuoteOutSol.toFixed(2)} SOL)`,
      );
    }
  });

  // v3.34: SS ?????? ? ShredStream ???? mint ? Pump AMM ???
  // ????? tokenRegistry + ?? LS ?????????????
  // ??: ??????? ? MIN_SELL_SOL ????????????
  const SS_NEW_MINT_MIN_SELL_SOL = parseFloat(process.env.SS_NEW_MINT_MIN_SELL_SOL || process.env.MIN_SELL_SOL || '20');
  const _newMintDedup = new Map(); // mint ? lastAddTs
  const NEW_MINT_DEDUP_MS = 60000; // ?? mint 60s ???? add
  tickStream.on('newMintDiscovered', (info) => {
    if (!info.mint) return;
    // ?????????????????????
    if (info.minQuoteOutSol < SS_NEW_MINT_MIN_SELL_SOL) return;
    const now = Date.now();
    const lastAdd = _newMintDedup.get(info.mint) || 0;
    if (now - lastAdd < NEW_MINT_DEDUP_MS) return;
    _newMintDedup.set(info.mint, now);

    // ???? tokenRegistry ????? LS ??????? pool ???
    const existing = tokenRegistry.getToken(info.mint);
    if (existing?.pool_address) {
      // ??????????????
      return;
    }

    console.log(
      `[main] ?? SS discovered new mint: ${info.mint.slice(0, 8)}.. ` +
      `pool=${info.poolAddress?.slice(0, 6)}.. min_quote=${info.minQuoteOutSol.toFixed(2)} SOL slot=${info.slot}`,
    );

    // ?? prewarm pool cache??? addToken ???
    // ?? VaultWatcher ??? dump ??buy ???? ready
    if (info.poolAddress && executor.poolStateCache) {
      executor.poolStateCache.refreshOne(info.poolAddress).catch(() => {});
    }

    // ????? tokenRegistry
    tokenRegistry.addToken(info.mint, {
      symbol: null, // SS ???????addToken ?? Helius DAS ??
      source: 'shredstream',
    }).then((token) => {
      if (token) {
        // SS ?? sell instruction ??? pool ???????
        if (info.poolAddress) {
          tokenRegistry.setPoolInfo(info.mint, {
            poolAddress: info.poolAddress,
            poolBaseVault: info.poolBaseVault,
            poolQuoteVault: info.poolQuoteVault,
          });
        }
        const freshToken = tokenRegistry.getToken(info.mint);
        console.log(
          `[main] ?? SS auto-added ${freshToken?.symbol || info.mint.slice(0, 8)}.. to tokenRegistry ` +
          `(pool=${freshToken?.pool_address?.slice(0, 6)}..)`,
        );
        // ?? LS ?????? dump ???????
        const mints = tokenRegistry.listActive().map(t => t.mint);
        tickStream.updateSubscription(mints);
        // ?? VaultWatcher ??
        vaultWatcher?.markDirty?.();
      }
    }).catch((err) => {
      console.warn(`[main] ?? SS auto-add failed for ${info.mint.slice(0, 8)}..: ${err.message}`);
    });
  });

  // ???? prewarmDedup + newMintDedup (??????)
  setInterval(() => {
    const now = Date.now();
    for (const [k, ts] of _prewarmDedup) {
      if (now - ts > PREWARM_DEDUP_MS * 5) _prewarmDedup.delete(k);
    }
    for (const [k, ts] of _newMintDedup) {
      if (now - ts > NEW_MINT_DEDUP_MS * 5) _newMintDedup.delete(k);
    }
  }, 30_000);

  // v3.17.21: ?????????? 60 ??????
  let _lastLoopTick = Date.now();
  setInterval(() => { _lastLoopTick = Date.now(); }, 1000);

  // v3.17.21: ?????? ? ? 10 ?????,?????
  // v3.17.26: ???? 60s?10s?RSS ????,60s ?????
  // v3.17.26: ?????? 1500MB?800MB??? 1500 ??,Rust ??? 2GB ? OOM kill?
  // v3.17.26: ? rss>500MB ??
  setInterval(() => {
    const u = process.memoryUsage();
    const rssMB = Math.round(u.rss / 1e6);
    const posCount = positionManager?.positions?.size ?? 0;
    const loopLagMs = Math.max(0, Date.now() - _lastLoopTick - 1000);  // ?? 1s ???,?????
    console.log(
      `[MEM] rss=${rssMB}MB heap=${(u.heapUsed/1e6).toFixed(0)}MB ` +
      `ext=${(u.external/1e6).toFixed(0)}MB arrBuf=${(u.arrayBuffers/1e6).toFixed(0)}MB ` +
      `poolCache=${executor?.poolStateCache?.cache?.size ?? '?'} ` +
      `hotMints=${executor?.poolStateCache?.hotMints?.size ?? '?'} ` +
      `recentSells=${dumpDetector?._recentSells?.size ?? '?'} ` +
      `slotSells=${dumpDetector?._slotSells?.size ?? '?'} ` +
      `prices=${priceTracker?.prices?.size ?? '?'} ` +
      `suspicious=${priceTracker?.suspicious?.size ?? '?'} ` +
      `dedup=${tickStream?.dedup?.size() ?? '?'} ` +
      `queue=${tickStream?._msgQueue?.length ?? '?'} ` +
      `queueDrop=${tickStream?._queueDropped ?? '?'} ` +
      `openLots=${competitorTracker?.openLots?.size ?? '?'} ` +
      `positions=${posCount} ` +
      `loopLag=${loopLagMs}ms`
    );
    // v3.17.26?v3.27: RSS ???? ? 7?gRPC????~550MB, ???600MB???????
    // ???? 700MB???550 + 150MB???????Rust??????
    if (rssMB > 700) {
      console.error(`[MEM] ??  rss=${rssMB}MB > 700MB ? Rust native ??????,???`);
      monitor.fireAlert('main.rss_high', 'warn', `rss=${rssMB}MB > 700MB, Rust native ??????`, { rssMB });
    } else {
      monitor.clearAlert('main.rss_high');
    }
    // ?????? 800MB???550 + 250MB????3-4????????
    // ?????? 1000MB???OOM kill?????DB?????
    if (rssMB > 1000 && posCount > 0) {
      console.log(`[MEM] ?? rss=${rssMB}MB > 1000MB ?? ${posCount} ???,???????OOM ???,???? DB ???`);
      process.exit(0);
    }
    if (rssMB > 800 && posCount === 0) {
      console.log(`[MEM] ?? rss=${rssMB}MB > 800MB ???,??????? Rust ????`);
      process.exit(0);  // systemd Restart=always ?????
    }
  }, 10_000);

  dumpDetector.on('priceTick', ({ mint, price, ts, poolAddress, side, solVolume, poolQuoteAfter }) => {
    priceTracker.update(mint, price, ts, poolAddress);
    // v3.17.41: ?????????? (? handleDumpSignal ???????? priceTick)
    signalEngine._sampleLongPrice(mint, priceTracker.getPrice(mint));
    // v3.17.17: ? RSI - ? feedTrade ?? volume,RSI ?? volume-weighted aggregation
    if (rsiCalculator) {
      // v3.17.38-fix: poolQuoteAfter=0 ?? tokenRegistry.liquidity ??
      //   CPI/balanceOnly ????? poolQuoteAfter ? 0
      //   ?? RSI ? lastPoolQuoteSol ??? null ? rsi_pre_dump ???
      let effectivePoolQuoteSol = poolQuoteAfter;
      if ((!effectivePoolQuoteSol || effectivePoolQuoteSol <= 0) && tokenRegistry) {
        const ti = tokenRegistry.getToken(mint);
        if (ti && ti.liquidity) {
          effectivePoolQuoteSol = ti.liquidity / 170; // USD ? SOL
        }
      }
      if (side && solVolume > 0) {
        rsiCalculator.feedTrade(mint, price, solVolume, side.toLowerCase(), ts, effectivePoolQuoteSol);
      } else {
        rsiCalculator.feedTick(mint, price, ts);
      }
    }
  });

  // v3.17.17: ? sellAnalyzed ? feedTrade ??????? priceTick ??(priceTick ???? swap)
  // ?????? sellAnalyzed ? RSI ??

  // sellAnalyzed: ???"????"?????????????
  dumpDetector.on('sellAnalyzed', (info) => {
    if (info.passSize && info.passImpact && info.passLiquidity) return; // ? dumpSignal
    const halfSize = config.strategy.minSellSol * 0.5;
    const halfImpact = config.strategy.minPriceImpactPct * 0.5;
    if (info.sellSol < halfSize || info.priceImpactPct < halfImpact) return;
    // ?????????
    const reasons = [];
    if (!info.passSize) reasons.push(`size:${info.sellSol.toFixed(1)}<${config.strategy.minSellSol}`);
    if (!info.passImpact) {
      if (info.priceImpactPct < config.strategy.minPriceImpactPct) {
        reasons.push(`impact:${info.priceImpactPct.toFixed(1)}%<${config.strategy.minPriceImpactPct}%`);
      } else {
        reasons.push(`impact:${info.priceImpactPct.toFixed(1)}%>${config.strategy.maxPriceImpactPct}% (pool dead?)`);
      }
    }
    if (!info.passLiquidity) {
      reasons.push(`liq:${(info.poolQuoteAfter ?? 0).toFixed(0)} SOL<${config.strategy.minPoolQuoteSol}`);
    }
    tradeLogger.logSignal({
      ts: info.ts,
      mint: info.mint,
      symbol: info.symbol,
      kind: 'DUMP_DETECTED',
      sellSol: info.sellSol,
      priceImpactPct: info.priceImpactPct,
      seller: info.seller,
      sellerTx: info.signature,
      notes: `near-miss: ${reasons.join(', ')}`,
      accepted: false,
      rejectReason: reasons.join('; '),
    });
  });

  dumpDetector.on('dumpSignal', (signal) => {
    // v3.17.16: ?? refreshOne ??
    //   handleDumpSignal ? buyOrder ? executor.buy ???? microtask ???,
    //   refreshOne ? RPC(30-100ms)??????? BUY,?????????
    //   PoolStateCache ??????(POOL_STATE_REFRESH_MS=5000)???? cache ???
    //   ??????????????,? POOL_STATE_REFRESH_MS ?? 2000-3000?
    if (orderFlowTracker.enabled && orderFlowTracker.replaceDumpSignal) {
      orderFlowTracker.noteSuppressedDumpSignal(signal);
      return;
    }
    signalEngine.handleDumpSignal(signal);
  });

  // v3.17.15: RUG ?? ? ? slot 5+ ?????? > 5 SOL ? ??????
  //   v3.17.27: ?????? RUG_PULL_EXIT?????????
  dumpDetector.on('rugSignal', (rug) => {
    const mint = rug.mint;
    const pids = positionManager.byMint.get(mint);
    if (!pids || pids.size === 0) return; // ??????
    console.log(
      `[RUG] ?? RUG PULL detected on ${rug.symbol || mint.slice(0,6)}: ${rug.sellCount} sells, ${rug.sellSol.toFixed(1)} SOL, ${rug.sellers.length} sellers ? RUG_PULL_EXIT disabled, skipping`,
    );
    // RUG_PULL_EXIT ??? ? ???????? trailing/TP ???????
    // for (const pid of pids) {
    //   const pos = positionManager.positions.get(pid);
    //   if (pos && !pos.exiting) {
    //     const px = positionManager.priceTracker.getPrice(mint) || pos.entryPrice;
    //     positionManager._exit(pos, px, 'RUG_PULL_EXIT');
    //   }
    // }
  });

  // ============ buyOrder ? BUY ? register position ============
  signalEngine.on('buyOrder', async (order) => {
    console.log(`[main] buyOrder received: ${order.symbol || order.mint.slice(0,6)} mint=${order.mint.slice(0,8)}.. reason=${order.reason} sig=${order.signature?.slice(0,12)}..`);
    const _t0 = Date.now();
    const tokenInfo = tokenRegistry.getToken(order.mint);
    const _t1 = Date.now();

    // ???? positionId ?? BUY trade / position ?
    const positionId = crypto.randomUUID();

    // ??? mint ?? buy ??????? dumpSignal ????????
    signalEngine.markBuyInflight(order.mint);

    // v3.17.11: BUY ??????? slot??? SLOT_EXIT ??
    executor.setLatestSlot(tickStream.latestSlot || 0);

    // v3.17.27: ???? pool state ? ?? executor.buy cache hit
    //   ?? cache miss?executor.buy ???? RPC(80-180ms)?
    //   ????? refreshOne(30-80ms) ? state ?? cache?
    //   buy ??? cache hit ? state=0ms ? ???? ~150ms ?? ~60ms?
    const preBuyPoolAddr = tokenInfo?.pool_address;
    if (preBuyPoolAddr && executor.poolStateCache) {
      const cachedState = executor.poolStateCache.get(preBuyPoolAddr);
      if (!cachedState) {
        const tPre = Date.now();
        try { await executor.poolStateCache.refreshOne(preBuyPoolAddr); } catch (_) { /* ?? */ }
        monitor.set('main.preBuyRefreshMs', Date.now() - tPre, 'main');
      }
    }

    const _t2 = Date.now();
    let buyResult;
    try {
      buyResult = await executor.buy({
        mint: order.mint,
        symbol: order.symbol,
        sizeSol: order.sizeSol,
        priceAfter: order.priceAfter, // ?? DRY_RUN ??
        baseDecimals: order.baseDecimals ?? tokenInfo?.decimals ?? 6,
        poolAddress: tokenInfo?.pool_address, // Pump SDK ?? pool address
      });
    } finally {
      signalEngine.markBuyDone(order.mint);
    }
    if (order._signalReceivedAt && buyResult && buyResult.success) {
      console.log('[main] buyOrder_timing: getToken=%dms preBuy=%dms buy=%dms', _t1-_t0, _t2-_t1, Date.now()-_t2);
    }

    // v3.17.16: ??????? ? ??????????????????
    //   signalToBuyMs: ??? tx ???? BUY ??????
    //   inEngineMs: ?? tx ?? SignalEngine ? emit buyOrder
    //   buyLatencyMs: executor.buy ????(? cache + ?? + ??)
    //   ??: signalToBuyMs ? 400ms (1 slot), buyLatencyMs ? 150ms
    if (order._signalReceivedAt && buyResult.success) {
      const signalToBuyMs = Date.now() - order._signalReceivedAt;
      const fromDumpTsMs = order.ts ? Date.now() - order.ts : null;
      console.log(
        `[main] ?  ${order.symbol || order.mint.slice(0, 6)} latency: ` +
        `signal?BUY=${signalToBuyMs}ms` +
        (fromDumpTsMs !== null ? ` dumpTs?BUY=${fromDumpTsMs}ms` : '') +
        ` (buy.latency=${buyResult.latencyMs}ms, state=${buyResult.stateLatencyMs}ms, send=${buyResult.sendLatencyMs}ms)`,
      );
    }

    // ?? BUY trade???? positionId?
    if (!order.mint) {
      console.error(`[main] BUG: buyOrder with null mint! order=`, JSON.stringify(order).slice(0, 200));
      return;
    }
    tradeLogger.logTrade({
      positionId,
      ts: Date.now(),
      mint: order.mint,
      symbol: order.symbol,
      side: 'BUY',
      solAmount: buyResult.solIn ?? order.sizeSol,
      tokenAmount: buyResult.tokenAmount,
      price: buyResult.price,
      signature: buyResult.signature,
      success: buyResult.success,
      dryRun: config.DRY_RUN,
      reason: order.reason,
      latencyMs: buyResult.latencyMs,
      error: buyResult.error,
    });

    if (!buyResult.success) {
      console.error(
        `[main] BUY failed for ${order.symbol || order.mint.slice(0, 6)}: ${buyResult.error}`,
      );
      // v3.26: pool dead/low-liquidity/mint-mismatch ? 24h ??????????? fee
      if (buyResult.poolDead || buyResult.poolLowLiquidity || buyResult.poolMintMismatch) {
        const cooldownMs = parseInt(process.env.POOL_FAIL_REBUY_COOLDOWN_MS || '86400000', 10);
        signalEngine._exitCooldowns.set(order.mint, Date.now() + cooldownMs);
        console.log(
          `[main] ?? Pool fail cooldown ${order.symbol || order.mint.slice(0, 6)} for ${Math.round(cooldownMs / 3600000)}h (poolDead=${!!buyResult.poolDead} poolLowLiq=${!!buyResult.poolLowLiquidity} mintMismatch=${!!buyResult.poolMintMismatch})`,
        );
      }
      return;
    }

    // ????????? entry_price????? v1 bug???? trigger ??
    // v3.17.21: ????? FDV / pool / liquidity????????????
    const entryFdv = tokenInfo?.fdv ?? null;
    const entryLiquidity = tokenInfo?.liquidity ?? null;
    const entryPoolSol = order.poolQuoteAfter ?? tokenInfo?.liquidity ?? null; // dumpSignal.poolQuoteAfter ???

    // v3.17.39: ?????????????????????
    let mintAgeAtBuySec = null;
    try {
      const firstSignal = tradeLogger.db.prepare(
        'SELECT MIN(ts) as ts FROM signals WHERE mint = ?'
      ).get(order.mint);
      if (firstSignal && firstSignal.ts) {
        mintAgeAtBuySec = Math.round((Date.now() - firstSignal.ts) / 1000);
      }
    } catch (_) {}

    positionManager.registerOpen({
      positionId,
      mint: order.mint,
      symbol: order.symbol,
      entrySol: buyResult.solIn ?? order.sizeSol,
      entryPrice: buyResult.price,         // ?????
      tokenAmount: buyResult.tokenAmount,  // ???????
      dryRun: config.DRY_RUN,
      signature: buyResult.signature,
      buyFeeLamports: buyResult.priorityFeeLamports || 0,  // v3.4: ???? PnL
      buySlot: buyResult.buySlot || 0,  // v3.17.11: BUY ???? slot
      dumpSlot: order.slot || 0,        // v3.17.19: ??? slot,??? BUY ?????? slot
      entryFdv,                          // v3.17.21: ???? FDV
      entryPoolSol,                      // v3.17.21: ?????? SOL
      entryLiquidity,                    // v3.17.21: ??????? USD
      sellCount10s: order._sellCount10s || 1,   // v3.17.36: ?????
      totalSellSol10s: order._totalSellSol10s || order.sellSol, // v3.17.36: ?????
      mintAgeAtBuySec,                           // v3.17.39: ????????
      rsiPreDump: order.rsiPreDump,              // v3.17.38: ??? RSI5s
      rsi1sPreDump: order.rsi1sPreDump,          // v3.17.38: ??? RSI1s
      rsi30sPreDump: order.rsi30sPreDump,        // v3.17.42: ??? RSI30s
      isEmaStrategy: false,  // EMA removed
      isAddOn: order._isAddOn || false,                 // ????
    });


    // ???? PriceTracker???????? entry baseline
    // ?????? LaserStream tx ????????? TP?
    priceTracker.forceSet(order.mint, buyResult.price);

    if (buyResult.signature) signalEngine.registerOurSignature(buyResult.signature);
  });

  positionManager.on('opened', (pos) =>
    server.broadcast({ type: 'positionOpened', position: pos }),
  );
  positionManager.on('closed', (pos) => {
    // v3.17.15: ?????????????K???
    signalEngine.lastTriggerTs.set(pos.mint, Date.now());
    server.broadcast({ type: 'positionClosed', position: pos });
  });

  // ============ ????? ============
  server.start();

  // ============ ????? pool ???????? ============
  if (config.autoFillPoolsOnStart) {
    backgroundFillPools(tokenRegistry).catch((err) =>
      console.error(`[main] backgroundFillPools error: ${err.message}`),
    );
  }

  // ============ ????? ============
  const initialMints = tokenRegistry.listActive().map((t) => t.mint);
  console.log(`[main] starting LaserStream with ${initialMints.length} initial tokens`);
  await tickStream.start(initialMints);

  // ============ ???? ============
  const shutdown = async (signal) => {
    console.log(`\n[main] ${signal} received, shutting down gracefully...`);
    try {
      await tickStream.stop();
      postExitTracker.shutdown();
      positionManager.stop();
      tokenWatchdog.stop();
      alertChecker.stop();
      monitor.stop();
      executor.stop && executor.stop();
      await new Promise((r) => setTimeout(r, 200));
    } catch (err) {
      console.error(`[main] shutdown error: ${err.message}`);
    }
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  process.on('uncaughtException', (err) => {
    if (err.code === 'EADDRINUSE') { console.warn('[main] port conflict, dashboard disabled - continuing'); return; }
    monitor.recordError('main', err, { phase: 'uncaughtException' });
    monitor.inc('main.uncaughtExceptions', 1, 'main');
    console.error('[main] uncaughtException:', err);
  });
  process.on('unhandledRejection', (reason) => {
    monitor.recordError('main', reason instanceof Error ? reason : new Error(String(reason)), {
      phase: 'unhandledRejection',
    });
    monitor.inc('main.unhandledRejections', 1, 'main');
    console.error('[main] unhandledRejection:', reason);
  });

  console.log('[main] startup complete');

  // v3.27: ??3????????? Rust native ?????? slot gap ??
  // ??RSS ~550MB (7?gRPC??)?3????? ~800MB ? slot gap ?????
  // ??? restoreFromDb ??????????????RSS>1000MB???
  const MAX_UPTIME_MS = parseInt(process.env.MAX_UPTIME_MS || '10800000', 10); // ??3??
  const startTime = Date.now();
  setInterval(() => {
    const uptimeMs = Date.now() - startTime;
    const posCount = positionManager?.positions?.size ?? 0;
    if (uptimeMs > MAX_UPTIME_MS && posCount === 0) {
      console.log(`[MEM] ?? uptime=${Math.round(uptimeMs/60000)}min > ${Math.round(MAX_UPTIME_MS/60000)}min ???, ?????? Rust native ??`);
      process.exit(0);
    } else if (uptimeMs > MAX_UPTIME_MS && posCount > 0) {
      console.log(`[MEM] ? uptime=${Math.round(uptimeMs/60000)}min > ${Math.round(MAX_UPTIME_MS/60000)}min ?? ${posCount} ???, ? RSS ??????????`);
    }
  }, 60_000);
}

/**
 * ???????? pool ???????????
 * ????? 250ms?
 */
async function backgroundFillPools(tokenRegistry) {
  const targets = tokenRegistry
    .listAll()
    .filter((t) => t.is_active && (!t.pool_address || !t.pool_base_vault || !t.pool_quote_vault));

  if (targets.length === 0) return;
  console.log(`[main] auto-fill pool for ${targets.length} tokens (background)`);

  const finder = new PoolFinder({});
  let ok = 0;
  let fail = 0;

  for (const t of targets) {
    try {
      const result = await finder.findPoolForMint(t.mint);
      if (result) {
        tokenRegistry.setPoolInfo(t.mint, result);
        ok += 1;
      } else {
        fail += 1;
      }
    } catch (err) {
      fail += 1;
      console.warn(`[fill-pools] ${t.symbol || t.mint.slice(0, 6)}: ${err.message}`);
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  console.log(`[main] auto-fill pool done: ${ok} OK, ${fail} failed`);
}

async function fillPoolForToken(tokenRegistry, mint) {
  try {
    const finder = new PoolFinder({});
    const result = await finder.findPoolForMint(mint);
    if (result) {
      tokenRegistry.setPoolInfo(mint, result);
      console.log(
        `[fill-pools] ${mint.slice(0, 6)}: pool=${result.poolAddress.slice(0, 6)}..`,
      );
    }
  } catch (err) {
    console.warn(`[fill-pools] ${mint.slice(0, 6)}: ${err.message}`);
  }
}

main().catch((err) => {
  console.error('[main] fatal error:', err);
  process.exit(1);
});

// v3.32b: ?????? ? ?? heap vs external vs arrayBuffers
setInterval(() => {
  const m = process.memoryUsage();
  console.log(`[MEM] rss=${(m.rss/1048576)|0}MB heapUsed=${(m.heapUsed/1048576)|0}MB external=${(m.external/1048576)|0}MB arrayBuffers=${(m.arrayBuffers/1048576)|0}MB`);
}, 30000);
