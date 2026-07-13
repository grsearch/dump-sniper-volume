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
  console.log('🎯 Dump Sniper V3.17.20 starting...');
  console.log(`Mode: ${config.DRY_RUN ? 'DRY_RUN' : '⚠️  LIVE TRADING ⚠️'}`);
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

  // ============ 数据层 ============
  const tokenRegistry = new TokenRegistry();
  const tradeLogger = new TradeLogger(tokenRegistry.db);

  // ============ 核心引擎 ============
  const priceTracker = new PriceTracker();
  const dumpDetector = new DumpDetector(tokenRegistry);
  const executor = new Executor();

  // v3.5: PoolStateCache - 后台预热所有监控代币的 Pump pool state
  // BUY 路径不再阻塞 swapSolanaState（80-150ms RPC），从内存读 0ms
  // v3.15: 用 executor.cacheSdk（独立实例，走普通 RPC），不占用 stakedRpc 通道
  if (!config.DRY_RUN && executor.cacheSdk && executor.keypair) {
    const PoolStateCache = require('./core/PoolStateCache');
    const poolStateCache = new PoolStateCache({
      onlineSdk: executor.cacheSdk,  // v3.15: 用 cacheSdk 而不是 onlineSdk
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

  // v3.17.31: 平仓后 5 分钟价格追踪(旁路,不影响主路径)
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
  // v3.17.7: tickStream 必须先于 signalEngine 创建（signalEngine 需要它的 latestSlot getter）
  const tickStream = new TickStream();
  // v3.17.11: PositionManager 需要 tickStream.latestSlot 来判断 SLOT_EXIT
  positionManager.tickStream = tickStream;
  // v3.17.12: DumpDetector 查询 sig 的首次来源（SS vs LS）
  dumpDetector._tickStream = tickStream;
  // v3.17.17: SS pre-warm 需要 tokenRegistry 做 base_vault → mint 反查
  tickStream.setTokenRegistry(tokenRegistry);

  // v3.17.17 (revised v2): RsiCalculator
  //   RSI_FILTER 三种模式:
  //     off    — 默认,不建 RSI 实例(强烈推荐先跑这个)
  //     peak   — 兜底模式,只拒绝 5s RSI > 92 的「山顶假砸盘」
  //     slope  — 旧"反弹起点"模式 ⚠️ 跟 sniper 矛盾,不推荐
  const RsiCalculator = require('./core/RsiCalculator');
  const rsiMode = process.env.RSI_FILTER || 'off';
  const rsiCalculator = (rsiMode === 'peak' || rsiMode === 'slope' || rsiMode === 'off')
    ? new RsiCalculator()
    : null;
  // v3.17.30: 即使 RSI_FILTER=off 也需要 RsiCalculator (RECENT_PUMP 用 buckets 数据)
  // 只有完全不需要 RSI 数据时才设 null
  if (rsiCalculator) {
    console.log(`[main] RSI filter enabled, mode=${rsiMode}`);
    if (rsiMode === 'slope') {
      console.warn('[main] ⚠️  RSI_FILTER=slope conflicts with sniper strategy. Consider RSI_FILTER=peak or off.');
    }
    setInterval(() => rsiCalculator.cleanup(), 60_000);

    // v3.17.42: 从price_samples预热RSI — 重启后立即有30s桶数据
    //   取最近5min的samples喂给RsiCalculator，避免重启后4min内RSI过滤失效
    try {
      const warmupStart = Date.now() - 600000; // 5min前
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

  // ============ EMA Service（EMA 砸单买入策略） ============
      // EMA watch removed

  // ============ Competitor Tracker（竞争对手钱包分析） ============
  //   v3.17.32: 移到 DailyReport 之前，以便注入 competitorTracker
  //   追踪指定钱包在我们监控代币上的买卖，配对成 round-trip 统计盈亏/胜率/持仓时长。
  //   数据复用 DumpDetector 的 swapParsed 事件，零额外 RPC、不影响 BUY 延迟。
  //   地址可在 .env COMPETITOR_WALLETS（逗号分隔）配置；默认内置用户给的两个。
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
    dumpDetector,                              // 零成本进场特征（触发砸单上下文）
    poolStateCache: executor.poolStateCache || null, // 买入瞬间池子 SOL 流动性
    fetchTokenInfo: async (mint) => {          // 代币侧特征（FDV/流动性/24h量），异步不阻塞
      try {
        const { fetchTokenFullInfo } = require('./utils/tokenMeta');
        const info = await fetchTokenFullInfo(mint);
        return { fdv: info.fdv, liquidity: info.liquidity, holders: info.holders ?? null, volume24h: info.volume24h };
      } catch (_) { return null; }
    },
    enrichEntry: (process.env.COMPETITOR_ENRICH ?? 'true').toLowerCase() === 'true',
    // 跟卖默认关闭（用户选择"只记录分析"）。看完数据后设 COMPETITOR_FOLLOW_SELL=true 即启用。
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

  // ============ 报告 ============
  const dailyReport = new DailyReport({ tradeLogger, tokenRegistry, competitorTracker });
  dailyReport.start();

  // 竞争对手卖出 → 可选跟卖（默认 followSell=false，仅记录分析）。
  //   优先级最高：他们一卖，我们若持有同币立即卖（早于 TP/trailing），但仅当 eligible=true
  //   （高胜率 + 足够样本的钱包）且 COMPETITOR_FOLLOW_SELL=true 时才执行。
  competitorTracker.on('competitorSell', (sig) => {
    if (!sig.eligible) return; // 关闭跟卖 或 该钱包未达胜率/样本门槛 → 只记录，不动作
    const pids = positionManager.byMint.get(sig.mint);
    if (!pids || pids.size === 0) return; // 我们没持有这个币
    console.log(
      `[main] 🔁 FOLLOW_SELL ${sig.symbol || sig.mint.slice(0, 6)}: competitor ${sig.wallet.slice(0, 6)}.. ` +
        `(winRate ${sig.walletWinRatePct.toFixed(0)}%, n=${sig.walletClosedCount}) sold → exiting our positions`,
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
    rsiCalculator,  // v3.17.17: 可为 null,SignalEngine 内部会跳过 RSI 过滤
    poolStateCache: executor.poolStateCache || null,  // v3.17.21: 信号触发时 addHot
    tokenRegistry,  // v3.26: 新币策略 — 按 token age 区分过滤逻辑
  });
  // v3.17.41: PositionManager blacklist needs signalEngine reference
  positionManager.signalEngine = signalEngine;
  orderFlowTracker.on('flowReversalSignal', (signal) => {
    Promise.resolve(signalEngine.handleDumpSignal(signal)).catch((err) => {
      console.error(`[OrderFlow] SignalEngine error: ${err.message}`);
    });
  });

  // ============ 服务器 ============
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
      // v2: 同步 EMA 监控列表
    },
    onTokenAdded: async (token) => {
      // 新增代币 → 后台异步补 pool 信息
      if (config.autoFillPoolsOnStart) {
        fillPoolForToken(tokenRegistry, token.mint).catch((err) => {
          console.warn(`[onTokenAdded] fillPool failed for ${token.symbol || token.mint.slice(0,8)}: ${err.message}`);
        });
      }
      // v2: 新币加入 EMA 监控
    },
  });

  // ============ 启动恢复未平仓持仓 ============
  const restored = positionManager.restoreFromDb();
  if (restored.length > 0) {
    console.log(`[main] restored ${restored.length} open position(s) from db`);
    monitor.inc('main.restoredPositions', restored.length, 'main');
  }

  // ============ Token Watchdog（监控超时 + FDV/LP 自动移除） ============
  const tokenWatchdog = new TokenWatchdog({
    tokenRegistry,
    positionManager,
    poolStateCache: executor.poolStateCache || null,
    tradeLogger: tradeLogger, // v3.17.41: 24h no-buy filter
    onTokenRemoved: () => {
      const mints = tokenRegistry.listActive().map((t) => t.mint);
      tickStream.updateSubscription(mints);
      // v2: 同步 EMA 监控列表
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
        `[CompetitorTracker] 📊 ${s.wallet.slice(0, 8)}..${s.label ? ` (${s.label})` : ''}: ` +
          `${s.closedCount} round-trips, win ${s.winRatePct.toFixed(0)}%, ` +
          `totalPnL=${s.totalPnlSol >= 0 ? '+' : ''}${s.totalPnlSol.toFixed(3)} SOL, ` +
          `avgPnL=${s.avgPnlPct.toFixed(1)}%, avgHold=${(s.avgHoldMs / 1000).toFixed(0)}s, ` +
          `openLots=${s.openLots}`,
      );
      // 进场阈值反推（对照我们自己的 MIN_SELL_SOL / MIN_PRICE_IMPACT_PCT）
      const e = competitorTracker.getEntryStats(s.wallet);
      if (e && e.n > 0) {
        const f = (x) => (x == null ? '?' : x.toFixed(1));
        console.log(
          `[CompetitorTracker] 🔬 entry(n=${e.n}): trigger sell ${f(e.triggerSellSol.min)}/${f(e.triggerSellSol.avg)}/${f(e.triggerSellSol.max)} SOL (min/avg/max), ` +
            `impact ${f(e.triggerImpactPct.min)}/${f(e.triggerImpactPct.avg)}/${f(e.triggerImpactPct.max)}%, ` +
            `poolLP avg ${f(e.poolLpSol.avg)} SOL, FDV avg $${e.fdvUsd.avg ? Math.round(e.fdvUsd.avg) : '?'}, ` +
            `holders avg ${e.avgHolders ? Math.round(e.avgHolders) : '?'}` +
            ` | our thresholds: MIN_SELL=${config.strategy.minSellSol} MIN_IMPACT=${config.strategy.minPriceImpactPct}%`,
        );
      }
    }
  }, 3600_000);

  // ============ v3.35: 自动移除超过24h的老币 ============
  // 只监控24小时内的新币，超过自动从监控移除（is_active=0）
  const TOKEN_MAX_AGE_MS = parseInt(process.env.TOKEN_MAX_AGE_MS || '86400000', 10); // 默认24h
  setInterval(() => {
    const removed = tokenRegistry.removeStaleByAge(TOKEN_MAX_AGE_MS);
    if (removed > 0) {
      // 通知 TickStream 和 PoolStateCache 更新
      if (tickStream && tickStream.watchedMints) {
        const activeMints = tokenRegistry.listActive().map(t => t.mint);
        // TickStream 会在下次 tick 时自动重建
      }
    }
  }, 300_000); // 每5分钟检查一次

  // ============ 定期补缺 pool 信息（每 60 秒扫描一次） ============
  // 防止 onTokenAdded 时 PoolFinder 失败导致代币永远没有 pool
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

  // ============ 健康监控 / 告警 ============
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

  // ============ 事件连线 ============

  tickStream.on('transaction', (tx) => dumpDetector.handleTransaction(tx));

  // ============ v3.17.23: VaultBalanceWatcher ============
  // 直接查链上 vault 余额变化检测砸单，不受 Jupiter 聚合路由影响
  if (!config.DRY_RUN && executor.rpc) {
    const VaultBalanceWatcher = require('./core/VaultBalanceWatcher');
    const vaultWatcher = new VaultBalanceWatcher({
      connection: executor.rpc,
      tokenRegistry,
    });
    vaultWatcher.on('vaultSell', (info) => {
      // v3.17.23: VaultWatcher 检测到的卖单作为辅助信号
      // 不直接触发买入！VaultWatcher 的 impact 计算是基于快照间隔内的累计变化，
      // 无法区分单笔大卖单和多笔小卖单累积，导致 impact 虚高。
      // 只做 priceTick 喂价 + PoolStateCache 预热 + 日志记录
      monitor.inc('VaultWatcher.vaultSellDetected', 1, 'VaultWatcher');

      // 喂价给 PriceTracker
      if (info.priceAfter > 0) {
        priceTracker.update(info.mint, info.priceAfter, info.ts, info.poolAddress);
      }

      // 预热 PoolStateCache
      if (executor.poolStateCache && info.poolAddress) {
        executor.poolStateCache.refreshOne(info.poolAddress).catch(() => {});
        // 如果不在 hotMints 里，加进去（低频刷新）
        if (!executor.poolStateCache.hotMints.has(info.mint)) {
          executor.poolStateCache.addHot(info.mint, info.poolAddress, false); // isPosition=false → 信号币低频
        }
      }
    });
    vaultWatcher.start();
    vaultWatcher.setTickStream(tickStream);
    // token 变化时刷新 watch list
    tokenRegistry.on?.('changed', () => vaultWatcher.markDirty());
  }

  // ─────────────────────────────────────────────────────────────────
  // v3.17.17: SS Pre-warm 处理器
  // ─────────────────────────────────────────────────────────────────
  // ShredStream 比 LaserStream 快 50-200ms (实测 ssLeadCounters 已有数据)。
  // SS 解析出 sell instruction 后立即触发 pool state RPC refresh,
  // 等 LaserStream 推完整 tx 触发 BUY 时,Executor 读 cache 几乎一定 hit,
  // 省下 80-150ms 的 RPC 时间 → BUY 提早 1 个 slot 落链。
  //
  // 注意:
  //   - 不触发 buyOrder,只 refresh (SS 来的 tx 无 meta,不能可靠判断 sellSol/impact)
  //   - dedup 1s 内同 pool 不重复 refresh (1 个 pool 1s 内的多笔卖单变化很小)
  // ─────────────────────────────────────────────────────────────────
  const _prewarmDedup = new Map(); // poolAddress → lastRefreshTs
  const PREWARM_DEDUP_MS = parseInt(process.env.SS_PREWARM_DEDUP_MS || '1000', 10);

  tickStream.on('prewarmSignal', (signal) => {
    if (!executor.poolStateCache || !signal.poolAddress) return;
    const now = Date.now();
    const last = _prewarmDedup.get(signal.poolAddress) || 0;
    if (now - last < PREWARM_DEDUP_MS) return;
    _prewarmDedup.set(signal.poolAddress, now);

    // 异步 refresh,不阻塞 SS loop
    executor.poolStateCache.refreshOne(signal.poolAddress).then(() => {
      monitor.inc('main.prewarmHit', 1, 'main');
    }).catch(() => {
      // 静默失败 (cache miss/RPC 暂时不通,后续 5s 轮询也会刷)
      monitor.inc('main.prewarmFail', 1, 'main');
    });

    if (process.env.SS_PREWARM_DEBUG === 'true') {
      console.log(
        `[main] 🔥 SS pre-warm → refresh pool ${signal.poolAddress.slice(0, 6)}.. ` +
        `(${signal.symbol || signal.mint.slice(0, 6)}, min_quote=${signal.minQuoteOutSol.toFixed(2)} SOL)`,
      );
    }
  });

  // v3.34: SS 自动发现新币 — ShredStream 收到未知 mint 的 Pump AMM 卖单时
  // 自动添加到 tokenRegistry + 更新 LS 订阅，让后续信号走实时路径
  // 阈值: 只自动添加卖单 ≥ MIN_SELL_SOL 的新币（避免添加垃圾币）
  const SS_NEW_MINT_MIN_SELL_SOL = parseFloat(process.env.SS_NEW_MINT_MIN_SELL_SOL || process.env.MIN_SELL_SOL || '20');
  const _newMintDedup = new Map(); // mint → lastAddTs
  const NEW_MINT_DEDUP_MS = 60000; // 同一 mint 60s 内不重复 add
  tickStream.on('newMintDiscovered', (info) => {
    if (!info.mint) return;
    // 只自动添加大额砸单（和小额卖单不值得监控）
    if (info.minQuoteOutSol < SS_NEW_MINT_MIN_SELL_SOL) return;
    const now = Date.now();
    const lastAdd = _newMintDedup.get(info.mint) || 0;
    if (now - lastAdd < NEW_MINT_DEDUP_MS) return;
    _newMintDedup.set(info.mint, now);

    // 如果已在 tokenRegistry 里，只更新 LS 订阅（可能没有 pool 信息）
    const existing = tokenRegistry.getToken(info.mint);
    if (existing?.pool_address) {
      // 已有完整信息，不需要重新添加
      return;
    }

    console.log(
      `[main] 🆕 SS discovered new mint: ${info.mint.slice(0, 8)}.. ` +
      `pool=${info.poolAddress?.slice(0, 6)}.. min_quote=${info.minQuoteOutSol.toFixed(2)} SOL slot=${info.slot}`,
    );

    // 立即 prewarm pool cache（不等 addToken 完成）
    // 这样 VaultWatcher 检测到 dump 时，buy 路径已经 ready
    if (info.poolAddress && executor.poolStateCache) {
      executor.poolStateCache.refreshOne(info.poolAddress).catch(() => {});
    }

    // 异步添加到 tokenRegistry
    tokenRegistry.addToken(info.mint, {
      symbol: null, // SS 没有符号信息，addToken 会从 Helius DAS 获取
      source: 'shredstream',
    }).then((token) => {
      if (token) {
        // SS 已从 sell instruction 提取了 pool 信息，直接写入
        if (info.poolAddress) {
          tokenRegistry.setPoolInfo(info.mint, {
            poolAddress: info.poolAddress,
            poolBaseVault: info.poolBaseVault,
            poolQuoteVault: info.poolQuoteVault,
          });
        }
        const freshToken = tokenRegistry.getToken(info.mint);
        console.log(
          `[main] 🆕 SS auto-added ${freshToken?.symbol || info.mint.slice(0, 8)}.. to tokenRegistry ` +
          `(pool=${freshToken?.pool_address?.slice(0, 6)}..)`,
        );
        // 更新 LS 订阅，让后续 dump 信号走实时路径
        const mints = tokenRegistry.listActive().map(t => t.mint);
        tickStream.updateSubscription(mints);
        // 通知 VaultWatcher 刷新
        vaultWatcher?.markDirty?.();
      }
    }).catch((err) => {
      console.warn(`[main] 🆕 SS auto-add failed for ${info.mint.slice(0, 8)}..: ${err.message}`);
    });
  });

  // 定期清理 prewarmDedup + newMintDedup (避免内存泄漏)
  setInterval(() => {
    const now = Date.now();
    for (const [k, ts] of _prewarmDedup) {
      if (now - ts > PREWARM_DEDUP_MS * 5) _prewarmDedup.delete(k);
    }
    for (const [k, ts] of _newMintDedup) {
      if (now - ts > NEW_MINT_DEDUP_MS * 5) _newMintDedup.delete(k);
    }
  }, 30_000);

  // v3.17.21: 事件循环延迟检测（每 60 秒采样一次）
  let _lastLoopTick = Date.now();
  setInterval(() => { _lastLoopTick = Date.now(); }, 1000);

  // v3.17.21: 内存分类监控 — 每 10 秒打印一次,定位泄漏源
  // v3.17.26: 采样间隔 60s→10s（RSS 飙升极快,60s 可能漏检）
  // v3.17.26: 空仓重启阈值 1500MB→800MB（之前 1500 太晚,Rust 泄漏到 2GB 才 OOM kill）
  // v3.17.26: 加 rss>500MB 告警
  setInterval(() => {
    const u = process.memoryUsage();
    const rssMB = Math.round(u.rss / 1e6);
    const posCount = positionManager?.positions?.size ?? 0;
    const loopLagMs = Math.max(0, Date.now() - _lastLoopTick - 1000);  // 预期 1s 内更新,超出即延迟
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
    // v3.17.26→v3.27: RSS 阈值调整 — 7个gRPC连接基线~550MB, 旧阈值600MB等于启动即告警
    // 告警阈值 700MB（基线550 + 150MB余量，超过说明Rust泄漏已开始）
    if (rssMB > 700) {
      console.error(`[MEM] ⚠️  rss=${rssMB}MB > 700MB — Rust native 内存可能泄漏,监控中`);
      monitor.fireAlert('main.rss_high', 'warn', `rss=${rssMB}MB > 700MB, Rust native 内存可能泄漏`, { rssMB });
    } else {
      monitor.clearAlert('main.rss_high');
    }
    // 空仓重启阈值 800MB（基线550 + 250MB增长，约3-4小时正常泄漏量）
    // 有持仓硬上限 1000MB（避免OOM kill，重启后从DB恢复持仓）
    if (rssMB > 1000 && posCount > 0) {
      console.log(`[MEM] 🔄 rss=${rssMB}MB > 1000MB 且有 ${posCount} 个持仓,强制优雅重启（OOM 前清零,持仓会从 DB 恢复）`);
      process.exit(0);
    }
    if (rssMB > 800 && posCount === 0) {
      console.log(`[MEM] 🔄 rss=${rssMB}MB > 800MB 且空仓,优雅重启以释放 Rust 堆外内存`);
      process.exit(0);  // systemd Restart=always 会自动拉起
    }
  }, 10_000);

  dumpDetector.on('priceTick', ({ mint, price, ts, poolAddress, side, solVolume, poolQuoteAfter }) => {
    priceTracker.update(mint, price, ts, poolAddress);
    // v3.17.41: 采样价格到长窗口缓存 (比 handleDumpSignal 更频繁，覆盖所有 priceTick)
    signalEngine._sampleLongPrice(mint, priceTracker.getPrice(mint));
    // v3.17.17: 喂 RSI - 用 feedTrade 带上 volume,RSI 能做 volume-weighted aggregation
    if (rsiCalculator) {
      // v3.17.38-fix: poolQuoteAfter=0 时用 tokenRegistry.liquidity 推算
      //   CPI/balanceOnly 路径算不出 poolQuoteAfter → 0
      //   导致 RSI 的 lastPoolQuoteSol 永远为 null → rsi_pre_dump 不缓存
      let effectivePoolQuoteSol = poolQuoteAfter;
      if ((!effectivePoolQuoteSol || effectivePoolQuoteSol <= 0) && tokenRegistry) {
        const ti = tokenRegistry.getToken(mint);
        if (ti && ti.liquidity) {
          effectivePoolQuoteSol = ti.liquidity / 170; // USD → SOL
        }
      }
      if (side && solVolume > 0) {
        rsiCalculator.feedTrade(mint, price, solVolume, side.toLowerCase(), ts, effectivePoolQuoteSol);
      } else {
        rsiCalculator.feedTick(mint, price, ts);
      }
    }
  });

  // v3.17.17: 旧 sellAnalyzed → feedTrade 接线已经合并到 priceTick 路径(priceTick 包含所有 swap)
  // 不需要单独的 sellAnalyzed → RSI 监听

  // sellAnalyzed: 只记录"接近触发"的（半阈值），避免写入风暴
  dumpDetector.on('sellAnalyzed', (info) => {
    if (info.passSize && info.passImpact && info.passLiquidity) return; // 已 dumpSignal
    const halfSize = config.strategy.minSellSol * 0.5;
    const halfImpact = config.strategy.minPriceImpactPct * 0.5;
    if (info.sellSol < halfSize || info.priceImpactPct < halfImpact) return;
    // 构造可读的拒绝原因
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
    // v3.17.16: 移除 refreshOne 调用
    //   handleDumpSignal → buyOrder → executor.buy 都在同一 microtask 链完成,
    //   refreshOne 的 RPC(30-100ms)永远追不上当次 BUY,对当前信号无意义。
    //   PoolStateCache 后台滚动刷新(POOL_STATE_REFRESH_MS=5000)已经保证 cache 新鲜。
    //   如果希望砸盘瞬间池子状态更新,把 POOL_STATE_REFRESH_MS 调到 2000-3000。
    if (orderFlowTracker.enabled && orderFlowTracker.replaceDumpSignal) {
      orderFlowTracker.noteSuppressedDumpSignal(signal);
      return;
    }
    signalEngine.handleDumpSignal(signal);
  });

  // v3.17.15: RUG 信号 — 同 slot 5+ 笔卖出、合计 > 5 SOL → 持仓立即卖出
  //   v3.17.27: 用户要求关闭 RUG_PULL_EXIT，改为仅记录不卖出
  dumpDetector.on('rugSignal', (rug) => {
    const mint = rug.mint;
    const pids = positionManager.byMint.get(mint);
    if (!pids || pids.size === 0) return; // 无持仓，忽略
    console.log(
      `[RUG] 🚨 RUG PULL detected on ${rug.symbol || mint.slice(0,6)}: ${rug.sellCount} sells, ${rug.sellSol.toFixed(1)} SOL, ${rug.sellers.length} sellers — RUG_PULL_EXIT disabled, skipping`,
    );
    // RUG_PULL_EXIT 已关闭 — 不再强制卖出，让 trailing/TP 等其他机制处理
    // for (const pid of pids) {
    //   const pos = positionManager.positions.get(pid);
    //   if (pos && !pos.exiting) {
    //     const px = positionManager.priceTracker.getPrice(mint) || pos.entryPrice;
    //     positionManager._exit(pos, px, 'RUG_PULL_EXIT');
    //   }
    // }
  });

  // ============ buyOrder → BUY → register position ============
  signalEngine.on('buyOrder', async (order) => {
    console.log(`[main] buyOrder received: ${order.symbol || order.mint.slice(0,6)} mint=${order.mint.slice(0,8)}.. reason=${order.reason} sig=${order.signature?.slice(0,12)}..`);
    const _t0 = Date.now();
    const tokenInfo = tokenRegistry.getToken(order.mint);
    const _t1 = Date.now();

    // 用同一个 positionId 贯穿 BUY trade / position 表
    const positionId = crypto.randomUUID();

    // 标记此 mint 正在 buy 中，让后续并发 dumpSignal 看到这个槽位被占
    signalEngine.markBuyInflight(order.mint);

    // v3.17.11: BUY 前记录当前链上 slot，用于 SLOT_EXIT 策略
    executor.setLatestSlot(tickStream.latestSlot || 0);

    // v3.17.27: 同步刷新 pool state → 确保 executor.buy cache hit
    //   如果 cache miss，executor.buy 会走同步 RPC(80-180ms)。
    //   在这里同步 refreshOne(30-80ms) 把 state 填入 cache，
    //   buy 时直接 cache hit → state=0ms → 总延迟从 ~150ms 降到 ~60ms。
    const preBuyPoolAddr = tokenInfo?.pool_address;
    if (preBuyPoolAddr && executor.poolStateCache) {
      const cachedState = executor.poolStateCache.get(preBuyPoolAddr);
      if (!cachedState) {
        const tPre = Date.now();
        try { await executor.poolStateCache.refreshOne(preBuyPoolAddr); } catch (_) { /* 静默 */ }
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
        priceAfter: order.priceAfter, // 用于 DRY_RUN 模拟
        baseDecimals: order.baseDecimals ?? tokenInfo?.decimals ?? 6,
        poolAddress: tokenInfo?.pool_address, // Pump SDK 需要 pool address
      });
    } finally {
      signalEngine.markBuyDone(order.mint);
    }
    if (order._signalReceivedAt && buyResult && buyResult.success) {
      console.log('[main] buyOrder_timing: getToken=%dms preBuy=%dms buy=%dms', _t1-_t0, _t2-_t1, Date.now()-_t2);
    }

    // v3.17.16: 端到端延迟监控 — 这是「能否紧跟着砸单买入」的核心指标
    //   signalToBuyMs: 从砸盘 tx 时间戳到 BUY 提交的总耗时
    //   inEngineMs: 砸盘 tx 进入 SignalEngine 到 emit buyOrder
    //   buyLatencyMs: executor.buy 内部耗时(读 cache + 构造 + 发送)
    //   理想: signalToBuyMs ≤ 400ms (1 slot), buyLatencyMs ≤ 150ms
    if (order._signalReceivedAt && buyResult.success) {
      const signalToBuyMs = Date.now() - order._signalReceivedAt;
      const fromDumpTsMs = order.ts ? Date.now() - order.ts : null;
      console.log(
        `[main] ⏱  ${order.symbol || order.mint.slice(0, 6)} latency: ` +
        `signal→BUY=${signalToBuyMs}ms` +
        (fromDumpTsMs !== null ? ` dumpTs→BUY=${fromDumpTsMs}ms` : '') +
        ` (buy.latency=${buyResult.latencyMs}ms, state=${buyResult.stateLatencyMs}ms, send=${buyResult.sendLatencyMs}ms)`,
      );
    }

    // 记录 BUY trade（用同一 positionId）
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
      // v3.26: pool dead/low-liquidity/mint-mismatch → 24h 冷却，防止同币反复浪费 fee
      if (buyResult.poolDead || buyResult.poolLowLiquidity || buyResult.poolMintMismatch) {
        const cooldownMs = parseInt(process.env.POOL_FAIL_REBUY_COOLDOWN_MS || '86400000', 10);
        signalEngine._exitCooldowns.set(order.mint, Date.now() + cooldownMs);
        console.log(
          `[main] 🔒 Pool fail cooldown ${order.symbol || order.mint.slice(0, 6)} for ${Math.round(cooldownMs / 3600000)}h (poolDead=${!!buyResult.poolDead} poolLowLiq=${!!buyResult.poolLowLiquidity} mintMismatch=${!!buyResult.poolMintMismatch})`,
        );
      }
      return;
    }

    // 用真实成交价初始化 entry_price（关键修复 v1 bug：之前用 trigger 价）
    // v3.17.21: 买入瞬间的 FDV / pool / liquidity（用于事后分析入场质量）
    const entryFdv = tokenInfo?.fdv ?? null;
    const entryLiquidity = tokenInfo?.liquidity ?? null;
    const entryPoolSol = order.poolQuoteAfter ?? tokenInfo?.liquidity ?? null; // dumpSignal.poolQuoteAfter 最准确

    // v3.17.39: 计算首信号到买入的秒数（用于回测入场时机）
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
      entryPrice: buyResult.price,         // 真实成交价
      tokenAmount: buyResult.tokenAmount,  // 真实买到的数量
      dryRun: config.DRY_RUN,
      signature: buyResult.signature,
      buyFeeLamports: buyResult.priorityFeeLamports || 0,  // v3.4: 用于真实 PnL
      buySlot: buyResult.buySlot || 0,  // v3.17.11: BUY 时的链上 slot
      dumpSlot: order.slot || 0,        // v3.17.19: 砸单的 slot,用于算 BUY 落链领先几个 slot
      entryFdv,                          // v3.17.21: 买入瞬间 FDV
      entryPoolSol,                      // v3.17.21: 买入瞬间池子 SOL
      entryLiquidity,                    // v3.17.21: 买入瞬间流动性 USD
      sellCount10s: order._sellCount10s || 1,   // v3.17.36: 连环拔回测
      totalSellSol10s: order._totalSellSol10s || order.sellSol, // v3.17.36: 连环拔回测
      mintAgeAtBuySec,                           // v3.17.39: 首信号到买入秒数
      rsiPreDump: order.rsiPreDump,              // v3.17.38: 砸单前 RSI5s
      rsi1sPreDump: order.rsi1sPreDump,          // v3.17.38: 砸单前 RSI1s
      rsi30sPreDump: order.rsi30sPreDump,        // v3.17.42: 砸单前 RSI30s
      isEmaStrategy: false,  // EMA removed
      isAddOn: order._isAddOn || false,                 // 加仓标记
    });


    // 立即同步 PriceTracker，用真实成交价做 entry baseline
    // （避免下一笔 LaserStream tx 推一个旧价格触发假 TP）
    priceTracker.forceSet(order.mint, buyResult.price);

    if (buyResult.signature) signalEngine.registerOurSignature(buyResult.signature);
  });

  positionManager.on('opened', (pos) =>
    server.broadcast({ type: 'positionOpened', position: pos }),
  );
  positionManager.on('closed', (pos) => {
    // v3.17.15: 卖出后设置冷却，防止同一根K线买卖
    signalEngine.lastTriggerTs.set(pos.mint, Date.now());
    server.broadcast({ type: 'positionClosed', position: pos });
  });

  // ============ 启动服务器 ============
  server.start();

  // ============ 启动前补充 pool 信息（异步后台） ============
  if (config.autoFillPoolsOnStart) {
    backgroundFillPools(tokenRegistry).catch((err) =>
      console.error(`[main] backgroundFillPools error: ${err.message}`),
    );
  }

  // ============ 启动数据流 ============
  const initialMints = tokenRegistry.listActive().map((t) => t.mint);
  console.log(`[main] starting LaserStream with ${initialMints.length} initial tokens`);
  await tickStream.start(initialMints);

  // ============ 优雅退出 ============
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

  // v3.27: 定时3小时自动重启，防止 Rust native 缓慢泄漏导致 slot gap 恶化
  // 基线RSS ~550MB (7个gRPC连接)，3小时泄漏到 ~800MB 时 slot gap 就开始恶化
  // 重启后 restoreFromDb 恢复持仓，有仓时延迟到空仓或RSS>1000MB再重启
  const MAX_UPTIME_MS = parseInt(process.env.MAX_UPTIME_MS || '10800000', 10); // 默认3小时
  const startTime = Date.now();
  setInterval(() => {
    const uptimeMs = Date.now() - startTime;
    const posCount = positionManager?.positions?.size ?? 0;
    if (uptimeMs > MAX_UPTIME_MS && posCount === 0) {
      console.log(`[MEM] 🔄 uptime=${Math.round(uptimeMs/60000)}min > ${Math.round(MAX_UPTIME_MS/60000)}min 且空仓, 定时重启释放 Rust native 内存`);
      process.exit(0);
    } else if (uptimeMs > MAX_UPTIME_MS && posCount > 0) {
      console.log(`[MEM] ⏳ uptime=${Math.round(uptimeMs/60000)}min > ${Math.round(MAX_UPTIME_MS/60000)}min 但有 ${posCount} 个持仓, 等 RSS 达到阈值或空仓后重启`);
    }
  }, 60_000);
}

/**
 * 后台扫描所有缺失 pool 信息的代币，逐个补上。
 * 节流：每个 250ms。
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

// v3.32b: 堆外内存监控 — 区分 heap vs external vs arrayBuffers
setInterval(() => {
  const m = process.memoryUsage();
  console.log(`[MEM] rss=${(m.rss/1048576)|0}MB heapUsed=${(m.heapUsed/1048576)|0}MB external=${(m.external/1048576)|0}MB arrayBuffers=${(m.arrayBuffers/1048576)|0}MB`);
}, 30000);
