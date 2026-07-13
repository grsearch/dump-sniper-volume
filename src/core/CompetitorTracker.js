'use strict';

/**
 * CompetitorTracker (v3.17.20)
 * ============================
 * 目的：追踪一组「竞争对手钱包」在我们**已监控代币**上的买卖行为，
 *       配对成 round-trip（买→卖），统计他们的策略特征与盈亏，
 *       帮助分析他们如何在「跟着大砸单买入」这件事上盈利。
 *
 * 思路可行性 & 设计取舍（写给维护者）：
 *   ✅ 数据来源：复用 DumpDetector 已经解析好的 swapParsed 事件（signer/side/solVolume/price/ts）。
 *      竞争对手只有在交易**我们监控的代币**时才会被记录 —— 这正是你关心的重叠场景，
 *      且完全不增加额外 RPC、不影响 BUY 关键路径延迟。
 *   ⚠️ 局限 1（覆盖率）：我们只订阅自己监控列表里的代币。竞争对手在我们没监控的币上的
 *      交易看不到，所以这里算出的盈亏是「在我们关心的币上的子集」，不是他全账户的真实盈亏。
 *      —— 对「他在我们也盯的币上怎么做」这个问题，这个子集恰恰是最相关的。
 *   ⚠️ 局限 2（成交价精度）：solVolume / price 来自池子 vault 余额变化推算，含我们自己 swap 的
 *      影响近似。统计趋势可靠，单笔精确成交价不保证（要精确得拉 enhanced tx，成本高，不值得）。
 *   ⚠️ 局限 3（配对歧义）：同一钱包对同一 mint 多笔买入再分批卖出时，用 FIFO 配对（先买先卖）。
 *      绝大多数 sniper 是「一次买、一次/几次卖光」，FIFO 足够。
 *
 * 改进建议（可后续加，已预留结构）：
 *   1. "影子跟单" 模式：当某个高胜率竞争对手 BUY 我们监控的币时，发一个 webhook/alert，
 *      人工或半自动跟进（先观察、再决定是否自动跟单）。本类已 emit 'competitorTrade' 事件。
 *   2. 领先关系分析：记录竞争对手 BUY 的 slot vs 砸单 slot、vs 我们 BUY 的 slot，
 *      看他是不是总比我们早 1-2 个 slot（已记录 slot 字段，可在 reports 里做）。
 *   3. 持仓时长分布：他是「秒级反弹就跑」还是「拿几分钟」，直接影响我们 MAX_HOLD_MS / trailing 调参。
 *
 * 持久化：competitor_trades（每笔买卖）+ 内存 open-lots（未配对的买入）。
 *   重启后从 DB 恢复最近 24h 未平的买入 lots，避免重启丢配对。
 */

const EventEmitter = require('events');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
monitor.registerModule('CompetitorTracker', { staleMs: 3600_000, label: 'Competitor Tracker' });

class CompetitorTracker extends EventEmitter {
  /**
   * @param {object} opts
   * @param {Database} opts.db - 共享 better-sqlite3 实例
   * @param {string[]} [opts.addresses] - 初始竞争对手钱包地址列表
   * @param {number} [opts.maxLotAgeMs] - 未配对买入 lot 的最大留存时间（默认 24h）
   * @param {import('./DumpDetector')} [opts.dumpDetector] - 取触发砸单上下文（零成本进场特征）
   * @param {import('./PoolStateCache')} [opts.poolStateCache] - 取买入瞬间池子 SOL 流动性
   * @param {function} [opts.fetchTokenInfo] - async (mint) => {fdv, liquidity, holders, volume24h}；代币侧特征 RPC
   * @param {boolean} [opts.enrichEntry] - 是否对竞争对手买入做代币侧 RPC 富化（默认 true）
   * @param {boolean} [opts.followSell] - 是否把竞争对手卖出接到实盘卖出（默认 false，仅记录分析）
   * @param {number} [opts.followSellMinWinRate] - 跟卖只对胜率≥此值的钱包生效（默认 60）
   * @param {number} [opts.followSellMinClosed] - 跟卖要求该钱包已配对样本数≥此值（默认 10）
   */
  constructor({
    db, addresses = [], maxLotAgeMs,
    dumpDetector = null, poolStateCache = null, fetchTokenInfo = null,
    enrichEntry = true, followSell = false,
    followSellMinWinRate = 60, followSellMinClosed = 10,
  }) {
    super();
    if (!db) throw new Error('CompetitorTracker requires a shared DB instance');
    this.db = db;
    this.maxLotAgeMs = maxLotAgeMs || 24 * 3600_000;
    this.dumpDetector = dumpDetector;
    this.poolStateCache = poolStateCache;
    this.fetchTokenInfo = fetchTokenInfo;
    this.enrichEntry = enrichEntry;
    // 跟卖配置（默认关闭；预留好，看完分析数据后开 followSell=true 即启用最高优先级跟卖）
    this.followSell = followSell;
    this.followSellMinWinRate = followSellMinWinRate;
    this.followSellMinClosed = followSellMinClosed;

    // 追踪地址集合（Set 便于 O(1) 命中判断）
    this.addresses = new Set();

    // 内存中未配对的买入 lots： `${wallet}:${mint}` → [{ qtyTokens, solIn, price, ts, slot, signature }, ...]
    //   注意：我们不知道竞争对手买到的精确 token 数量（要解析他的 ATA 变化），
    //   这里用 solIn / price 近似 qtyTokens 做 FIFO 配对；统计层面足够。
    this.openLots = new Map();

    this._initSchema();
    this._prepareStatements();

    for (const a of addresses) this.addAddress(a);

    this._restoreOpenLots();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS competitor_addresses (
        address TEXT PRIMARY KEY,
        label TEXT,
        added_at INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
      );

      CREATE TABLE IF NOT EXISTS competitor_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT NOT NULL,
        mint TEXT NOT NULL,
        symbol TEXT,
        side TEXT NOT NULL,            -- BUY | SELL
        sol_amount REAL,              -- 这笔的 SOL 体积
        price REAL,
        ts INTEGER NOT NULL,
        slot INTEGER,
        signature TEXT,
        -- ===== v3.17.20 进场特征（仅 BUY 行；零成本，来自解析流）=====
        trigger_max_sell_sol REAL,    -- 触发买入的最近窗口内单笔最大砸单 SOL
        trigger_max_impact_pct REAL,  -- 该最大砸单的跌幅 %
        trigger_total_sell_sol REAL,  -- 最近窗口累计砸单 SOL
        trigger_sell_count INTEGER,   -- 最近窗口砸单笔数
        pool_quote_sol REAL,          -- 买入瞬间池子 SOL 流动性（链上）
        dump_to_buy_slot INTEGER,     -- 竞争对手 BUY slot - 最近砸单 slot（领先/落后）
        -- ===== v3.17.20 代币侧特征（异步 RPC 回填；可能为 NULL）=====
        fdv_usd REAL,                 -- 买入瞬间 FDV（Birdeye, USD）
        liquidity_usd REAL,           -- Birdeye 流动性
        holders INTEGER,              -- 持有人数（Birdeye）
        volume24h_usd REAL,           -- 24h 交易量
        enriched INTEGER DEFAULT 0,   -- 1 = 代币侧特征已回填
        -- ===== round-trip 配对结果（仅 SELL 行回填）=====
        matched_buy_id INTEGER,       -- 配对到的 BUY 行 id（FIFO）
        pnl_sol REAL,                 -- 本次配对实现盈亏（SOL）
        pnl_pct REAL,                 -- 本次配对盈亏 %
        hold_ms INTEGER               -- 持仓时长（买→卖）
      );
      CREATE INDEX IF NOT EXISTS idx_comp_trades_wallet ON competitor_trades(wallet);
      CREATE INDEX IF NOT EXISTS idx_comp_trades_mint ON competitor_trades(mint);
      CREATE INDEX IF NOT EXISTS idx_comp_trades_ts ON competitor_trades(ts);
    `);

    // 兼容旧表：逐列尝试 ADD COLUMN（SQLite 不支持 IF NOT EXISTS）
    const addCols = [
      'trigger_max_sell_sol REAL', 'trigger_max_impact_pct REAL',
      'trigger_total_sell_sol REAL', 'trigger_sell_count INTEGER',
      'pool_quote_sol REAL', 'dump_to_buy_slot INTEGER',
      'fdv_usd REAL', 'liquidity_usd REAL', 'holders INTEGER',
      'volume24h_usd REAL', 'enriched INTEGER DEFAULT 0',
    ];
    for (const col of addCols) {
      try { this.db.exec(`ALTER TABLE competitor_trades ADD COLUMN ${col}`); } catch (_) { /* exists */ }
    }
  }

  _prepareStatements() {
    this.stmts = {
      upsertAddress: this.db.prepare(`
        INSERT INTO competitor_addresses (address, label, added_at, active)
        VALUES (@address, @label, @addedAt, 1)
        ON CONFLICT(address) DO UPDATE SET active = 1, label = COALESCE(excluded.label, competitor_addresses.label)
      `),
      deactivateAddress: this.db.prepare(`UPDATE competitor_addresses SET active = 0 WHERE address = ?`),
      listActiveAddresses: this.db.prepare(`SELECT address, label FROM competitor_addresses WHERE active = 1`),

      insertTrade: this.db.prepare(`
        INSERT INTO competitor_trades
          (wallet, mint, symbol, side, sol_amount, price, ts, slot, signature,
           trigger_max_sell_sol, trigger_max_impact_pct, trigger_total_sell_sol,
           trigger_sell_count, pool_quote_sol, dump_to_buy_slot,
           matched_buy_id, pnl_sol, pnl_pct, hold_ms)
        VALUES (@wallet, @mint, @symbol, @side, @solAmount, @price, @ts, @slot, @signature,
                @triggerMaxSellSol, @triggerMaxImpactPct, @triggerTotalSellSol,
                @triggerSellCount, @poolQuoteSol, @dumpToBuySlot,
                @matchedBuyId, @pnlSol, @pnlPct, @holdMs)
      `),

      // v3.17.20: 异步回填代币侧特征（FDV/流动性/持有人/24h量）
      enrichBuy: this.db.prepare(`
        UPDATE competitor_trades SET
          fdv_usd = @fdvUsd,
          liquidity_usd = @liquidityUsd,
          holders = @holders,
          volume24h_usd = @volume24hUsd,
          enriched = 1
        WHERE id = @id
      `),

      // v3.17.20: 进场特征分布（只看 BUY 行），用于反推竞争对手买入阈值
      entryStats: this.db.prepare(`
        SELECT
          COUNT(*) AS n,
          MIN(trigger_max_sell_sol) AS min_trigger_sell,
          AVG(trigger_max_sell_sol) AS avg_trigger_sell,
          MAX(trigger_max_sell_sol) AS max_trigger_sell,
          MIN(trigger_max_impact_pct) AS min_impact,
          AVG(trigger_max_impact_pct) AS avg_impact,
          MAX(trigger_max_impact_pct) AS max_impact,
          AVG(pool_quote_sol) AS avg_pool_sol,
          MIN(pool_quote_sol) AS min_pool_sol,
          AVG(fdv_usd) AS avg_fdv,
          MIN(fdv_usd) AS min_fdv,
          AVG(holders) AS avg_holders,
          AVG(dump_to_buy_slot) AS avg_dump_to_buy_slot
        FROM competitor_trades
        WHERE wallet = ? AND side = 'BUY' AND trigger_max_sell_sol IS NOT NULL
      `),

      // 恢复未配对买入：最近 maxLotAgeMs 内、没有任何 SELL 配对过它的 BUY
      //   简化：取最近窗口内所有 BUY，减去已被 matched_buy_id 引用的，剩下的就是 open lots
      recentBuys: this.db.prepare(`
        SELECT * FROM competitor_trades
        WHERE side = 'BUY' AND ts >= ?
        ORDER BY ts ASC
      `),
      matchedBuyIds: this.db.prepare(`
        SELECT DISTINCT matched_buy_id FROM competitor_trades
        WHERE matched_buy_id IS NOT NULL
      `),

      // 统计查询（用 SUM(CASE WHEN ...) 而非 FILTER，兼容所有 SQLite 版本）
      walletStats: this.db.prepare(`
        SELECT
          wallet,
          SUM(CASE WHEN side = 'BUY'  THEN 1 ELSE 0 END) AS buy_count,
          SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_count,
          SUM(CASE WHEN side = 'SELL' AND pnl_sol IS NOT NULL THEN 1 ELSE 0 END) AS closed_count,
          SUM(CASE WHEN side = 'SELL' AND pnl_sol > 0 THEN 1 ELSE 0 END) AS win_count,
          COALESCE(SUM(pnl_sol), 0) AS total_pnl_sol,
          COALESCE(AVG(pnl_pct), 0) AS avg_pnl_pct,
          COALESCE(AVG(hold_ms), 0) AS avg_hold_ms
        FROM competitor_trades
        WHERE wallet = ?
        GROUP BY wallet
      `),
      recentTradesByWallet: this.db.prepare(`
        SELECT * FROM competitor_trades WHERE wallet = ? ORDER BY ts DESC LIMIT ?
      `),
    };
  }

  // ============================================================
  // 地址管理（支持加多个；目前你要加 2 个）
  // ============================================================

  addAddress(address, label = null) {
    if (!address || typeof address !== 'string') return false;
    this.stmts.upsertAddress.run({ address, label, addedAt: Date.now() });
    this.addresses.add(address);
    console.log(`[CompetitorTracker] tracking wallet ${address.slice(0, 8)}..${label ? ` (${label})` : ''}`);
    monitor.set('CompetitorTracker.trackedWallets', this.addresses.size, 'CompetitorTracker');
    return true;
  }

  removeAddress(address) {
    this.stmts.deactivateAddress.run(address);
    this.addresses.delete(address);
    monitor.set('CompetitorTracker.trackedWallets', this.addresses.size, 'CompetitorTracker');
  }

  listAddresses() {
    return this.stmts.listActiveAddresses.all();
  }

  // ============================================================
  // 核心：消费 swapParsed
  // ============================================================

  /**
   * 由 main 接到 DumpDetector 'swapParsed' 后调用。
   * @param {{mint, symbol, signer, side, solVolume, price, ts, slot, signature}} swap
   */
  handleSwap(swap) {
    if (!swap || !swap.signer) return;
    if (!this.addresses.has(swap.signer)) return; // 不是被追踪钱包，忽略

    monitor.beat('CompetitorTracker', 'swap');
    monitor.inc('CompetitorTracker.observedTrades', 1, 'CompetitorTracker');

    const { mint, symbol, signer: wallet, side, solVolume, price, ts, slot, signature, poolAddress } = swap;
    if (side === 'BUY') {
      this._recordBuy({ wallet, mint, symbol, solVolume, price, ts, slot, signature, poolAddress });
    } else if (side === 'SELL') {
      this._recordSell({ wallet, mint, symbol, solVolume, price, ts, slot, signature });
    }
  }

  _lotKey(wallet, mint) {
    return `${wallet}:${mint}`;
  }

  /** 统一 insertTrade 行的默认字段，避免漏列导致 SQLite 报错。
   *  better-sqlite3 / node:sqlite 都不接受 undefined 绑定值，统一兜成 null。 */
  _tradeRow(over) {
    const row = {
      wallet: null, mint: null, symbol: null, side: null,
      solAmount: null, price: null, ts: Date.now(), slot: null, signature: null,
      triggerMaxSellSol: null, triggerMaxImpactPct: null, triggerTotalSellSol: null,
      triggerSellCount: null, poolQuoteSol: null, dumpToBuySlot: null,
      matchedBuyId: null, pnlSol: null, pnlPct: null, holdMs: null,
      ...over,
    };
    for (const k in row) if (row[k] === undefined) row[k] = null;
    return row;
  }

  _recordBuy({ wallet, mint, symbol, solVolume, price, ts, slot, signature, poolAddress }) {
    // ===== 进场特征（零成本，来自解析流） =====
    let triggerMaxSellSol = null, triggerMaxImpactPct = null;
    let triggerTotalSellSol = null, triggerSellCount = null, dumpToBuySlot = null;
    if (this.dumpDetector && typeof this.dumpDetector.getRecentMaxSell === 'function') {
      const ms = this.dumpDetector.getRecentMaxSell(mint);
      if (ms) {
        triggerMaxSellSol = ms.maxSingleSellSol;
        triggerMaxImpactPct = ms.maxSellImpactPct;
        triggerTotalSellSol = ms.totalSellSol;
        triggerSellCount = ms.sellCount;
        // 竞争对手 BUY slot - 触发砸单 slot（>0 表示落后砸单几个 slot；越小越快）
        if (ms.maxSellSlot != null && slot != null) {
          dumpToBuySlot = slot - ms.maxSellSlot;
        }
      }
    }
    // 池子流动性（买入瞬间，链上缓存）
    let poolQuoteSol = null;
    if (this.poolStateCache && poolAddress) {
      try {
        const st = this.poolStateCache.get(poolAddress);
        const lamports = st && st.poolQuoteAmount;
        if (lamports != null) {
          // BN / BigInt / number 统一走 toString() 再 Number，避免 BN.toNumber() 溢出抛错
          const v = Number(lamports.toString());
          if (Number.isFinite(v)) poolQuoteSol = v / 1e9;
        }
      } catch (_) { /* ignore */ }
    }

    const row = this._tradeRow({
      wallet, mint, symbol, side: 'BUY',
      solAmount: solVolume, price, ts, slot: slot || null, signature: signature || null,
      triggerMaxSellSol, triggerMaxImpactPct, triggerTotalSellSol, triggerSellCount,
      poolQuoteSol, dumpToBuySlot,
    });
    const info = this.stmts.insertTrade.run(row);
    const buyId = info.lastInsertRowid;

    // qtyTokens 近似：solVolume / price
    //   注意 DumpDetector CPI 路径可能给 BUY 一个 quoteAmount=0（无法精确计算 SOL 体积）。
    //   这种情况记录这笔 BUY 供查看，但不建 FIFO lot（qty=0 永远配不上卖出，会污染配对）。
    const solIn = Number(solVolume) || 0;
    const px = Number(price) || 0;
    const qtyTokens = px > 0 ? solIn / px : 0;
    if (qtyTokens > 0) {
      const key = this._lotKey(wallet, mint);
      if (!this.openLots.has(key)) this.openLots.set(key, []);
      this.openLots.get(key).push({ buyId, qtyTokens, solIn, price: px, ts, slot, signature });
    }

    const ctxStr = triggerMaxSellSol != null
      ? `[trigger: maxSell=${triggerMaxSellSol.toFixed(1)} SOL, impact=${triggerMaxImpactPct.toFixed(1)}%, poolLP=${poolQuoteSol != null ? poolQuoteSol.toFixed(0) + ' SOL' : '?'}]`
      : '[no recent dump seen — non-dump-driven entry?]';
    console.log(
      `[CompetitorTracker] 🟢 ${wallet.slice(0, 6)}.. BUY ${symbol || mint.slice(0, 6)} ` +
        `${solIn.toFixed(2)} SOL @ ${px > 0 ? px.toExponential(3) : '?'} ${ctxStr}` +
        `${qtyTokens === 0 ? ' (qty unknown — no FIFO lot)' : ''}`,
    );
    this.emit('competitorTrade', { ...row, buyId, qtyTokens });

    // ===== 代币侧特征：异步 RPC 富化（不阻塞） =====
    if (this.enrichEntry && this.fetchTokenInfo) {
      this.fetchTokenInfo(mint).then((info2) => {
        if (!info2) return;
        this.stmts.enrichBuy.run({
          id: buyId,
          fdvUsd: info2.fdv ?? null,
          liquidityUsd: info2.liquidity ?? null,
          holders: info2.holders ?? null,
          volume24hUsd: info2.volume24h ?? null,
        });
      }).catch((err) => {
        monitor.recordError('CompetitorTracker', err, { phase: 'enrichBuy', mint });
      });
    }
  }

  _recordSell({ wallet, mint, symbol, solVolume, price, ts, slot, signature }) {
    const key = this._lotKey(wallet, mint);
    const lots = this.openLots.get(key) || [];

    const solOut = Number(solVolume) || 0;
    const px = Number(price) || 0;
    // 卖出的 token 数量近似：solVolume / price
    const qtyToSellTotal = px > 0 ? solOut / px : 0;
    let qtyToSell = qtyToSellTotal;

    // FIFO 配对：从最早的买入 lot 开始抵消
    let realizedSol = 0;     // 已配对买入部分的成本（SOL）
    let matchedQty = 0;      // 实际配对上的 token 数量
    const proceedsSol = solOut; // 本次卖出收到的总 SOL
    let matchedBuyId = null;
    let earliestBuyTs = null;

    while (qtyToSell > 1e-9 && lots.length > 0) {
      const lot = lots[0];
      if (matchedBuyId == null) matchedBuyId = lot.buyId; // 记第一个配对的 buy（主配对）
      if (earliestBuyTs == null) earliestBuyTs = lot.ts;

      const take = Math.min(qtyToSell, lot.qtyTokens);
      const costFraction = lot.qtyTokens > 0 ? take / lot.qtyTokens : 1;
      const costSol = lot.solIn * costFraction;
      realizedSol += costSol;
      matchedQty += take;

      lot.qtyTokens -= take;
      lot.solIn -= costSol;
      qtyToSell -= take;

      if (lot.qtyTokens <= 1e-9) lots.shift(); // 这个 lot 卖光了
    }

    if (lots.length === 0) this.openLots.delete(key);

    // 计算配对盈亏（只在配到了至少一个买入时才算 round-trip）
    //   关键：只对"配对上的那部分 token"算盈亏，避免我们漏看了他们的买入时虚高收益。
    //   按 matchedQty / qtyToSellTotal 的占比，把本次卖出 proceeds 也只取对应部分。
    let pnlSol = null;
    let pnlPct = null;
    let holdMs = null;
    if (matchedBuyId != null && realizedSol > 0 && matchedQty > 0 && qtyToSellTotal > 0) {
      const matchedFraction = Math.min(1, matchedQty / qtyToSellTotal);
      const matchedProceeds = proceedsSol * matchedFraction;
      pnlSol = matchedProceeds - realizedSol;
      pnlPct = (pnlSol / realizedSol) * 100;
      holdMs = earliestBuyTs != null ? ts - earliestBuyTs : null;
    }

    const row = this._tradeRow({
      wallet, mint, symbol, side: 'SELL',
      solAmount: solOut, price: px, ts, slot: slot || null, signature: signature || null,
      matchedBuyId, pnlSol, pnlPct, holdMs,
    });
    this.stmts.insertTrade.run(row);

    const pnlStr = pnlSol != null
      ? `pnl=${pnlSol >= 0 ? '+' : ''}${pnlSol.toFixed(3)} SOL (${pnlPct.toFixed(1)}%, held ${holdMs != null ? (holdMs / 1000).toFixed(0) + 's' : '?'})`
      : '(no matched buy — unseen entry)';
    console.log(
      `[CompetitorTracker] 🔴 ${wallet.slice(0, 6)}.. SELL ${symbol || mint.slice(0, 6)} ` +
        `${solOut.toFixed(2)} SOL @ ${px > 0 ? px.toExponential(3) : '?'} ${pnlStr}`,
    );
    if (pnlSol != null) {
      monitor.inc('CompetitorTracker.roundTrips', 1, 'CompetitorTracker');
      if (pnlSol > 0) monitor.inc('CompetitorTracker.roundTripWins', 1, 'CompetitorTracker');
    }
    this.emit('competitorTrade', row);

    // ===== v3.17.20 跟卖信号 =====
    //   始终 emit 'competitorSell'，main 那边决定要不要接到实盘卖出（followSell 开关）。
    //   只对"高胜率 + 足够样本"的钱包标记 eligible=true，避免跟到亏钱的钱包。
    const stats = this.getWalletStats(wallet);
    const eligible =
      this.followSell &&
      stats.closedCount >= this.followSellMinClosed &&
      stats.winRatePct >= this.followSellMinWinRate;
    this.emit('competitorSell', {
      wallet, mint, symbol, ts, slot, signature,
      walletWinRatePct: stats.winRatePct,
      walletClosedCount: stats.closedCount,
      eligible, // main 只在 eligible=true 时执行实盘跟卖
    });
  }

  // ============================================================
  // 统计 / 报表
  // ============================================================

  getWalletStats(wallet) {
    const s = this.stmts.walletStats.get(wallet);
    if (!s) {
      return {
        wallet, buyCount: 0, sellCount: 0, closedCount: 0, winCount: 0,
        winRatePct: 0, totalPnlSol: 0, avgPnlPct: 0, avgHoldMs: 0, openLots: 0,
      };
    }
    const openLotsCount = this._countOpenLotsForWallet(wallet);
    return {
      wallet,
      buyCount: s.buy_count,
      sellCount: s.sell_count,
      closedCount: s.closed_count,
      winCount: s.win_count,
      winRatePct: s.closed_count > 0 ? (s.win_count / s.closed_count) * 100 : 0,
      totalPnlSol: s.total_pnl_sol,
      avgPnlPct: s.avg_pnl_pct,
      avgHoldMs: s.avg_hold_ms,
      openLots: openLotsCount,
    };
  }

  getAllStats() {
    return this.listAddresses().map((a) => ({
      ...this.getWalletStats(a.address),
      label: a.label || null,
    }));
  }

  getRecentTrades(wallet, limit = 50) {
    return this.stmts.recentTradesByWallet.all(wallet, limit);
  }

  /**
   * v3.17.20: 进场特征分布 —— 反推竞争对手买入阈值。
   *   返回他们买入时对应的"触发砸单大小/跌幅/池子流动性/FDV/持有人/领先 slot"的
   *   min/avg/max。对照你自己的 MIN_SELL_SOL / MIN_PRICE_IMPACT_PCT 看差异。
   */
  getEntryStats(wallet) {
    const s = this.stmts.entryStats.get(wallet);
    if (!s || !s.n) return { wallet, n: 0 };
    return {
      wallet,
      n: s.n,
      triggerSellSol: { min: s.min_trigger_sell, avg: s.avg_trigger_sell, max: s.max_trigger_sell },
      triggerImpactPct: { min: s.min_impact, avg: s.avg_impact, max: s.max_impact },
      poolLpSol: { min: s.min_pool_sol, avg: s.avg_pool_sol },
      fdvUsd: { min: s.min_fdv, avg: s.avg_fdv },
      avgHolders: s.avg_holders,
      avgDumpToBuySlot: s.avg_dump_to_buy_slot,
    };
  }

  getAllEntryStats() {
    return this.listAddresses().map((a) => ({ ...this.getEntryStats(a.address), label: a.label || null }));
  }

  _countOpenLotsForWallet(wallet) {
    let n = 0;
    for (const [key, lots] of this.openLots) {
      if (key.startsWith(`${wallet}:`)) n += lots.length;
    }
    return n;
  }

  // ============================================================
  // 启动恢复 + 清理
  // ============================================================

  _restoreOpenLots() {
    try {
      const cutoff = Date.now() - this.maxLotAgeMs;
      const buys = this.stmts.recentBuys.all(cutoff);
      const matched = new Set(
        this.stmts.matchedBuyIds.all().map((r) => r.matched_buy_id).filter((x) => x != null),
      );
      let restored = 0;
      for (const b of buys) {
        if (matched.has(b.id)) continue; // 已被卖出配对过
        if (!this.addresses.has(b.wallet)) continue; // 只恢复当前仍在追踪的钱包
        const qtyTokens = b.price > 0 ? b.sol_amount / b.price : 0;
        const key = this._lotKey(b.wallet, b.mint);
        if (!this.openLots.has(key)) this.openLots.set(key, []);
        this.openLots.get(key).push({
          buyId: b.id, qtyTokens, solIn: b.sol_amount, price: b.price,
          ts: b.ts, slot: b.slot, signature: b.signature,
        });
        restored += 1;
      }
      if (restored > 0) {
        console.log(`[CompetitorTracker] restored ${restored} open buy lot(s) from DB`);
      }
    } catch (err) {
      monitor.recordError('CompetitorTracker', err, { phase: 'restoreOpenLots' });
    }
  }

  /** 定期清理过期未配对 lots（竞争对手买了但我们没看到卖出 —— 可能在我们没监控的币上卖了） */
  cleanupExpiredLots() {
    const cutoff = Date.now() - this.maxLotAgeMs;
    let removed = 0;
    for (const [key, lots] of this.openLots) {
      const kept = lots.filter((l) => l.ts >= cutoff);
      removed += lots.length - kept.length;
      if (kept.length === 0) this.openLots.delete(key);
      else this.openLots.set(key, kept);
    }
    if (removed > 0) {
      console.log(`[CompetitorTracker] cleaned ${removed} expired open lot(s)`);
    }
  }
}

module.exports = CompetitorTracker;
