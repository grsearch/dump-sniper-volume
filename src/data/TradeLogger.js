'use strict';

/**
 * TradeLogger
 * ===========
 * SQLite-backed persistence for:
 *   - signals  : every detected dump (accepted=1 + rejected ones)
 *   - trades   : every BUY/SELL submission (success or fail)
 *   - positions: open/close lifecycle of each entry
 *
 * Reconstructed from call sites since the module was missing in the v3.17.13
 * handoff zip. Schema choices preserve the column names that PositionManager
 * (row.entry_price, row.opened_at, row.pending_sell_signature, etc.) and the
 * Dashboard SQL ("SELECT DISTINCT mint FROM positions") expect.
 */

class TradeLogger {
  /**
   * @param {Database} db - shared better-sqlite3 instance from TokenRegistry
   */
  constructor(db) {
    if (!db) throw new Error('TradeLogger requires a shared DB instance');
    this.db = db;
    this._initSchema();
    this._prepareStatements();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        mint TEXT NOT NULL,
        symbol TEXT,
        kind TEXT,
        sell_sol REAL,
        price_impact_pct REAL,
        seller TEXT,
        seller_tx TEXT,
        notes TEXT,
        accepted INTEGER NOT NULL DEFAULT 0,
        reject_reason TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts);
      CREATE INDEX IF NOT EXISTS idx_signals_seller_tx_accepted ON signals(seller_tx, accepted);
      CREATE INDEX IF NOT EXISTS idx_signals_mint ON signals(mint);

      CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id TEXT,
        ts INTEGER NOT NULL,
        mint TEXT NOT NULL,
        symbol TEXT,
        side TEXT,
        sol_amount REAL,
        token_amount REAL,
        price REAL,
        signature TEXT,
        success INTEGER NOT NULL DEFAULT 0,
        dry_run INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        latency_ms INTEGER,
        error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
      CREATE INDEX IF NOT EXISTS idx_trades_pos ON trades(position_id);

      CREATE TABLE IF NOT EXISTS positions (
        position_id TEXT PRIMARY KEY,
        mint TEXT NOT NULL,
        symbol TEXT,
        opened_at INTEGER NOT NULL,
        closed_at INTEGER,
        entry_sol REAL,
        entry_price REAL,
        exit_price REAL,
        exit_sol REAL,
        pnl_sol REAL,
        pnl_pct REAL,
        token_amount REAL,
        dry_run INTEGER NOT NULL DEFAULT 0,
        buy_signature TEXT,
        sell_signature TEXT,
        buy_fee_lamports INTEGER DEFAULT 0,
        buy_slot INTEGER DEFAULT 0,
        dump_slot INTEGER DEFAULT 0,
        exit_reason TEXT,
        exit_intent TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        sell_attempts INTEGER DEFAULT 0,
        next_retry_at INTEGER,
        last_retry_at INTEGER,
        last_error TEXT,
        pending_sell_signature TEXT,
        stuck_reason TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_positions_opened ON positions(opened_at);
      CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
      CREATE INDEX IF NOT EXISTS idx_positions_mint ON positions(mint);

      -- v3.17.31: post-exit price tracking table (backtest)
      CREATE TABLE IF NOT EXISTS price_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mint TEXT NOT NULL,
        ts INTEGER NOT NULL,
        price REAL NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_price_samples_mint_ts ON price_samples(mint, ts);

      CREATE TABLE IF NOT EXISTS post_exit_stats (
        position_id TEXT PRIMARY KEY,
        mint TEXT NOT NULL,
        exit_price REAL NOT NULL,
        exit_ts INTEGER NOT NULL,
        max_price REAL NOT NULL,
        max_price_ts INTEGER NOT NULL,
        max_pump_pct REAL NOT NULL,
        min_price REAL NOT NULL,
        min_price_ts INTEGER NOT NULL,
        max_dump_pct REAL NOT NULL,
        sample_count INTEGER NOT NULL,
        snapshots TEXT,
        finalized_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_post_exit_stats_mint ON post_exit_stats(mint);
      CREATE INDEX IF NOT EXISTS idx_post_exit_stats_exit_ts ON post_exit_stats(exit_ts);
    `);

    // v3.17.19: migrate dump_slot column for upgrading from earlier schemas
    //   SQLite 不支持 ADD COLUMN IF NOT EXISTS,直接尝试,失败就忽略
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN dump_slot INTEGER DEFAULT 0');
    } catch (_) { /* column already exists */ }

    // v3.17.21: entry quality fields for post-hoc analysis
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN entry_fdv REAL');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN entry_pool_sol REAL');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN entry_liquidity REAL');
    } catch (_) { /* column already exists */ }

    // v3.17.36: 连环拔回测字段
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN sell_count_10s INTEGER');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN total_sell_sol_10s REAL');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN mint_age_at_buy_sec INTEGER');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN rsi_pre_dump REAL');       // v3.17.38: 砸单前 RSI5s
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN rsi_1s_pre_dump REAL');    // v3.17.38: 砸单前 RSI1s
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN rsi_30s_pre_dump REAL');   // v3.17.42: 砸单前 RSI30s
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN is_ema_strategy INTEGER DEFAULT 0');
    } catch (_) { /* column already exists */ }
    try {
      this.db.exec('ALTER TABLE positions ADD COLUMN is_addon INTEGER DEFAULT 0');
    } catch (_) { /* column already exists */ }
  }

  _prepareStatements() {
    this.stmts = {
      // ============ signals ============
      insertSignal: this.db.prepare(`
        INSERT INTO signals
          (ts, mint, symbol, kind, sell_sol, price_impact_pct, seller, seller_tx, notes, accepted, reject_reason)
        VALUES (@ts, @mint, @symbol, @kind, @sellSol, @priceImpactPct, @seller, @sellerTx, @notes, @accepted, @rejectReason)
      `),

      recentAcceptedSellerTxs: this.db.prepare(`
        SELECT seller_tx, ts FROM signals
        WHERE accepted = 1 AND seller_tx IS NOT NULL AND seller_tx != ''
          AND ts >= ?
        ORDER BY ts DESC
        LIMIT 5000
      `),

      signalsInRange: this.db.prepare(`
        SELECT * FROM signals WHERE ts >= ? AND ts < ? ORDER BY ts ASC
      `),

      recentSignals: this.db.prepare(`
        SELECT * FROM signals ORDER BY ts DESC LIMIT ?
      `),

      // ============ trades ============
      insertTrade: this.db.prepare(`
        INSERT INTO trades
          (position_id, ts, mint, symbol, side, sol_amount, token_amount, price, signature,
           success, dry_run, reason, latency_ms, error)
        VALUES (@positionId, @ts, @mint, @symbol, @side, @solAmount, @tokenAmount, @price, @signature,
                @success, @dryRun, @reason, @latencyMs, @error)
      `),

      tradesInRange: this.db.prepare(`
        SELECT * FROM trades WHERE ts >= ? AND ts < ? ORDER BY ts ASC
      `),

      recentTrades: this.db.prepare(`
        SELECT * FROM trades ORDER BY ts DESC LIMIT ?
      `),

      // ============ positions ============
      openPosition: this.db.prepare(`
        INSERT INTO positions
          (position_id, mint, symbol, opened_at, entry_sol, entry_price, token_amount,
           dry_run, buy_signature, buy_fee_lamports, buy_slot, dump_slot,
           entry_fdv, entry_pool_sol, entry_liquidity,
           sell_count_10s, total_sell_sol_10s,
           mint_age_at_buy_sec, rsi_pre_dump, rsi_1s_pre_dump, rsi_30s_pre_dump,
           is_ema_strategy, is_addon, status)
        VALUES (@positionId, @mint, @symbol, @openedAt, @entrySol, @entryPrice, @tokenAmount,
                @dryRun, @buySignature, @buyFeeLamports, @buySlot, @dumpSlot,
                @entryFdv, @entryPoolSol, @entryLiquidity,
                @sellCount10s, @totalSellSol10s,
                @mintAgeAtBuySec, @rsiPreDump, @rsi1sPreDump, @rsi30sPreDump,
                @isEmaStrategy, @isAddOn, 'open')
        ON CONFLICT(position_id) DO UPDATE SET
          opened_at = excluded.opened_at,
          entry_sol = excluded.entry_sol,
          entry_price = excluded.entry_price,
          token_amount = excluded.token_amount,
          buy_signature = excluded.buy_signature,
          buy_fee_lamports = excluded.buy_fee_lamports,
          buy_slot = excluded.buy_slot,
          dump_slot = excluded.dump_slot,
          entry_fdv = excluded.entry_fdv,
          entry_pool_sol = excluded.entry_pool_sol,
          entry_liquidity = excluded.entry_liquidity,
          sell_count_10s = excluded.sell_count_10s,
          total_sell_sol_10s = excluded.total_sell_sol_10s,
          mint_age_at_buy_sec = excluded.mint_age_at_buy_sec,
          rsi_pre_dump = excluded.rsi_pre_dump,
          rsi_1s_pre_dump = excluded.rsi_1s_pre_dump,
          rsi_30s_pre_dump = excluded.rsi_30s_pre_dump,
          is_ema_strategy = excluded.is_ema_strategy,
          is_addon = excluded.is_addon,
          status = 'open'
      `),

      updateEntry: this.db.prepare(`
        UPDATE positions SET
          entry_sol = @entrySol,
          entry_price = @entryPrice,
          token_amount = @tokenAmount,
          buy_fee_lamports = @buyFeeLamports
        WHERE position_id = @positionId
      `),

      closePosition: this.db.prepare(`
        UPDATE positions SET
          closed_at = @closedAt,
          exit_price = @exitPrice,
          exit_sol = @exitSol,
          pnl_sol = @pnlSol,
          pnl_pct = @pnlPct,
          exit_reason = @exitReason,
          sell_signature = @sellSignature,
          peak_pnl_pct = @peakPnlPct,
          peak_price = @peakPrice,
          peak_ts = @peakTs,
          time_to_peak_ms = @timeToPeakMs,
          price_tick_count = @priceTickCount,
          status = 'closed'
        WHERE position_id = @positionId
      `),

      updatePeak: this.db.prepare(`
        UPDATE positions SET
          peak_price = @peakPrice,
          peak_ts = @peakTs,
          peak_pnl_pct = @peakPnlPct
        WHERE position_id = @positionId
      `),

      markSellPending: this.db.prepare(`
        UPDATE positions SET
          status = 'sell_confirming',
          pending_sell_signature = ?,
          exit_intent = ?,
          last_retry_at = ?
        WHERE position_id = ?
      `),

      markSellFailedPendingRetry: this.db.prepare(`
        UPDATE positions SET
          status = 'sell_pending',
          next_retry_at = ?,
          last_error = ?,
          exit_intent = ?,
          last_retry_at = ?
        WHERE position_id = ?
      `),

      markStuck: this.db.prepare(`
        UPDATE positions SET
          status = 'stuck',
          stuck_reason = ?,
          last_retry_at = ?
        WHERE position_id = ?
      `),

      recordSellAttempt: this.db.prepare(`
        UPDATE positions SET
          sell_attempts = COALESCE(sell_attempts, 0) + 1,
          last_error = ?,
          last_retry_at = ?
        WHERE position_id = ?
      `),

      // PositionManager.restoreFromDb expects open + sell_pending + sell_confirming + stuck
      getOpenPositions: this.db.prepare(`
        SELECT * FROM positions
        WHERE status IN ('open', 'sell_pending', 'sell_confirming', 'stuck')
        ORDER BY opened_at ASC
      `),

      getDuePendingRetries: this.db.prepare(`
        SELECT * FROM positions
        WHERE status IN ('sell_pending', 'sell_confirming')
          AND (next_retry_at IS NULL OR next_retry_at <= ?)
      `),

      positionsInRange: this.db.prepare(`
        SELECT * FROM positions
        WHERE closed_at IS NOT NULL AND closed_at >= ? AND closed_at < ?
        ORDER BY opened_at ASC
      `),

      recentPositions: this.db.prepare(`
        SELECT *, pre_vol_5m_pct as pre_vol_5m FROM positions ORDER BY opened_at DESC LIMIT ?
      `),

      // ============ price_samples ============
      insertPriceSample: this.db.prepare(`
        INSERT INTO price_samples (mint, ts, price) VALUES (@mint, @ts, @price)
      `),

      loadRecentPriceSamples: this.db.prepare(`
        SELECT mint, ts, price FROM price_samples
        WHERE ts >= ?
        ORDER BY mint, ts ASC
      `),

      cleanOldPriceSamples: this.db.prepare(`
        DELETE FROM price_samples WHERE ts < ?
      `),

      stuckPositions: this.db.prepare(`
        SELECT * FROM positions WHERE status = 'stuck' ORDER BY opened_at DESC
      `),
    };
  }

  // ============================================================
  // Signal API
  // ============================================================

  logSignal({ ts, mint, symbol, kind, sellSol, priceImpactPct, seller, sellerTx,
             notes, accepted, rejectReason }) {
    this.stmts.insertSignal.run({
      ts: ts || Date.now(),
      mint: mint || null,
      symbol: symbol || null,
      kind: kind || null,
      sellSol: sellSol ?? null,
      priceImpactPct: priceImpactPct ?? null,
      seller: seller || null,
      sellerTx: sellerTx || null,
      notes: notes || null,
      accepted: accepted ? 1 : 0,
      rejectReason: rejectReason || null,
    });
  }

  /** SignalEngine._restoreSellerTxsFromDb expects rows with seller_tx/ts. */
  getRecentAcceptedSellerTxs(dedupMs) {
    const cutoff = Date.now() - dedupMs;
    return this.stmts.recentAcceptedSellerTxs.all(cutoff);
  }

  // ============================================================
  // Trade API
  // ============================================================

  logTrade({ positionId, ts, mint, symbol, side, solAmount, tokenAmount, price, signature,
             success, dryRun, reason, latencyMs, error }) {
    this.stmts.insertTrade.run({
      positionId: positionId || null,
      ts: ts || Date.now(),
      mint: mint || null,
      symbol: symbol || null,
      side: side || null,
      solAmount: solAmount ?? null,
      tokenAmount: tokenAmount ?? null,
      price: price ?? null,
      signature: signature || null,
      success: success ? 1 : 0,
      dryRun: dryRun ? 1 : 0,
      reason: reason || null,
      latencyMs: latencyMs ?? null,
      error: error || null,
    });
  }

  // ============================================================
  // Position lifecycle API
  // ============================================================

  openPosition({ positionId, mint, symbol, openedAt, entrySol, entryPrice, tokenAmount,
                 dryRun, buySignature, buyFeeLamports, buySlot, dumpSlot,
                 entryFdv, entryPoolSol, entryLiquidity,
                 sellCount10s, totalSellSol10s,
                 mintAgeAtBuySec, rsiPreDump, rsi1sPreDump, rsi30sPreDump,
                 isEmaStrategy = 0, isAddOn = 0 }) {
    this.stmts.openPosition.run({
      positionId,
      mint,
      symbol: symbol || null,
      openedAt: openedAt || Date.now(),
      entrySol: entrySol ?? null,
      entryPrice: entryPrice ?? null,
      tokenAmount: tokenAmount ?? null,
      dryRun: dryRun ? 1 : 0,
      buySignature: buySignature || null,
      buyFeeLamports: buyFeeLamports || 0,
      buySlot: buySlot || 0,
      dumpSlot: dumpSlot || 0,
      entryFdv: entryFdv ?? null,
      entryPoolSol: entryPoolSol ?? null,
      entryLiquidity: entryLiquidity ?? null,
      sellCount10s: sellCount10s ?? null,        // v3.17.36: 连环拔回测
      totalSellSol10s: totalSellSol10s ?? null,  // v3.17.36: 连环拔回测
      mintAgeAtBuySec: mintAgeAtBuySec ?? null,  // v3.17.39: 首信号到买入的秒数
      rsiPreDump: rsiPreDump ?? null,              // v3.17.38: 砸单前 RSI5s
      rsi1sPreDump: rsi1sPreDump ?? null,          // v3.17.38: 砸单前 RSI1s
      rsi30sPreDump: rsi30sPreDump ?? null,        // v3.17.42: 砸单前 RSI30s
      isEmaStrategy: isEmaStrategy ?? 0,            // EMA策略标记
      isAddOn: isAddOn ?? 0,                       // 加仓标记
    });
  }

  updatePositionEntry(positionId, { entrySol, entryPrice, tokenAmount, buyFeeLamports, buySlot, dumpSlot }) {
    // v3.17.20-fix: 支持 buySlot/dumpSlot 更新
    const hasSlotUpdate = buySlot != null || dumpSlot != null;
    if (hasSlotUpdate) {
      this.db.prepare(`
        UPDATE positions SET
          entry_sol = @entrySol,
          entry_price = @entryPrice,
          token_amount = @tokenAmount,
          buy_fee_lamports = @buyFeeLamports,
          buy_slot = COALESCE(@buySlot, buy_slot),
          dump_slot = COALESCE(@dumpSlot, dump_slot)
        WHERE position_id = @positionId
      `).run({
        positionId,
        entrySol: entrySol ?? null,
        entryPrice: entryPrice ?? null,
        tokenAmount: tokenAmount ?? null,
        buyFeeLamports: buyFeeLamports ?? 0,
        buySlot: buySlot ?? null,
        dumpSlot: dumpSlot ?? null,
      });
    } else {
      this.stmts.updateEntry.run({
        positionId,
        entrySol: entrySol ?? null,
        entryPrice: entryPrice ?? null,
        tokenAmount: tokenAmount ?? null,
        buyFeeLamports: buyFeeLamports ?? 0,
      });
    }
  }

  closePosition(positionId, { closedAt, exitPrice, exitSol, pnlSol, pnlPct, exitReason, sellSignature, peakPnlPct, peakPrice, peakTs, timeToPeakMs, priceTickCount }) {
    this.stmts.closePosition.run({
      positionId,
      closedAt: closedAt || Date.now(),
      exitPrice: exitPrice ?? null,
      exitSol: exitSol ?? null,
      pnlSol: pnlSol ?? null,
      pnlPct: pnlPct ?? null,
      exitReason: exitReason || null,
      sellSignature: sellSignature || null,
      peakPnlPct: peakPnlPct ?? null,
      peakPrice: peakPrice ?? null,
      peakTs: peakTs ?? null,
      timeToPeakMs: timeToPeakMs ?? null,
      priceTickCount: priceTickCount ?? 0,
    });
  }

  // v3.17.31: 平仓后价格追踪写入
  recordPostExitStats({ positionId, mint, exitPrice, exitTs, maxPrice, maxPriceTs,
    maxPumpPct, minPrice, minPriceTs, maxDumpPct, sampleCount,
    snapshots, finalizedAt }) {
    if (!this.stmts.recordPostExitStats) {
      this.stmts.recordPostExitStats = this.db.prepare(`
        INSERT OR REPLACE INTO post_exit_stats (
          position_id, mint, exit_price, exit_ts,
          max_price, max_price_ts, max_pump_pct,
          min_price, min_price_ts, max_dump_pct,
          sample_count, snapshots, finalized_at
        ) VALUES (
          @positionId, @mint, @exitPrice, @exitTs,
          @maxPrice, @maxPriceTs, @maxPumpPct,
          @minPrice, @minPriceTs, @maxDumpPct,
          @sampleCount, @snapshots, @finalizedAt
        )
      `);
    }
    this.stmts.recordPostExitStats.run({
      positionId, mint, exitPrice, exitTs,
      maxPrice, maxPriceTs, maxPumpPct,
      minPrice, minPriceTs, maxDumpPct,
      sampleCount, snapshots, finalizedAt,
    });
  }

  markSellPending(positionId, signature, exitReason) {
    this.stmts.markSellPending.run(signature || null, exitReason || null, Date.now(), positionId);
  }

  markSellFailedPendingRetry(positionId, nextRetryAt, errorMsg, exitReason) {
    this.stmts.markSellFailedPendingRetry.run(
      nextRetryAt,
      errorMsg || null,
      exitReason || null,
      Date.now(),
      positionId,
    );
  }

  markStuck(positionId, reason) {
    this.stmts.markStuck.run(reason || null, Date.now(), positionId);
  }

  recordSellAttempt(positionId, errorMsg) {
    this.stmts.recordSellAttempt.run(errorMsg || null, Date.now(), positionId);
  }

  getOpenPositions() {
    return this.stmts.getOpenPositions.all();
  }

  getDuePendingRetries(now) {
    return this.stmts.getDuePendingRetries.all(now);
  }

  // ============================================================
  // Reporting / dashboard queries
  // ============================================================

  getSignalsInRange(startMs, endMs) {
    return this.stmts.signalsInRange.all(startMs, endMs);
  }

  getTradesInRange(startMs, endMs) {
    return this.stmts.tradesInRange.all(startMs, endMs);
  }

  getPositionsInRange(startMs, endMs) {
    return this.stmts.positionsInRange.all(startMs, endMs);
  }

  getRecentSignals(limit = 100) {
    return this.stmts.recentSignals.all(limit);
  }

  getRecentTrades(limit = 100) {
    return this.stmts.recentTrades.all(limit);
  }

  getRecentPositions(limit = 100) {
    return this.stmts.recentPositions.all(limit);
  }

  getStuckPositions() {
    return this.stmts.stuckPositions.all();
  }

  // ============ price_samples ============

  /**
   * Save a single price sample (called from SignalEngine._sampleLongPrice)
   */
  savePriceSample(mint, ts, price) {
    try {
      this.stmts.insertPriceSample.run({ mint, ts, price });
    } catch (_) { /* best effort */ }
  }

  /**
   * Load price samples from the last N milliseconds.
   * Returns Map<mint, [{ts, price}, ...]>
   */
  loadRecentPriceSamples(sinceMs) {
    const rows = this.stmts.loadRecentPriceSamples.all(sinceMs);
    const map = new Map();
    for (const row of rows) {
      let arr = map.get(row.mint);
      if (!arr) {
        arr = [];
        map.set(row.mint, arr);
      }
      arr.push({ ts: row.ts, price: row.price });
    }
    return map;
  }

  /**
   * v3.17.41: Count positions opened for a mint since a timestamp
   */
  countRecentBuysByMint(mint, sinceMs) {
    try {
      const row = this.db.prepare(
        'SELECT count(*) as cnt FROM positions WHERE mint = ? AND opened_at > ?'
      ).get(mint, sinceMs);
      return row ? row.cnt : 0;
    } catch (_) { return -1; }
  }

  /**
   * Delete price samples older than cutoffMs
   */
  cleanOldPriceSamples(cutoffMs) {
    try {
      this.stmts.cleanOldPriceSamples.run(cutoffMs);
    } catch (_) { /* best effort */ }
  }
}

module.exports = TradeLogger;
