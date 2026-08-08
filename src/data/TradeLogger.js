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
    this._researchBuffer = [];
    const configuredFlushMs = parseInt(
      process.env.POSITION_RESEARCH_FLUSH_MS || '250',
      10,
    );
    const configuredFlushMax = parseInt(
      process.env.POSITION_RESEARCH_FLUSH_MAX || '1000',
      10,
    );
    this._researchFlushMs = Number.isFinite(configuredFlushMs)
      ? Math.max(50, configuredFlushMs)
      : 250;
    this._researchFlushMax = Number.isFinite(configuredFlushMax)
      ? Math.max(100, configuredFlushMax)
      : 1000;
    this._researchFlushTimer = setInterval(
      () => this.flushResearchEvents(),
      this._researchFlushMs,
    );
    if (this._researchFlushTimer.unref) this._researchFlushTimer.unref();
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
        error TEXT,
        confirmation_status TEXT NOT NULL DEFAULT 'final'
      );
      CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);
      CREATE INDEX IF NOT EXISTS idx_trades_pos ON trades(position_id);
      CREATE INDEX IF NOT EXISTS idx_trades_signature ON trades(signature);

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
        pending_sell_last_valid_block_height INTEGER,
        stuck_reason TEXT,
        runner_armed INTEGER DEFAULT 0,
        runner_armed_at INTEGER
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

      CREATE TABLE IF NOT EXISTS swap_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        mint TEXT NOT NULL,
        symbol TEXT,
        signer TEXT,
        side TEXT NOT NULL,
        sol_volume REAL,
        price REAL,
        price_before REAL,
        price_change_pct REAL,
        slot INTEGER,
        signature TEXT,
        pool_address TEXT,
        pool_quote_after REAL
      );
      CREATE INDEX IF NOT EXISTS idx_swap_events_ts ON swap_events(ts);
      CREATE INDEX IF NOT EXISTS idx_swap_events_mint_ts ON swap_events(mint, ts);
      CREATE INDEX IF NOT EXISTS idx_swap_events_signature ON swap_events(signature);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_swap_events_sig_mint_side
        ON swap_events(signature, mint, side)
        WHERE signature IS NOT NULL AND signature != '';

      CREATE TABLE IF NOT EXISTS position_research_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        position_id TEXT NOT NULL,
        mint TEXT NOT NULL,
        symbol TEXT,
        event_type TEXT NOT NULL,
        ts INTEGER NOT NULL,
        received_at INTEGER NOT NULL,
        hold_ms INTEGER,
        slot INTEGER,
        signature TEXT,
        side TEXT,
        signer TEXT,
        sol_volume REAL,
        price REAL,
        raw_price REAL,
        pool_address TEXT,
        pool_base_after REAL,
        pool_quote_after REAL,
        base_decimals INTEGER,
        supply_ui REAL,
        fdv_usd REAL,
        liquidity_usd REAL,
        entry_sol REAL,
        entry_price REAL,
        signal_price REAL,
        pre_entry_vwap_5s REAL,
        token_amount REAL,
        market_pnl_pct REAL,
        peak_pnl_pct REAL,
        drawdown_pct REAL,
        trailing_armed INTEGER NOT NULL DEFAULT 0,
        reconciled INTEGER NOT NULL DEFAULT 0,
        metrics_json TEXT,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_position_research_pos_ts
        ON position_research_events(position_id, received_at);
      CREATE INDEX IF NOT EXISTS idx_position_research_mint_ts
        ON position_research_events(mint, received_at);
      CREATE INDEX IF NOT EXISTS idx_position_research_type_ts
        ON position_research_events(event_type, received_at);

      CREATE TABLE IF NOT EXISTS migration_risk_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mint TEXT NOT NULL,
        symbol TEXT,
        migration_signature TEXT NOT NULL UNIQUE,
        migration_slot INTEGER,
        migration_time INTEGER NOT NULL,
        captured_at INTEGER NOT NULL,
        window_before_ms INTEGER NOT NULL,
        window_after_ms INTEGER NOT NULL,
        swap_event_count INTEGER,
        buy_count INTEGER,
        sell_count INTEGER,
        buy_sol REAL,
        sell_sol REAL,
        net_flow_sol REAL,
        unique_buyers INTEGER,
        unique_sellers INTEGER,
        largest_buy_share REAL,
        price_return_pct REAL,
        peak_return_pct REAL,
        trough_return_pct REAL,
        max_drawdown_pct REAL,
        pool_quote_change_pct REAL,
        mint_to_count INTEGER,
        large_transfer_count INTEGER,
        same_tx_buy_count INTEGER,
        audit_incomplete INTEGER NOT NULL DEFAULT 0,
        metrics_json TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_migration_risk_mint_time
        ON migration_risk_snapshots(mint, migration_time);
      CREATE INDEX IF NOT EXISTS idx_migration_risk_time
        ON migration_risk_snapshots(migration_time);

      CREATE TABLE IF NOT EXISTS migration_detection_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mint TEXT NOT NULL UNIQUE,
        migration_signature TEXT,
        migration_slot INTEGER,
        migration_time INTEGER NOT NULL,
        detected_at INTEGER NOT NULL,
        detection_path TEXT,
        detection_slot INTEGER,
        pool_address TEXT,
        details_json TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_migration_detection_time
        ON migration_detection_events(migration_time);

      CREATE TABLE IF NOT EXISTS token_lifecycle_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT NOT NULL UNIQUE,
        mint TEXT NOT NULL,
        symbol TEXT,
        event_type TEXT NOT NULL,
        ts INTEGER NOT NULL,
        source TEXT,
        reason TEXT,
        migration_time INTEGER,
        migration_age_ms INTEGER,
        added_at INTEGER,
        watch_age_ms INTEGER,
        fdv_usd REAL,
        liquidity_usd REAL,
        price_usd REAL,
        price_sol REAL,
        pool_quote_sol REAL,
        market_source TEXT,
        details_json TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_token_lifecycle_mint_ts
        ON token_lifecycle_events(mint, ts);
      CREATE INDEX IF NOT EXISTS idx_token_lifecycle_type_ts
        ON token_lifecycle_events(event_type, ts);

      CREATE TABLE IF NOT EXISTS token_market_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mint TEXT NOT NULL,
        symbol TEXT,
        ts INTEGER NOT NULL,
        trigger TEXT NOT NULL,
        source TEXT,
        fdv_usd REAL,
        liquidity_usd REAL,
        price_usd REAL,
        price_sol REAL,
        supply_ui REAL,
        pool_quote_sol REAL,
        pool_address TEXT,
        market_fetched_at INTEGER,
        migration_age_ms INTEGER,
        watch_age_ms INTEGER,
        details_json TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_token_market_mint_ts
        ON token_market_snapshots(mint, ts);
      CREATE INDEX IF NOT EXISTS idx_token_market_ts
        ON token_market_snapshots(ts);

      CREATE TABLE IF NOT EXISTS migration_holder_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mint TEXT NOT NULL,
        symbol TEXT,
        migration_signature TEXT NOT NULL UNIQUE,
        migration_slot INTEGER,
        migration_time INTEGER NOT NULL,
        captured_at INTEGER NOT NULL,
        capture_delay_ms INTEGER,
        source TEXT,
        is_complete INTEGER NOT NULL DEFAULT 0,
        page_count INTEGER,
        holder_count INTEGER,
        token_account_count INTEGER,
        supply_ui REAL,
        excluded_pool_amount REAL,
        top1_pct REAL,
        top5_pct REAL,
        top10_pct REAL,
        top20_pct REAL,
        largest_holder_owner TEXT,
        largest_holder_pct REAL,
        holders_json TEXT,
        error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_migration_holders_mint_time
        ON migration_holder_snapshots(mint, migration_time);
      CREATE INDEX IF NOT EXISTS idx_migration_holders_time
        ON migration_holder_snapshots(migration_time);

      CREATE TABLE IF NOT EXISTS quote_asset_movements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        signature TEXT NOT NULL,
        mint TEXT NOT NULL DEFAULT '',
        side TEXT,
        success INTEGER NOT NULL DEFAULT 0,
        native_sol_delta REAL,
        wallet_wsol_delta REAL,
        wallet_wsol_reserve_delta REAL,
        jupiter_escrow_wsol_delta REAL,
        quote_asset_delta REAL,
        pre_native_sol REAL,
        post_native_sol REAL,
        pre_wallet_wsol_sol REAL,
        post_wallet_wsol_sol REAL,
        pre_jupiter_escrow_wsol_sol REAL,
        post_jupiter_escrow_wsol_sol REAL,
        fee_lamports INTEGER,
        details_json TEXT,
        created_at INTEGER NOT NULL,
        UNIQUE(signature, mint, side)
      );
      CREATE INDEX IF NOT EXISTS idx_quote_asset_movements_ts
        ON quote_asset_movements(ts);

      CREATE TABLE IF NOT EXISTS quote_asset_reconciliations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        reason TEXT,
        status TEXT NOT NULL,
        native_sol REAL,
        wallet_wsol_sol REAL,
        wallet_wsol_rent_sol REAL,
        jupiter_pending_wsol_sol REAL,
        total_equity_sol REAL,
        wallet_wsol_account_count INTEGER,
        action TEXT,
        details_json TEXT,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_quote_asset_reconciliations_ts
        ON quote_asset_reconciliations(ts);
    `);

    for (const sql of [
      "ALTER TABLE trades ADD COLUMN confirmation_status TEXT NOT NULL DEFAULT 'final'",
      'ALTER TABLE quote_asset_movements ADD COLUMN jupiter_escrow_wsol_delta REAL',
      'ALTER TABLE quote_asset_movements ADD COLUMN wallet_wsol_reserve_delta REAL',
      'ALTER TABLE quote_asset_movements ADD COLUMN pre_jupiter_escrow_wsol_sol REAL',
      'ALTER TABLE quote_asset_movements ADD COLUMN post_jupiter_escrow_wsol_sol REAL',
      'ALTER TABLE quote_asset_reconciliations ADD COLUMN wallet_wsol_rent_sol REAL',
      'ALTER TABLE quote_asset_movements ADD COLUMN details_json TEXT',
    ]) {
      try { this.db.exec(sql); } catch (_) { /* column already exists */ }
    }

    // External router/vault accounts were previously attributed to this wallet.
    // Rebuild those audit rows from wallet-controlled components only, then
    // clear the legacy columns so future exports cannot repeat that mistake.
    this.db.exec(`
      UPDATE quote_asset_movements
      SET quote_asset_delta =
            COALESCE(native_sol_delta, 0) +
            COALESCE(wallet_wsol_delta, 0) +
            COALESCE(wallet_wsol_reserve_delta, 0),
          jupiter_escrow_wsol_delta = NULL,
          pre_jupiter_escrow_wsol_sol = NULL,
          post_jupiter_escrow_wsol_sol = NULL
      WHERE jupiter_escrow_wsol_delta IS NOT NULL
         OR pre_jupiter_escrow_wsol_sol IS NOT NULL
         OR post_jupiter_escrow_wsol_sol IS NOT NULL;

      UPDATE quote_asset_reconciliations
      SET total_equity_sol = COALESCE(native_sol, 0) + COALESCE(wallet_wsol_sol, 0),
          jupiter_pending_wsol_sol = NULL
      WHERE jupiter_pending_wsol_sol IS NOT NULL
         OR ABS(
              COALESCE(total_equity_sol, 0) -
              (COALESCE(native_sol, 0) + COALESCE(wallet_wsol_sol, 0))
            ) > 0.000000001;
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

    const positionResearchColumns = [
      // Columns referenced by PositionManager/closePosition but absent from
      // some fresh or older database schemas.
      'peak_price REAL',
      'peak_ts INTEGER',
      'peak_pnl_pct REAL',
      'time_to_peak_ms INTEGER',
      'price_tick_count INTEGER DEFAULT 0',
      'pre_vol_5m_pct REAL',
      'range_support REAL',
      'entry_signal_price REAL',
      'pre_entry_vwap_5s REAL',
      'pre_entry_unique_buyers_3s INTEGER',
      'entry_metrics_json TEXT',
      'runner_armed INTEGER DEFAULT 0',
      'runner_armed_at INTEGER',
      'pending_sell_last_valid_block_height INTEGER',
    ];
    for (const definition of positionResearchColumns) {
      try {
        this.db.exec(`ALTER TABLE positions ADD COLUMN ${definition}`);
      } catch (_) { /* column already exists */ }
    }

    const swapResearchColumns = [
      'received_at INTEGER',
      'raw_price REAL',
      'pool_base_after REAL',
      'base_decimals INTEGER',
      'virtual_quote_reserve_sol REAL',
      'effective_quote_reserve_sol REAL',
      'supply_ui REAL',
      'fdv_usd REAL',
      'liquidity_usd REAL',
      'price_usd REAL',
      'market_fetched_at INTEGER',
    ];
    for (const definition of swapResearchColumns) {
      try {
        this.db.exec(`ALTER TABLE swap_events ADD COLUMN ${definition}`);
      } catch (_) { /* column already exists */ }
    }
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
           success, dry_run, reason, latency_ms, error, confirmation_status)
        VALUES (@positionId, @ts, @mint, @symbol, @side, @solAmount, @tokenAmount, @price, @signature,
                @success, @dryRun, @reason, @latencyMs, @error, @confirmationStatus)
      `),

      updateTradeConfirmation: this.db.prepare(`
        UPDATE trades SET
          success = @success,
          sol_amount = COALESCE(@solAmount, sol_amount),
          token_amount = COALESCE(@tokenAmount, token_amount),
          price = COALESCE(@price, price),
          error = @error,
          confirmation_status = @confirmationStatus
        WHERE signature = @signature AND side = @side
      `),

      tradesInRange: this.db.prepare(`
        SELECT * FROM trades WHERE ts >= ? AND ts < ? ORDER BY ts ASC
      `),

      recentTrades: this.db.prepare(`
        SELECT * FROM trades ORDER BY ts DESC LIMIT ?
      `),

      insertQuoteAssetMovement: this.db.prepare(`
        INSERT OR IGNORE INTO quote_asset_movements (
          ts, signature, mint, side, success, native_sol_delta,
          wallet_wsol_delta, wallet_wsol_reserve_delta,
          jupiter_escrow_wsol_delta, quote_asset_delta, pre_native_sol,
          post_native_sol, pre_wallet_wsol_sol, post_wallet_wsol_sol,
          pre_jupiter_escrow_wsol_sol, post_jupiter_escrow_wsol_sol,
          fee_lamports, details_json, created_at
        ) VALUES (
          @ts, @signature, @mint, @side, @success, @nativeSolDelta,
          @walletWsolDelta, @walletWsolReserveDelta,
          @jupiterEscrowWsolDelta, @quoteAssetDelta, @preNativeSol,
          @postNativeSol, @preWalletWsolSol, @postWalletWsolSol,
          @preJupiterEscrowWsolSol, @postJupiterEscrowWsolSol,
          @feeLamports, @detailsJson, @createdAt
        )
      `),

      insertQuoteAssetReconciliation: this.db.prepare(`
        INSERT INTO quote_asset_reconciliations (
          ts, reason, status, native_sol, wallet_wsol_sol, wallet_wsol_rent_sol,
          jupiter_pending_wsol_sol, total_equity_sol,
          wallet_wsol_account_count, action, details_json, created_at
        ) VALUES (
          @ts, @reason, @status, @nativeSol, @walletWsolSol, @walletWsolRentSol,
          @jupiterPendingWsolSol, @totalEquitySol,
          @walletWsolAccountCount, @action, @detailsJson, @createdAt
        )
      `),

      latestQuoteAssetReconciliation: this.db.prepare(`
        SELECT * FROM quote_asset_reconciliations ORDER BY ts DESC LIMIT 1
      `),

      insertMigrationRiskSnapshot: this.db.prepare(`
        INSERT INTO migration_risk_snapshots (
          mint, symbol, migration_signature, migration_slot, migration_time,
          captured_at, window_before_ms, window_after_ms, swap_event_count,
          buy_count, sell_count, buy_sol, sell_sol, net_flow_sol,
          unique_buyers, unique_sellers, largest_buy_share,
          price_return_pct, peak_return_pct, trough_return_pct,
          max_drawdown_pct, pool_quote_change_pct, mint_to_count,
          large_transfer_count, same_tx_buy_count, audit_incomplete,
          metrics_json
        ) VALUES (
          @mint, @symbol, @migrationSignature, @migrationSlot, @migrationTime,
          @capturedAt, @windowBeforeMs, @windowAfterMs, @swapEventCount,
          @buyCount, @sellCount, @buySol, @sellSol, @netFlowSol,
          @uniqueBuyers, @uniqueSellers, @largestBuyShare,
          @priceReturnPct, @peakReturnPct, @troughReturnPct,
          @maxDrawdownPct, @poolQuoteChangePct, @mintToCount,
          @largeTransferCount, @sameTxBuyCount, @auditIncomplete,
          @metricsJson
        )
        ON CONFLICT(migration_signature) DO UPDATE SET
          captured_at = excluded.captured_at,
          swap_event_count = excluded.swap_event_count,
          buy_count = excluded.buy_count,
          sell_count = excluded.sell_count,
          buy_sol = excluded.buy_sol,
          sell_sol = excluded.sell_sol,
          net_flow_sol = excluded.net_flow_sol,
          unique_buyers = excluded.unique_buyers,
          unique_sellers = excluded.unique_sellers,
          largest_buy_share = excluded.largest_buy_share,
          price_return_pct = excluded.price_return_pct,
          peak_return_pct = excluded.peak_return_pct,
          trough_return_pct = excluded.trough_return_pct,
          max_drawdown_pct = excluded.max_drawdown_pct,
          pool_quote_change_pct = excluded.pool_quote_change_pct,
          mint_to_count = excluded.mint_to_count,
          large_transfer_count = excluded.large_transfer_count,
          same_tx_buy_count = excluded.same_tx_buy_count,
          audit_incomplete = excluded.audit_incomplete,
          metrics_json = excluded.metrics_json
      `),

      insertMigrationDetection: this.db.prepare(`
        INSERT INTO migration_detection_events (
          mint, migration_signature, migration_slot, migration_time,
          detected_at, detection_path, detection_slot, pool_address, details_json
        ) VALUES (
          @mint, @migrationSignature, @migrationSlot, @migrationTime,
          @detectedAt, @detectionPath, @detectionSlot, @poolAddress, @detailsJson
        )
        ON CONFLICT(mint) DO UPDATE SET
          migration_signature = COALESCE(
            migration_detection_events.migration_signature,
            excluded.migration_signature
          ),
          migration_slot = COALESCE(
            migration_detection_events.migration_slot,
            excluded.migration_slot
          ),
          migration_time = MIN(
            migration_detection_events.migration_time,
            excluded.migration_time
          ),
          detected_at = MIN(
            migration_detection_events.detected_at,
            excluded.detected_at
          ),
          detection_path = COALESCE(
            migration_detection_events.detection_path,
            excluded.detection_path
          ),
          detection_slot = COALESCE(
            migration_detection_events.detection_slot,
            excluded.detection_slot
          ),
          pool_address = COALESCE(
            migration_detection_events.pool_address,
            excluded.pool_address
          ),
          details_json = COALESCE(
            migration_detection_events.details_json,
            excluded.details_json
          )
      `),

      insertTokenLifecycleEvent: this.db.prepare(`
        INSERT OR IGNORE INTO token_lifecycle_events (
          event_key, mint, symbol, event_type, ts, source, reason,
          migration_time, migration_age_ms, added_at, watch_age_ms,
          fdv_usd, liquidity_usd, price_usd, price_sol, pool_quote_sol,
          market_source, details_json
        ) VALUES (
          @eventKey, @mint, @symbol, @eventType, @ts, @source, @reason,
          @migrationTime, @migrationAgeMs, @addedAt, @watchAgeMs,
          @fdvUsd, @liquidityUsd, @priceUsd, @priceSol, @poolQuoteSol,
          @marketSource, @detailsJson
        )
      `),

      selectOpenTokenLifecycleSession: this.db.prepare(`
        SELECT a.added_at, a.ts
        FROM token_lifecycle_events a
        WHERE a.mint = ?
          AND a.event_type = 'TOKEN_ADDED'
          AND NOT EXISTS (
            SELECT 1
            FROM token_lifecycle_events r
            WHERE r.mint = a.mint
              AND r.event_type = 'TOKEN_REMOVED'
              AND r.added_at = a.added_at
          )
        ORDER BY a.ts DESC, a.id DESC
        LIMIT 1
      `),

      insertTokenMarketSnapshot: this.db.prepare(`
        INSERT INTO token_market_snapshots (
          mint, symbol, ts, trigger, source, fdv_usd, liquidity_usd,
          price_usd, price_sol, supply_ui, pool_quote_sol, pool_address,
          market_fetched_at, migration_age_ms, watch_age_ms, details_json
        ) VALUES (
          @mint, @symbol, @ts, @trigger, @source, @fdvUsd, @liquidityUsd,
          @priceUsd, @priceSol, @supplyUi, @poolQuoteSol, @poolAddress,
          @marketFetchedAt, @migrationAgeMs, @watchAgeMs, @detailsJson
        )
      `),

      insertMigrationHolderSnapshot: this.db.prepare(`
        INSERT INTO migration_holder_snapshots (
          mint, symbol, migration_signature, migration_slot, migration_time,
          captured_at, capture_delay_ms, source, is_complete, page_count,
          holder_count, token_account_count, supply_ui, excluded_pool_amount,
          top1_pct, top5_pct, top10_pct, top20_pct, largest_holder_owner,
          largest_holder_pct, holders_json, error
        ) VALUES (
          @mint, @symbol, @migrationSignature, @migrationSlot, @migrationTime,
          @capturedAt, @captureDelayMs, @source, @isComplete, @pageCount,
          @holderCount, @tokenAccountCount, @supplyUi, @excludedPoolAmount,
          @top1Pct, @top5Pct, @top10Pct, @top20Pct, @largestHolderOwner,
          @largestHolderPct, @holdersJson, @error
        )
        ON CONFLICT(migration_signature) DO UPDATE SET
          symbol = COALESCE(excluded.symbol, migration_holder_snapshots.symbol),
          captured_at = excluded.captured_at,
          capture_delay_ms = excluded.capture_delay_ms,
          source = excluded.source,
          is_complete = excluded.is_complete,
          page_count = excluded.page_count,
          holder_count = excluded.holder_count,
          token_account_count = excluded.token_account_count,
          supply_ui = excluded.supply_ui,
          excluded_pool_amount = excluded.excluded_pool_amount,
          top1_pct = excluded.top1_pct,
          top5_pct = excluded.top5_pct,
          top10_pct = excluded.top10_pct,
          top20_pct = excluded.top20_pct,
          largest_holder_owner = excluded.largest_holder_owner,
          largest_holder_pct = excluded.largest_holder_pct,
          holders_json = excluded.holders_json,
          error = excluded.error
      `),

      selectMigrationHolderSnapshotStatus: this.db.prepare(`
        SELECT is_complete, captured_at, source
        FROM migration_holder_snapshots
        WHERE migration_signature = ?
        LIMIT 1
      `),

      selectMigrationHolderSnapshotStatusByMint: this.db.prepare(`
        SELECT is_complete, captured_at, source, migration_signature
        FROM migration_holder_snapshots
        WHERE mint = ?
        ORDER BY is_complete DESC, captured_at DESC
        LIMIT 1
      `),

      updateMigrationHolderSnapshotSymbol: this.db.prepare(`
        UPDATE migration_holder_snapshots
        SET symbol = COALESCE(?, symbol)
        WHERE migration_signature = ?
      `),

      updateMigrationHolderSnapshotSymbolByMint: this.db.prepare(`
        UPDATE migration_holder_snapshots
        SET symbol = COALESCE(?, symbol)
        WHERE mint = ?
      `),

      // ============ swap_events ============
      insertSwapEvent: this.db.prepare(`
        INSERT OR IGNORE INTO swap_events
          (ts, mint, symbol, signer, side, sol_volume, price, price_before, price_change_pct,
           slot, signature, pool_address, pool_quote_after, received_at, raw_price,
           pool_base_after, base_decimals, virtual_quote_reserve_sol,
           effective_quote_reserve_sol, supply_ui, fdv_usd, liquidity_usd,
           price_usd, market_fetched_at)
        VALUES
          (@ts, @mint, @symbol, @signer, @side, @solVolume, @price, @priceBefore, @priceChangePct,
           @slot, @signature, @poolAddress, @poolQuoteAfter, @receivedAt, @rawPrice,
           @poolBaseAfter, @baseDecimals, @virtualQuoteReserveSol,
           @effectiveQuoteReserveSol, @supplyUi, @fdvUsd, @liquidityUsd,
           @priceUsd, @marketFetchedAt)
      `),

      swapEventsInRange: this.db.prepare(`
        SELECT * FROM swap_events WHERE ts >= ? AND ts < ? ORDER BY mint, ts ASC
      `),

      swapEventsForMintInRange: this.db.prepare(`
        SELECT * FROM swap_events
        WHERE mint = ? AND ts >= ? AND ts <= ?
        ORDER BY ts ASC, id ASC
      `),

      // ============ positions ============
      openPosition: this.db.prepare(`
        INSERT INTO positions
          (position_id, mint, symbol, opened_at, entry_sol, entry_price, token_amount,
           dry_run, buy_signature, buy_fee_lamports, buy_slot, dump_slot,
           entry_fdv, entry_pool_sol, entry_liquidity,
           sell_count_10s, total_sell_sol_10s,
           mint_age_at_buy_sec, rsi_pre_dump, rsi_1s_pre_dump, rsi_30s_pre_dump,
           entry_signal_price, pre_entry_vwap_5s, pre_entry_unique_buyers_3s,
           entry_metrics_json, is_ema_strategy, is_addon, status)
        VALUES (@positionId, @mint, @symbol, @openedAt, @entrySol, @entryPrice, @tokenAmount,
                @dryRun, @buySignature, @buyFeeLamports, @buySlot, @dumpSlot,
                @entryFdv, @entryPoolSol, @entryLiquidity,
                @sellCount10s, @totalSellSol10s,
                @mintAgeAtBuySec, @rsiPreDump, @rsi1sPreDump, @rsi30sPreDump,
                @entrySignalPrice, @preEntryVwap5s, @preEntryUniqueBuyers3s,
                @entryMetricsJson, @isEmaStrategy, @isAddOn, 'open')
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
          entry_signal_price = excluded.entry_signal_price,
          pre_entry_vwap_5s = excluded.pre_entry_vwap_5s,
          pre_entry_unique_buyers_3s = excluded.pre_entry_unique_buyers_3s,
          entry_metrics_json = excluded.entry_metrics_json,
          is_ema_strategy = excluded.is_ema_strategy,
          is_addon = excluded.is_addon,
          status = 'open'
      `),

      insertPositionResearchEvent: this.db.prepare(`
        INSERT INTO position_research_events (
          position_id, mint, symbol, event_type, ts, received_at, hold_ms,
          slot, signature, side, signer, sol_volume, price, raw_price,
          pool_address, pool_base_after, pool_quote_after, base_decimals,
          supply_ui, fdv_usd, liquidity_usd, entry_sol, entry_price,
          signal_price, pre_entry_vwap_5s, token_amount, market_pnl_pct,
          peak_pnl_pct, drawdown_pct, trailing_armed, reconciled,
          metrics_json, created_at
        ) VALUES (
          @positionId, @mint, @symbol, @eventType, @ts, @receivedAt, @holdMs,
          @slot, @signature, @side, @signer, @solVolume, @price, @rawPrice,
          @poolAddress, @poolBaseAfter, @poolQuoteAfter, @baseDecimals,
          @supplyUi, @fdvUsd, @liquidityUsd, @entrySol, @entryPrice,
          @signalPrice, @preEntryVwap5s, @tokenAmount, @marketPnlPct,
          @peakPnlPct, @drawdownPct, @trailingArmed, @reconciled,
          @metricsJson, @createdAt
        )
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

      updateRunnerState: this.db.prepare(`
        UPDATE positions SET
          runner_armed = @runnerArmed,
          runner_armed_at = @runnerArmedAt
        WHERE position_id = @positionId
      `),

      markSellPending: this.db.prepare(`
        UPDATE positions SET
          status = 'sell_confirming',
          pending_sell_signature = ?,
          pending_sell_last_valid_block_height = ?,
          exit_intent = ?,
          last_retry_at = ?,
          next_retry_at = ?
        WHERE position_id = ?
      `),

      updatePositionTokenAmount: this.db.prepare(`
        UPDATE positions SET token_amount = ?, last_error = ?
        WHERE position_id = ?
      `),

      deferSellConfirmation: this.db.prepare(`
        UPDATE positions SET next_retry_at = ? WHERE position_id = ?
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
          AND next_retry_at IS NOT NULL
          AND next_retry_at <= ?
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
    this._insertResearchBatch = this.db.transaction((rows) => {
      for (const row of rows) {
        this.stmts.insertPositionResearchEvent.run(row);
      }
    });
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
             success, dryRun, reason, latencyMs, error, confirmationStatus }) {
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
      confirmationStatus: confirmationStatus || (success ? 'confirmed' : 'failed'),
    });
  }

  updateTradeConfirmation(signature, {
    side = 'SELL',
    success = false,
    solAmount = null,
    tokenAmount = null,
    price = null,
    error = null,
    confirmationStatus = success ? 'confirmed' : 'failed',
  } = {}) {
    if (!signature) return;
    this.stmts.updateTradeConfirmation.run({
      signature,
      side,
      success: success ? 1 : 0,
      solAmount,
      tokenAmount,
      price,
      error: error || null,
      confirmationStatus,
    });
  }

  logQuoteAssetMovement(movement) {
    if (!movement?.signature) return;
    this.stmts.insertQuoteAssetMovement.run({
      ts: movement.ts || Date.now(),
      signature: movement.signature,
      mint: movement.mint || '',
      side: movement.side || 'UNKNOWN',
      success: movement.success ? 1 : 0,
      nativeSolDelta: movement.nativeSolDelta ?? null,
      walletWsolDelta: movement.walletWsolDelta ?? null,
      walletWsolReserveDelta: movement.walletWsolReserveDelta ?? null,
      // Legacy schema fields are intentionally always NULL.
      jupiterEscrowWsolDelta: null,
      quoteAssetDelta: movement.quoteAssetDelta ?? null,
      preNativeSol: movement.preNativeSol ?? null,
      postNativeSol: movement.postNativeSol ?? null,
      preWalletWsolSol: movement.preWalletWsolSol ?? null,
      postWalletWsolSol: movement.postWalletWsolSol ?? null,
      preJupiterEscrowWsolSol: null,
      postJupiterEscrowWsolSol: null,
      feeLamports: movement.feeLamports ?? null,
      detailsJson: movement.details ? JSON.stringify(movement.details) : null,
      createdAt: Date.now(),
    });
  }

  logQuoteAssetReconciliation(row) {
    const detailsJson = row.details == null
      ? null
      : JSON.stringify(row.details, (_, value) =>
        typeof value === 'bigint' ? value.toString() : value);
    this.stmts.insertQuoteAssetReconciliation.run({
      ts: row.ts || Date.now(),
      reason: row.reason || null,
      status: row.status || 'unknown',
      nativeSol: row.nativeSol ?? null,
      walletWsolSol: row.walletWsolSol ?? null,
      walletWsolRentSol: row.walletWsolRentSol ?? null,
      // Legacy schema field is intentionally always NULL.
      jupiterPendingWsolSol: null,
      totalEquitySol: row.totalEquitySol ?? null,
      walletWsolAccountCount: row.walletWsolAccountCount ?? null,
      action: row.action || null,
      detailsJson,
      createdAt: Date.now(),
    });
  }

  getLatestQuoteAssetReconciliation() {
    return this.stmts.latestQuoteAssetReconciliation.get() || null;
  }

  logMigrationRiskSnapshot(row) {
    if (!row?.mint || !row?.migrationSignature) return;
    let metricsJson = null;
    try {
      metricsJson = typeof row.metrics === 'string'
        ? row.metrics
        : JSON.stringify(row.metrics || {});
    } catch (_) {
      metricsJson = JSON.stringify({ serializationError: true });
    }
    this.stmts.insertMigrationRiskSnapshot.run({
      mint: row.mint,
      symbol: row.symbol || null,
      migrationSignature: row.migrationSignature,
      migrationSlot: row.migrationSlot ?? null,
      migrationTime: row.migrationTime,
      capturedAt: row.capturedAt || Date.now(),
      windowBeforeMs: row.windowBeforeMs ?? 0,
      windowAfterMs: row.windowAfterMs ?? 0,
      swapEventCount: row.swapEventCount ?? 0,
      buyCount: row.buyCount ?? 0,
      sellCount: row.sellCount ?? 0,
      buySol: row.buySol ?? 0,
      sellSol: row.sellSol ?? 0,
      netFlowSol: row.netFlowSol ?? 0,
      uniqueBuyers: row.uniqueBuyers ?? 0,
      uniqueSellers: row.uniqueSellers ?? 0,
      largestBuyShare: row.largestBuyShare ?? null,
      priceReturnPct: row.priceReturnPct ?? null,
      peakReturnPct: row.peakReturnPct ?? null,
      troughReturnPct: row.troughReturnPct ?? null,
      maxDrawdownPct: row.maxDrawdownPct ?? null,
      poolQuoteChangePct: row.poolQuoteChangePct ?? null,
      mintToCount: row.mintToCount ?? 0,
      largeTransferCount: row.largeTransferCount ?? 0,
      sameTxBuyCount: row.sameTxBuyCount ?? 0,
      auditIncomplete: row.auditIncomplete ? 1 : 0,
      metricsJson,
    });
  }

  logMigrationDetection(row) {
    if (!row?.mint || !row?.migrationTime) return;
    let detailsJson = null;
    try {
      detailsJson = row.details == null ? null : JSON.stringify(row.details);
    } catch (_) {
      detailsJson = JSON.stringify({ serializationError: true });
    }
    try {
      this.stmts.insertMigrationDetection.run({
        mint: row.mint,
        migrationSignature: row.migrationSignature || null,
        migrationSlot: row.migrationSlot ?? null,
        migrationTime: row.migrationTime,
        detectedAt: row.detectedAt || Date.now(),
        detectionPath: row.detectionPath || null,
        detectionSlot: row.detectionSlot ?? null,
        poolAddress: row.poolAddress || null,
        detailsJson,
      });
    } catch (_) { /* research only */ }
  }

  logSwapEvent(swap) {
    if (!swap || !swap.mint) return;
    const side = String(swap.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;

    const num = (value) => {
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    };

    try {
      this.stmts.insertSwapEvent.run({
        ts: num(swap.ts) || Date.now(),
        mint: swap.mint,
        symbol: swap.symbol || null,
        signer: swap.signer || null,
        side,
        solVolume: num(swap.solVolume),
        price: num(swap.price),
        priceBefore: num(swap.priceBefore),
        priceChangePct: num(swap.priceChangePct),
        slot: num(swap.slot),
        signature: swap.signature || null,
        poolAddress: swap.poolAddress || null,
        poolQuoteAfter: num(swap.poolQuoteAfter),
        receivedAt: num(swap.receivedAt) || Date.now(),
        rawPrice: num(swap.rawPrice),
        poolBaseAfter: num(swap.poolBaseAfter),
        baseDecimals: num(swap.baseDecimals),
        virtualQuoteReserveSol: num(swap.virtualQuoteReserveSol),
        effectiveQuoteReserveSol: num(swap.effectiveQuoteReserveSol),
        supplyUi: num(swap.supplyUi),
        fdvUsd: num(swap.fdvUsd),
        liquidityUsd: num(swap.liquidityUsd),
        priceUsd: num(swap.priceUsd),
        marketFetchedAt: num(swap.marketFetchedAt),
      });
    } catch (_) { /* best effort; strategy must never block on analytics writes */ }
  }

  logTokenLifecycleEvent(event) {
    if (!event?.mint || !event?.eventType) return;
    const numberOrNull = (value) => {
      if (value == null || value === '') return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };
    const ts = numberOrNull(event.ts) || Date.now();
    const eventKey = event.eventKey ||
      `${event.eventType}:${event.mint}:${numberOrNull(event.addedAt) || ts}`;
    let detailsJson = null;
    try {
      detailsJson = event.details == null ? null : JSON.stringify(event.details);
    } catch (_) {
      detailsJson = JSON.stringify({ serializationError: true });
    }
    try {
      this.stmts.insertTokenLifecycleEvent.run({
        eventKey,
        mint: event.mint,
        symbol: event.symbol || null,
        eventType: event.eventType,
        ts,
        source: event.source || null,
        reason: event.reason || null,
        migrationTime: numberOrNull(event.migrationTime),
        migrationAgeMs: numberOrNull(event.migrationAgeMs),
        addedAt: numberOrNull(event.addedAt),
        watchAgeMs: numberOrNull(event.watchAgeMs),
        fdvUsd: numberOrNull(event.fdvUsd),
        liquidityUsd: numberOrNull(event.liquidityUsd),
        priceUsd: numberOrNull(event.priceUsd),
        priceSol: numberOrNull(event.priceSol),
        poolQuoteSol: numberOrNull(event.poolQuoteSol),
        marketSource: event.marketSource || null,
        detailsJson,
      });
    } catch (_) { /* research only */ }
  }

  getOpenTokenLifecycleSession(mint) {
    if (!mint) return null;
    try {
      return this.stmts.selectOpenTokenLifecycleSession.get(mint) || null;
    } catch (_) {
      return null;
    }
  }

  logTokenMarketSnapshot(snapshot) {
    if (!snapshot?.mint || !snapshot?.trigger) return;
    const numberOrNull = (value) => {
      if (value == null || value === '') return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };
    let detailsJson = null;
    try {
      detailsJson = snapshot.details == null ? null : JSON.stringify(snapshot.details);
    } catch (_) {
      detailsJson = JSON.stringify({ serializationError: true });
    }
    try {
      this.stmts.insertTokenMarketSnapshot.run({
        mint: snapshot.mint,
        symbol: snapshot.symbol || null,
        ts: numberOrNull(snapshot.ts) || Date.now(),
        trigger: snapshot.trigger,
        source: snapshot.source || null,
        fdvUsd: numberOrNull(snapshot.fdvUsd),
        liquidityUsd: numberOrNull(snapshot.liquidityUsd),
        priceUsd: numberOrNull(snapshot.priceUsd),
        priceSol: numberOrNull(snapshot.priceSol),
        supplyUi: numberOrNull(snapshot.supplyUi),
        poolQuoteSol: numberOrNull(snapshot.poolQuoteSol),
        poolAddress: snapshot.poolAddress || null,
        marketFetchedAt: numberOrNull(snapshot.marketFetchedAt),
        migrationAgeMs: numberOrNull(snapshot.migrationAgeMs),
        watchAgeMs: numberOrNull(snapshot.watchAgeMs),
        detailsJson,
      });
    } catch (_) { /* research only */ }
  }

  logMigrationHolderSnapshot(snapshot) {
    if (!snapshot?.mint || !snapshot?.migrationSignature || !snapshot?.migrationTime) return;
    const numberOrNull = (value) => {
      if (value == null || value === '') return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };
    let holdersJson = null;
    try {
      holdersJson = snapshot.holders == null ? null : JSON.stringify(snapshot.holders);
    } catch (_) {
      holdersJson = JSON.stringify({ serializationError: true });
    }
    try {
      this.stmts.insertMigrationHolderSnapshot.run({
        mint: snapshot.mint,
        symbol: snapshot.symbol || null,
        migrationSignature: snapshot.migrationSignature,
        migrationSlot: numberOrNull(snapshot.migrationSlot),
        migrationTime: numberOrNull(snapshot.migrationTime),
        capturedAt: numberOrNull(snapshot.capturedAt) || Date.now(),
        captureDelayMs: numberOrNull(snapshot.captureDelayMs),
        source: snapshot.source || null,
        isComplete: snapshot.isComplete ? 1 : 0,
        pageCount: numberOrNull(snapshot.pageCount),
        holderCount: numberOrNull(snapshot.holderCount),
        tokenAccountCount: numberOrNull(snapshot.tokenAccountCount),
        supplyUi: numberOrNull(snapshot.supplyUi),
        excludedPoolAmount: numberOrNull(snapshot.excludedPoolAmount),
        top1Pct: numberOrNull(snapshot.top1Pct),
        top5Pct: numberOrNull(snapshot.top5Pct),
        top10Pct: numberOrNull(snapshot.top10Pct),
        top20Pct: numberOrNull(snapshot.top20Pct),
        largestHolderOwner: snapshot.largestHolderOwner || null,
        largestHolderPct: numberOrNull(snapshot.largestHolderPct),
        holdersJson,
        error: snapshot.error || null,
      });
    } catch (_) { /* research only */ }
  }

  getMigrationHolderSnapshotStatus(migrationSignature) {
    if (!migrationSignature) return null;
    try {
      return this.stmts.selectMigrationHolderSnapshotStatus.get(migrationSignature) || null;
    } catch (_) {
      return null;
    }
  }

  getMigrationHolderSnapshotStatusByMint(mint) {
    try {
      return this.stmts.selectMigrationHolderSnapshotStatusByMint.get(mint) || null;
    } catch (_) {
      return null;
    }
  }

  updateMigrationHolderSnapshotSymbol(migrationSignature, symbol) {
    if (!migrationSignature || !symbol) return;
    try {
      this.stmts.updateMigrationHolderSnapshotSymbol.run(symbol, migrationSignature);
    } catch (_) { /* research only */ }
  }

  updateMigrationHolderSnapshotSymbolByMint(mint, symbol) {
    if (!mint || !symbol) return;
    try {
      this.stmts.updateMigrationHolderSnapshotSymbolByMint.run(symbol, mint);
    } catch (_) { /* research only */ }
  }

  logPositionResearchEvent(event) {
    if (!event || !event.positionId || !event.mint || !event.eventType) return;
    const num = (value) => {
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    };
    let metricsJson = null;
    try {
      metricsJson = typeof event.metricsJson === 'string'
        ? event.metricsJson
        : JSON.stringify(event.metrics || {});
    } catch (_) {
      metricsJson = JSON.stringify({ serializationError: true });
    }

    this._researchBuffer.push({
      positionId: event.positionId,
      mint: event.mint,
      symbol: event.symbol || null,
      eventType: event.eventType,
      ts: num(event.ts) || Date.now(),
      receivedAt: num(event.receivedAt) || Date.now(),
      holdMs: num(event.holdMs),
      slot: num(event.slot),
      signature: event.signature || null,
      side: event.side || null,
      signer: event.signer || null,
      solVolume: num(event.solVolume),
      price: num(event.price),
      rawPrice: num(event.rawPrice),
      poolAddress: event.poolAddress || null,
      poolBaseAfter: num(event.poolBaseAfter),
      poolQuoteAfter: num(event.poolQuoteAfter),
      baseDecimals: num(event.baseDecimals),
      supplyUi: num(event.supplyUi),
      fdvUsd: num(event.fdvUsd),
      liquidityUsd: num(event.liquidityUsd),
      entrySol: num(event.entrySol),
      entryPrice: num(event.entryPrice),
      signalPrice: num(event.signalPrice),
      preEntryVwap5s: num(event.preEntryVwap5s),
      tokenAmount: num(event.tokenAmount),
      marketPnlPct: num(event.marketPnlPct),
      peakPnlPct: num(event.peakPnlPct),
      drawdownPct: num(event.drawdownPct),
      trailingArmed: event.trailingArmed ? 1 : 0,
      reconciled: event.reconciled ? 1 : 0,
      metricsJson,
      createdAt: Date.now(),
    });

    if (this._researchBuffer.length >= this._researchFlushMax) {
      this.flushResearchEvents();
    }
  }

  flushResearchEvents() {
    if (!this._researchBuffer || this._researchBuffer.length === 0) return 0;
    const rows = this._researchBuffer.splice(0, this._researchBuffer.length);
    try {
      this._insertResearchBatch(rows);
      return rows.length;
    } catch (err) {
      const retryCapacity = this._researchFlushMax * 10;
      this._researchBuffer = rows
        .concat(this._researchBuffer)
        .slice(-retryCapacity);
      console.warn(`[TradeLogger] research event flush failed (${rows.length} rows): ${err.message}`);
      return 0;
    }
  }

  shutdown() {
    if (this._researchFlushTimer) {
      clearInterval(this._researchFlushTimer);
      this._researchFlushTimer = null;
    }
    this.flushResearchEvents();
  }

  // ============================================================
  // Position lifecycle API
  // ============================================================

  openPosition({ positionId, mint, symbol, openedAt, entrySol, entryPrice, tokenAmount,
                 dryRun, buySignature, buyFeeLamports, buySlot, dumpSlot,
                 entryFdv, entryPoolSol, entryLiquidity,
                 sellCount10s, totalSellSol10s,
                 mintAgeAtBuySec, rsiPreDump, rsi1sPreDump, rsi30sPreDump,
                 entrySignalPrice, preEntryVwap5s, preEntryUniqueBuyers3s,
                 entryMetrics, isEmaStrategy = 0, isAddOn = 0 }) {
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
      entrySignalPrice: entrySignalPrice ?? null,
      preEntryVwap5s: preEntryVwap5s ?? null,
      preEntryUniqueBuyers3s: preEntryUniqueBuyers3s ?? null,
      entryMetricsJson: entryMetrics ? JSON.stringify(entryMetrics) : null,
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

  updateRunnerState(positionId, runnerArmed, runnerArmedAt = null) {
    this.stmts.updateRunnerState.run({
      positionId,
      runnerArmed: runnerArmed ? 1 : 0,
      runnerArmedAt: runnerArmedAt || null,
    });
  }

  updatePositionTokenAmount(positionId, tokenAmount, reason = null) {
    this.stmts.updatePositionTokenAmount.run(
      tokenAmount ?? null,
      reason || null,
      positionId,
    );
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

  markSellPending(
    positionId,
    signature,
    exitReason,
    nextRetryAt = Date.now() + 30_000,
    lastValidBlockHeight = null,
  ) {
    this.stmts.markSellPending.run(
      signature || null,
      lastValidBlockHeight ?? null,
      exitReason || null,
      Date.now(),
      nextRetryAt,
      positionId,
    );
  }

  deferSellConfirmation(positionId, nextRetryAt) {
    this.stmts.deferSellConfirmation.run(nextRetryAt, positionId);
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

  getSwapEventsInRange(startMs, endMs) {
    return this.stmts.swapEventsInRange.all(startMs, endMs);
  }

  getSwapEventsForMintInRange(mint, startMs, endMs) {
    return this.stmts.swapEventsForMintInRange.all(mint, startMs, endMs);
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
