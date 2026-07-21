'use strict';

const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { config } = require('../config');
const TokenRegistry = require('../data/TokenRegistry');
const { extractMigrationInfo } = require('../utils/migrationTime');

class Server {
  constructor({
    tokenRegistry,
    tradeLogger,
    positionManager,
    signalEngine,
    dailyReport,
    competitorTracker,
    onTokenListChanged,
    onTokenAdded,
  }) {
    this.tokenRegistry = tokenRegistry;
    this.tradeLogger = tradeLogger;
    this.positionManager = positionManager;
    this.signalEngine = signalEngine;
    this.dailyReport = dailyReport;
    this.competitorTracker = competitorTracker || null;
    this.onTokenListChanged = onTokenListChanged;
    this.onTokenAdded = onTokenAdded;

    this.app = express();
    this.app.use(express.json({ limit: '64kb' }));

    // 可选：dashboard 访问保护（X-Dashboard-Token header / ?token= query）
    if (config.server.dashboardToken) {
      this.app.use('/api', this._authMiddleware());
      this.app.use('/dashboard.html', this._authMiddleware());
      this.app.use('/index.html', this._authMiddleware());
      this.app.use('/', (req, res, next) => {
        if (req.path === '/' || req.path === '/health') return next();
        return next();
      });
    }

    this.app.use(express.static(path.join(__dirname, 'public'), {
      // v3.17.27: dashboard 经常更新，禁止缓存 HTML
      setHeaders: (res, filePath) => {
        if (filePath.endsWith('.html')) {
          res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
          res.setHeader('Pragma', 'no-cache');
          res.setHeader('Expires', '0');
        }
      },
    }));

    this._setupRoutes();

    this.httpServer = http.createServer(this.app);
    this.wss = new WebSocketServer({
      server: this.httpServer,
      path: '/ws',
      verifyClient: (info, cb) => {
        if (!config.server.dashboardToken) return cb(true);
        try {
          const url = new URL(info.req.url, 'http://localhost');
          const token = url.searchParams.get('token');
          if (token === config.server.dashboardToken) return cb(true);
          return cb(false, 401, 'Unauthorized');
        } catch (_) {
          return cb(false, 401, 'Unauthorized');
        }
      },
    });
    this.wss.on('error', (err) => { if (err.code === 'EADDRINUSE') { console.warn('[Server] WebSocket port conflict, dashboard disabled'); } else { throw err; } });
    this.wss.on('connection', (ws) => {
      ws.send(JSON.stringify({ type: 'hello', dryRun: config.DRY_RUN, ts: Date.now() }));
    });
  }

  _authMiddleware() {
    const token = config.server.dashboardToken;
    return (req, res, next) => {
      const provided = req.headers['x-dashboard-token'] || req.query.token;
      if (provided !== token) {
        return res.status(401).json({ ok: false, error: 'unauthorized' });
      }
      next();
    };
  }

  _validateWebhookSecret(req) {
    if (!config.server.webhookSecret) return true; // 未配置则跳过
    const provided =
      req.headers['x-webhook-secret'] ||
      req.headers['authorization']?.replace(/^Bearer\s+/i, '');
    return provided === config.server.webhookSecret;
  }

  _setupRoutes() {
    const app = this.app;

    // ============ Webhook ============
    app.post('/webhook/add-token', async (req, res) => {
      try {
        if (!this._validateWebhookSecret(req)) {
          return res.status(401).json({ ok: false, error: 'invalid webhook secret' });
        }
        const payload = req.body || {};
        const { network, address, symbol } = payload;
        if (network && network.toLowerCase() !== 'solana') {
          return res.status(400).json({ ok: false, error: 'only solana network supported' });
        }
        if (!address || typeof address !== 'string') {
          return res.status(400).json({ ok: false, error: 'address required' });
        }
        try {
          TokenRegistry.validateMint(address);
        } catch (err) {
          return res.status(400).json({ ok: false, error: err.message });
        }

        // Max token rotation: if at capacity, evict lowest-value tokens
        const evicted = await this._evictIfNeeded(address);

        const token = await this.tokenRegistry.addToken(address, {
          symbol,
          source: 'webhook',
          ...extractMigrationInfo(payload),
        });
        if (this.onTokenListChanged) this.onTokenListChanged();
        if (this.onTokenAdded) this.onTokenAdded(token);
        this.broadcast({ type: 'tokenAdded', token });
        const resp = { ok: true, token };
        if (evicted.length > 0) {
          resp.evicted = evicted;
          console.log(
            `[webhook] evicted ${evicted.length} token(s) to make room: ` +
            evicted.map(e => `${e.symbol}(${e.reason})`).join(', '),
          );
        }
        res.json(resp);
      } catch (err) {
        console.error(`[webhook] add-token error: ${err.message}`);
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ Token list ============
    app.get('/api/tokens', (req, res) => {
      res.json({ ok: true, tokens: this.tokenRegistry.listAll() });
    });

    app.post('/api/tokens', async (req, res) => {
      try {
        const { address, symbol } = req.body || {};
        if (!address) return res.status(400).json({ ok: false, error: 'address required' });
        try {
          TokenRegistry.validateMint(address);
        } catch (err) {
          return res.status(400).json({ ok: false, error: err.message });
        }
        const token = await this.tokenRegistry.addToken(address, { symbol, source: 'manual' });
        if (this.onTokenListChanged) this.onTokenListChanged();
        if (this.onTokenAdded) this.onTokenAdded(token);
        this.broadcast({ type: 'tokenAdded', token });
        res.json({ ok: true, token });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    /**
     * 批量添加：避免每次添加都触发 LaserStream 重建。
     * Body: { tokens: [{ address, symbol }, ...] }
     */
    app.post('/api/tokens/batch', async (req, res) => {
      try {
        const { tokens } = req.body || {};
        if (!Array.isArray(tokens) || tokens.length === 0) {
          return res.status(400).json({ ok: false, error: 'tokens array required' });
        }

        // Pre-evict to make room for all incoming tokens at once
        const newMints = tokens.map(t => t.address).filter(m => {
          try { TokenRegistry.validateMint(m); return true; } catch (_) { return false; }
        });
        const allEvicted = [];
        for (const mint of newMints) {
          const evicted = await this._evictIfNeeded(mint);
          allEvicted.push(...evicted);
        }
        if (allEvicted.length > 0) {
          console.log(
            `[batch] evicted ${allEvicted.length} token(s): ` +
            allEvicted.map(e => `${e.symbol}(${e.reason})`).join(', '),
          );
        }

        const results = [];
        const errors = [];
        for (const t of tokens) {
          try {
            TokenRegistry.validateMint(t.address);
            const token = await this.tokenRegistry.addToken(t.address, {
              symbol: t.symbol,
              source: 'batch',
              ...extractMigrationInfo(t, 'batch_payload'),
            });
            results.push(token);
            if (this.onTokenAdded) this.onTokenAdded(token);
          } catch (err) {
            errors.push({ address: t.address, error: err.message });
          }
        }
        // 全部加完后只通知一次（重建 LaserStream 一次）
        if (this.onTokenListChanged) this.onTokenListChanged();
        this.broadcast({ type: 'tokensAdded', count: results.length });
        const resp = { ok: true, added: results.length, failed: errors.length, errors };
        if (allEvicted.length > 0) resp.evicted = allEvicted;
        res.json(resp);
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    app.delete('/api/tokens/:mint', (req, res) => {
      try {
        this.tokenRegistry.removeToken(req.params.mint);
        if (this.onTokenListChanged) this.onTokenListChanged();
        this.broadcast({ type: 'tokenRemoved', mint: req.params.mint });
        res.json({ ok: true });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ Logs ============
    app.get('/api/signals', (req, res) => {
      const limit = parseInt(req.query.limit || '100', 10);
      res.json({ ok: true, signals: this.tradeLogger.getRecentSignals(limit) });
    });

    app.get('/api/trades', (req, res) => {
      const limit = parseInt(req.query.limit || '100', 10);
      res.json({ ok: true, trades: this.tradeLogger.getRecentTrades(limit) });
    });

    app.get('/api/positions', (req, res) => {
      const limit = parseInt(req.query.limit || '100', 10);
      // v3.17.13: 序列化 open positions 时过滤掉不可序列化的字段
      const open = this.positionManager.listOpen().map(p => {
        let currentPrice = this.positionManager.priceTracker?.getPrice(p.mint) || 0;
        // v3.17.42: priceTracker没价格时，用PositionManager内部的最新价格
        if (!currentPrice && p._lastTickPrice) {
          currentPrice = p._lastTickPrice;
        }
        // v3.17.42: 过滤异常价格 — 价格跳变>10倍或<0.1倍时置零，避免前端PnL爆炸
        // v3.26: stuck 仓位（pool已死/卖不出）价格不可信，置零
        if (p.status === 'stuck') {
          currentPrice = 0;
        } else if (currentPrice > 0 && p.entryPrice > 0) {
          const ratio = currentPrice / p.entryPrice;
          if (ratio > 10 || ratio < 0.1) {
            currentPrice = 0; // 价格异常，不显示PnL
          }
        }
        const unrealizedPnlPct = currentPrice > 0 ? ((currentPrice - p.entryPrice) / p.entryPrice * 100) : null;
        return {
          positionId: p.positionId,
          mint: p.mint,
          symbol: p.symbol,
          entrySol: p.entrySol,
          entryPrice: p.entryPrice,
          currentPrice,
          unrealizedPnlPct,
          tokenAmount: p.tokenAmount,
          openedAt: p.openedAt,
          dryRun: p.dryRun,
          exiting: p.exiting,
          reconciled: p.reconciled,
          exitReason: p.exitReason,
          status: p.status,
          sellAttempts: p.sellAttempts,
          highWaterMark: p.highWaterMark,
          trailingArmed: p.trailingArmed,
          stabilizing: p.stabilizing,
          preVol5m: p.preVol5m ?? null,
          volTier: p.preVol5m != null && p.preVol5m >= 0
            ? (p.preVol5m < (parseFloat(process.env.VOL_LOW_THRESHOLD || '10')) ? 'low'
               : p.preVol5m >= (parseFloat(process.env.VOL_HIGH_THRESHOLD || '15')) ? 'high'
               : 'mid')
            : 'unknown',
        };
      });
      res.json({
        ok: true,
        open,
        recent: this.tradeLogger.getRecentPositions(limit),
        stuck: this.tradeLogger.getStuckPositions(),
      });
    });

    // ============ Manual sell endpoint ============
    app.post('/api/positions/:id/sell', async (req, res) => {
      try {
        const pos = this.positionManager.positions.get(req.params.id);
        if (!pos) {
          return res.status(404).json({ ok: false, error: 'position not found' });
        }
        if (pos.exiting || pos.status === 'sell_pending' || pos.status === 'sell_confirming') {
          return res.status(409).json({ ok: false, error: 'position already exiting', symbol: pos.symbol });
        }
        const px = this.positionManager.priceTracker?.getPrice(pos.mint) || pos.entryPrice;
        this.positionManager._exit(pos, px, 'MANUAL_SELL');
        res.json({ ok: true, symbol: pos.symbol, mint: pos.mint, exitPrice: px, reason: 'MANUAL_SELL' });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    app.post('/api/positions/sell-all', async (req, res) => {
      try {
        const open = this.positionManager.listOpen().filter(p => !p.exiting);
        const results = [];
        for (const pos of open) {
          const px = this.positionManager.priceTracker?.getPrice(pos.mint) || pos.entryPrice;
          this.positionManager._exit(pos, px, 'MANUAL_SELL');
          results.push({ symbol: pos.symbol, mint: pos.mint, exitPrice: px });
          // 间隔 500ms 防止并发卖出滑点问题
          await new Promise(r => setTimeout(r, 500));
        }
        res.json({ ok: true, count: results.length, positions: results });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ Manual report trigger ============
    app.post('/api/reports/generate', async (req, res) => {
      try {
        const { date } = req.body || {};
        const target = date ? new Date(date) : new Date(Date.now() - 24 * 60 * 60 * 1000);
        const filepath = await this.dailyReport.generateForDate(target);
        res.json({ ok: true, filepath });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ Status ============
    app.get('/api/status', (req, res) => {
      res.json({
        ok: true,
        dryRun: config.DRY_RUN,
        watchedTokens: this.tokenRegistry.listActive().length,
        openPositions: this.positionManager.openPositionCount(),
        config: {
          entryMinVolumeUsd: config.activityRsi.minVolumeUsd,
          solPriceUsd: config.activityRsi.solPriceUsd,
          rsi5sPeriod: config.activityRsi.rsi5sPeriod,
          rsiBuyCross: config.activityRsi.rsiBuyCross,
          rsiExitDownCross: config.strategy.rsi5sExitDownCross,
          rsiExitOverbought: config.strategy.rsi5sExitOverbought,
          trailingActivatePct: config.strategy.trailingActivatePct,
          trailingDrawdownPct: config.strategy.trailingDrawdownPct,
          positionSizeSol: config.strategy.positionSizeSol,
        },
      });
    });

    app.get('/health', (req, res) => res.json({ ok: true, ts: Date.now() }));

    // ============ 竞争对手分析 ============
    app.get('/api/competitors', (req, res) => {
      if (!this.competitorTracker) return res.json({ ok: true, competitors: [] });
      res.json({
        ok: true,
        competitors: this.competitorTracker.getAllStats(),
        entryStats: this.competitorTracker.getAllEntryStats(),
      });
    });

    app.get('/api/competitors/:wallet/entry-stats', (req, res) => {
      if (!this.competitorTracker) return res.json({ ok: true, entryStats: null });
      res.json({ ok: true, entryStats: this.competitorTracker.getEntryStats(req.params.wallet) });
    });

    app.get('/api/competitors/:wallet/trades', (req, res) => {
      if (!this.competitorTracker) return res.json({ ok: true, trades: [] });
      const limit = Math.min(parseInt(req.query.limit || '50', 10), 500);
      res.json({
        ok: true,
        wallet: req.params.wallet,
        stats: this.competitorTracker.getWalletStats(req.params.wallet),
        trades: this.competitorTracker.getRecentTrades(req.params.wallet, limit),
      });
    });

    app.post('/api/competitors', (req, res) => {
      if (!this.competitorTracker) return res.status(400).json({ ok: false, error: 'tracker disabled' });
      const { address, label } = req.body || {};
      if (!address) return res.status(400).json({ ok: false, error: 'address required' });
      try {
        const { PublicKey } = require('@solana/web3.js');
        new PublicKey(address); // validate
      } catch (_) {
        return res.status(400).json({ ok: false, error: 'invalid wallet address' });
      }
      this.competitorTracker.addAddress(address, label || null);
      res.json({ ok: true, competitors: this.competitorTracker.getAllStats() });
    });

    app.delete('/api/competitors/:wallet', (req, res) => {
      if (!this.competitorTracker) return res.status(400).json({ ok: false, error: 'tracker disabled' });
      this.competitorTracker.removeAddress(req.params.wallet);
      res.json({ ok: true });
    });

    // ============ 健康监控 ============
    app.get('/api/health', (req, res) => {
      const { getMonitor } = require('../monitor/HealthMonitor');
      res.json({ ok: true, report: getMonitor().report() });
    });

    app.get('/api/health/summary', (req, res) => {
      const { getMonitor } = require('../monitor/HealthMonitor');
      res.type('text/plain').send(getMonitor().summary());
    });

    // ============ v3.17.32: 列出已生成的报告 ============
    app.get('/api/reports', (req, res) => {
      try {
        const reportsDir = path.resolve(config.storage.reportsDir);
        if (!fs.existsSync(reportsDir)) return res.json({ ok: true, reports: [] });
        const files = fs.readdirSync(reportsDir)
          .filter((f) => /^\d{4}-\d{2}-\d{2}\.md$/.test(f))
          .map((f) => {
            const fp = path.join(reportsDir, f);
            const stat = fs.statSync(fp);
            return {
              filename: f,
              date: f.replace('.md', ''),
              sizeBytes: stat.size,
              modifiedAt: stat.mtimeMs,
            };
          })
          .sort((a, b) => b.date.localeCompare(a.date)); // 最新优先
        res.json({ ok: true, reports: files });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ v3.17.32: 读取指定日报 markdown 内容 ============
    app.get('/api/reports/:date', (req, res) => {
      try {
        const date = req.params.date;
        // 安全校验:只允许 YYYY-MM-DD 格式
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
          return res.status(400).json({ ok: false, error: 'invalid date format (expected YYYY-MM-DD)' });
        }
        const filepath = path.join(path.resolve(config.storage.reportsDir), `${date}.md`);
        if (!fs.existsSync(filepath)) {
          return res.status(404).json({ ok: false, error: 'report not found' });
        }
        const content = fs.readFileSync(filepath, 'utf-8');
        res.json({ ok: true, date, content });
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });

    // ============ v3.17.32: 下载指定日报原始 .md 文件 ============
    app.get('/api/reports/:date/download', (req, res) => {
      try {
        const date = req.params.date;
        if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
          return res.status(400).json({ ok: false, error: 'invalid date format' });
        }
        const filepath = path.join(path.resolve(config.storage.reportsDir), `${date}.md`);
        if (!fs.existsSync(filepath)) {
          return res.status(404).json({ ok: false, error: 'report not found' });
        }
        res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${date}.md"`);
        fs.createReadStream(filepath).pipe(res);
      } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
      }
    });
  }

  /**
   * Max token rotation: evict tokens when at capacity (default 95).
   * Priority: tokens with NO trade history first, sorted by 24h volume ascending.
   * If all tokens have trade history, evict the one with lowest 24h volume.
   * Never evict the incoming mint (already being added).
   * @param {string} incomingMint - the mint about to be added
   * @returns {Array<{mint, symbol, reason}>} evicted tokens
   */
  async _evictIfNeeded(incomingMint) {
    const MAX_TOKENS = parseInt(process.env.MAX_WATCHED_TOKENS || '500', 10);
    const currentTokens = this.tokenRegistry.listAll().filter(t => t.is_active);
    const currentCount = currentTokens.length;

    // If incoming mint already exists, no need to evict
    if (currentTokens.some(t => t.mint === incomingMint)) return [];

    if (currentCount < MAX_TOKENS) return [];

    // Get mints with trade history in the LAST 24 HOURS (not all-time)
    // v3.17.30: 优先淘汰 24h 内没交易过的币
    const tradeLogger = this.tradeLogger;
    const cutoff24h = Date.now() - 24 * 60 * 60 * 1000;
    const mintsTraded24h = new Set(
      tradeLogger.db
        .prepare('SELECT DISTINCT mint FROM positions WHERE opened_at > ? UNION SELECT DISTINCT mint FROM signals WHERE accepted = 1 AND ts > ?')
        .all(cutoff24h, cutoff24h)
        .map(r => r.mint),
    );
    // fallback: 也查 positions 里还没平仓的(仍在持仓的永远不淘汰)
    const mintsOpenPosition = new Set(
      tradeLogger.db
        .prepare('SELECT DISTINCT mint FROM positions WHERE closed_at IS NULL')
        .all()
        .map(r => r.mint),
    );

    // Build candidates with volume info
    const candidates = currentTokens
      .filter(t => t.mint !== incomingMint)
      .map(t => {
        let vol24h = 0;
        try {
          const meta = t.meta_json ? JSON.parse(t.meta_json) : {};
          vol24h = meta.volume24h || 0;
        } catch (_) {}
        return {
          mint: t.mint,
          symbol: t.symbol || '???',
          vol24h,
          hasTrades24h: mintsTraded24h.has(t.mint),
          hasOpenPosition: mintsOpenPosition.has(t.mint),
        };
      });

    // Sort: 
    // 1. 有持仓的永远不淘汰(最安全)
    // 2. 24h 内没交易过的优先淘汰
    // 3. 同级别按 vol24h 升序排
    candidates.sort((a, b) => {
      if (a.hasOpenPosition !== b.hasOpenPosition) return a.hasOpenPosition ? 1 : -1; // 有持仓排最后
      if (a.hasTrades24h !== b.hasTrades24h) return a.hasTrades24h ? 1 : -1; // 24h有交易排最后
      return a.vol24h - b.vol24h; // lower volume first
    });

    // Evict how many?
    const slotsNeeded = currentCount + 1 - MAX_TOKENS;
    const toEvict = candidates.slice(0, Math.max(1, slotsNeeded));

    const evicted = [];
    for (const t of toEvict) {
      this.tokenRegistry.removeToken(t.mint);
      evicted.push({
        mint: t.mint,
        symbol: t.symbol,
        reason: t.hasOpenPosition ? 'has_position(CHECK_BUG)' : (t.hasTrades24h ? `low_vol(${t.vol24h.toFixed(0)})` : 'no_trades_24h'),
      });
    }

    return evicted;
  }

  broadcast(message) {
    const data = JSON.stringify(message);
    for (const client of this.wss.clients) {
      if (client.readyState === 1) {
        try { client.send(data); } catch (_) {}
      }
    }
  }

  start() {
    const host = config.server.bindHost || '0.0.0.0';
    this.httpServer.on("error", (err) => { if (err.code === "EADDRINUSE") { console.warn("[Server] port " + config.server.port + " in use, dashboard disabled"); } else { throw err; } });
    this.httpServer.listen(config.server.port, host, () => {
      console.log(`[Server] listening on ${host}:${config.server.port}`);
      console.log(`[Server] dashboard: http://${host}:${config.server.port}`);
      console.log(`[Server] webhook:   POST http://${host}:${config.server.port}/webhook/add-token`);
      if (config.server.webhookSecret) console.log('[Server] webhook secret: ENABLED');
      if (config.server.dashboardToken) console.log('[Server] dashboard auth: ENABLED');
    });
  }
}

module.exports = Server;

// v3.20: Inject vol-badge.js into dashboard HTML (bypasses browser cache)
