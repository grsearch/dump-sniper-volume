'use strict';

/**
 * PositionManager (v2)
 * ====================
 * 维护当前持仓。每次 PriceTracker 更新价格时检查是否止盈/紧急止损/超时。
 * 100ms tick 兜底，防止价格不更新时无法触发超时退出。
 *
 * 关键修复（v2）：
 *
 * 1. 双确认止盈：连续 N 次（默认 2）满足 TP 条件，且首次和最近一次间隔
 *    >= tpConfirmMinGapMs（默认 300ms），才真正触发卖出。挡住单次价格污染。
 *
 * 2. 紧急止损：跌幅 <= emergencyStopLossPct（默认 -15%）立即出场。
 *    防止 PRATT/Goblin/COMPUTA 那种 -97% 灾难。
 *
 * 3. PnL 用真实成交价计算：sellResult.solOut 来自钱包真实余额变化（LIVE）
 *    或 Jupiter quote 的 outAmount。entry_price 来自 BUY 真实成交比率。
 *    不再用"trigger 时的 price tracker 价格"做 PnL 分母。
 *
 * 4. SELL 失败按指数退避重试，且重试时使用最新价格做 sanity 检查
 *
 * 5. registerOpen 接受外部 positionId（与 BUY trade 配对）
 *
 * 6. restoreFromDb 启动时恢复未平仓持仓
 */

const EventEmitter = require('events');
const crypto = require('crypto');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const monitor = getMonitor();
monitor.registerModule('PositionManager', { staleMs: 10_000, label: 'Position Manager' });

const SELL_RETRY_DELAYS_MS = [500, 1500, 3000, 5000, 10_000, 20_000]; // 之后保持 30s

function sumSolVolume(items) {
  return items.reduce((sum, x) => sum + (Number.isFinite(x.solVolume) ? x.solVolume : 0), 0);
}

function uniqueCount(items, field) {
  const set = new Set();
  for (const item of items) {
    const v = item[field];
    if (v) set.add(v);
  }
  return set.size;
}

class PositionManager extends EventEmitter {
  constructor({ tradeLogger, executor, priceTracker, tokenRegistry, tickStream, postExitTracker }) {
    super();
    this.tradeLogger = tradeLogger;
    this.executor = executor;
    this.priceTracker = priceTracker;
    this.tokenRegistry = tokenRegistry;
    this.tickStream = tickStream;  // v3.17.11: 用于读 latestSlot
    this._recentlyClosed = [];  // v3.30: 最近平仓缓存（cooldown 用）
    this.postExitTracker = postExitTracker || null; // v3.17.31


    // v3.17.13: 同币多仓卖出队列
    //   同一 mint 的卖出请求排队，等上一笔确认后再卖下一笔
    //   防止多仓并发卖导致滑点不够全部失败
    this._sellQueues = new Map();    // mint → [{pos, exitPrice}, ...]
    this._sellInProgress = new Set(); // 正在卖出的 mint
    this._tickCount = 0;  // v3.26: tick counter for PoolStateCache price check
    this._flowExitEvents = new Map(); // mint -> recent BUY/SELL swaps while holding

    this.positions = new Map(); // positionId → position obj
    this.byMint = new Map();    // mint → Set<positionId> (v3.17.13: 同币多仓)

    this.tickTimer = setInterval(() => {
      monitor.beat('PositionManager', 'tick');
      monitor.inc('PositionManager.ticks', 1, 'PositionManager');
      this._tick();
    }, 100);

    // v3.3: 重试 reconciler — 每 5 秒扫描 DB 找到期的 pending sell 和 stuck position
    // 处理重启场景（setTimeout 丢失）+ 长时间错过的重试
    this.reconcilerTimer = setInterval(() => {
      this._reconcileRetries().catch((err) => {
        monitor.recordError('PositionManager', err, { phase: 'reconciler' });
      });
    }, 5000);

    // v3.4: 主动池子轮询 — 持仓期间每 500ms 拉一次每个 token 的 pool state 算价格
    // 修复：原来 PriceTracker 只在外部砸盘交易触发时更新；微盘币 15s 内可能没有任何 swap
    // → 价格永远是 entryPrice → 永远不止盈也不止损 → 全部 TIMEOUT 出场
    this.poolPollIntervalMs = parseInt(process.env.POOL_POLL_INTERVAL_MS || '500', 10);
    this.poolPollTimer = setInterval(() => {
      this._pollPoolPrices().catch((err) => {
        monitor.recordError('PositionManager', err, { phase: 'pool_poll' });
      });
    }, this.poolPollIntervalMs);

    // v3.23: 价格采样 — 持仓币每10秒写一条price_samples，提高区间数据覆盖率
    this._priceSampleLastTs = new Map(); // mint → lastSampleTs
    this._priceSampleIntervalMs = parseInt(process.env.PRICE_SAMPLE_INTERVAL_MS || '10000', 10);

    this.priceTracker.on('update', ({ mint, price }) => {
      const pids = this.byMint.get(mint);

      // 价格采样：持仓币才采样，按间隔写入DB
      if (pids && pids.size > 0 && this.tradeLogger) {
        const now = Date.now();
        const lastTs = this._priceSampleLastTs.get(mint) || 0;
        if (now - lastTs >= this._priceSampleIntervalMs) {
          try {
            this.tradeLogger.savePriceSample(mint, now, price);
            this._priceSampleLastTs.set(mint, now);
          } catch (_) {}
        }
      }

      if (!pids || pids.size === 0) return;
      for (const pid of pids) {
        this._checkExit(pid, price);
      }
    });
  }

  stop() {
    clearInterval(this.tickTimer);
    clearInterval(this.reconcilerTimer);
    clearInterval(this.poolPollTimer);
  }

  hasOpenPosition(mint) {
    const pids = this.byMint.get(mint);
    return pids != null && pids.size > 0;
  }

  handleSwapForExit(swap) {
    const s = config.strategy;
    if (!s.flowReversalExitEnabled || !swap || !swap.mint) return;

    const pids = this.byMint.get(swap.mint);
    if (!pids || pids.size === 0) return;

    const side = String(swap.side || '').toUpperCase();
    if (side !== 'BUY' && side !== 'SELL') return;

    const price = Number(swap.price);
    const solVolume = Number(swap.solVolume);
    if (!Number.isFinite(price) || price <= 0 || !Number.isFinite(solVolume) || solVolume <= 0) return;

    const ev = {
      side,
      price,
      solVolume,
      signer: swap.signer || null,
      ts: Number.isFinite(swap.ts) ? swap.ts : Date.now(),
      signature: swap.signature || null,
    };

    let events = this._flowExitEvents.get(swap.mint);
    if (!events) {
      events = [];
      this._flowExitEvents.set(swap.mint, events);
    }
    events.push(ev);
    this._pruneFlowExitEvents(swap.mint, ev.ts);

    for (const pid of pids) {
      const pos = this.positions.get(pid);
      if (pos && !pos.exiting && pos.status !== 'stuck') {
        this._maybeFlowReversalExit(pos, price, ev.ts);
      }
    }
  }

  _pruneFlowExitEvents(mint, now) {
    const events = this._flowExitEvents.get(mint);
    if (!events) return;

    const s = config.strategy;
    const maxWindowMs = Math.max(
      s.flowReversalExitWindow5Ms || 5_000,
      s.flowReversalExitWindow15Ms || 15_000,
    ) + 1_000;
    const cutoff = now - maxWindowMs;
    const kept = events.filter((ev) => ev.ts >= cutoff);
    if (kept.length > 0) this._flowExitEvents.set(mint, kept);
    else this._flowExitEvents.delete(mint);
  }

  _flowExitStats(mint, now, windowMs) {
    const events = (this._flowExitEvents.get(mint) || [])
      .filter((ev) => ev.ts >= now - windowMs && ev.ts <= now)
      .sort((a, b) => a.ts - b.ts);
    const buys = events.filter((ev) => ev.side === 'BUY');
    const sells = events.filter((ev) => ev.side === 'SELL');
    const buySol = sumSolVolume(buys);
    const sellSol = sumSolVolume(sells);
    const volumeSol = buySol + sellSol;
    const first = events[0] || null;
    const last = events[events.length - 1] || null;

    return {
      tradeCount: events.length,
      buyCount: buys.length,
      sellCount: sells.length,
      buySol,
      sellSol,
      volumeSol,
      sellBuyRatio: sellSol / Math.max(buySol, 0.001),
      imbalance: (sellSol - buySol) / Math.max(volumeSol, 0.001),
      uniqueSellers: uniqueCount(sells, 'signer'),
      uniqueBuyers: uniqueCount(buys, 'signer'),
      firstPrice: first ? first.price : 0,
      lastPrice: last ? last.price : 0,
      lastSide: last ? last.side : null,
    };
  }

  _maybeFlowReversalExit(pos, price, now) {
    const s = config.strategy;
    if (!s.flowReversalExitEnabled) return;
    if (!pos || pos.exiting || pos.status === 'stuck') return;
    if (!pos.reconciled && !pos.dryRun) return;
    if (!Number.isFinite(price) || price <= 0 || !pos.entryPrice || pos.entryPrice <= 0) return;

    const holdStart = pos.reconciledAt || pos.openedAt || now;
    if (now - holdStart < s.flowReversalExitMinHoldMs) return;

    const st5 = this._flowExitStats(pos.mint, now, s.flowReversalExitWindow5Ms);
    if (st5.tradeCount < s.flowReversalExitMinTrades5s) return;
    if (st5.volumeSol < s.flowReversalExitMinVolume5sSol) return;
    if (st5.sellBuyRatio < s.flowReversalExitSellBuyRatio5s) return;
    if (st5.imbalance < s.flowReversalExitImbalance5s) return;
    if (st5.lastSide !== 'SELL') return;

    const st15 = this._flowExitStats(pos.mint, now, s.flowReversalExitWindow15Ms);
    if (st15.tradeCount < s.flowReversalExitMinTrades15s) return;
    if (st15.volumeSol < s.flowReversalExitMinVolume15sSol) return;
    if (st15.sellBuyRatio < s.flowReversalExitSellBuyRatio15s) return;
    if (st15.imbalance < s.flowReversalExitImbalance15s) return;

    const hwm = Math.max(pos.highWaterMark || 0, pos.entryPrice);
    const pnlPct = ((price - pos.entryPrice) / pos.entryPrice) * 100;
    const peakPnlPct = ((hwm - pos.entryPrice) / pos.entryPrice) * 100;
    const drawdownPct = hwm > 0 ? ((hwm - price) / hwm) * 100 : 0;
    const peakDropPct = peakPnlPct - pnlPct;

    if (peakPnlPct < s.flowReversalExitMinPeakPnlPct) return;
    if (drawdownPct < s.flowReversalExitMinDrawdownPct && peakDropPct < s.flowReversalExitMinPeakDropPct) return;

    console.log(
      `[PositionManager] FLOW_REVERSAL_EXIT ${pos.symbol || pos.mint.slice(0, 6)} ` +
        `pnl=${pnlPct.toFixed(2)}% peak=${peakPnlPct.toFixed(2)}% dd=${drawdownPct.toFixed(2)}% ` +
        `5s=${st5.sellSol.toFixed(2)}/${st5.buySol.toFixed(2)}SOL r=${st5.sellBuyRatio.toFixed(2)} ` +
        `15s=${st15.sellSol.toFixed(2)}/${st15.buySol.toFixed(2)}SOL r=${st15.sellBuyRatio.toFixed(2)}`,
    );
    monitor.inc('PositionManager.flowReversalExit', 1, 'PositionManager');
    this._exit(pos, price, 'FLOW_REVERSAL_EXIT');
  }

  // v3.17.40b: 加仓策略 — 自最近一笔买入价跌15%以上才允许加仓
  //   首仓后：当前价 < 首仓entryPrice * 0.85 → 允许第1次加仓
  //   第1次加仓后：当前价 < 加仓entryPrice * 0.85 → 允许第2次加仓
  //   最多加仓2次（同币3仓）
  canAddOn(mint) {
    // v3.26: 重新开启加仓 — 距首仓价跌20%以上允许加仓一次
    //   加仓和首仓独立运行，互不干扰（无 ADDON_CASCADE）
    const addonEnabled = process.env.ADDON_ENABLED !== '0';
    if (!addonEnabled) return { allowed: false, reason: 'addon_disabled' };

    const MAX_ADDON = 1; // 最多加仓1次（首仓+加仓=2个仓位）
    const ADDON_DROP_PCT = parseFloat(process.env.ADDON_DROP_PCT || '20'); // 距首仓价跌幅阈值

    const pids = this.byMint.get(mint);
    if (!pids || pids.size === 0) return { allowed: false, reason: 'no_position' };
    if (pids.size > MAX_ADDON) return { allowed: false, reason: 'max_addons' };

    const price = this.priceTracker ? this.priceTracker.getPrice(mint) : null;
    if (!price) return { allowed: false, reason: 'no_price' };

    // 找到首仓（按 openedAt 最早的 = 第一个买入的）
    let firstPos = null;
    for (const pid of pids) {
      const pos = this.positions.get(pid);
      if (!pos) continue;
      if (!firstPos || pos.openedAt < firstPos.openedAt) {
        firstPos = pos;
      }
    }
    if (!firstPos || !firstPos.entryPrice) return { allowed: false, reason: 'no_entry_price' };
    if (!firstPos.reconciled && !firstPos.dryRun) return { allowed: false, reason: 'first_not_reconciled' };

    // 比较当前价 vs 首仓价
    const dropPct = ((firstPos.entryPrice - price) / firstPos.entryPrice) * 100;
    if (dropPct >= ADDON_DROP_PCT) {
      console.log(
        `[PositionManager] 📦 ADDON allowed ${firstPos.symbol || mint.slice(0, 6)}: ` +
        `dropPct=${dropPct.toFixed(1)}% (firstEntry=${firstPos.entryPrice.toExponential(4)} current=${price.toExponential(4)})`,
      );
      return { allowed: true, dropPct, firstEntryPrice: firstPos.entryPrice, currentPrice: price, positionCount: pids.size };
    }
    return { allowed: false, reason: 'drop_not_enough', dropPct, needPct: ADDON_DROP_PCT, firstEntryPrice: firstPos.entryPrice, currentPrice: price };
  }

  /**
   * v3.17.13: 获取同币持仓数量
   */
  openPositionCountByMint(mint) {
    const pids = this.byMint.get(mint);
    return pids ? pids.size : 0;
  }

  /**
   * v3.17.13: 添加 mint → positionId 映射（支持同币多仓）
   */
  _addByMint(mint, positionId) {
    let pids = this.byMint.get(mint);
    if (!pids) {
      pids = new Set();
      this.byMint.set(mint, pids);
    }
    pids.add(positionId);
  }

  /**
   * v3.17.13: 移除 mint → positionId 映射（Set 为空时删除 key）
   */
  _removeByMint(mint, positionId) {
    const pids = this.byMint.get(mint);
    if (!pids) return;
    pids.delete(positionId);
    if (pids.size === 0) {
      this.byMint.delete(mint);
      this._flowExitEvents.delete(mint);
    }
  }

  /**
   * v3.17.15: 卖出同币所有持仓（RSI 超买等场景）
   * 所有仓位排队卖出（_exit 内部有 sellQueue 机制防并发）
   */
  _exitAllByMint(mint, price, reason) {
    const pids = this.byMint.get(mint);
    if (!pids || pids.size === 0) return;
    let count = 0;
    for (const pid of pids) {
      const pos = this.positions.get(pid);
      if (pos && !pos.exiting) {
        count++;
        this._exit(pos, price, reason);
      }
    }
    console.log(
      `[PositionManager] _exitAllByMint ${mint.slice(0, 8)}: triggered ${count} exits (${reason})`,
    );
  }

  openPositionCount() {
    return this.positions.size;
  }

  /**
   * v3.17.13: 从 PoolStateCache 获取代币当前价格
   *   用于 PriceTracker 没有价格时的 fallback
   */
  _getPoolPrice(mint) {
    try {
      const token = this.tokenRegistry.getToken(mint);
      if (!token || !token.pool_address) return null;
      // PoolStateCache 在 Executor 上
      const cache = this.executor?.poolStateCache;
      if (!cache) return null;
      const cached = cache.get(token.pool_address);
      if (!cached?.state) return null;
      const state = cached.state;
      // poolQuoteAmount / poolBaseAmount = price per token in SOL
      const quoteLamports = typeof state.poolQuoteAmount === 'object' && state.poolQuoteAmount.toNumber
        ? state.poolQuoteAmount.toNumber() : Number(state.poolQuoteAmount);
      const baseRaw = typeof state.poolBaseAmount === 'object' && state.poolBaseAmount.toNumber
        ? state.poolBaseAmount.toNumber() : Number(state.poolBaseAmount);
      if (!quoteLamports || !baseRaw) return null;
      const quoteSol = quoteLamports / 1e9;
      const baseTokens = baseRaw / Math.pow(10, token.decimals || 6);
      if (baseTokens <= 0) return null;
      return quoteSol / baseTokens;
    } catch (_) {
      return null;
    }
  }

  listOpen() {
    // v3.26: 排除 stuck 仓位（pool已死/卖不出，单独显示在 stuck 列表）
    return Array.from(this.positions.values()).filter(p => p.status !== 'stuck');
  }

  /**
   * v3.29: 查询指定 mint 最近 N ms 内平仓的记录（用于 EMA 冷却判断）
   */
  listRecentlyClosed(mint, withinMs) {
    const cutoff = Date.now() - withinMs;
    const results = [];
    // 先查内存缓存
    if (this._recentlyClosed) {
      for (const pos of this._recentlyClosed) {
        if (pos.mint === mint && pos.closed_at && pos.closed_at >= cutoff) {
          results.push(pos);
        }
      }
    }
    // 始终查 DB（防止内存缓存不完整，特别是在重启后）
    if (this.tradeLogger) {
      try {
        const rows = this.tradeLogger.db.prepare(
          'SELECT * FROM positions WHERE mint = ? AND status = ? AND closed_at >= ? ORDER BY closed_at DESC LIMIT 10'
        ).all(mint, 'closed', cutoff);
        // 合并去重
        for (const row of rows) {
          if (!results.some(r => r.position_id === row.position_id)) {
            results.push(row);
          }
        }
      } catch (_) {}
    }
    return results;
  }

  /**
   * 启动时从 DB 恢复未平仓的持仓。
   * 对每个恢复的持仓：
   *   - 如果 openedAt + maxHoldMs 已过：立即触发 SELL（exitReason=TIMEOUT_RESTORED）
   *   - 否则：正常进入 _tick 循环
   */
  restoreFromDb() {
    const open = this.tradeLogger.getOpenPositions();
    if (open.length === 0) return [];

    const restored = [];
    for (const row of open) {
      const pos = {
        positionId: row.position_id,
        mint: row.mint,
        symbol: row.symbol,
        entrySol: row.entry_sol,
        entryPrice: row.entry_price,
        tokenAmount: row.token_amount,
        openedAt: row.opened_at,
        dryRun: !!row.dry_run,
        buySignature: row.buy_signature,
        buySlot: row.buy_slot || 0,  // v3.17.11: 恢复时可能没有 buySlot
        dumpSlot: row.dump_slot || 0, // v3.17.19: 恢复时可能没有 dumpSlot
        exiting: false,
        sellAttempts: row.sell_attempts || 0,
        // 双确认状态
        _tpConfirmCount: 0,
        _tpFirstTriggerTs: null,
        // v3.3: 重试相关
        status: row.status || 'open',
        exitReason: row.exit_intent || row.exit_reason || null,
        nextRetryAt: row.next_retry_at || null,
        _lastSellSignature: row.pending_sell_signature || null,
        // v3.17: trailing 字段
        // v3.17.21: 从 DB 恢复 peak_price，避免重启丢失高点
        highWaterMark: row.peak_price > 0 ? row.peak_price : row.entry_price,
        highWaterMarkTs: row.peak_ts || Date.now(),
        // v3.17.27: 有 peak_price 时根据已恢复的 HWM 重新评估 trailingArmed
        //   避免重启后又要重新涨8%才能激活（之前的高点白攒了）
        //   v3.20: 使用 DB 中的 pre_vol_5m_pct 决定 activate 阈值
        trailingArmed: (() => {
          if (!row.peak_price || row.peak_price <= 0) return false;
          const preVol = row.pre_vol_5m_pct ?? null;
          const activatePct = preVol != null && preVol >= 0
            ? (preVol < (parseFloat(process.env.VOL_LOW_THRESHOLD || '10')) ? parseFloat(process.env.VOL_LOW_TRAILING_ACTIVATE_PCT || '20')
               : preVol >= (parseFloat(process.env.VOL_HIGH_THRESHOLD || '15')) ? parseFloat(process.env.VOL_HIGH_TRAILING_ACTIVATE_PCT || '15')
               : config.strategy.trailingActivatePct || 10)
            : config.strategy.trailingActivatePct || 10;
          if (activatePct <= 0) return false;
          const peakPnlPct = ((row.peak_price - row.entry_price) / row.entry_price) * 100;
          return peakPnlPct >= activatePct;
        })(),
        // v3.17.28: 恢复 armedHwm，确保重启后 trailing drawdown 计算正确
        _armedHwm: (() => {
          if (!row.peak_price || row.peak_price <= 0) return undefined;
          const preVol = row.pre_vol_5m_pct ?? null;
          const activatePct = preVol != null && preVol >= 0
            ? (preVol < (parseFloat(process.env.VOL_LOW_THRESHOLD || '10')) ? parseFloat(process.env.VOL_LOW_TRAILING_ACTIVATE_PCT || '20')
               : preVol >= (parseFloat(process.env.VOL_HIGH_THRESHOLD || '15')) ? parseFloat(process.env.VOL_HIGH_TRAILING_ACTIVATE_PCT || '15')
               : config.strategy.trailingActivatePct || 10)
            : config.strategy.trailingActivatePct || 10;
          if (activatePct <= 0) return undefined;
          const peakPnlPct = ((row.peak_price - row.entry_price) / row.entry_price) * 100;
          return peakPnlPct >= activatePct ? row.peak_price : undefined;
        })(),
        _armedHwmTs: row.peak_ts || undefined,
        // v3.17.6: 重启时也进入 stabilization 期
        //   避免重启后第一个 tick 拿到的剧烈波动价格污染 HWM
        // v3.17.21: 如果已有 peak_price（持仓期间涨过），跳过 stabilization
        //   旧持仓重启后不应重跑 stabilization，否则 HWM 会被重置到低于真实峰值
        stabilizing: !row.peak_price || row.peak_price <= 0,
        reconciledAt: Date.now(),
        _stabilizeSamples: [],
        // 恢复时已经 reconciled（DB 里的 entryPrice 已经是真实成交价）
        reconciled: true,
        // v3.20: 从DB恢复买入前波动率
        preVol5m: row.pre_vol_5m_pct ?? null,
        rangeSupport: row.range_support ?? null,
        // EMA 策略：从 DB 持久化字段恢复（不再靠环境变量推断）
        isEmaStrategy: false,  // EMA removed
        isAddOn: !!row.is_addon,
      };
      // 已经在 sell flow 中：标记 exiting=true 防止重新触发 _exit
      if (pos.status === 'sell_pending' || pos.status === 'sell_confirming') {
        pos.exiting = true;
      }
      this.positions.set(pos.positionId, pos);
      this._addByMint(pos.mint, pos.positionId);
      // v3.17.22: 恢复的持仓也加入 hotMints (isPosition=true → 500ms 刷新)
      const tokenInfo = this.tokenRegistry.getToken(pos.mint);
      if (tokenInfo?.pool_address && this.executor?.poolStateCache) {
        this.executor.poolStateCache.addHot(pos.mint, tokenInfo.pool_address, true);
      }
      restored.push(pos);
      const statusBadge = pos.status === 'open' ? '' : ` [status=${pos.status}, attempts=${pos.sellAttempts}]`;
      console.log(
        `[PositionManager] 🔄 RESTORED ${pos.symbol || pos.mint.slice(0, 6)} ` +
          `opened ${Math.round((Date.now() - pos.openedAt) / 1000)}s ago, ` +
          `${(pos.tokenAmount ?? 0).toFixed(2)} tokens${statusBadge}`,
      );
    }
    monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
    return restored;
  }

  /**
   * BUY 成功后由 main 流程调用。
   * @param {object} p
   * @param {string} [p.positionId] - 必须传，与 BUY trade 同 ID
   * @param {string} p.mint
   * @param {string} p.symbol
   * @param {number} p.entrySol - 真实付出的 SOL（含滑点和 fee 损耗）
   * @param {number} p.entryPrice - 真实成交价 = entrySol / tokenAmount
   * @param {number} p.tokenAmount - 真实买到的 token UI amount
   * @param {boolean} p.dryRun
   * @param {string} p.signature
   * @param {number} [p.buyFeeLamports] - BUY tx 的 priority fee + base fee (lamports)
   */
  registerOpen({ positionId, mint, symbol, entrySol, entryPrice, tokenAmount, dryRun, signature, buyFeeLamports, buySlot, dumpSlot, entryFdv, entryPoolSol, entryLiquidity, sellCount10s, totalSellSol10s, mintAgeAtBuySec, rsiPreDump, rsi1sPreDump, rsi30sPreDump, isEmaStrategy = false, isAddOn = false }) {
    const pid = positionId || crypto.randomUUID();
    const pos = {
      positionId: pid,
      mint,
      symbol,
      entrySol,
      entryPrice,
      tokenAmount,
      openedAt: Date.now(),
      dryRun: !!dryRun,
      buySignature: signature,
      buyFeeLamports: buyFeeLamports || 0,  // v3.4: 真实成本
      sellFeeLamports: 0,                    // 卖出时累加（包括所有重试的 fee）
      buySlot: buySlot || 0,                // v3.17.11: BUY 时的链上 slot
      dumpSlot: dumpSlot || 0,              // v3.17.19: 砸单的链上 slot (用于计算 BUY 落链领先几个 slot)
      exiting: false,
      sellAttempts: 0,
      _tpConfirmCount: 0,
      _tpFirstTriggerTs: null,
      // v3.12: 等 _reconcileBuyAsync 完成才允许触发 exit；防止用错的 entryPrice 误判 PnL
      // DRY_RUN 不走 reconcile，直接标 true
      reconciled: !!dryRun,
      // v3.17: 移动止盈追踪
      highWaterMark: entryPrice,
      highWaterMarkTs: Date.now(),
      trailingArmed: false,
      // v3.17.6: stabilization 期
      //   DRY_RUN：开仓即进入 stabilization(用估算价格作起点)
      //   LIVE：reconcile 完成时进入 stabilization
      stabilizing: !!dryRun,
      reconciledAt: dryRun ? Date.now() : null,
      _stabilizeSamples: dryRun ? [] : null,
      // v3.20: 买入前波动率 — 同步计算(用 RsiCalculator 的内存数据，避免异步竞态丢失)
      preVol5m: null,
      rangeSupport: null,  // v3.23: range stop support line
      // EMA 策略标记
      isEmaStrategy,
      isAddOn,
    };
    this.positions.set(pid, pos);
    this._addByMint(mint, pid);

    // v3.26: 标记持仓代币 — PriceTracker 对持仓代币用更宽松的跳变阈值
    if (this.priceTracker) {
      this.priceTracker.markPosition(mint, true);
    }

    // v3.17.42: 同步计算买入前波动率 — 之前是 async，竞态条件下 position 可能已关闭导致写丢失
    this._computePreVol5mSync(pid, mint, pos.openedAt);
    // v3.23: compute range support for range-based stop-loss
    this._computeRangeSupport(pid, mint, pos.openedAt);

    // v3.17.22: 持仓中 → 加入 hotMints (isPosition=true → 500ms 刷新)
    const tokenInfo = this.tokenRegistry.getToken(mint);
    if (tokenInfo?.pool_address && this.executor?.poolStateCache) {
      this.executor.poolStateCache.addHot(mint, tokenInfo.pool_address, true);
    } else if (!tokenInfo?.pool_address) {
      // v3.17.27: 告警——没有 pool_address 的持仓是"瞎仓"
      console.warn(
        `[PositionManager] ⚠️ OPEN ${symbol || mint.slice(0, 6)} has no pool_address in tokenRegistry — ` +
        `trailing stop will NOT work! Pool info may still be loading.`,
      );
    }

    try {
      this.tradeLogger.openPosition({
        positionId: pid,
        mint,
        symbol,
        openedAt: pos.openedAt,
        entrySol,
        entryPrice,
        tokenAmount,
        dryRun: !!dryRun,
        buySignature: signature,
        buyFeeLamports: pos.buyFeeLamports,
        buySlot: pos.buySlot,                 // v3.17.19: 之前没传,SQLite 一直是 0
        dumpSlot: pos.dumpSlot,                // v3.17.19: 新增
        entryFdv: entryFdv ?? null,            // v3.17.21: 买入瞬间 FDV
        entryPoolSol: entryPoolSol ?? null,     // v3.17.21: 买入瞬间池子 SOL
        entryLiquidity: entryLiquidity ?? null, // v3.17.21: 买入瞬间流动性 USD
        sellCount10s: sellCount10s ?? null,     // v3.17.36: 连环拔回测
        totalSellSol10s: totalSellSol10s ?? null, // v3.17.36: 连环拔回测
        mintAgeAtBuySec: mintAgeAtBuySec ?? null, // v3.17.39: 首信号到买入秒数
        rsiPreDump: rsiPreDump ?? null,           // v3.17.38: 砸单前 RSI5s
        rsi1sPreDump: rsi1sPreDump ?? null,       // v3.17.38: 砸单前 RSI1s
        rsi30sPreDump: rsi30sPreDump ?? null,     // v3.17.42: 砸单前 RSI30s
        isEmaStrategy: 0,  // EMA removed (v3.30: EMA策略标记持久化)
        isAddOn: isAddOn ? 1 : 0,                 // v3.30: 加仓标记持久化
      });
    } catch (dbErr) {
      console.error(`[PositionManager] ❌ openPosition DB write FAILED for ${symbol || mint.slice(0,6)}: ${dbErr.message}`);
    }

    // v3.17.19: log slot lag (砸单 → BUY 落链相差几个 slot, 0 = 同 slot 抢入)
    // ⚠️ 注意：此时 buySlot 是提交前的 latestSlot，不是真实落链 slot
    //    真实 lag 在 _reconcileBuyAsync 完成后打印
    if (pos.dumpSlot > 0 && pos.buySlot > 0) {
      const slotLag = pos.buySlot - pos.dumpSlot;
      console.log(
        `[PositionManager] 📈 OPEN ${symbol || mint.slice(0, 6)} @ ${entryPrice.toExponential(4)}, ` +
          `${tokenAmount.toFixed(2)} tokens, ${entrySol.toFixed(4)} SOL ` +
          `(dump_slot=${pos.dumpSlot}, buy_slot=${pos.buySlot}, lag=${slotLag} slot${slotLag === 0 ? ' ⚡ SAME-SLOT' : ''})`,
      );
    } else {
      console.log(
        `[PositionManager] 📈 OPEN ${symbol || mint.slice(0, 6)} @ ${entryPrice.toExponential(4)}, ` +
          `${tokenAmount.toFixed(2)} tokens, ${entrySol.toFixed(4)} SOL`,
      );
    }

    monitor.inc('PositionManager.opened', 1, 'PositionManager');
    monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
    this.emit('opened', pos);

    // v3.6: 异步等链上确认并用真实数据修正 position
    // 这是关键 PnL 准确性修复：sizeSol 是配置值（如 3.0），但实际链上花费可能是 2.6
    // SDK 的 buyQuoteInput 把 quote 当 max；slippage 让链上以更优价格成交，少花一些 SOL
    if (!dryRun && signature && !signature.startsWith('DRYRUN')) {
      this._reconcileBuyAsync(pid, mint, signature).catch((err) => {
        monitor.recordError('PositionManager', err, {
          phase: 'reconcile_buy',
          mint,
          signature,
        });
      });

      // v3.17.9: reconcile watchdog —— 兜底机制
      //   背景:openclaw 实战发现 1 笔 BUY 链上 ProgramFailedToComplete,
      //         token 没到账,但 status 一直停在 open(认为买成功)。
      //         理论上 _reconcileBuyAsync 应该检测到 confirmed=false 并关闭 position,
      //         但实战中存在异常路径导致 reconcile 没正常工作:
      //           - setImmediate / Promise 异常被吞(已 catch 但实际可能没触发)
      //           - confirmTx 内部 RPC 长期阻塞(>60s)
      //           - getSignatureStatuses 对失败 tx 返回 null,poll 到超时(8s)然后正常关闭
      //             但极端情况下 poll 异常 → reconcile 退出但没标记
      //   兜底:开仓后 60 秒 watchdog
      //         如果 position 仍存在 AND reconciled=false → 强制按"BUY chain failed"处理
      //         (60s 远大于正常 reconcile 完成时间 1-3s,正常路径不会触发)
      const watchdogTimer = setTimeout(async () => {
        const p = this.positions.get(pid);
        if (p && !p.reconciled && !p.exiting) {
          // v3.17.13: 先查钱包余额 — 可能 confirmTx 超时但买入实际成功了
          const walletBalance = await this.executor.getWalletTokenBalance(mint).catch(() => 0);
          if (walletBalance > 0) {
            // 买入成功，只是 reconcile 没完成
            console.warn(
              `[PositionManager] ⚠️ reconcile watchdog: ${p.symbol || mint.slice(0, 6)} ` +
                `still un-reconciled after 60s but wallet has ${walletBalance} tokens → treating as BUY success`,
            );
            p.reconciled = true;
            p.reconciledAt = Date.now();
            p.stabilizing = true;
            p._stabilizeSamples = [];
            if (p._reconcileWatchdog) {
              clearTimeout(p._reconcileWatchdog);
              p._reconcileWatchdog = null;
            }
            monitor.inc('PositionManager.reconcileWatchdogRecovered', 1, 'PositionManager');
            return;
          }

          console.error(
            `[PositionManager] ⚠️ reconcile watchdog: ${p.symbol || mint.slice(0, 6)} ` +
              `still un-reconciled after 60s, no tokens in wallet → forcing BUY_CHAIN_FAILED`,
          );
          monitor.inc('PositionManager.reconcileWatchdog', 1, 'PositionManager');
          const feeSol = ((p.buyFeeLamports || 0) + 5000) / 1e9;
          try {
            this.tradeLogger.closePosition(pid, {
              closedAt: Date.now(),
              exitPrice: p.entryPrice,
              exitSol: 0,
              pnlSol: -feeSol,
              pnlPct: -100,
              exitReason: 'BUY_RECONCILE_TIMEOUT',
              sellSignature: null,
            });
          } catch (err) {
            monitor.recordError('PositionManager', err, { phase: 'watchdog_close' });
          }
          this.positions.delete(pid);
          this._removeByMint(mint, pid);
          // v3.17.21: BUY reconcile 超时 → 从 hotMints 移除
          if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(mint);
          monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
        }
      }, 60_000);
      if (watchdogTimer.unref) watchdogTimer.unref();
      pos._reconcileWatchdog = watchdogTimer;
    }
    return pos;
  }

  /**
   * v3.6: BUY 提交后异步等链上确认，用真实 SOL 出账 / 真实 token 入账 修正 position
   * 解决 BUY 实际花费 ≠ 配置 sizeSol 的问题（典型偏差 5-15%）
   */
  async _reconcileBuyAsync(positionId, mint, signature) {
    // v3.7: 等 1 秒让 tx 落链（BUY 通常 400-800ms 落链，1s 是合理初始延迟）
    await new Promise((r) => setTimeout(r, 1000));

    // 短超时确认（confirmTx 内部 poll，最多 15 秒）
    // v3.17.14: 从 8s 提到 15s，三路 race 后 tx 可能走慢通道需要更长时间落链
    let result = await this.executor.confirmTx(signature, {
      timeoutMs: 15_000,
      pollIntervalMs: 500,
    });

    const pos = this.positions.get(positionId);
    if (!pos) return; // position 已被外部清理

    // v3.17.14: confirmTx 超时返回 not_landed 时，先查钱包余额做二次验证
    // 实战发现：三路 race 后 tx 可能走慢通道，>15s 才确认但实际已上链
    // 直接判死会导致：钱包有 token 但 position 被关闭 → token 卡死无法卖出
    if (!result.confirmed) {
      const walletBalance = await this.executor.getWalletTokenBalance(mint);
      if (walletBalance > 0) {
        console.log(
          `[PositionManager] ⚠️ confirmTx timeout but wallet has ${walletBalance} tokens ` +
            `→ treating as confirmed (slow channel)`,
        );
        result = { confirmed: true, slot: null };
      }
    }

    // ============ 分支 A: BUY tx 链上失败 ============
    if (!result.confirmed) {
      monitor.inc('PositionManager.buyChainFail', 1, 'PositionManager');
      const errMsg = result.error || 'not_landed';
      console.error(
        `[PositionManager] ⚠️ BUY tx FAILED on chain: ${pos.symbol || mint.slice(0, 6)} ` +
          `sig=${signature.slice(0, 8)}.. error=${errMsg}`,
      );

      // 真实损失 = 已付 priority fee + base fee（链上 tx 失败也扣 fee）
      // 没买到 token，所以 exitSol = 0, tokenAmount 应该是 0
      const feeSol = ((pos.buyFeeLamports || 0) + 5000) / 1e9;

      this.tradeLogger.closePosition(positionId, {
        closedAt: Date.now(),
        exitPrice: pos.entryPrice,
        exitSol: 0,
        pnlSol: -feeSol, // 仅损失 fee
        pnlPct: -100,
        exitReason: 'BUY_CHAIN_FAILED',
        sellSignature: null,
      });

      this.positions.delete(positionId);
      this._removeByMint(mint, positionId);
      // v3.17.21: BUY 失败 → 从 hotMints 移除
      if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(mint);
      monitor.inc('PositionManager.buyFailedClosed', 1, 'PositionManager');
      monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
      monitor.recordError('PositionManager', new Error('BUY chain failed'), {
        mint,
        symbol: pos.symbol,
        signature,
        error: errMsg,
      });
      // v3.17.9: 清 watchdog
      if (pos._reconcileWatchdog) {
        clearTimeout(pos._reconcileWatchdog);
        pos._reconcileWatchdog = null;
      }
      this.emit('buyChainFailed', { positionId, mint, symbol: pos.symbol, signature, error: errMsg });

      // v3.32: IncorrectProgramId → pool 已迁移到 Raydium，标记 pool dead 防止再次浪费费
      if (errMsg && (errMsg.includes('IncorrectProgramId') || errMsg.includes('IncorrectProgramId'))) {
        if (this.executor?.poolStateCache && pos.poolAddress) {
          this.executor.poolStateCache.markDead(pos.poolAddress);
          console.warn(
            `[PositionManager] 🪦 Pool marked dead (IncorrectProgramId): ${pos.poolAddress.slice(0, 8)}.. for ${symbol || mint.slice(0, 6)} — likely migrated to Raydium`,
          );
        }
      }

      // v3.26: BUY_CHAIN_FAILED → 24h 冷却，防止同币反复买入失败
      if (this.signalEngine && this.signalEngine._exitCooldowns) {
        const buyFailedCooldownMs = parseInt(process.env.BUY_FAILED_REBUY_COOLDOWN_MS || '86400000', 10);
        this.signalEngine._exitCooldowns.set(mint, Date.now() + buyFailedCooldownMs);
        console.log(
          `[PositionManager] 🔒 BUY_CHAIN_FAILED cooldown ${symbol || mint.slice(0, 6)} for ${Math.round(buyFailedCooldownMs / 3600000)}h (no rebuy)`,
        );
      }
      // v3.17.42: 广播关闭事件给前端，否则前端不知道仓位已关闭
      this.emit('closed', {
        positionId,
        mint,
        symbol: pos.symbol,
        exitReason: 'BUY_CHAIN_FAILED',
        pnlSol: -feeSol,
        pnlPct: -100,
      });
      return;
    }

    // ============ 分支 B: BUY 链上成功，但解析失败 ============
    // v3.17.14: 三路 race 时 Slipstream 返回的 sig 可能不是链上 sig
    // 如果 fetchTxSwapResult 失败，先用钱包余额判断是否真的买到了
    const swap = await this.executor.fetchTxSwapResult(signature, mint);
    if (!swap || !swap.success) {
      // 二次验证：钱包里有 token 就说明买入成功，只是 sig 不对
      const walletBalance = await this.executor.getWalletTokenBalance(mint);
      if (walletBalance > 0) {
        console.log(
          `[PositionManager] ⚠️ tx parse failed but wallet has ${walletBalance} tokens ` +
            `→ treating as BUY success (sig likely from Slipstream internal ID)`,
        );
        // 不关闭 position，保持开放让后续止盈/止损逻辑正常工作
        // 用估算值 reconcile
        pos.reconciled = true;
        pos.reconciledAt = Date.now();
        monitor.inc('PositionManager.buyReconcileFallback', 1, 'PositionManager');
        if (pos._reconcileWatchdog) {
          clearTimeout(pos._reconcileWatchdog);
          pos._reconcileWatchdog = null;
        }
        return;
      }

      // 钱包也没 token → 真的失败了
      monitor.inc('PositionManager.buyReconcileFetchFail', 1, 'PositionManager');
      console.error(
        `[PositionManager] ⚠️ BUY confirmed but tx parse failed: ${pos.symbol || mint.slice(0, 6)} ` +
          `sig=${signature.slice(0, 8)}..`,
      );
      // 同样按链上失败处理（保险起见）
      const feeSol = ((pos.buyFeeLamports || 0) + 5000) / 1e9;
      this.tradeLogger.closePosition(positionId, {
        closedAt: Date.now(),
        exitPrice: pos.entryPrice,
        exitSol: 0,
        pnlSol: -feeSol,
        pnlPct: -100,
        exitReason: 'BUY_PARSE_FAILED',
        sellSignature: null,
      });
      this.positions.delete(positionId);
      this._removeByMint(mint, positionId);
      // v3.17.21: BUY 失败 → 从 hotMints 移除
      if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(mint);
      monitor.inc('PositionManager.buyFailedClosed', 1, 'PositionManager');
      monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
      // v3.17.9: 清 watchdog
      if (pos._reconcileWatchdog) {
        clearTimeout(pos._reconcileWatchdog);
        pos._reconcileWatchdog = null;
      }
      return;
    }

    // ============ 分支 C: BUY 成功，回写真实数据 ============
    // realSolDelta 是负数（出账）。priority fee + base fee 也含在内
    const realSolSpent = -swap.realSolDelta;
    const realTokenReceived = swap.realTokenDelta;

    if (realSolSpent <= 0 || realTokenReceived <= 0) {
      monitor.recordError('PositionManager', new Error('reconcile: invalid swap deltas'), {
        signature,
        realSolSpent,
        realTokenReceived,
      });
      return;
    }

    const oldEntrySol = pos.entrySol;
    const oldEntryPrice = pos.entryPrice;
    const oldTokenAmount = pos.tokenAmount;

    // 修正：扣掉 priority fee + base fee，剩下的才是真正花在 swap 上
    // 但对策略判断来说，"我亏了多少 SOL" 用 realSolSpent 全口径比较合理
    pos.entrySol = realSolSpent;
    pos.tokenAmount = realTokenReceived;
    pos.entryPrice = realSolSpent / realTokenReceived;
    // realSolSpent 已含 priority fee 与 base fee；为避免双重扣减，把 buyFeeLamports 清零
    pos.buyFeeLamports = 0;

    // v3.17.6 关键修复（基于实战数据三个 bug 的根治方案）：
    //
    // Bug #1 修复：OPEN 时 highWaterMark = 估算 entryPrice（高估 5-15%）
    //              reconcile 后真实 entryPrice 更低 → 旧 HWM 变成"虚假高点"
    //              → 紧接着的真实价格被误判为"从 peak 大幅回撤" → trailing 误杀
    //              修复：reconcile 完成时重置 HWM 到真实 entryPrice
    //
    // Bug #3 修复：reconcile 完成那一刻，砸盘后价格还在剧烈波动 + 我们自买入
    //              推高了 AMM 池子价格 5-10%。第一个 priceTick 拿到的就是这个
    //              虚高瞬态值 → trailing 立刻 armed → 真实价格回归被误判为
    //              "回撤" → trailing 误杀
    //              修复：进入 stabilization 期（默认 5 秒），期间：
    //                - 不更新 highWaterMark（让 _checkExit 跳过 trailing 流程）
    //                - 收集所有 priceTick 到 _stabilizeSamples
    //                - emergency_stop 正常工作（救命路径不能屏蔽）
    //              stabilization 期结束时，取中位数作为 stabilizedBaseline，
    //              作为 trailing 的新起点。
    pos.highWaterMark = pos.entryPrice;
    pos.highWaterMarkTs = Date.now();
    pos.trailingArmed = false;
    pos._tpConfirmCount = 0;
    pos._tpFirstTriggerTs = null;

    // v3.17.13: reconcile 后 drift 检查 — 防止 RUG 后继续持有
    //   drift 必须在这里先算（原来在后面 console.log 那行定义的）
    const drift = ((realSolSpent - oldEntrySol) / oldEntrySol) * 100;
    const maxReconcileDriftPct = parseFloat(process.env.MAX_RECONCILE_DRIFT_PCT || '-40');
    if (maxReconcileDriftPct < 0 && drift < maxReconcileDriftPct) {
      console.warn(
        `[PositionManager] 🚨 RECONCILE_RUG ${pos.symbol || mint.slice(0, 6)}: ` +
          `drift=${drift.toFixed(2)}% < ${maxReconcileDriftPct}%, ` +
          `entrySol ${oldEntrySol.toFixed(4)}→${realSolSpent.toFixed(4)}, ` +
          `immediate sell`,
      );
      monitor.inc('PositionManager.reconcileRug', 1, 'PositionManager');
      // 不进入 stabilization，直接卖出
      pos.reconciled = true;
      pos.reconciledAt = Date.now();
      this._exit(pos, pos.entryPrice, 'RECONCILE_RUG');
      return;
    }

    // 进入 stabilization 期
    pos.reconciledAt = Date.now();
    pos.stabilizing = true;
    pos._stabilizeSamples = []; // 期间收到的所有价格

    // v3.12: 标记 reconciled，解除 _checkExit 的"完全跳过"锁定
    //        进入 stabilization 模式（_checkExit 内部判断 stabilizing 时只跑 emergency）
    pos.reconciled = true;

    // v3.17.9: reconcile 正常完成,清掉 watchdog 避免 60s 后误触发
    if (pos._reconcileWatchdog) {
      clearTimeout(pos._reconcileWatchdog);
      pos._reconcileWatchdog = null;
    }

    // v3.17.20-fix: 用 confirmTx 返回的真实落链 slot 更新 buySlot
    //    之前 buySlot = BUY 提交前 TickStream.latestSlot ≈ dumpSlot（偏小）
    //    实际 BUY tx 落链通常晚 1-2 slot，result.slot 才是真实值
    if (result.slot && result.slot > 0) {
      pos.buySlot = result.slot;
      const realLag = result.slot - pos.dumpSlot;
      console.log(
        `[PositionManager] 🔧 buySlot corrected: ${pos.symbol || mint.slice(0, 6)} ` +
          `dump_slot=${pos.dumpSlot} buy_slot=${result.slot} lag=${realLag} slot${realLag <= 1 ? ' ⚡' : ''}`,
      );
    }

    // 同步到 DB
    this.tradeLogger.updatePositionEntry(positionId, {
      entrySol: pos.entrySol,
      entryPrice: pos.entryPrice,
      tokenAmount: pos.tokenAmount,
      buyFeeLamports: 0,
      buySlot: pos.buySlot,
      dumpSlot: pos.dumpSlot,
    });

    monitor.inc('PositionManager.buyReconciled', 1, 'PositionManager');
    console.log(
      `[PositionManager] 🔧 BUY reconciled ${pos.symbol || mint.slice(0, 6)}: ` +
        `entrySol ${oldEntrySol.toFixed(4)}→${realSolSpent.toFixed(4)} (${drift.toFixed(2)}%), ` +
        `tokens ${oldTokenAmount.toFixed(2)}→${realTokenReceived.toFixed(2)}, ` +
        `entryPrice ${oldEntryPrice.toExponential(4)}→${pos.entryPrice.toExponential(4)}`,
    );

    // v3.9: 监控真实 CU 消耗，逼近 limit 时告警
    const cuConsumed = swap.computeUnitsConsumed || 0;
    const cuLimit = this.executor.computeUnitLimit || 200000;
    if (cuConsumed > 0) {
      monitor.set('PositionManager.lastBuyCuConsumed', cuConsumed, 'PositionManager');
      const cuUtilPct = (cuConsumed / cuLimit) * 100;
      monitor.set('PositionManager.lastBuyCuUtilPct', Math.round(cuUtilPct), 'PositionManager');

      if (cuUtilPct >= 90) {
        monitor.inc('PositionManager.cuNearLimit', 1, 'PositionManager');
        console.warn(
          `[PositionManager] ⚠️ ${pos.symbol || mint.slice(0, 6)} CU 消耗 ${cuConsumed} / ${cuLimit} ` +
            `(${cuUtilPct.toFixed(0)}%) — 接近上限，建议调高 COMPUTE_UNIT_LIMIT 或观察是否有 BUY_CHAIN_FAILED`,
        );
      }
    }
  }

  _tick() {
    const now = Date.now();

    for (const pos of this.positions.values()) {
      if (pos.exiting) continue;

      this._fillPreVolFallback(pos);
      const age = now - pos.openedAt;
      // v3.19: peak 感知梯度超时 — 替代固定 maxHoldMs
      // peak<=0 → 5min, peak 0-5% → 10min, peak 5-8% → 25min, peak>=8% → maxHoldMs
      const peakPnlForTimeout = (pos.highWaterMark && pos.entryPrice > 0)
        ? ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100
        : 0;
      const timeoutMs = this.getPeakAwareTimeoutMs(peakPnlForTimeout, pos.preVol5m, pos.mint);
      if (timeoutMs > 0 && age >= timeoutMs) {
        const lastPrice = this.priceTracker.getPrice(pos.mint) || pos.entryPrice;
        console.log(
          `[PositionManager] ⏱️ TIMEOUT (peak-aware) ${pos.symbol || pos.mint.slice(0, 6)} ` +
          `peak=${peakPnlForTimeout.toFixed(1)}% timeout=${(timeoutMs/1000).toFixed(0)}s age=${(age/1000).toFixed(0)}s`,
        );
        // v3.19: exit_reason 带 timeout 时长，区分不同梯度
        const timeoutMin = Math.round(timeoutMs / 60000);
        this._exit(pos, lastPrice, `TIMEOUT_${timeoutMin}M`);
      }

      // v3.18: EARLY_LOW_PEAK_CUT — 死币早砍,连续时间覆盖
      // v3.32b: 可通过 EARLY_LOW_PEAK_CUT_ENABLED=0 禁用
      if (process.env.EARLY_LOW_PEAK_CUT_ENABLED === '1') {
      // 依据:6/2 LOW_PEAK_TIMEOUT 漏网 132 笔死币扛 18-20min 亏 -8~-11%
      // peak<1% 124笔 扛18.3min -11.5% -> 2min 该切
      // peak 1-2% 34笔 扛19.4min -8.2% -> 3min 该切
      // peak 2-3% 24笔 扛20.3min -4.5% -> 5min 该切
      // 关键:peak<1% 不要求 PnL 条件(旧 Phase1 要求 PnL<-5% 漏了一批)
      // 边界:peak<3 严格小于归死币早砍, peak>=3 归 trailing, 不重不漏
      {
        const peakPnlPct = (pos.highWaterMark && pos.entryPrice > 0)
          ? ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100
          : 0;
        const ageMin = age / 60000;

        let shouldCut = false;
        if (ageMin >= 4 && peakPnlPct < 1) shouldCut = true;       // peak<1% 死透,4min 切
        else if (ageMin >= 6 && peakPnlPct < 2) shouldCut = true;   // 6min 切
        else if (ageMin >= 10 && peakPnlPct < 1) shouldCut = true;   // 10min 切 peak<1% 兜底

        if (shouldCut) {
          const lastPrice = this.priceTracker.getPrice(pos.mint) || pos.entryPrice;
          console.log(
            '[PositionManager] EARLY_LOW_PEAK_CUT ' + (pos.symbol || pos.mint.slice(0, 6)) +
            ' peak=' + peakPnlPct.toFixed(1) + '% age=' + ageMin.toFixed(1) + 'min'
          );
          monitor.inc('PositionManager.earlyLowPeakCut', 1, 'PositionManager');
          this._exit(pos, lastPrice, 'EARLY_LOW_PEAK_CUT');
          continue;
        }
      } // end EARLY_LOW_PEAK_CUT
      }
      // v3.17.13: stabilization 超时
      //   实战：BABYELON reconcile watchdog 恢复后进入 stabilization，
      //   但 PriceTracker 不再推价格（代币交易冷清），stabilization 永远结束不了
      if (pos.stabilizing && pos.reconciledAt) {
        const stabElapsed = now - pos.reconciledAt;
        if (stabElapsed >= config.strategy.stabilizationMs) {
          const samples = pos._stabilizeSamples ? pos._stabilizeSamples.slice().sort((a, b) => a - b) : [];
          let baseline;
          if (samples.length === 0) {
            // 没有 tick 数据，用 PoolStateCache 或 entryPrice
            const poolPrice = this._getPoolPrice(pos.mint);
            baseline = poolPrice || pos.entryPrice;
          } else {
            const mid = Math.floor(samples.length / 2);
            baseline = samples.length % 2 === 0
              ? (samples[mid - 1] + samples[mid]) / 2
              : samples[mid];
          }
          // v3.17.26: HWM = max(baseline, entryPrice)
          //   如果 baseline < entryPrice（买入后价格回落），HWM 不应低于 entryPrice
          //   否则 trailing arm 从 entryPrice 算 peakPnl 时，需要从 baseline 多涨几% 才能到达 entryPrice*1.08
          //   实测：baseline 比 entryPrice 低 2-5% 时，实际需要涨 10-13% 才触发 8% arm
          pos.highWaterMark = Math.max(baseline, pos.entryPrice);
          pos.highWaterMarkTs = Date.now();
          pos.stabilizing = false;
          pos._stabilizeSamples = null;
          pos.baselinePrice = baseline; // v3.17.15: 记录稳定期结束时的实际价格
          // v3.17.27: stabilization 结束时立即持久化 peak_price
          //   避免重启后 stabilization 重来 + HWM 被重置
          const stabPeakPnlPct = ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100;
          try {
            this.tradeLogger.stmts.updatePeak.run({
              positionId: pos.positionId,
              peakPrice: pos.highWaterMark,
              peakTs: pos.highWaterMarkTs,
              peakPnlPct: stabPeakPnlPct,
            });
            pos._lastPeakFlush = Date.now();
          } catch (_) { /* best effort */ }
          const baselinePnlPct = ((baseline - pos.entryPrice) / pos.entryPrice) * 100;
          console.log(
            `[PositionManager] ✅ stabilization done (tick-timeout) ${pos.symbol || pos.mint.slice(0, 6)}: ` +
              `samples=${samples.length}, baseline=${baseline.toExponential(4)} (${baselinePnlPct.toFixed(2)}%), ` +
              `HWM set to ${pos.highWaterMark.toExponential(4)}`,
          );
        }
      }
    }

    // v3.26: 持仓价格主动检查
    // 问题: _checkExit 只在 priceTracker.on('update') 时触发
    // 如果 DumpDetector 没有新 priceTick → 价格不更新 → 止盈/止损不检查
    // 修复: 每500ms用 priceTracker 当前价格主动调用 _checkExit
    // 同时从 PoolStateCache 获取实时价格（比 priceTracker 更可靠）
    this._tickCount++;
    if (this._tickCount % 5 === 0) { // 每500ms
      for (const pos of this.positions.values()) {
        if (pos.exiting || pos.status === 'stuck') continue;

        // 优先用 PoolStateCache 的实时价格
        let price = 0;
        if (this.executor?.poolStateCache && this.tokenRegistry) {
          const tokenInfo = this.tokenRegistry.getToken(pos.mint);
          if (tokenInfo?.pool_address) {
            const poolState = this.executor.poolStateCache.get(tokenInfo.pool_address);
            if (poolState) {
              const baseAmt = Number(poolState.poolBaseAmount?.toString() || 0);
              const quoteAmt = Number(poolState.poolQuoteAmount?.toString() || 0);
              if (baseAmt > 0 && quoteAmt > 0) {
                // PoolStateCache 的 baseAmt/quoteAmt 是链上原始单位(lamports)
                // 需要转换: price = (quoteAmt/1e9) / (baseAmt/10^decimals) SOL/token
                const decimals = tokenInfo.decimals ?? 6; // Pump.fun = 6
                price = (quoteAmt / 1e9) / (baseAmt / Math.pow(10, decimals));
              }
            }
          }
        }

        // fallback: 用 priceTracker 的当前价格
        if (!price || !Number.isFinite(price) || price <= 0) {
          price = this.priceTracker?.getPrice(pos.mint) || 0;
        }

        if (price > 0 && Number.isFinite(price)) {
          // 同步 priceTracker（让 dashboard 显示正确价格）
          const trackerPrice = this.priceTracker?.getPrice(pos.mint) || 0;
          if (Math.abs(price - trackerPrice) / (trackerPrice || price) > 0.005) {
            this.priceTracker?.forceSet(pos.mint, price);
          }
          // 主动检查退出条件
          this._checkExit(pos.positionId, price);
        }
      }
    }
  }

  // v3.27: 波动率感知 + 新老币差异化 trailing drawdown
  getTrailingDrawdownPct(peakPnlPct, preVol5m, mint) {
    const newCoinTrailingDrawdown = parseFloat(process.env.NEW_COIN_TRAILING_DRAWDOWN_PCT || '5');
    const oldCoinTrailingDrawdown = parseFloat(process.env.OLD_COIN_TRAILING_DRAWDOWN_PCT || '5');
    const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');

    if (mint) {
      const tokenInfo = this.tokenRegistry?.getToken(mint);
      if (tokenInfo && tokenInfo.added_at) {
        const tokenAgeMs = Date.now() - tokenInfo.added_at;
        if (tokenAgeMs >= newCoinThresholdMs) {
          return oldCoinTrailingDrawdown;
        } else {
          return newCoinTrailingDrawdown;
        }
      }
    }

    // fallback: tokenRegistry查不到时默认用老币值
    return oldCoinTrailingDrawdown;
  }

  // v3.20: 波动率感知 trailing activate
  getTrailingActivatePct(preVol5m, mint) {
    // v3.27: 新老币差异化
    const newCoinTrailingActivate = parseFloat(process.env.NEW_COIN_TRAILING_ACTIVATE_PCT || '10');
    const oldCoinTrailingActivate = parseFloat(process.env.OLD_COIN_TRAILING_ACTIVATE_PCT || '15');
    const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');

    if (mint) {
      const tokenInfo = this.tokenRegistry?.getToken(mint);
      if (tokenInfo && tokenInfo.added_at) {
        const tokenAgeMs = Date.now() - tokenInfo.added_at;
        if (tokenAgeMs >= newCoinThresholdMs) {
          return oldCoinTrailingActivate;
        } else {
          return newCoinTrailingActivate;
        }
      }
    }

    // fallback: tokenRegistry查不到时默认用老币值
    return oldCoinTrailingActivate;
  }

  // v3.22: 波动率感知 take profit — 三波段
  getTakeProfitPct(preVol5m, mint) {
    // v3.27: 新老币差异化
    const newCoinTakeProfit = parseFloat(process.env.NEW_COIN_TAKE_PROFIT_PCT || '15');
    const oldCoinTakeProfit = parseFloat(process.env.OLD_COIN_TAKE_PROFIT_PCT || '25');
    const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');

    if (mint) {
      const tokenInfo = this.tokenRegistry?.getToken(mint);
      if (tokenInfo && tokenInfo.added_at) {
        const tokenAgeMs = Date.now() - tokenInfo.added_at;
        if (tokenAgeMs >= newCoinThresholdMs) {
          return oldCoinTakeProfit;
        } else {
          return newCoinTakeProfit;
        }
      }
    }

    // fallback: tokenRegistry查不到 → 用旧波动率逻辑，
    // 但低波动/中波动TP也应走OLD_COIN值（20/25），避免老币被低TP(15%)误杀
    if (preVol5m != null && preVol5m >= 0) {
      const lowThreshold = parseFloat(process.env.VOL_LOW_THRESHOLD || '10');
      const highThreshold = parseFloat(process.env.VOL_HIGH_THRESHOLD || '15');
      if (preVol5m < lowThreshold) {
        return parseFloat(process.env.VOL_LOW_TAKE_PROFIT_PCT || '20');
      }
      if (preVol5m >= highThreshold) {
        return parseFloat(process.env.VOL_HIGH_TAKE_PROFIT_PCT || '25');
      }
      return parseFloat(process.env.VOL_MID_TAKE_PROFIT_PCT || oldCoinTakeProfit);
    }
    // 完全无数据时，默认用老币TP(25%) — 宁可多拿也不少拿
    return oldCoinTakeProfit;
  }

  // v3.20: 波动率感知超时 — 替代纯peak感知梯度
  // 低波动币(pre_vol<10%): 不设TIMEOUT — 竞对数据证明低波币死扛84%能弹回
  // 高波动币(pre_vol>=15%): 60min — 给高波币更多时间弹回
  // 默认(10-15%): 保留peak感知梯度超时
  getPeakAwareTimeoutMs(peakPnlPct, preVol5m, mint) {
    // v3.27: 老币关闭超时(竞对avg hold 398min, 我们TIMEOUT是最大亏损源)
    // 新币保持波动率感知超时
    const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');
    const oldCoinTimeoutMs = parseInt(process.env.OLD_COIN_TIMEOUT_MS || '0'); // 0=不超时

    if (mint) {
      const tokenInfo = this.tokenRegistry?.getToken(mint);
      if (tokenInfo && tokenInfo.added_at) {
        const tokenAgeMs = Date.now() - tokenInfo.added_at;
        if (tokenAgeMs >= newCoinThresholdMs) {
          // 老币: 用 OLD_COIN_TIMEOUT_MS
          return oldCoinTimeoutMs;
        }
      }
    }

    // 新币: 波动率感知超时
    const lowThreshold = parseFloat(process.env.VOL_LOW_THRESHOLD || '10');
    const highThreshold = parseFloat(process.env.VOL_HIGH_THRESHOLD || '15');
    if (preVol5m != null && preVol5m >= 0) {
      if (preVol5m < lowThreshold) {
        return parseInt(process.env.VOL_LOW_TIMEOUT_MS || '0'); // 0=不超时(死扛)
      }
      if (preVol5m >= highThreshold) {
        return parseInt(process.env.VOL_HIGH_TIMEOUT_MS || '0');
      }
      return parseInt(process.env.VOL_MID_TIMEOUT_MS || '0');
    }
    // preVol5m为null → 中波处理
    return parseInt(process.env.VOL_MID_TIMEOUT_MS || '0');
  }

  // v3.17.42: 同步版本 — 直接查 DB，避免异步竞态导致波动率写丢失
  // 之前 async 版本在快速卖出场景下，position 可能在回调前已关闭
  _computePreVol5mSync(pid, mint, openedAt) {
    try {
      const db = this.tradeLogger?.db;
      if (!db) return;
      const startTs = openedAt - 300000;
      const rows = db.prepare(
        'SELECT price FROM price_samples WHERE mint = ? AND ts >= ? AND ts < ? ORDER BY ts'
      ).all(mint, startTs, openedAt);
      if (rows.length >= 2) {
        const prices = rows.map(r => r.price).filter(p => p > 0);
        if (prices.length >= 2) {
          const high = Math.max(...prices);
          const low = Math.min(...prices);
          const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
          if (avg > 0) {
            const vol5m = +((high - low) / avg * 100).toFixed(2);
            const pos = this.positions.get(pid);
            if (pos) {
              pos.preVol5m = vol5m;
              console.log(
                `[PositionManager] 📊 pre_vol_5m=${vol5m.toFixed(1)}% ${pos.symbol || pos.mint.slice(0,6)} ` +
                `${vol5m < 10 ? '🟢低波动(死扛+高止盈)' : vol5m >= 15 ? '🔴高波动(宽trailing+长timeout)' : '🟡中波动'}`
              );
              try {
                db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(vol5m, pid);
              } catch (_) {}
            }
            return;
          }
        }
      }
      // 买入前没数据，尝试用买入后5min数据(给 _fillPreVolFallback 用)
    } catch (err) {
      // non-critical
    }
  }

  // v3.20: 异步计算买入前5分钟波动率 (保留作 fallback)
  // v3.23: compute pre-buy 5min range support line
  _computeRangeSupport(pid, mint, openedAt) {
    try {
      const db = this.tradeLogger?.db;
      if (!db) return;
      const startTs = openedAt - 300000;
      const rows = db.prepare(
        'SELECT price FROM price_samples WHERE mint = ? AND ts >= ? AND ts < ? ORDER BY ts'
      ).all(mint, startTs, openedAt);
      if (rows.length >= 3) {
        const prices = rows.map(r => r.price).filter(p => p > 0);
        if (prices.length >= 3) {
          const rangeLow = Math.min(...prices);
          const pos = this.positions.get(pid);
          if (pos) {
            pos.rangeSupport = rangeLow;
            console.log(
              '[PositionManager] rangeSupport=' + rangeLow.toExponential(3) + ' ' + (pos.symbol || pos.mint.slice(0,6))
            );
            try {
              db.prepare('UPDATE positions SET range_support = ? WHERE position_id = ?').run(rangeLow, pid);
            } catch (_) {}
          }
        }
      }
    } catch (err) {
      // non-critical
    }
  }

  // 从 price_samples 表取买入前5min价格序列,计算 (max-min)/avg*100
  async _computePreVol5m(pid, mint, openedAt) {
    try {
      const db = this.tradeLogger?.db;
      if (!db) return;
      const startTs = openedAt - 300000; // 5min
      const rows = db.prepare(
        'SELECT price FROM price_samples WHERE mint = ? AND ts >= ? AND ts < ? ORDER BY ts'
      ).all(mint, startTs, openedAt);
      if (rows.length >= 2) {
        const prices = rows.map(r => r.price).filter(p => p > 0);
        if (prices.length >= 2) {
          const high = Math.max(...prices);
          const low = Math.min(...prices);
          const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
          if (avg > 0) {
            const vol5m = +((high - low) / avg * 100).toFixed(2);
            const pos = this.positions.get(pid);
            if (pos) {
              pos.preVol5m = vol5m;
              console.log(
                `[PositionManager] 📊 pre_vol_5m=${vol5m.toFixed(1)}% ${pos.symbol || pos.mint.slice(0,6)} ` +
                `${vol5m < 10 ? '🟢低波动(死扛+高止盈)' : vol5m >= 15 ? '🔴高波动(宽trailing+长timeout)' : '🟡中波动'}`
              );
              // 写入DB
              try {
                db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(vol5m, pid);
              } catch (_) {}
            }
          }
        }
      }
    } catch (err) {
      // non-critical but log for debugging
      console.error(`[PositionManager] ❌ _computePreVol5m error for ${mint.slice(0,8)}:`, err.message);
    }
  }

  // v3.21: 当 preVol5m 为 null 时，用持仓内实时波动率 fallback
  _fillPreVolFallback(pos) {
    if (pos.preVol5m != null) return;
    const age = Date.now() - pos.openedAt;
    if (age < 30000) return;

    // 方法1: stabilization 样本
    if (pos._stabilizeSamples && pos._stabilizeSamples.length >= 5) {
      const prices = pos._stabilizeSamples.map(s => s.price).filter(p => p > 0);
      if (prices.length >= 5) {
        const high = Math.max(...prices);
        const low = Math.min(...prices);
        const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
        if (avg > 0) {
          const vol = +((high - low) / avg * 100).toFixed(2);
          pos.preVol5m = vol;
          console.log('[PositionManager] pre_vol FALLBACK(stab)=' + vol.toFixed(1) + '% ' + (pos.symbol || pos.mint.slice(0,6)) + ' ' + (vol < 10 ? 'LOW' : vol >= 15 ? 'HIGH' : 'MID'));
          try { const db = this.tradeLogger?.db; if (db) db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(vol, pos.positionId); } catch (_) {}
          return;
        }
      }
    }

    // 方法2: 当前价格偏差估算
    const currentPrice = this.priceTracker?.getPrice(pos.mint);
    if (currentPrice && pos.entryPrice > 0) {
      const instantVol = Math.abs(currentPrice - pos.entryPrice) / pos.entryPrice * 100;
      const estimatedVol = +(instantVol * 2.5).toFixed(2);
      const vol = Math.max(estimatedVol, 1.0);
      pos.preVol5m = vol;
      console.log('[PositionManager] pre_vol FALLBACK(instant)=' + vol.toFixed(1) + '% ' + (pos.symbol || pos.mint.slice(0,6)) + ' ' + (vol < 10 ? 'LOW' : vol >= 15 ? 'HIGH' : 'MID'));
      try { const db = this.tradeLogger?.db; if (db) db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(vol, pos.positionId); } catch (_) {}
      return;
    }

    // 方法3: DB price_samples 查买入后5min波动率
    if (age >= 300000) {
      try {
        const db = this.tradeLogger?.db;
        if (db) {
          const rows = db.prepare('SELECT price FROM price_samples WHERE mint = ? AND ts >= ? AND ts < ? ORDER BY ts').all(pos.mint, pos.openedAt, pos.openedAt + 300000);
          if (rows.length >= 3) {
            const prices = rows.map(r => r.price).filter(p => p > 0);
            if (prices.length >= 3) {
              const high = Math.max(...prices);
              const low = Math.min(...prices);
              const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
              if (avg > 0) {
                const vol = +((high - low) / avg * 100).toFixed(2);
                pos.preVol5m = vol;
                console.log('[PositionManager] pre_vol FALLBACK(db5m)=' + vol.toFixed(1) + '% ' + (pos.symbol || pos.mint.slice(0,6)) + ' ' + (vol < 10 ? 'LOW' : vol >= 15 ? 'HIGH' : 'MID'));
                db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(vol, pos.positionId);
                return;
              }
            }
          }
        }
      } catch (_) {}
      pos.preVol5m = 12;
      console.log('[PositionManager] pre_vol FALLBACK(default)=12% ' + (pos.symbol || pos.mint.slice(0,6)) + ' MID');
      try { const db = this.tradeLogger?.db; if (db) db.prepare('UPDATE positions SET pre_vol_5m_pct = ? WHERE position_id = ?').run(12, pos.positionId); } catch (_) {}
    }
  }

  _checkExit(positionId, price) {
    const pos = this.positions.get(positionId);
    if (!pos || pos.exiting) return;

    // v3.26: stuck 仓位不再触发退出逻辑（pool已死，卖出会循环失败）
    if (pos.status === 'stuck') return;

    // v3.20: 获取此仓位的买入前波动率
    const preVol5m = pos.preVol5m;

    // v3.17.21: 计数每个 position 收到的 price tick 数（平仓时写入 DB，
    //   用于判断峰值数据可信度：tick 多 → 峰值可靠，tick 少 → 可能是噪音）
    pos.tickCount = (pos.tickCount || 0) + 1;

    // v3.17.27 DEBUG: 每个 price tick 都打印（临时，验证后删除）
    if (pos.tickCount <= 5 || pos.tickCount % 20 === 0 || price > pos.highWaterMark) {
      const pnl = ((price - pos.entryPrice) / pos.entryPrice * 100).toFixed(2);
      console.log(
        `[PositionManager] 📊 tick #${pos.tickCount} ${pos.symbol || pos.mint.slice(0,6)} ` +
        `price=${price.toExponential(4)} hwm=${pos.highWaterMark.toExponential(4)} pnl=${pnl}% ` +
        `${price > pos.highWaterMark ? '📈 NEW HWM' : ''}`,
      );
    }

    // v3.17.13: 价格合理性检查 — 防止假价格污染 HWM 和触发错误卖出
    //   实战：STICKO 出现价格从 8e-7 突变到 ~1.0 (ratio > 1M)，
    //   导致 HWM 被设成 1.0，trailing 在 -99.9% 时触发
    //   修复：如果价格相对 HWM 涨超 10x 或跌超 90%，忽略这个 tick
    if (pos.highWaterMark > 0 && price > 0) {
      const priceRatio = price / pos.highWaterMark;
      if (priceRatio > 10 || priceRatio < 0.1) {
        // 极端跳变，忽略
        return;
      }
    }
    // 对 entryPrice 也做类似检查（stabilization 期间 HWM=entryPrice）
    if (pos.entryPrice > 0 && price > 0) {
      const entryRatio = price / pos.entryPrice;
      if (entryRatio > 10 || entryRatio < 0.1) {
        return;
      }
    }

    // v3.12: reconcile 完成前完全跳过（entryPrice 是估算值，所有 exit 检查都不可靠）
    //        例外：MAX_HOLD_MS 超时由 _tick 那条路径触发
    if (!pos.reconciled && !pos.dryRun) {
      return;
    }

    // v3.17.42: 记录最新tick价格，供前端API使用(priceTracker可能没追踪该mint)
    pos._lastTickPrice = price;
    const pnlPct = ((price - pos.entryPrice) / pos.entryPrice) * 100;

    // ============ v3.17.7 stabilization 期处理 ============
    // 期间只收集价格样本，不更新 HWM，不武装 trailing，不检查 TP
    // 期满时取样本中位数作为 stabilizedBaseline，过滤砸盘瞬态 + 自买入推高
    if (pos.stabilizing) {
      // v3.17.13: stabilization 样本也过滤极端价格
      if (pos.entryPrice > 0 && price > 0) {
        const sr = price / pos.entryPrice;
        if (sr < 0.1 || sr > 10) {
          // 忽略极端样本
        } else {
          pos._stabilizeSamples.push(price);
        }
      } else {
        pos._stabilizeSamples.push(price);
      }

      // ============ stabilization 期内的紧急止损 ============
      //
      // 设计动机：
      //   实战发现 stabilization 期内直接用"相对 entryPrice"的 -15% 阈值会误杀。
      //   根因：3 SOL 买入推高 30 SOL 池子 ~10%，然后价格回归，第一个 tick 就是
      //         "相对 entryPrice -15%" 的假信号。但这是自买入造成的虚高回归，
      //         不是市场灾难。
      //
      //   openclaw 的修复：stabilization 期 emergency 阈值放宽到 -30%
      //     问题：拍脑袋的数字。如果真的暴跌 -25%，会被放过 5 秒。
      //
      //   我的方案：stabilization 期改用"相对样本最高价的回撤"判断 emergency
      //     - max(samples) ≈ 自买入推高的峰值
      //     - 从这个峰值真的跌 stabilizationEmergencyDrawdownPct%（默认 20%）才认作灾难
      //     - 这样能区分"AMM 自然回归" vs "真实大跌"
      //     - 例：entryPrice 估算 8.2e-6，buy 后样本 [7.5, 7.3, 7.1]，max=7.5
      //           当前 6.8 → 回撤 9.3%，不触发（这正是 openclaw 想避免的误杀场景）
      //           若当前 5.8 → 回撤 22.7%，触发 emergency（真灾难）
      const sampleMax = pos._stabilizeSamples.reduce(
        (m, p) => (p > m ? p : m),
        pos.entryPrice,
      );
      const drawdownFromMax = ((sampleMax - price) / sampleMax) * 100;
      const stabEmergencyDD = config.strategy.stabilizationEmergencyDrawdownPct;
      // v3.17.13: stabilization emergency 也受 EMERGENCY_STOP_LOSS_PCT=0 控制
      //   如果用户关闭了紧急止损，stabilization 期内也不应触发
      const emergencyEnabled = config.strategy.emergencyStopLossPct !== 0;
      // v3.17.16: stabilization 期内的 emergency 不再加额外 grace
      //   stabilization 期本身就是保护(只用相对峰值回撤判断,不用 entryPrice PnL),
      //   再加 5 分钟 grace 等于"前 5 分钟即使从稳定期峰值跌 50% 也不卖"。
      //   这跟"反弹就卖"的策略完全矛盾。stabEmergencyDD=20% 已经足够宽容。
      if (emergencyEnabled && stabEmergencyDD > 0 && drawdownFromMax >= stabEmergencyDD) {
        console.warn(
          `[PositionManager] 🚨 EMERGENCY_STOP (stabilization) ${pos.symbol || pos.mint.slice(0, 6)} ` +
            `drawdown ${drawdownFromMax.toFixed(2)}% from stabilization peak ` +
            `(sampleMax=${sampleMax.toExponential(3)}, current=${price.toExponential(3)}, pnl=${pnlPct.toFixed(2)}%)`,
        );
        this._exit(pos, price, 'EMERGENCY_STOP');
        return;
      }

      const elapsed = Date.now() - pos.reconciledAt;
      const stabilizeMs = config.strategy.stabilizationMs;
      if (elapsed >= stabilizeMs) {
        // stabilization 结束 — 计算中位数 baseline
        const samples = pos._stabilizeSamples.slice().sort((a, b) => a - b);
        let baseline;
        if (samples.length === 0) {
          baseline = pos.entryPrice; // 期内没收到任何 tick（罕见）
        } else {
          const mid = Math.floor(samples.length / 2);
          baseline = samples.length % 2 === 0
            ? (samples[mid - 1] + samples[mid]) / 2
            : samples[mid];
        }
        // v3.17.26: HWM = max(baseline, entryPrice) — 同上 tick-timeout 分支的修复
        pos.highWaterMark = Math.max(baseline, pos.entryPrice);
        pos.highWaterMarkTs = Date.now();
        pos.stabilizing = false;
        pos._stabilizeSamples = null; // 释放内存
        pos.baselinePrice = baseline; // v3.17.15: 记录稳定期结束时的实际价格
        // v3.17.27: stabilization 结束时立即持久化 peak_price
        const stabPeakPnlPct = ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100;
        try {
          this.tradeLogger.stmts.updatePeak.run({
            positionId: pos.positionId,
            peakPrice: pos.highWaterMark,
            peakTs: pos.highWaterMarkTs,
            peakPnlPct: stabPeakPnlPct,
          });
          pos._lastPeakFlush = Date.now();
        } catch (_) { /* best effort */ }

        const baselinePnlPct = ((baseline - pos.entryPrice) / pos.entryPrice) * 100;
        console.log(
          `[PositionManager] ✅ stabilization done ${pos.symbol || pos.mint.slice(0, 6)}: ` +
            `samples=${samples.length}, baseline=${baseline.toExponential(4)} (${baselinePnlPct.toFixed(2)}%), ` +
            `HWM set to ${pos.highWaterMark.toExponential(4)}`,
        );
        monitor.inc('PositionManager.stabilizationDone', 1, 'PositionManager');
        monitor.set('PositionManager.lastStabilizeSamples', samples.length, 'PositionManager');
      }
      // stabilizing 期内不进入 TP / trailing 流程
      return;
    }

    // ============ 1. 紧急止损：救命路径，最高优先级 ============
    //    v3.17.16: grace 从 5min 降到 30s（默认）。
    //
    //    原 5min 的设计是为了避开"刚买入时滑点+自买入推高造成的假亏损 PnL"。
    //    但 stabilization 期(默认 5s)已经用"相对峰值回撤"模式做了同样保护,
    //    stabilization 结束后 entryPrice 已经是真实成交价、HWM 已经是稳定期 baseline,
    //    pnlPct 是可靠的。再加 5min grace 等于"前 5 分钟跌 30%、50% 都不卖",
    //    跟"反弹就卖"的策略矛盾。
    //
    //    新默认 30s 给一个小缓冲(stabilization 5s + 一点反弹观察期),
    //    之后 -15% 必须立即出场。如果想恢复旧行为,设 EMERGENCY_STOP_GRACE_MS=300000。
    const emergencyPct = config.strategy.emergencyStopLossPct;
    const emergencyGraceMs = parseInt(process.env.EMERGENCY_STOP_GRACE_MS || '30000', 10); // v3.17.16: 30秒
    const posAge = Date.now() - (pos.openedAt || pos.ts);
    if (emergencyPct < 0 && pnlPct <= emergencyPct && posAge >= emergencyGraceMs) {
      console.warn(
        `[PositionManager] 🚨 EMERGENCY_STOP ${pos.symbol || pos.mint.slice(0, 6)} ` +
          `pnl=${pnlPct.toFixed(2)}% (age=${(posAge/1000).toFixed(0)}s)`,
      );
      this._exit(pos, price, 'EMERGENCY_STOP');
      return;
    }

    // ============ 1.5 v3.17.42: 智能止损（分波动率） ============
    // 比简单固定止损更聪明：trailing已armed时不触发，只救trailing永远不armed的死扛仓位
    //
    // 数据支撑(2026-06-08回测):
    //   简单止损-15%在高波误杀12笔已止盈币，智能止损仅误杀1笔
    //   因为所有误杀币都是"先涨后跌再涨回来"——trailing已armed，回撤时trailing先触发
    //
    // 规则：
    //   1. trailing已armed → 不触发（trailing自行处理回撤）
    //   2. 持仓 < smartStopGraceMs → 不触发（避免刚买入就止损）
    //   3. stabilization期内 → 不触发（上面已经return了，这里也不会到）
    //   4. PnL跌破波动率对应阈值 → 触发止损
    //   v3.26: 新币(<24h) SMART_STOP 收至 -25%，老币保持 -50%
    //     7天数据回测：新币 SMART_STOP 22笔亏 -16.88 SOL，收至 -25% 可省 ~8 SOL
    {
      let volStopPct = 0;
      const lowThreshold = parseFloat(process.env.VOL_LOW_THRESHOLD || '10');
      const highThreshold = parseFloat(process.env.VOL_HIGH_THRESHOLD || '15');
      if (preVol5m != null && preVol5m >= 0) {
        if (preVol5m < lowThreshold) {
          volStopPct = config.strategy.volLowEmergencyStopPct;
        } else if (preVol5m >= highThreshold) {
          volStopPct = config.strategy.volHighEmergencyStopPct;
        } else {
          volStopPct = config.strategy.volMidEmergencyStopPct;
        }
      }

      // v3.27: 新老币差异化 SMART_STOP
      // 新币: 用更紧的止损(竞对avg loss -1.43 SOL相对小,我们stop不能太宽)
      // 老币: 放宽止损(竞对avg hold 398min,peak 21%只拿到8%说明我们杀得太早)
      const newCoinSmartStopPct = parseFloat(process.env.NEW_COIN_SMART_STOP_PCT || '0');
      const oldCoinSmartStopPct = parseFloat(process.env.OLD_COIN_SMART_STOP_PCT || '0');
      const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');

      if (volStopPct < 0 && newCoinSmartStopPct < 0) {
        const tokenInfo = this.tokenRegistry?.getToken(pos.mint);
        if (tokenInfo && tokenInfo.added_at) {
          const tokenAgeMs = Date.now() - tokenInfo.added_at;
          if (tokenAgeMs < newCoinThresholdMs) {
            // 新币
            if (volStopPct < newCoinSmartStopPct) {
              volStopPct = newCoinSmartStopPct;
            }
          } else {
            // 老币: 用更宽松的止损
            if (oldCoinSmartStopPct < 0 && volStopPct < oldCoinSmartStopPct) {
              volStopPct = oldCoinSmartStopPct;
            }
          }
        }
      }

      // v3.27: 新老币差异化 grace — 新币无grace(立即止损), 老币5min grace
      const newCoinSmartStopGraceMs = parseInt(process.env.NEW_COIN_SMART_STOP_GRACE_MS || '0');
      const oldCoinSmartStopGraceMs = config.strategy.smartStopGraceMs;
      let effectiveGraceMs = oldCoinSmartStopGraceMs;
      if (newCoinSmartStopGraceMs < oldCoinSmartStopGraceMs) {
        const tokenInfo = this.tokenRegistry?.getToken(pos.mint);
        if (tokenInfo && tokenInfo.added_at) {
          const tokenAgeMs = Date.now() - tokenInfo.added_at;
          const newCoinThresholdMs2 = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');
          if (tokenAgeMs < newCoinThresholdMs2) {
            effectiveGraceMs = newCoinSmartStopGraceMs;
          }
        }
      }

      if (volStopPct < 0 && !pos.trailingArmed && posAge >= effectiveGraceMs) {
        if (pnlPct <= volStopPct) {
          const volLabel = preVol5m < lowThreshold ? '低波' : preVol5m >= highThreshold ? '高波' : '中波';
          console.warn(
            `[PositionManager] 🛡️ SMART_STOP ${pos.symbol || pos.mint.slice(0, 6)} ` +
              `pnl=${pnlPct.toFixed(2)}% <= ${volStopPct}% (${volLabel}, vol=${preVol5m?.toFixed(1) || '?'}%) ` +
              `trailingArmed=false, age=${(posAge/1000).toFixed(0)}s`,
          );
          monitor.inc('PositionManager.smartStop', 1, 'PositionManager');
          this._exit(pos, price, 'SMART_STOP');
          return;
        }
      }
    }

    // ============ 1.6 v3.23: RANGE_STOP (interval-based stop-loss) ============
    // Buy price broke below pre-buy 5min range support + PnL < -10% + trailing not armed
    // v3.24: disabled via RANGE_STOP_ENABLED=0
    {
      const rangeStopEnabled = parseInt(process.env.RANGE_STOP_ENABLED || '0', 10);
      const rangeSupportPct = parseFloat(process.env.RANGE_STOP_SUPPORT_TOLERANCE_PCT || '3');
      const rangeStopPnlThreshold = parseFloat(process.env.RANGE_STOP_PNL_THRESHOLD_PCT || '-10');
      if (rangeStopEnabled && pos.rangeSupport && pos.rangeSupport > 0 && !pos.trailingArmed &&
          posAge >= config.strategy.smartStopGraceMs && pnlPct <= rangeStopPnlThreshold) {
        const supportLine = pos.rangeSupport * (1 - rangeSupportPct / 100);
        if (price < supportLine) {
          const breakPct = ((price - pos.rangeSupport) / pos.rangeSupport * 100).toFixed(1);
          console.warn(
            '[PositionManager] RANGE_STOP ' + (pos.symbol || pos.mint.slice(0, 6)) +
              ' pnl=' + pnlPct.toFixed(2) + '% support=' + pos.rangeSupport.toExponential(3) +
              ' price=' + price.toExponential(3) + ' break=' + breakPct + '% trailingArmed=false age=' + (posAge/1000).toFixed(0) + 's',
          );
          monitor.inc('PositionManager.rangeStop', 1, 'PositionManager');
          this._exit(pos, price, 'RANGE_STOP');
          return;
        }
      }
    }

    // ============ 1.7 v3.24: TREND_STOP (real-time trend break stop-loss) ============
    // Price breaks below 5-minute moving average + PnL < threshold + trailing not armed
    // Data: break+PnL<-8% saves avg 0.7% vs holding, 66% of broken-trend trades continue falling
    {
      const trendStopEnabled = process.env.TREND_STOP_ENABLED === '1';
      const trendStopPnlThreshold = parseFloat(process.env.TREND_STOP_PNL_THRESHOLD_PCT || '-8');
      const trendStopMaWindow = parseInt(process.env.TREND_STOP_MA_WINDOW_SEC || '300') * 1000; // default 5min
      const trendStopMinAge = parseInt(process.env.TREND_STOP_MIN_AGE_SEC || '120') * 1000; // default 2min
      const trendStopBreakPct = parseFloat(process.env.TREND_STOP_BREAK_PCT || '1'); // price < MA*(1-breakPct/100)
      if (trendStopEnabled && !pos.trailingArmed && pnlPct <= trendStopPnlThreshold &&
          posAge >= trendStopMinAge) {
        // Compute 5-minute moving average from price_samples
        const now = Date.now();
        const maStart = now - trendStopMaWindow;
        const _db = this.tradeLogger?.db;
        if (!_db) return;
        const maRows = _db.prepare('SELECT price FROM price_samples WHERE mint = ? AND ts >= ? AND ts < ? AND price > 0 ORDER BY ts')
          .all(pos.mint, maStart, now);
        if (maRows.length >= 5) {
          const ma = maRows.reduce((a, r) => a + r.price, 0) / maRows.length;
          const breakLine = ma * (1 - trendStopBreakPct / 100);
          if (price < breakLine) {
            const breakPctFromMa = ((price - ma) / ma * 100).toFixed(1);
            console.warn(
              '[PositionManager] TREND_STOP ' + (pos.symbol || pos.mint.slice(0, 6)) +
                ' pnl=' + pnlPct.toFixed(2) + '% MA=' + ma.toExponential(3) +
                ' price=' + price.toExponential(3) + ' break=' + breakPctFromMa + '% belowMA' +
                ' trailingArmed=false age=' + (posAge/1000).toFixed(0) + 's samples=' + maRows.length,
            );
            monitor.inc('PositionManager.trendStop', 1, 'PositionManager');
            this._exit(pos, price, 'TREND_STOP');
            return;
          }
        }
      }
    }

    // ============ 1.8 v3.24: TIMED_TAKE_PROFIT (阶梯定时止盈) ============
    // 在特定持仓时间窗口内，如果PnL达到阈值就止盈
    // 目的：捕获5-10%的"脉冲后回撤"行情，防止涨了不到10%就跌回来
    // 不在窗口内或涨幅超过10%的交给trailing stop处理
    {
      const timedTpEnabled = process.env.TIMED_TP_ENABLED === '1';
      if (timedTpEnabled && !pos.trailingArmed) {
        // 阶梯阈值：越晚持仓时间，阈值越高（给拉盘更多空间触发trailing）
        const windows = [
          { startMs: 180000, endMs: 300000, pnlPct: 5 },   // 3-5min: 5%
          { startMs: 480000, endMs: 600000, pnlPct: 7 },   // 8-10min: 7%
          { startMs: 1080000, endMs: 1200000, pnlPct: 8 }, // 18-20min: 8%
          { startMs: 1680000, endMs: 1800000, pnlPct: 8 }, // 28-30min: 8%
        ];
        for (const w of windows) {
          if (posAge >= w.startMs && posAge <= w.endMs && pnlPct >= w.pnlPct) {
            console.warn(
              '[PositionManager] TIMED_TP ' + (pos.symbol || pos.mint.slice(0, 6)) +
                ' pnl=' + pnlPct.toFixed(2) + '% age=' + (posAge/1000).toFixed(0) + 's' +
                ' window=' + (w.startMs/60000).toFixed(0) + '-' + (w.endMs/60000).toFixed(0) + 'min' +
                ' threshold=' + w.pnlPct + '% trailingArmed=false',
            );
            monitor.inc('PositionManager.timedTp', 1, 'PositionManager');
            this._exit(pos, price, 'TIMED_TP');
            return;
          }
        }
      }
    }

    // ============ 2. 始终更新 HWM 和 trailing 状态 ============
    //    v3.17.27: HWM 2-tick 确认机制
    //    根因(PP420): 竞争对手大单买入推高池子 mid price → _pollPoolPrices
    //    读到虚假高价 → 单 tick 就更新 HWM → trailing armed → 价格回落 → 亏损卖出。
    //    修复：新 HWM 需要连续 2 个 tick 确认才能生效，单 tick spike 自动丢弃。
    //    pendingHwm: 待确认的新高价格
    //    pendingHwmTicks: 已连续 >= pendingHwm 的 tick 数
    if (price > pos.highWaterMark) {
      // v3.17.28: HWM 2-tick 确认 — 保留最高 pending，不因次高价覆盖
      //   旧 bug：price > HWM 但 < pendingHwm 时重置 pending 为更低值，
      //   导致真实峰值（如 +10.71%）只出现 1 tick 就被次高价覆盖，
      //   最终确认的 HWM 只有 +7.80%，差 0.2% 没到 trailing 激活线 → 死扛亏损。
      //   修复：pendingHwm 只升不降。price >= pendingHwm 时刷新 pending + 累计 ticks；
      //   price 在 HWM~pendingHwm 之间时只累计 tick（视为价格在高位区间延续），
      //   不覆盖 pendingHwm，也不重置 ticks。
      if (!pos._pendingHwm || price >= pos._pendingHwm) {
        // 新的高点 → 刷新 pending + 累计 ticks
        pos._pendingHwm = price;
        pos._pendingHwmTicks = (pos._pendingHwmTicks || 0) + 1;
      } else {
        // price > HWM 但 < pendingHwm → 高位延续，只累计 tick 不覆盖 pending
        pos._pendingHwmTicks = (pos._pendingHwmTicks || 0) + 1;
      }
      if (pos._pendingHwmTicks >= 2) {
        // 连续 2 个 tick 确认 → 正式更新 HWM
        pos.highWaterMark = pos._pendingHwm;
        pos.highWaterMarkTs = Date.now();
        pos._pendingHwm = null;
        pos._pendingHwmTicks = 0;
      }
    } else {
      // price <= HWM → 清除 pending（高点没延续）
      if (pos._pendingHwm) {
        pos._pendingHwm = null;
        pos._pendingHwmTicks = 0;
      }
    }
    // v3.17.27: 持久化 peak_price — 节流5秒，不管是否创新高都写
    //   之前只在 price > HWM 时写，导致没涨过的仓 peak_price 永远 null → 重启 HWM 丢失
    //   现在每次 _checkExit 都检查是否该 flush，确保 peak_price 不落后太多
    if (!pos._lastPeakFlush || Date.now() - pos._lastPeakFlush > 5000) {
      const peakPnlPct = ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100;
      try {
        this.tradeLogger.stmts.updatePeak.run({
          positionId: pos.positionId,
          peakPrice: pos.highWaterMark,
          peakTs: pos.highWaterMarkTs || Date.now(),
          peakPnlPct,
        });
        pos._lastPeakFlush = Date.now();
      } catch (_) { /* best effort */ }
    }

    // v3.17.20 (MILHOUSE bug fix):
    //   旧逻辑 peakPnlPct 用 baselinePrice（稳定期价格，通常 < entryPrice）算，
    //   导致"相对 baseline 涨 8%"就武装 trailing — 但相对真实买入价 entryPrice
    //   可能还在亏损。实战 MILHOUSE：entryPrice=2.777e-6，baseline≈2.5e-6，
    //   价格涨到 2.7e-6 → 相对 baseline +8% 武装 trailing，但相对 entryPrice 仍 -2.8%，
    //   再回撤 3% → TRAILING_STOP 在 -10.12% 亏损卖出。
    //   25% 的移动止盈(11/44)都是这个 bug 造成的亏损卖出。
    //   修复：trailing 武装 + TP 一律以 entryPrice 为基准（真实买入价），
    //         只有真正赚到 TRAILING_ACTIVATE_PCT% 才武装，不会再"亏着卖但写移动止盈"。
    const peakPnlPct = ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100;

    const trailingActivatePct = this.getTrailingActivatePct(preVol5m, pos.mint);
    const trailingDrawdownPct = this.getTrailingDrawdownPct(peakPnlPct, preVol5m, pos.mint);
    const trailingMinHwmAgeMs = config.strategy.trailingMinHwmAgeMs;
    const tpPct = this.getTakeProfitPct(preVol5m, pos.mint);

    // trailing armed 状态始终更新（基于 entryPrice 的 peakPnl）
    // v3.17.28: armed 时锁定 _armedHwm，drawdown 从锁定值算
    //   旧 bug：trailing armed 后 HWM 仍被短暂反弹刷新 → hwmAge 重置 →
    //   trailing 止盈永远等不到 minHwmAge → 从盈利拖到亏损才触发。
    //   修复：armed 瞬间锁定当前 HWM 为 _armedHwm，后续 HWM 正常更新
    //   （捕捉更高峰值），但 drawdown 始终从 _armedHwm 算，不受后续刷新影响。
    if (trailingActivatePct > 0 && trailingDrawdownPct > 0) {
      if (!pos.trailingArmed && peakPnlPct >= trailingActivatePct) {
        pos.trailingArmed = true;
        pos._armedHwm = pos.highWaterMark;
        pos._armedHwmTs = pos.highWaterMarkTs || Date.now();
        console.log(
          `[PositionManager] 🎯 TRAILING_ARMED ${pos.symbol || pos.mint.slice(0, 6)} ` +
            `peakPnl=${peakPnlPct.toFixed(2)}% (vs entryPrice), ` +
            `armedHwm=${pos._armedHwm.toExponential(4)}, currentHwm=${pos.highWaterMark.toExponential(4)}`,
        );
        monitor.inc('PositionManager.trailingArmed', 1, 'PositionManager');
      } else if (pos.trailingArmed && pos.highWaterMark > (pos._armedHwm || 0)) {
        // HWM 涨超 armedHwm → 更新锁定值（捕捉更高峰值，这对 trailing 是好事）
        pos._armedHwm = pos.highWaterMark;
        // v3.17.35: 不再重置 armedHwmTs！bug根因：HWM反复触摸高点 → armedHwmTs反复重置 → hwmAge永远<5s → trailing卖不出去
        // pos._armedHwmTs = pos.highWaterMarkTs || Date.now();  // 已删除
      }
    }

    // ============ 3. 评估退出条件 ============
    //    v3.17.20 策略改造（用户需求）：
    //      优先级：TP(固定止盈) > trailing(移动止盈)
    //      - 8% 激活 trailing
    //      - 涨到 10%(TAKE_PROFIT_PCT) → 立即 TP 卖出，不等双确认
    //      - 涨到 8%-10% 之间回撤 3% → trailing 卖出
    //    所以先检查 TP，再检查 trailing，两者不冲突。

    // v3.17.40: 加仓策略改为每仓独立卖出，不再做合计盈亏判断

    // 3a. 固定止盈：到 TAKE_PROFIT_PCT 立即卖
    //   v3.17.40: 加价格确认 — 如果 pnlPct 单 tick 暴涨超过 TP 阈值 15% 以上，
    //   可能是价格污染/假信号，需要下一个 tick 确认
    if (tpPct > 0 && pnlPct >= tpPct) {
      const prevPnl = pos._prevTickPnl ?? 0;
      const pnlJump = pnlPct - prevPnl;
      // 如果 pnl 单 tick 跳了 > 15%（可能是虚假高价），等下一个 tick 确认
      if (pnlJump > 15 && !pos._tpConfirmPending) {
        pos._tpConfirmPending = true;
        console.warn(
          `[PositionManager] ⚠️ TP pending confirmation ${pos.symbol || pos.mint.slice(0, 6)}: ` +
          `pnl=${pnlPct.toFixed(2)}% >= ${tpPct}%, but jump=${pnlJump.toFixed(2)}% from prev ${prevPnl.toFixed(2)}% — waiting next tick`,
        );
      } else {
        // 确认通过（正常到达或已等了一个 tick）
        console.log(
          `[PositionManager] ✅ TAKE_PROFIT ${pos.symbol || pos.mint.slice(0, 6)} ` +
            `pnl=${pnlPct.toFixed(2)}% >= ${tpPct}% (jump=${pnlJump.toFixed(2)}%${pos._tpConfirmPending ? ', confirmed' : ''})`,
        );
        this._exit(pos, price, 'TAKE_PROFIT');
        return;
      }
    } else {
      // pnlPct 不满足 TP 时清除 pending 状态
      pos._tpConfirmPending = false;
    }
    // 记录当前 tick 的 pnlPct 供下次比较
    pos._prevTickPnl = pnlPct;

    // 3b. 移动止盈：armed 后从 _armedHwm 回撤 TRAILING_DRAWDOWN_PCT% 卖出
    //   v3.17.28: drawdown 从 _armedHwm 算（不受后续 HWM 刷新影响），
    //   hwmAge 也从 _armedHwmTs 算（不受后续 HWM 时间戳重置影响）。
    //   旧 bug：下跌途中短暂 2-tick 反弹刷新 HWM → hwmAge 归零 → trailing 永远等不到
    if (trailingActivatePct > 0 && trailingDrawdownPct > 0 && pos.trailingArmed) {
      const armedHwm = pos._armedHwm || pos.highWaterMark;
      const armedHwmTs = pos._armedHwmTs || pos.highWaterMarkTs || Date.now();
      const drawdownPct = ((armedHwm - price) / armedHwm) * 100;
      const dynamicDrawdown = this.getTrailingDrawdownPct(peakPnlPct, preVol5m, pos.mint);
      const hwmAge = Date.now() - armedHwmTs;
      if (drawdownPct >= dynamicDrawdown && hwmAge >= trailingMinHwmAgeMs) {
        console.log(
          `[PositionManager] 📉 TRAILING_STOP ${pos.symbol || pos.mint.slice(0, 6)} ` +
            `peakPnl=${peakPnlPct.toFixed(2)}% → currentPnl=${pnlPct.toFixed(2)}% ` +
            `(drawdown ${drawdownPct.toFixed(2)}% from armedHwm, hwmAge=${hwmAge}ms)`,
        );
        this._exit(pos, price, 'TRAILING_STOP');
        return;
      }
    }
// ============ 3c. v3.17.33: 防御模式 — 利润激活的移动止损 ============
    //
    //   PnL >= defenseProfitActivatePct(3%) 时激活防御trailing
    //   DEFENSE_STOP_LOSS 已禁用 (defenseStopLossPct=0)
    //   激活后从高点回撤 defenseTrailingDrawdownPct(3%) 卖出
    //
    //   优先级: TP(20%) > 原trailing(8%/3%) > 防御trailing(3%激活/3%回撤) > MAX_HOLD
    //
    const defenseProfitActivatePct = config.strategy.defenseProfitActivatePct || 0;
    const defenseStopLossPct = config.strategy.defenseStopLossPct;
    const defenseDrawdownPct = config.strategy.defenseTrailingDrawdownPct;
    const defenseActivateMs = config.strategy.defenseActivateMs || 0;

    if (defenseDrawdownPct > 0) {
      // 3c-1. 防御止损: 已禁用 (defenseStopLossPct=0)
      if (defenseStopLossPct < 0 && pnlPct <= defenseStopLossPct) {
        console.log(
          `[PositionManager] 🛡️ DEFENSE_STOP_LOSS ${pos.symbol || pos.mint.slice(0, 6)} ` +
            `pnl=${pnlPct.toFixed(2)}% <= ${defenseStopLossPct}%`,
        );
        this._exit(pos, price, 'DEFENSE_STOP_LOSS');
        return;
      }

      // 3c-2. 防御 trailing: 持仓满 defenseActivateMs(20min) 后，PnL >= defenseProfitActivatePct(3%) 时激活
      //   v3.17.35: 必须满20分钟才激活防御，20分钟前只走原trailing(8%/3%)
      if (!pos._defenseArmed) {
        const posAgeForDefense = Date.now() - (pos.openedAt || pos.ts);
        if (posAgeForDefense >= defenseActivateMs && defenseProfitActivatePct > 0 && pnlPct >= defenseProfitActivatePct) {
          pos._defenseArmed = true;
          pos._defenseHwm = price;
          pos._defenseHwmTs = Date.now();
          const posAgeMs = Date.now() - (pos.openedAt || pos.ts);
          console.log(
            `[PositionManager] 🛡️ DEFENSE_ARMED ${pos.symbol || pos.mint.slice(0, 6)} ` +
              `pnl=${pnlPct.toFixed(2)}% >= ${defenseProfitActivatePct}%, defenseHwm=${price.toExponential(4)} (held ${(posAgeMs/60000).toFixed(1)}min)`,
          );
          monitor.inc('PositionManager.defenseArmed', 1, 'PositionManager');
        }
      } else {
        if (price > pos._defenseHwm) {
          pos._defenseHwm = price;
          pos._defenseHwmTs = Date.now();
        }
        const defenseDrawdown = ((pos._defenseHwm - price) / pos._defenseHwm) * 100;
        const defenseHwmAge = Date.now() - pos._defenseHwmTs;
        const posAgeMs = Date.now() - (pos.openedAt || pos.ts);
        if (defenseDrawdown >= defenseDrawdownPct && defenseHwmAge >= 2000) {
          console.log(
            `[PositionManager] 🛡️ DEFENSE_TRAILING_STOP ${pos.symbol || pos.mint.slice(0, 6)} ` +
              `pnl=${pnlPct.toFixed(2)}%, drawdown=${defenseDrawdown.toFixed(2)}% from defenseHwm ` +
              `(held ${(posAgeMs/60000).toFixed(1)}min)`,
          );
          this._exit(pos, price, 'DEFENSE_TRAILING_STOP');
          return;
        }
      }

      // 3c-3. v3.17.35: 防御到期 — 持仓满 defenseActivateMs(20min) 时，盈利>0 立即卖出
      //   只在满20分钟那一刻触发一次（_defenseTimeoutChecked 标记防止重复）
      //   盈利≤0 则不卖，继续走防御trailing或其他策略
      if (defenseActivateMs > 0 && !pos._defenseTimeoutChecked) {
        const posAgeMs = Date.now() - (pos.openedAt || pos.ts);
        if (posAgeMs >= defenseActivateMs) {
          pos._defenseTimeoutChecked = true;
          if (pnlPct > 0) {
            console.log(
              `[PositionManager] 🛡️ DEFENSE_TIMEOUT_PROFIT ${pos.symbol || pos.mint.slice(0, 6)} ` +
                `pnl=${pnlPct.toFixed(2)}% > 0 at ${(posAgeMs/60000).toFixed(1)}min → immediate sell`,
            );
            this._exit(pos, price, 'DEFENSE_TIMEOUT_PROFIT');
            return;
          } else {
            console.log(
              `[PositionManager] 🛡️ DEFENSE_TIMEOUT_SKIP ${pos.symbol || pos.mint.slice(0, 6)} ` +
                `pnl=${pnlPct.toFixed(2)}% ≤ 0 at ${(posAgeMs/60000).toFixed(1)}min → continue defense trailing`,
            );
          }
        }
      }
    }

  }


  /**
   * Exit a position — sell tokens and close.
   */
  async _exit(pos, exitPrice, reason) {
    if (pos.exiting) return;
    pos.exiting = true;
    pos.exitReason = reason;

    // v3.17.41: same-mint daily blacklist on bad exits
    // LOW_PEAK / EARLY_LOW_PEAK / EMERGENCY_STOP = blacklisted 24h
    const blacklistReasons = ['LOW_PEAK_TIMEOUT', 'EARLY_LOW_PEAK_CUT', 'EMERGENCY_STOP', 'RANGE_STOP'];
    if (blacklistReasons.includes(reason) && this.signalEngine) {
      const blacklistMs = 24 * 3600 * 1000; // 24h
      // v3.18: blacklist disabled — skip blacklistMint call
      console.log(
        `[PositionManager] 🔒 BLACKLIST ${pos.symbol || pos.mint.slice(0, 6)} for 24h (reason=${reason})`,
      );
    }

    // v3.24: set rebuy cooldown on this mint after any exit
    // v3.26: SMART_STOP 退出 → 24h 冷却，防止同币反复止损
    // v3.27: 老币(>=24h) SMART_STOP → 从监控列表移除24h，节省资源
    if (this.signalEngine && this.signalEngine._exitCooldowns) {
      const isSmartStop = reason === 'SMART_STOP';
      const isTimeout = reason.startsWith('TIMEOUT');
      let rebuyCooldownMs;
      if (isSmartStop) {
        rebuyCooldownMs = parseInt(process.env.SMART_STOP_REBUY_COOLDOWN_MS || '86400000', 10); // 24h
      } else if (isTimeout) {
        rebuyCooldownMs = parseInt(process.env.TIMEOUT_REBUY_COOLDOWN_MS || '86400000', 10); // 24h
      } else {
        rebuyCooldownMs = parseInt(process.env.REBUY_COOLDOWN_MS || '0', 10);
      }
      if (rebuyCooldownMs > 0) {
        this.signalEngine._exitCooldowns.set(pos.mint, Date.now() + rebuyCooldownMs);
      }
      if (isSmartStop) {
        console.log(
          `[PositionManager] 🔒 SMART_STOP cooldown ${pos.symbol || pos.mint.slice(0, 6)} for ${Math.round(rebuyCooldownMs / 3600000)}h (no rebuy until cooldown expires)`,
        );
        // v3.27: 老币SMART_STOP后从监控列表移除24h
        const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '86400000');
        const tokenInfo = this.tokenRegistry?.getToken(pos.mint);
        if (tokenInfo && tokenInfo.added_at) {
          const tokenAgeMs = Date.now() - tokenInfo.added_at;
          if (tokenAgeMs >= newCoinThresholdMs) {
            // 老币: 移除监控，24h后Watchdog不会再检查回来(因为已被移除)
            // 设置定时器24h后恢复
            const oldCoinSmartStopRemoveMs = parseInt(process.env.OLD_COIN_SMART_STOP_REMOVE_MS || '86400000');
            this.tokenRegistry.removeToken(pos.mint);
            console.log(
              `[PositionManager] 🔒 OLD_COIN SMART_STOP remove from watchlist: ${pos.symbol || pos.mint.slice(0, 6)} for ${Math.round(oldCoinSmartStopRemoveMs / 3600000)}h`,
            );
            // 24h后恢复监控
            setTimeout(() => {
              this.tokenRegistry.addToken(pos.mint, {
                symbol: pos.symbol,
                pool_address: tokenInfo.pool_address,
                pool_base_vault: tokenInfo.pool_base_vault,
                pool_quote_vault: tokenInfo.pool_quote_vault,
                source: 'smart_stop_restore',
              }).catch(err => {
                console.log(`[PositionManager] ⚠️ SMART_STOP restore failed for ${pos.symbol}: ${err.message}`);
              });
              console.log(
                `[PositionManager] 🔓 OLD_COIN SMART_STOP restore to watchlist: ${pos.symbol || pos.mint.slice(0, 6)}`,
              );
            }, oldCoinSmartStopRemoveMs);
          }
        }
      }
    }

    monitor.inc(`PositionManager.exitsBy_${reason}`, 1, 'PositionManager');

    console.log(
      `[PositionManager] 📉 EXIT ${pos.symbol || pos.mint.slice(0, 6)} reason=${reason} ` +
        `triggerPnl=${(((exitPrice - pos.entryPrice) / pos.entryPrice) * 100).toFixed(2)}%`,
    );

    // v3.17.13: 同币卖出排队 — 防止多仓并发卖出导致滑点不够全部失败
    const mint = pos.mint;
    if (!this._sellQueues.has(mint)) {
      this._sellQueues.set(mint, []);
    }
    this._sellQueues.get(mint).push({ pos, exitPrice });
    this._processSellQueue(mint);
  }

  /**
   * v3.17.13: 处理同币卖出队列 — 串行卖出，上一笔完成后再卖下一笔
   */
  _processSellQueue(mint) {
    if (this._sellInProgress.has(mint)) return; // 上一笔还在卖
    const queue = this._sellQueues.get(mint);
    if (!queue || queue.length === 0) return;

    const { pos, exitPrice } = queue.shift();
    if (queue.length === 0) this._sellQueues.delete(mint);

    this._sellInProgress.add(mint);
    this._attemptSell(pos, exitPrice).finally(() => {
      this._sellInProgress.delete(mint);
      // 检查是否还有排队
      this._processSellQueue(mint);
    });
  }

  async _attemptSell(pos, triggerPrice) {
    const tokenInfo = this.tokenRegistry.getToken(pos.mint);

    // v3.17.40: 卖出时用最新实时价格，不用触发时的价格
    //   触发到执行之间价格可能已暴跌，用旧价格会导致 slippage 计算不准
    const latestPrice = this.priceTracker.getPrice(pos.mint) || triggerPrice;
    if (latestPrice !== triggerPrice) {
      const latestPnl = ((latestPrice - pos.entryPrice) / pos.entryPrice) * 100;
      const triggerPnl = ((triggerPrice - pos.entryPrice) / pos.entryPrice) * 100;
      console.log(
        `[PositionManager] 📊 sell price update ${pos.symbol || pos.mint.slice(0, 6)}: ` +
        `triggerPnl=${triggerPnl.toFixed(2)}% → latestPnl=${latestPnl.toFixed(2)}% ` +
        `(trigger=${triggerPrice.toExponential(4)} → latest=${latestPrice.toExponential(4)})`,
      );
    }

    let sellResult;
    try {
      sellResult = await this.executor.sell({
        mint: pos.mint,
        symbol: pos.symbol,
        poolAddress: tokenInfo?.pool_address,
        poolBaseVault: tokenInfo?.pool_base_vault,
        poolQuoteVault: tokenInfo?.pool_quote_vault,
        tokenAmount: pos.tokenAmount,
        baseDecimals: tokenInfo?.decimals ?? 6,
        currentPrice: latestPrice,
      });
    } catch (err) {
      monitor.recordError('PositionManager', err, {
        phase: 'sell_throw',
        mint: pos.mint,
        symbol: pos.symbol,
      });
      sellResult = { success: false, error: err.message, latencyMs: 0 };
    }

    pos.sellAttempts = (pos.sellAttempts || 0) + 1;

    const realSolOut = sellResult.solOut ?? null;
    const realExitPrice = sellResult.price ?? triggerPrice;
    // v3.17.40c: 实际卖出的代币数（链上余额可能 < pos.tokenAmount）
    const actualSellAmount = sellResult.sellAmount ?? pos.tokenAmount;

    // v3.4: 累加每次 sell 尝试的 priority fee（含失败的，因为失败也消耗了 fee）
    if (sellResult.priorityFeeLamports) {
      pos.sellFeeLamports = (pos.sellFeeLamports || 0) + sellResult.priorityFeeLamports;
    }

    // 记录 trade 提交事件（成功/失败都记）
    this.tradeLogger.logTrade({
      positionId: pos.positionId,
      ts: Date.now(),
      mint: pos.mint,
      symbol: pos.symbol,
      side: 'SELL',
      solAmount: realSolOut,
      tokenAmount: pos.tokenAmount,
      price: realExitPrice,
      signature: sellResult.signature,
      success: sellResult.success,
      dryRun: pos.dryRun,
      reason: pos.exitReason + (pos.sellAttempts > 1 ? `_retry_${pos.sellAttempts}` : ''),
      latencyMs: sellResult.latencyMs,
      error: sellResult.error,
    });

    // ============ 分支 A：提交本身失败（拿不到 signature） ============
    if (!sellResult.success) {
      monitor.inc('PositionManager.sellSubmitFail', 1, 'PositionManager');
      this.tradeLogger.recordSellAttempt(pos.positionId, sellResult.error);

      if (pos.dryRun) {
        monitor.recordError('PositionManager', new Error('DRY_RUN sell unexpectedly failed'), {
          mint: pos.mint,
          symbol: pos.symbol,
          error: sellResult.error,
        });
        console.error(
          `[PositionManager] DRY_RUN sell unexpectedly failed for ${pos.mint}; abandoning`,
        );
        this.tradeLogger.closePosition(pos.positionId, {
          closedAt: Date.now(),
          exitPrice: triggerPrice,
          exitSol: 0,
          pnlSol: -pos.entrySol,
          pnlPct: -100,
          exitReason: pos.exitReason + '_FAILED',
          sellSignature: null,
        });
        this.positions.delete(pos.positionId);
        this._removeByMint(pos.mint, pos.positionId);
        // v3.17.21: SELL 失败强关 → 从 hotMints 移除
        if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(pos.mint);
        monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
        return;
      }

      this._scheduleRetryOrStuck(pos, triggerPrice, sellResult.error);
      return;
    }

    // ============ 分支 B：提交成功，但还需等链上确认 ============
    // v3.17.40: 如果卖出 SOL ≈ 0，说明代币已被其他仓位卖光，直接关闭
    if (realSolOut !== null && realSolOut < 0.0001) {
      monitor.inc('PositionManager.sellAbandoned_zeroOut', 1, 'PositionManager');
      console.warn(
        `[PositionManager] 🚫 SELL zero-out ${pos.symbol || pos.mint.slice(0, 6)}: ` +
        `solOut=${realSolOut?.toExponential(3)} — token already sold, force closing`,
      );
      this.tradeLogger.closePosition(pos.positionId, {
        closedAt: Date.now(),
        exitPrice: realExitPrice,
        exitSol: 0,
        pnlSol: -pos.entrySol,
        pnlPct: -100,
        exitReason: pos.exitReason + '_ZERO_OUT',
        sellSignature: sellResult.signature,
      });
      this.positions.delete(pos.positionId);
      this._removeByMint(pos.mint, pos.positionId);
      if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(pos.mint);
      monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
      return;
    }

    // 此时 ⚠️ 不能立即 closePosition！tx 可能在 mempool 被丢、滑点超限被 reject
    // 标记 sell_confirming 状态，启动后台确认
    this.tradeLogger.markSellPending(pos.positionId, sellResult.signature, pos.exitReason);
    pos._lastSellSignature = sellResult.signature;

    if (pos.dryRun) {
      // DRY_RUN 直接当成功
      this._finalizeSuccess(pos, realExitPrice, realSolOut, sellResult.signature, actualSellAmount);
      return;
    }

    // 异步确认（不 await，避免阻塞下一笔操作；失败会自己触发 retry）
    this._confirmSellAsync(pos, sellResult.signature, realExitPrice, realSolOut, triggerPrice, actualSellAmount).catch(
      (err) => {
        monitor.recordError('PositionManager', err, {
          phase: 'confirm_async_crash',
          mint: pos.mint,
          signature: sellResult.signature,
        });
      },
    );
  }

  /**
   * 异步等待 sell tx 落链确认。
   * 三种结果：
   *   1. 链上确认无 err  → fetchTxSwapResult 拉真实 solOut → finalizeSuccess
   *   2. 链上 tx 报错      → scheduleRetry
   *   3. 超时未找到 tx    → scheduleRetry（mempool 丢弃）
   *
   * v3.17.6 修复：SELL 也用链上真实值替代 SDK 估算
   *   - 之前 exitPrice/solOut 是 _attemptSell 里 SDK 报价的 expectedSolOut
   *   - SDK 估算可能偏低 3-10%（不含 priority fee 扣减、池子状态略滞后）
   *   - 实测：DB 记录 -0.012 SOL 亏损，链上真实 +0.091 SOL 盈利
   *   - 修复：落链确认后，调 fetchTxSwapResult 拿真实 realSolDelta，覆盖 SDK 估算
   */
  async _confirmSellAsync(pos, signature, exitPrice, solOut, triggerPrice, actualSellAmount) {
    const result = await this.executor.confirmTx(signature, { timeoutMs: 15_000 });

    if (!this.positions.has(pos.positionId)) return; // 期间被其他流程关掉

    if (result.confirmed) {
      monitor.inc('PositionManager.sellConfirmed', 1, 'PositionManager');

      // v3.17.6: 拉链上真实 SOL 增量
      let realExitPrice = exitPrice;
      let realSolOut = solOut;
      try {
        const swap = await this.executor.fetchTxSwapResult(signature, pos.mint);
        // SELL 的 realSolDelta 是正数（钱包 SOL 增加）
        if (swap && swap.realSolDelta > 0 && pos.tokenAmount > 0) {
          realSolOut = swap.realSolDelta;
          // v3.17.40c: 用实际卖出的代币数算价格（链上余额可能 < pos.tokenAmount）
          const sellAmt = actualSellAmount || pos.tokenAmount;
          realExitPrice = realSolOut / sellAmt;
          monitor.inc('PositionManager.sellReconciled', 1, 'PositionManager');
          const drift = solOut ? ((realSolOut - solOut) / solOut) * 100 : 0;
          console.log(
            `[PositionManager] 🔧 SELL reconciled ${pos.symbol || pos.mint.slice(0, 6)}: ` +
              `SDK est ${(solOut ?? 0).toFixed(4)} → real ${realSolOut.toFixed(4)} SOL (${drift.toFixed(2)}%)`,
          );
        } else {
          // fetchTxSwapResult 失败：保留 SDK 估算（旧行为）
          monitor.inc('PositionManager.sellReconcileFallback', 1, 'PositionManager');
          console.warn(
            `[PositionManager] SELL reconcile fallback to SDK estimate: ${pos.symbol || pos.mint.slice(0, 6)} ` +
              `sig=${signature.slice(0, 8)}.. (fetch returned no realSolDelta)`,
          );
        }
      } catch (err) {
        monitor.recordError('PositionManager', err, {
          phase: 'sell_reconcile_fetch',
          mint: pos.mint,
          signature,
        });
        // 异常时也 fallback 到 SDK 估算
      }

      this._finalizeSuccess(pos, realExitPrice, realSolOut, signature, actualSellAmount);
      return;
    }

    monitor.inc('PositionManager.sellNotLanded', 1, 'PositionManager');
    const errMsg = `tx ${signature.slice(0, 8)}.. ${result.error || 'not_landed'}`;
    console.warn(
      `[PositionManager] SELL submitted but not confirmed: ${pos.symbol || pos.mint.slice(0, 6)}: ${errMsg}`,
    );

    // v3.17.8: 双保险 — confirmTx 超时(15s)不等于交易失败
    //   实战发现:10 笔 stuck position 链上 SELL 其实都成功了,只是 confirmTx 没等到
    //   原因:网络抖动 / RPC subscribeSignature 错过通知 / tx 实际在 18-30s 后才确认
    //   修复:confirm 失败后再直接拉一次链上 tx,如果 fetchTxSwapResult 成功 → 走 success 路径
    //   不能完全依赖这条路径 — 它可能也失败(tx 真的没落链),所以失败时仍走 retry
    try {
      const swap = await this.executor.fetchTxSwapResult(signature, pos.mint);
      if (swap && swap.realSolDelta > 0 && pos.tokenAmount > 0) {
        const realSolOut = swap.realSolDelta;
        // v3.17.40c: 用 actualSellAmount 算正确的 exitPrice
        const sellAmt5b = actualSellAmount || pos.tokenAmount;
        const realExitPrice = realSolOut / sellAmt5b;
        monitor.inc('PositionManager.sellRecoveredFromTimeout', 1, 'PositionManager');
        console.log(
          `[PositionManager] ✅ SELL actually landed (recovered from confirm timeout) ` +
            `${pos.symbol || pos.mint.slice(0, 6)}: realSol=${realSolOut.toFixed(4)}`,
        );
        this._finalizeSuccess(pos, realExitPrice, realSolOut, signature, actualSellAmount);
        return;
      }
    } catch (err) {
      // fetchTxSwapResult 也失败 → 真的没落链或链上 tx 失败,继续 retry 流程
      monitor.recordError('PositionManager', err, {
        phase: 'sell_recovery_fetch',
        mint: pos.mint,
        signature,
      });
    }

    this._scheduleRetryOrStuck(pos, triggerPrice, errMsg);
  }

  _finalizeSuccess(pos, exitPrice, solOut, signature, actualSellAmount) {
    // v3.17.40c: 用实际卖出的代币数算 exitSol
    // 如果链上余额不足（其他仓位先卖了），actualSellAmount < pos.tokenAmount
    const sellAmt = actualSellAmount != null ? actualSellAmount : pos.tokenAmount;
    const exitSol = solOut ?? sellAmt * exitPrice;

    // v3.17.14: PnL 计算修复
    //   - entrySol: BUY reconcile 后的真实 SOL 出账（已含 buy priority fee + base fee）
    //   - exitSol: SELL reconcile 后的真实 SOL 入账（已含 sell priority fee + base fee）
    //   - 两者都是钱包净变化，所以 PnL = exitSol - entrySol，不需要再扣 fee
    //   - 如果 exitSol 还是 SDK 估算值（reconcile 失败），也直接用，因为 SDK 估算不含 fee
    //     导致 PnL 偏高一点，但比双重扣 fee 准确
    const grossPnl = exitSol - pos.entrySol;
    const pnlSol = grossPnl;
    const pnlPct = (pnlSol / pos.entrySol) * 100;
    const feeSol = ((pos.buyFeeLamports || 0) + (pos.sellFeeLamports || 0)) / 1e9;

    // v3.17.15: 校正 exitReason — 如果实际亏损但 reason 是 TAKE_PROFIT，修正为 TAKE_PROFIT_LOSS
    //   防止价格污染触发假止盈，实际卖出亏损但记录显示"止盈"的误导
    let finalReason = pos.exitReason;
    if (pnlSol < 0 && finalReason === 'TAKE_PROFIT') {
      finalReason = finalReason + '_LOSS';
      console.log(
        `[PositionManager] ⚠️ exitReason corrected: ${pos.exitReason} → ${finalReason} ` +
        `(trigger said profit but actual PnL=${pnlSol.toFixed(4)} SOL / ${pnlPct.toFixed(2)}%)`,
      );
    }

    // v3.17.21: 记录持仓期间的峰值数据（highWaterMark 已在 _checkExit 中实时维护）
    const peakPnlPct = pos.entryPrice > 0
      ? ((pos.highWaterMark - pos.entryPrice) / pos.entryPrice) * 100
      : null;
    const peakPrice = pos.highWaterMark ?? null;
    const peakTs = pos.highWaterMarkTs ?? null;
    const timeToPeakMs = (pos.highWaterMarkTs && pos.openedAt)
      ? pos.highWaterMarkTs - pos.openedAt
      : null;

    this.tradeLogger.closePosition(pos.positionId, {
      closedAt: Date.now(),
      exitPrice,
      exitSol,
      pnlSol,
      pnlPct,
      exitReason: finalReason,
      sellSignature: signature,
      peakPnlPct,
      peakPrice,
      peakTs,
      timeToPeakMs,
      priceTickCount: pos.tickCount || 0,   // v3.17.21: 持仓期间 price tick 数
    });

    // v3.17.31: 启动平仓后 5 分钟价格追踪(旁路,不影响主路径)
    if (this.postExitTracker && exitPrice > 0) {
      try {
        this.postExitTracker.startTracking(
          pos.positionId,
          pos.mint,
          exitPrice,
          Date.now(),
        );
      } catch (err) {
        // 即使 tracker 挂了也不能影响平仓流程
        console.warn(`[PositionManager] postExitTracker.startTracking failed: ${err.message}`);
      }
    }

    this.positions.delete(pos.positionId);
    this._removeByMint(pos.mint, pos.positionId);

    // v3.30: 添加到 _recentlyClosed 缓存（cooldown 用）
    this._recentlyClosed.push({ mint: pos.mint, closed_at: Date.now() });
    // 只保留最近 100 条，避免内存泄漏
    if (this._recentlyClosed.length > 100) {
      this._recentlyClosed = this._recentlyClosed.slice(-50);
    }

    // v3.26: 平仓后取消持仓代币标记 — PriceTracker 恢复严格跳变过滤
    if (!this.hasOpenPosition(pos.mint) && this.priceTracker) {
      this.priceTracker.markPosition(pos.mint, false);
    }

    // v3.17.21: 平仓完成 → 从 hotMints 移除（不再高频刷此币）
    //   如果同 mint 还有其他持仓，不急移除（下次 _finalizeSuccess 会再检查）
    if (!this.hasOpenPosition(pos.mint) && this.executor?.poolStateCache) {
      this.executor.poolStateCache.removeHot(pos.mint);
    }

    monitor.inc('PositionManager.closed', 1, 'PositionManager');
    monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
    if (pnlSol > 0) monitor.inc('PositionManager.winners', 1, 'PositionManager');
    else monitor.inc('PositionManager.losers', 1, 'PositionManager');

    console.log(
      `[PositionManager] 🏁 CLOSED ${pos.symbol || pos.mint.slice(0, 6)} ` +
        `gross=${grossPnl.toFixed(4)} fee=${feeSol.toFixed(4)} net=${pnlSol.toFixed(4)} SOL (${pnlPct.toFixed(2)}%)`,
    );

    this.emit('closed', {
      ...pos,
      exitPrice,
      exitSol,
      pnlSol,
      pnlPct,
      exitReason: finalReason,
      grossPnlSol: grossPnl,
      feeSol,
    });

    // v3.26: 加仓和首仓独立运行，不级联卖出
    // 每个仓位独立 TP/trailing/stop，互不干扰
  }

  _scheduleRetryOrStuck(pos, triggerPrice, errMsg) {
    monitor.inc('PositionManager.sellRetries', 1, 'PositionManager');

    // v3.17.40: 如果错误是 Custom:6053 或 Custom:1 (Insufficient tokens)，说明代币已被其他仓位卖光
    //   不再 retry，直接关闭避免空转 12 次
    // v3.17.40 + hotfix: JSON error format is {"Custom":1} not Custom:1
    //   Must match both JSON-quoted "Custom":1 and plain Custom:1
    const hasTokenGone6053 = errMsg && (errMsg.includes('Custom:6053') || errMsg.includes('Custom":6053'));
    const hasTokenGone1 = errMsg && (errMsg.includes('Custom:1}') || errMsg.includes('Custom":1}'));
    if (hasTokenGone6053 || hasTokenGone1) {
      monitor.inc('PositionManager.sellAbandoned_tokenGone', 1, 'PositionManager');
      const errType = hasTokenGone6053 ? 'Custom:6053' : 'Custom:1';
      console.warn(
        `[PositionManager] 🚫 SELL abandoned ${pos.symbol || pos.mint.slice(0, 6)}: ` +
        `${errType} (token balance 0) — likely sold by another position, force closing`,
      );
      this.tradeLogger.closePosition(pos.positionId, {
        closedAt: Date.now(),
        exitPrice: triggerPrice,
        exitSol: 0,
        pnlSol: -pos.entrySol,
        pnlPct: -100,
        exitReason: pos.exitReason + '_TOKEN_GONE',
        sellSignature: pos._lastSellSignature || null,
      });
      // v3.26: TOKEN_GONE (rug) 后 24h 冷却，防止继续买入归零币
      if (this.signalEngine && this.signalEngine._exitCooldowns) {
        const rugCooldownMs = parseInt(process.env.RUG_REBUY_COOLDOWN_MS || '86400000', 10);
        this.signalEngine._exitCooldowns.set(pos.mint, Date.now() + rugCooldownMs);
        console.log(
          `[PositionManager] 🔒 RUG cooldown ${pos.symbol || pos.mint.slice(0, 6)} for ${Math.round(rugCooldownMs / 3600000)}h (token gone, no rebuy)`,
        );
      }
      this.positions.delete(pos.positionId);
      this._removeByMint(pos.mint, pos.positionId);
      if (this.executor?.poolStateCache) this.executor.poolStateCache.removeHot(pos.mint);
      monitor.set('PositionManager.openCount', this.positions.size, 'PositionManager');
      return;
    }

    // 重试上限：默认 12 次（SELL_RETRY_DELAYS_MS × 2）。超过标 stuck
    const MAX_RETRIES = SELL_RETRY_DELAYS_MS.length * 2;
    if (pos.sellAttempts >= MAX_RETRIES) {
      monitor.inc('PositionManager.sellStuck', 1, 'PositionManager');
      this.tradeLogger.markStuck(
        pos.positionId,
        `gave up after ${pos.sellAttempts} attempts: ${errMsg}`,
      );
      console.error(
        `[PositionManager] ⚠️ STUCK ${pos.symbol || pos.mint.slice(0, 6)}: ` +
          `${pos.sellAttempts} 次重试均失败 — token 留在钱包中，需人工干预`,
      );
      // 关键：保持 exiting=true 防止 tick/priceUpdate 再次触发 _exit 进入无限循环
      // 也不从 this.positions 删除：保留以便 reconciler 监控、dashboard 显示警告
      pos.exiting = true;
      pos.status = 'stuck';
      return;
    }

    const delayIdx = Math.min(pos.sellAttempts - 1, SELL_RETRY_DELAYS_MS.length - 1);
    const delay = SELL_RETRY_DELAYS_MS[delayIdx] || 30_000;
    const nextRetryAt = Date.now() + delay;

    // 持久化下次重试时间，重启后 reconciler 会按时唤醒
    this.tradeLogger.markSellFailedPendingRetry(
      pos.positionId,
      nextRetryAt,
      errMsg,
      pos.exitReason,
    );

    console.warn(
      `[PositionManager] SELL retry scheduled: ${pos.symbol || pos.mint.slice(0, 6)} ` +
        `(attempt ${pos.sellAttempts}/${MAX_RETRIES}) in ${delay}ms — ${errMsg}`,
    );

    setTimeout(() => {
      if (!this.positions.has(pos.positionId)) return;
      const latestPrice = this.priceTracker.getPrice(pos.mint) || triggerPrice;
      this._attemptSell(pos, latestPrice).catch((err) => {
        monitor.recordError('PositionManager', err, {
          phase: 'sell_retry_crash',
          mint: pos.mint,
        });
      });
    }, delay);
  }

  /**
   * v3.3 重试 reconciler
   * ====================
   * 每 5 秒扫一遍 DB，找出所有 status='sell_pending' 且 next_retry_at <= now 的 position
   * 这覆盖两种场景：
   *   1. 重启后 setTimeout 丢失 → 找回所有过期的 retry
   *   2. confirm_async 失败但 setTimeout 也未触发（edge case）
   *
   * 同时检查 sell_confirming 状态：如果最后一次提交超过 30s 还在 sell_confirming，
   * 主动调一次 confirmTx，没确认就触发重试。
   */
  /**
   * v3.4 主动轮询持仓 token 的 pool state，算出当前实时价格。
   * 修复 TIMEOUT 主导问题：微盘币 15s 内可能没有任何外部 swap → PriceTracker 永远不更新
   * → 永远不触发止盈止损 → 全部强平。
   *
   * 实现：用 Executor 的 onlineSdk 直接拉 pool state，从 reserves 算 mid price。
   * 频率：每 poolPollIntervalMs (默认 500ms)
   * 仅持仓期间轮询（持仓为空时不发 RPC）
   */
  async _pollPoolPrices() {
    if (this.positions.size === 0) return;
    if (this._polling) return; // 防止上一轮还没跑完
    this._polling = true;
    try {
      // 收集所有需要查的 (mint, poolAddress) 组合
      const queries = [];
      for (const pos of this.positions.values()) {
        if (pos.exiting) continue; // 正在卖的不需要再轮询
        const tokenInfo = this.tokenRegistry.getToken(pos.mint);
        if (!tokenInfo?.pool_address) {
          // v3.17.27: 告警——没有 pool_address 的持仓是"瞎仓"，两条价格链路都喂不了
          if (!pos._noPoolWarned) {
            console.warn(
              `[PositionManager] ⚠️ position ${pos.symbol || pos.mint.slice(0, 6)} has no pool_address, ` +
              `skipping price poll — trailing stop will NOT work for this position!`,
            );
            pos._noPoolWarned = true;
          }
          continue;
        }
        queries.push({ mint: pos.mint, poolAddress: tokenInfo.pool_address, decimals: tokenInfo.decimals ?? 6 });
      }
      if (queries.length === 0) return;

      const MAX_CACHE_AGE_MS = 1000; // 缓存超过 1 秒视为过期，fallback 到 RPC

      // 并行拉，不阻塞
      await Promise.all(
        queries.map(async (q) => {
          try {
            // v3.17.27: 优先从 PoolStateCache 读缓存（省 ~92% RPC）
            //   保护1: cache miss → fallback 到现查 RPC（保住对未进 hotMints 持仓的兜底）
            //   保护2: 缓存太旧(>1s) → fallback 到现查 RPC（避免过期数据影响 trailing）
            let price = null;
            const cache = this.executor?.poolStateCache;
            if (cache) {
              const cachedState = cache.get(q.poolAddress);
              const cacheAge = cache.getAge(q.poolAddress);
              if (cachedState && cacheAge !== null && cacheAge <= MAX_CACHE_AGE_MS) {
                price = this._priceFromState(cachedState, q.decimals);
                monitor.inc('PositionManager.poolPollCacheHit', 1, 'PositionManager');
              }
            }
            // fallback: cache miss 或缓存太旧 → 走 RPC
            if (!price) {
              price = await this._fetchPoolMidPrice(q.poolAddress, q.decimals);
              monitor.inc('PositionManager.poolPollRpcFallback', 1, 'PositionManager');
            }
            if (price && price > 0) {
              this.priceTracker.update(q.mint, price, Date.now(), q.poolAddress);
              monitor.inc('PositionManager.poolPollOk', 1, 'PositionManager');
              // 直接检查退出，不等 priceTracker 事件 — 减少延迟
              const pids = this.byMint.get(q.mint);
              if (pids) {
                for (const pid of pids) {
                  this._checkExit(pid, price);
                }
              }
            }
          } catch (err) {
            monitor.inc('PositionManager.poolPollFail', 1, 'PositionManager');
          }
        }),
      );
    } finally {
      this._polling = false;
    }
  }

  /**
   * v3.17.27: 从 PoolStateCache 的 state 算价格（纯内存，零 RPC）
   */
  _priceFromState(state, baseDecimals) {
    if (!state?.poolBaseAmount || !state?.poolQuoteAmount) return null;
    const baseRaw = typeof state.poolBaseAmount === 'object' && state.poolBaseAmount.toString
      ? Number(state.poolBaseAmount.toString()) : Number(state.poolBaseAmount);
    const quoteRaw = typeof state.poolQuoteAmount === 'object' && state.poolQuoteAmount.toString
      ? Number(state.poolQuoteAmount.toString()) : Number(state.poolQuoteAmount);
    if (baseRaw <= 0 || quoteRaw <= 0) return null;
    return (quoteRaw / 1e9) / (baseRaw / Math.pow(10, baseDecimals));
  }

  /**
   * 从 pool 的 reserves 算 mid price = quoteReserve / baseReserve（按 decimals 调整）
   * 用 Executor 已加载的 onlineSdk（fallback: 仅 cache miss 时调用）
   */
  async _fetchPoolMidPrice(poolAddress, baseDecimals) {
    if (!this.executor.onlineSdk || !this.executor.keypair) return null;
    const { PublicKey } = require('@solana/web3.js');
    const poolKey = new PublicKey(poolAddress);
    const state = await this.executor.onlineSdk.swapSolanaState(poolKey, this.executor.keypair.publicKey);
    if (!state || !state.poolBaseAmount || !state.poolQuoteAmount) return null;

    // Number 精度对小价格够用（small floats），不用 BigInt 除
    const baseRaw = Number(state.poolBaseAmount.toString());
    const quoteRaw = Number(state.poolQuoteAmount.toString());
    if (baseRaw <= 0 || quoteRaw <= 0) return null;

    // mid_price = (quote / 1e9) / (base / 10^baseDecimals)
    //          = quote * 10^baseDecimals / (base * 1e9)
    const price = (quoteRaw / 1e9) / (baseRaw / Math.pow(10, baseDecimals));
    return price;
  }

  async _reconcileRetries() {
    if (this._reconciling) return; // 防止上一轮还没跑完，新轮就启动
    this._reconciling = true;
    try {
      await this._reconcileRetriesInner();
    } finally {
      this._reconciling = false;
    }
  }

  async _reconcileRetriesInner() {
    const now = Date.now();
    const due = this.tradeLogger.getDuePendingRetries(now);

    for (const row of due) {
      const pos = this.positions.get(row.position_id);
      if (!pos) continue; // 已被删除

      // 跳过 stuck 的（不再自动重试，等人工干预）
      if (row.status === 'stuck' || pos.status === 'stuck') continue;

      // sell_confirming：还在等链上确认；只有 last_retry_at 已经超过 30s 才主动重试
      if (row.status === 'sell_confirming') {
        const lastRetry = row.last_retry_at || 0;
        if (now - lastRetry < 30_000) continue;

        // 已经 30s+ 没动静，主动 confirmTx 一次
        const sig = row.pending_sell_signature || pos._lastSellSignature;
        if (sig) {
          const result = await this.executor.confirmTx(sig, { timeoutMs: 3000, pollIntervalMs: 500 });
          if (result.confirmed) {
            monitor.inc('PositionManager.reconcilerConfirmed', 1, 'PositionManager');

            // v3.17 修复 PnL bug：
            // 之前用 pos.entryPrice 作为 exitPrice 占位 → _finalizeSuccess 里 exitSol
            // 退化为 tokenAmount * entryPrice = entrySol → 净 PnL ≈ -feeSol（误显示亏损）。
            // 现在从链上 fetch 真实 SOL 收入，按真实成交价回写。
            let exitPrice = pos.entryPrice;
            let solOut = null;
            try {
              const swap = await this.executor.fetchTxSwapResult(sig, pos.mint);
              // SELL 的 realSolDelta 是正数（钱包 SOL 增加），需 > 0 才有效
              if (swap && swap.realSolDelta > 0 && pos.tokenAmount > 0) {
                solOut = swap.realSolDelta;
                exitPrice = solOut / pos.tokenAmount;
                // 同时累加 SELL tx 的 base fee（priority fee 已包含在 realSolDelta 里）
                if (swap.fee && !pos._reconcilerSellFeeAccounted) {
                  // realSolDelta 已经扣过 priority fee + base fee；这里不再叠加
                  // （避免双重扣减）
                  pos._reconcilerSellFeeAccounted = true;
                }
                console.log(
                  `[PositionManager] 🔄 reconciler found landed sell: ${pos.symbol || pos.mint.slice(0, 6)}, ` +
                    `solOut=${solOut.toFixed(4)} SOL, exitPrice=${exitPrice.toExponential(4)}`,
                );
              } else {
                console.warn(
                  `[PositionManager] 🔄 reconciler found landed sell: ${pos.symbol || pos.mint.slice(0, 6)}, ` +
                    `但 fetchTxSwapResult 拿不到 realSolDelta — fallback 用 entryPrice 占位（PnL 将不准）`,
                );
              }
            } catch (err) {
              monitor.recordError('PositionManager', err, {
                phase: 'reconciler_fetch_swap',
                mint: pos.mint,
                signature: sig,
              });
            }

            this._finalizeSuccess(pos, exitPrice, solOut, sig, null);  // v3.17.40c: reconciler path, no actualSellAmount
            continue;
          }
        }
        // 没确认，触发重试
        monitor.inc('PositionManager.reconcilerRetried', 1, 'PositionManager');
      }

      // sell_pending（明确等待重试）：直接触发
      const latestPrice = this.priceTracker.getPrice(pos.mint) || pos.entryPrice;
      console.log(
        `[PositionManager] 🔄 reconciler retrying ${pos.symbol || pos.mint.slice(0, 6)} ` +
          `(status=${row.status}, attempts=${pos.sellAttempts})`,
      );
      // 不 await，让多个 retry 并行（但同一 pos 不会并发，因为 status 字段 + lock）
      this._attemptSell(pos, latestPrice).catch((err) => {
        monitor.recordError('PositionManager', err, {
          phase: 'reconciler_retry',
          mint: pos.mint,
        });
      });
    }
  }
}

module.exports = PositionManager;
