/**
 * PostExitTracker v3.17.31
 *
 * 平仓后 5 分钟价格追踪 — 回测用,完全旁路。
 *
 * 工作流:
 * 1. PositionManager 平仓时调用 startTracking(positionId, mint, exitPrice, exitTs)
 * 2. 内部订阅 priceTracker 'update' 事件
 * 3. 在 5 分钟窗口内,记录该 mint 的 maxPrice / minPrice / 5 个时间点 snapshot
 * 4. 5 分钟到 → flush DB → 从内存移除
 *
 * 数据用于(回测查询):
 * - 计算 "post-exit max pump %" 和 "post-exit max dump %"
 * - 评估每个 exitReason 平均"卖早了多少"
 * - 校准 TAKE_PROFIT / TRAILING / EMERGENCY / TIMEOUT 阈值
 *
 * 不影响:
 * - BUY/SELL 决策延迟(纯被动监听 EventEmitter)
 * - 新 RPC 调用(数据源是现有 priceTracker update 流)
 * - 主数据流(只读 priceTracker.on('update'))
 *
 * 窗口选择:5 分钟匹配策略时间尺度(持仓 73% 30s 到峰值, 2min 超时窗口)
 * 超过 5 分钟的价格信息对策略决策无可操作意义
 */
class PostExitTracker {
  /**
   * @param {EventEmitter} priceTracker - 来自 PriceTracker 实例
   * @param {Object} tradeLogger - 写入 DB 用
   * @param {Object} options
   * @param {number} options.windowMs - 追踪窗口长度,默认 5 分钟
   * @param {number[]} options.snapshotOffsetsMs - 快照时间点(相对 exitTs)
   */
  constructor(priceTracker, tradeLogger, options = {}) {
    this.priceTracker = priceTracker;
    this.tradeLogger = tradeLogger;
    this.windowMs = options.windowMs || 5 * 60 * 1000; // 5 min
    // snapshot 时间点:10s/30s 对齐"73% 30s 到峰值"统计;1min/2min 对齐 MAX_HOLD 窗口;5min 终点
    this.snapshotOffsetsMs = options.snapshotOffsetsMs || [
      10_000, // 10s
      30_000, // 30s
      60_000, // 1min
      2 * 60_000, // 2min
      5 * 60_000, // 5min (= windowMs,在 finalize 时记录)
    ];

    // positionId → tracking state
    this.tracking = new Map();
    // mint → Set<positionId> 反向索引,O(1) 找该 mint 的所有 tracker(同 mint 可能多仓)
    this.byMint = new Map();

    // 绑定 priceTracker update 事件
    this._onPriceUpdate = this._onPriceUpdate.bind(this);
    this.priceTracker.on('update', this._onPriceUpdate);

    console.log(
      `[PostExitTracker] initialized: windowMs=${this.windowMs}, ` +
      `snapshots=[${this.snapshotOffsetsMs.map(ms => this._offsetLabel(ms)).join(',')}]`
    );
  }

  /**
   * 由 PositionManager 在平仓完成后调用(只在 _finalizeExit 真平仓路径调用)
   *
   * @param {string} positionId
   * @param {string} mint
   * @param {number} exitPrice - 真实成交退出价(SOL/token)
   * @param {number} exitTs - 退出时间戳
   */
  startTracking(positionId, mint, exitPrice, exitTs = Date.now()) {
    if (!positionId || !mint || !Number.isFinite(exitPrice) || exitPrice <= 0) {
      return;
    }
    // 已有该 positionId 的 tracker?重复调用直接跳过(防止 retry sell 路径重复挂)
    if (this.tracking.has(positionId)) return;

    const state = {
      positionId,
      mint,
      exitPrice,
      exitTs,
      maxPrice: exitPrice,
      maxPriceTs: exitTs,
      minPrice: exitPrice,
      minPriceTs: exitTs,
      lastPrice: exitPrice,
      lastPriceTs: exitTs,
      sampleCount: 0,
      snapshots: {},
      finalizeTimer: null,
      snapshotTimers: [],
    };

    // 主 finalize timer
    state.finalizeTimer = setTimeout(() => {
      this._finalize(positionId);
    }, this.windowMs);
    if (state.finalizeTimer.unref) state.finalizeTimer.unref();

    // 每个 snapshot 独立 timer(终点 snapshot 由 finalize 统一处理,这里跳过)
    for (const offsetMs of this.snapshotOffsetsMs) {
      if (offsetMs >= this.windowMs) continue;
      const t = setTimeout(() => {
        const s = this.tracking.get(positionId);
        if (!s) return;
        const label = this._offsetLabel(offsetMs);
        s.snapshots[label] = {
          price: s.lastPrice,
          ts: Date.now(),
          sampleCount: s.sampleCount,
        };
      }, offsetMs);
      if (t.unref) t.unref();
      state.snapshotTimers.push(t);
    }

    this.tracking.set(positionId, state);

    // 反向索引
    if (!this.byMint.has(mint)) this.byMint.set(mint, new Set());
    this.byMint.get(mint).add(positionId);
  }

  /**
   * priceTracker 推过来的每笔价格更新,O(k) k=该 mint 上正在追踪的仓位数(通常 1)
   */
  _onPriceUpdate({ mint, price, ts }) {
    const positionIds = this.byMint.get(mint);
    if (!positionIds || positionIds.size === 0) return;
    if (!Number.isFinite(price) || price <= 0) return;

    for (const positionId of positionIds) {
      const s = this.tracking.get(positionId);
      if (!s) continue;
      // 超过窗口的不更新(应该已被 finalize 清理,但稳妥起见)
      if (ts - s.exitTs > this.windowMs) continue;

      s.sampleCount++;
      s.lastPrice = price;
      s.lastPriceTs = ts;
      if (price > s.maxPrice) {
        s.maxPrice = price;
        s.maxPriceTs = ts;
      }
      if (price < s.minPrice) {
        s.minPrice = price;
        s.minPriceTs = ts;
      }
    }
  }

  /**
   * 5 分钟到,写 DB,清理内存
   */
  _finalize(positionId) {
    const s = this.tracking.get(positionId);
    if (!s) return;

    // 终点 snapshot
    s.snapshots[this._offsetLabel(this.windowMs)] = {
      price: s.lastPrice,
      ts: Date.now(),
      sampleCount: s.sampleCount,
    };

    const maxPumpPct = ((s.maxPrice - s.exitPrice) / s.exitPrice) * 100;
    const maxDumpPct = ((s.minPrice - s.exitPrice) / s.exitPrice) * 100; // 负数

    try {
      this.tradeLogger.recordPostExitStats({
        positionId: s.positionId,
        mint: s.mint,
        exitPrice: s.exitPrice,
        exitTs: s.exitTs,
        maxPrice: s.maxPrice,
        maxPriceTs: s.maxPriceTs,
        maxPumpPct,
        minPrice: s.minPrice,
        minPriceTs: s.minPriceTs,
        maxDumpPct,
        sampleCount: s.sampleCount,
        snapshots: JSON.stringify(s.snapshots),
        finalizedAt: Date.now(),
      });
    } catch (err) {
      console.warn(`[PostExitTracker] recordPostExitStats failed for ${positionId}: ${err.message}`);
    }

    // 清理
    this.tracking.delete(positionId);
    const set = this.byMint.get(s.mint);
    if (set) {
      set.delete(positionId);
      if (set.size === 0) this.byMint.delete(s.mint);
    }
    if (s.finalizeTimer) clearTimeout(s.finalizeTimer);
    for (const t of s.snapshotTimers) clearTimeout(t);
  }

  _offsetLabel(ms) {
    if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
    return `${Math.round(ms / 60_000)}min`;
  }

  /**
   * 进程关闭时调用 — 把所有未到 windowMs 的 tracker 立刻 flush
   */
  shutdown() {
    console.log(`[PostExitTracker] shutdown: flushing ${this.tracking.size} active tracker(s)`);
    const ids = Array.from(this.tracking.keys());
    for (const id of ids) {
      this._finalize(id);
    }
    this.priceTracker.off('update', this._onPriceUpdate);
  }

  /**
   * 监控:当前正在追踪多少仓
   */
  getStats() {
    return {
      activeTracking: this.tracking.size,
      activeMints: this.byMint.size,
    };
  }
}

module.exports = PostExitTracker;
