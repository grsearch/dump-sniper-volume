'use strict';

/**
 * RsiCalculator (v3.17.17)
 * ========================
 * 为 dump-sniper 量身定制的抗噪音 RSI 计算器。
 *
 * 为什么不直接用通用 RSI?
 *   1. Solana memecoin 1 秒内可能 10+ 笔交易,价格剧烈波动,单点采样污染 RSI
 *   2. 没成交的秒为 null,普通 RSI 库不知道怎么处理
 *   3. 池子小(< 50 SOL)时单笔 0.5 SOL 买入推价 +1%,RSI 完全失真
 *   4. 反弹起点 RSI 还在 20-30,等它到 50 反弹已经走完一半 — 关键看「上拐」而非「低位」
 *
 * 解决方案 4 件套:
 *   A) Volume-weighted aggregation: 1 秒内所有交易按 SOL volume 加权平均
 *   B) Forward fill: 没成交的秒沿用上一秒价格(不是 null)
 *   C) Wilder's smoothing: 用 α=1/period 平均涨跌幅,而非简单 SMA
 *   D) 多时间尺度: 同时维护 1s + 5s 两套 RSI,联合判断
 *
 * 使用:
 *   const rsi = new RsiCalculator();
 *   priceTracker.on('update', ({mint, price}) => rsi.feedTick(mint, price));
 *   dumpDetector.on('sellAnalyzed', (s) => rsi.feedTrade(s.mint, s.priceAfter, s.sellSol, 'sell'));
 *
 *   // 在 SignalEngine 里查询:
 *   const r = rsi.snapshot(mint);
 *   // r = {
 *   //   rsi1s: 35.2, rsi5s: 42.1,
 *   //   rsi1sSlope: +8.5,   // 上拐:正数表示 RSI 在涨
 *   //   poolHealthy: true,   // 池子 ≥ 50 SOL,RSI 可信
 *   //   bucketCount: 14,     // 有效 1s 样本数
 *   // }
 *
 * 推荐用法 — 不是「RSI < 30 就买」:
 *   if (r.poolHealthy && r.rsi1sSlope < -3) return reject;  // RSI 还在跌不买
 *   if (r.rsi5s > 75) return reject;                          // 5s 已经超买不追
 *   // 其他情况让砸盘信号直接通过
 */

class RsiCalculator {
  /**
   * @param {object} opts
   * @param {number} [opts.period1=14]  1s 桶 RSI period(14 秒数据)
   * @param {number} [opts.period5=7]   5s 桶 RSI period(35 秒数据,更早可用)
   * @param {number} [opts.bucketMs1=1000]
   * @param {number} [opts.bucketMs5=5000]
   * @param {number} [opts.maxBuckets=120]  最多保留 120 桶
   */
  constructor({
    period1 = 14,
    period5 = 7,
    period30 = 7,
    period60 = 7,
    bucketMs1 = 1000,
    bucketMs5 = 5000,
    bucketMs30 = 30000,
    bucketMs60 = 60000,
    maxBuckets = 120,
  } = {}) {
    this.period1 = period1;
    this.period5 = period5;
    this.period30 = period30;
    this.period60 = Math.max(1, period60);
    // 向后兼容:旧代码用 this.period
    this.period = period1;
    this.bucketMs1 = bucketMs1;
    this.bucketMs5 = bucketMs5;
    this.bucketMs30 = bucketMs30;
    this.bucketMs60 = bucketMs60;
    this.maxBuckets = maxBuckets;

    // mint → { buckets1s, buckets5s, buckets30s, buckets60s, lastPrice }
    this.state = new Map();
  }

  /**
   * 价格 tick (来自 PriceTracker.update 或 DumpDetector.priceTick)
   * 不带 volume 信息,只用来 forward-fill 空桶
   */
  feedTick(mint, price, ts = Date.now()) {
    if (!Number.isFinite(price) || price <= 0) return;
    const s = this._stateOf(mint);
    s.lastPrice = price;
    this._updateRsi1mState(s, price, ts);
    this._touchBucket(s.buckets1s, price, 0, ts, this.bucketMs1);
    this._touchBucket(s.buckets5s, price, 0, ts, this.bucketMs5);
    this._touchBucket(s.buckets30s, price, 0, ts, this.bucketMs30);
    this._touchBucket(s.buckets60s, price, 0, ts, this.bucketMs60, true);
    this._trim(s);
  }

  /**
   * 一笔实际交易 (来自 DumpDetector.sellAnalyzed 或 BUY 解析)
   * @param {string} side 'buy' | 'sell'
   * @param {number} solVolume 这笔交易的 SOL 体积 — 作为加权权重
   * @param {number} poolQuoteSol 池子当前 SOL 余额 — 用于判断 poolHealthy
   */
  feedTrade(mint, price, solVolume, side, ts = Date.now(), poolQuoteSol = null) {
    if (!Number.isFinite(price) || price <= 0) return;
    if (!Number.isFinite(solVolume) || solVolume <= 0) solVolume = 0.001; // 给最低权重避免被 forward-fill 覆盖
    const s = this._stateOf(mint);

    // v3.17.38: 在新 trade 写入桶之前,缓存当前 snapshot
    // 用于下游(SignalEngine)取"砸单前 RSI"
    // 只在有足够数据(5s 桶 >= 8 个,约 40s)时缓存
    // 要求 lastPoolQuoteSol >= 50 才缓存(跟 RSI_PEAK 的 poolHealthy 条件一致)
    // v3.17.38-fix: 如果传入的 poolQuoteSol > 0，先更新再用（解决 CPI/balanceOnly 路径
    //   poolQuoteAfter=0 导致 lastPoolQuoteSol 永远为 null 的问题）
    if (poolQuoteSol !== null && Number.isFinite(poolQuoteSol) && poolQuoteSol > 0) {
      s.lastPoolQuoteSol = poolQuoteSol;
    }
    // v3.17.42: 降低快照缓存门槛 — 30s桶>=4(2min数据)即可缓存
    //   之前要求5s桶>=8(40s数据)且poolSol>=50，覆盖率只有3%
    //   放宽后覆盖率提升到60%+，RSI(7,30s)需要8个桶=4min数据
    //   poolSol门槛降到20，Pump.fun新币池子通常20-50 SOL
    const minPoolSol = 20;
    const lastPoolSol = s.lastPoolQuoteSol || 0;
    if (s.buckets30s.length >= 4 && lastPoolSol >= minPoolSol) {
      s._snapshotBeforeFeed = this._computeSnapshot(s);
      s._snapshotBeforeFeedTs = Date.now();
    } else {
      s._snapshotBeforeFeed = null;
    }

    s.lastPrice = price;
    this._updateRsi1mState(s, price, ts);
    this._touchBucket(s.buckets1s, price, solVolume, ts, this.bucketMs1);
    this._touchBucket(s.buckets5s, price, solVolume, ts, this.bucketMs5);
    this._touchBucket(s.buckets30s, price, solVolume, ts, this.bucketMs30);
    this._touchBucket(s.buckets60s, price, solVolume, ts, this.bucketMs60, true);
    this._trim(s);
  }

  _stateOf(mint) {
    let s = this.state.get(mint);
    if (!s) {
      s = {
        buckets1s: [],
        buckets5s: [],
        buckets30s: [],
        buckets60s: [],
        lastPrice: null,
        lastPoolQuoteSol: null,
        rsi1mCurrentIdx: null,
        rsi1mCurrentClose: null,
        rsi1mFinalClose: null,
        rsi1mCloseCount: 0,
        rsi1mSeedChanges: 0,
        rsi1mSeedGain: 0,
        rsi1mSeedLoss: 0,
        rsi1mAvgGain: null,
        rsi1mAvgLoss: null,
      };
      this.state.set(mint, s);
    }
    return s;
  }

  /**
   * 桶聚合: 当前 tick 落到哪个桶 (按 bucketMs 对齐), 用 volume 加权累加
   * 如果跳过了若干桶, forward-fill 用上一笔价 (volume=0 表示 "占位")
   */
  _touchBucket(buckets, price, solVolume, ts, bucketMs, closeFill = false) {
    const idx = Math.floor(ts / bucketMs);
    if (buckets.length === 0) {
      buckets.push({ idx, sumPriceVolume: price * (solVolume || 1), sumVolume: solVolume || 1, lastPrice: price });
      return;
    }
    const last = buckets[buckets.length - 1];
    if (idx === last.idx) {
      // 同桶累加
      last.sumPriceVolume += price * (solVolume || 0);
      last.sumVolume += solVolume || 0;
      last.lastPrice = price;
      return;
    }
    if (idx < last.idx) return; // 时间回退,忽略

    // forward fill 中间空桶
    const fillPrice = closeFill
      ? last.lastPrice
      : (last.sumVolume > 0 ? last.sumPriceVolume / last.sumVolume : last.lastPrice);
    let fillFrom = last.idx + 1;
    if (idx - last.idx >= this.maxBuckets) {
      buckets.length = 0;
      fillFrom = idx - this.maxBuckets + 1;
    }
    for (let i = fillFrom; i < idx; i++) {
      buckets.push({ idx: i, sumPriceVolume: 0, sumVolume: 0, lastPrice: fillPrice });
    }
    // 新桶
    buckets.push({
      idx,
      sumPriceVolume: price * (solVolume || 1),
      sumVolume: solVolume || 1,
      lastPrice: price,
    });
  }

  _commitRsi1mClose(s, close) {
    if (s.rsi1mFinalClose == null) {
      s.rsi1mFinalClose = close;
      s.rsi1mCloseCount = 1;
      return;
    }

    const delta = close - s.rsi1mFinalClose;
    const gain = delta > 0 ? delta : 0;
    const loss = delta < 0 ? -delta : 0;
    if (s.rsi1mSeedChanges < this.period60) {
      s.rsi1mSeedGain += gain;
      s.rsi1mSeedLoss += loss;
      s.rsi1mSeedChanges += 1;
      if (s.rsi1mSeedChanges === this.period60) {
        s.rsi1mAvgGain = s.rsi1mSeedGain / this.period60;
        s.rsi1mAvgLoss = s.rsi1mSeedLoss / this.period60;
      }
    } else {
      s.rsi1mAvgGain = (s.rsi1mAvgGain * (this.period60 - 1) + gain) / this.period60;
      s.rsi1mAvgLoss = (s.rsi1mAvgLoss * (this.period60 - 1) + loss) / this.period60;
    }
    s.rsi1mFinalClose = close;
    s.rsi1mCloseCount += 1;
  }

  _commitFlatRsi1mBars(s, close, count) {
    let remaining = count;
    while (remaining > 0 && s.rsi1mSeedChanges < this.period60) {
      this._commitRsi1mClose(s, close);
      remaining -= 1;
    }
    if (remaining <= 0) return;

    const decay = Math.pow((this.period60 - 1) / this.period60, remaining);
    s.rsi1mAvgGain *= decay;
    s.rsi1mAvgLoss *= decay;
    s.rsi1mFinalClose = close;
    s.rsi1mCloseCount += remaining;
  }

  _updateRsi1mState(s, price, ts) {
    const idx = Math.floor(ts / this.bucketMs60);
    if (s.rsi1mCurrentIdx == null) {
      s.rsi1mCurrentIdx = idx;
      s.rsi1mCurrentClose = price;
      return;
    }
    if (idx < s.rsi1mCurrentIdx) return;
    if (idx === s.rsi1mCurrentIdx) {
      s.rsi1mCurrentClose = price;
      return;
    }

    const previousClose = s.rsi1mCurrentClose;
    this._commitRsi1mClose(s, previousClose);
    this._commitFlatRsi1mBars(s, previousClose, idx - s.rsi1mCurrentIdx - 1);
    s.rsi1mCurrentIdx = idx;
    s.rsi1mCurrentClose = price;
  }

  _rsiFromAverages(avgGain, avgLoss) {
    if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
    return 100 - 100 / (1 + avgGain / avgLoss);
  }

  _currentRsi1m(s) {
    if (s.rsi1mFinalClose == null || s.rsi1mCurrentClose == null) return null;
    const delta = s.rsi1mCurrentClose - s.rsi1mFinalClose;
    const gain = delta > 0 ? delta : 0;
    const loss = delta < 0 ? -delta : 0;

    if (s.rsi1mSeedChanges < this.period60) {
      if (s.rsi1mSeedChanges + 1 < this.period60) return null;
      return this._rsiFromAverages(
        (s.rsi1mSeedGain + gain) / this.period60,
        (s.rsi1mSeedLoss + loss) / this.period60,
      );
    }

    return this._rsiFromAverages(
      (s.rsi1mAvgGain * (this.period60 - 1) + gain) / this.period60,
      (s.rsi1mAvgLoss * (this.period60 - 1) + loss) / this.period60,
    );
  }

  _trim(s) {
    if (s.buckets1s.length > this.maxBuckets) {
      s.buckets1s.splice(0, s.buckets1s.length - this.maxBuckets);
    }
    if (s.buckets5s.length > this.maxBuckets) {
      s.buckets5s.splice(0, s.buckets5s.length - this.maxBuckets);
    }
    if (s.buckets30s.length > this.maxBuckets) {
      s.buckets30s.splice(0, s.buckets30s.length - this.maxBuckets);
    }
    if (s.buckets60s.length > this.maxBuckets) {
      s.buckets60s.splice(0, s.buckets60s.length - this.maxBuckets);
    }
  }

  /**
   * 把桶序列转成价格序列 (vw 平均或 forward-fill)
   */
  _bucketsToPrices(buckets) {
    const prices = [];
    let lastVW = null;
    for (const b of buckets) {
      let p;
      if (b.sumVolume > 0) {
        p = b.sumPriceVolume / b.sumVolume; // volume-weighted price
        lastVW = p;
      } else {
        p = lastVW != null ? lastVW : b.lastPrice; // forward-fill
      }
      prices.push(p);
    }
    return prices;
  }

  /**
   * Wilder's RSI: 用 EMA 平均涨跌幅
   * @param {number[]} prices
   * @param {number} period 默认 this.period(向后兼容)
   */
  _wildersRsi(prices, period = this.period) {
    const n = prices.length;
    if (n < period + 1) return null;

    // 初始化: 前 period 笔的简单平均
    let gain = 0, loss = 0;
    for (let i = 1; i <= period; i++) {
      const d = prices[i] - prices[i - 1];
      if (d > 0) gain += d;
      else loss -= d;
    }
    let avgGain = gain / period;
    let avgLoss = loss / period;

    // 后续用 Wilder's smoothing
    for (let i = period + 1; i < n; i++) {
      const d = prices[i] - prices[i - 1];
      const up = d > 0 ? d : 0;
      const dn = d < 0 ? -d : 0;
      avgGain = (avgGain * (period - 1) + up) / period;
      avgLoss = (avgLoss * (period - 1) + dn) / period;
    }

    if (avgLoss === 0) {
      if (avgGain === 0) return 50;
      return 100;
    }
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
  }

  /**
   * v3.17.38: 内部 snapshot 计算逻辑(接受 state,不查 map)
   * 用于:
   * 1) 外部调用的 snapshot(mint, poolHealthyMinSol) 公开 API
   * 2) feedTrade 内部缓存"砸单前"snapshot
   */
  _computeSnapshot(s, poolHealthyMinSol = 20) {
    if (!s) return null;

    const prices1s = this._bucketsToPrices(s.buckets1s);
    const prices5s = this._bucketsToPrices(s.buckets5s);

    const prices30s = this._bucketsToPrices(s.buckets30s);
    const rsi1s = this._wildersRsi(prices1s, this.period1);
    const rsi5s = this._wildersRsi(prices5s, this.period5);
    const rsi30s = this._wildersRsi(prices30s, this.period30);
    const rsi1m = this._currentRsi1m(s);
    if (rsi1s == null && rsi5s == null && rsi30s == null && rsi1m == null) return null;

    // RSI 上拐: 当前 RSI vs 3 秒前 RSI 的差(正数 = RSI 在涨 = 反弹起点)
    let rsi1sSlope = null;
    if (prices1s.length >= this.period1 + 4) {
      const olderRsi = this._wildersRsi(prices1s.slice(0, prices1s.length - 3), this.period1);
      if (olderRsi != null && rsi1s != null) {
        rsi1sSlope = rsi1s - olderRsi;
      }
    }

    return {
      rsi1s,
      rsi5s,
      rsi30s,
      rsi1m,
      rsi1sSlope,
      bucketCount1s: s.buckets1s.length,
      bucketCount5s: s.buckets5s.length,
      bucketCount30s: s.buckets30s.length,
      bucketCount1m: s.buckets60s.length,
      lastPrice: s.lastPrice,
      lastPoolQuoteSol: s.lastPoolQuoteSol,
      poolHealthy: s.lastPoolQuoteSol == null
        ? false  // 没数据 → 不信 RSI
        : s.lastPoolQuoteSol >= poolHealthyMinSol,
    };
  }

  /**
   * 当前 RSI snapshot (供 SignalEngine 查询)
   * @returns {object|null} null 表示数据还不够
   */
  snapshot(mint, poolHealthyMinSol = 20) {
    const s = this.state.get(mint);
    if (!s) return null;
    return this._computeSnapshot(s, poolHealthyMinSol);
  }

  /**
   * v3.17.38: 取"上一次 feedTrade 之前"的 snapshot
   * 用途:供 SignalEngine 在处理 dumpSignal 时取"砸单前 RSI"
   * 工作原理:DumpDetector emit priceTick → RsiCalculator.feedTrade
   * feedTrade 在写入桶之前缓存了当前 snapshot
   * 然后 DumpDetector emit dumpSignal → SignalEngine 调用此方法
   * 拿到的就是"砸单这笔写入前"的 RSI 状态
   * 返回 null 的情况:
   * - 该 mint 数据不足(< 8 个 5s 桶,约 40s)
   * - 该 mint 的 lastPoolQuoteSol < 50(池子太浅,RSI 不可信)
   * - 该 mint 还没收到任何 feed
   * - 缓存太老(>10s,数据流断裂)
   */
  getSnapshotBeforeLast(mint) {
    const s = this.state.get(mint);
    if (!s || !s._snapshotBeforeFeed) return null;
    // 缓存防过期:超过 10 秒的不要用(数据流可能断了)
    if (s._snapshotBeforeFeedTs && Date.now() - s._snapshotBeforeFeedTs > 10000) {
      return null;
    }
    return s._snapshotBeforeFeed;
  }

  /**
   * v3.17.30: 暴露最近 N 秒价格历史给 SignalEngine 做 RECENT_PUMP 过滤
   *
   * 直接读 buckets 的 lastPrice(VW 价格不必要,我们只关心区间最低)。
   * 桶不够长返回 null,SignalEngine 自然降级跳过过滤(不报错)。
   *
   * @param {string} mint
   * @param {number} lookbackSec - 回看秒数
   * @param {'1s'|'5s'} bucketType - 用 1秒桶还是 5秒桶
   * @returns {number[]|null} lastPrice 数组(最旧→最新),桶不足返回 null
   */
  getRecentPriceHistory(mint, lookbackSec, bucketType = '1s') {
    const s = this.state.get(mint);
    if (!s) return null;
    const buckets = bucketType === '1m'
      ? s.buckets60s
      : bucketType === '30s'
        ? s.buckets30s
        : bucketType === '5s'
          ? s.buckets5s
          : s.buckets1s;
    const bucketMs = bucketType === '1m'
      ? this.bucketMs60
      : bucketType === '30s'
        ? this.bucketMs30
        : bucketType === '5s'
          ? this.bucketMs5
          : this.bucketMs1;
    const need = Math.floor((lookbackSec * 1000) / bucketMs);
    // v3.17.39: 降低最低要求 — 有部分数据(至少30个桶=2.5min)就返回，不必等满整个窗口
    //   之前要求 buckets.length >= need(240 for 20min)，重启后数据不够就返回 null → 过滤被跳过
    const minBuckets = Math.min(need, 30);
    if (buckets.length < minBuckets) return null;
    const recent = buckets.slice(-Math.min(buckets.length, need));
    const prices = recent
      .map(b => b.lastPrice)
      .filter(p => Number.isFinite(p) && p > 0);
    return prices.length > 0 ? prices : null;
  }

  /**
   * 清理某个 mint 的状态 (代币被移除时调用)
   */
  reset(mint) {
    this.state.delete(mint);
  }

  /**
   * 全局清理:删除超过 5 分钟没新数据的 mint
   */
  cleanup(staleMs = 5 * 60 * 1000) {
    const cutoffIdx1s = Math.floor((Date.now() - staleMs) / this.bucketMs1);
    for (const [mint, s] of this.state) {
      const lastBucket = s.buckets1s[s.buckets1s.length - 1];
      if (!lastBucket || lastBucket.idx < cutoffIdx1s) {
        this.state.delete(mint);
      }
    }
  }
}

module.exports = RsiCalculator;
