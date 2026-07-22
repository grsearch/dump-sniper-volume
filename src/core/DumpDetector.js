'use strict';

/**
 * DumpDetector (v3)
 * =================
 * 接收 LaserStream 推送的交易,解析其是否为:
 *   - 涉及监控代币的 swap
 *   - 方向为 SELL(base → SOL)
 *   - 卖出 SOL >= 阈值
 *   - 单笔自身造成 priceImpact <= -10%
 *
 * v3 vs v2:
 * 新增 CPI 路由检测(Jupiter / OKX / Flash / Trojan 等 bot 通过 CPI 调 Pump AMM)。
 * 这类交易 pool_base_vault 在 accountKeys 中,但 pool_quote_vault 不在
 * (Jupiter 用中间 WSOL wrapping 账户路由)。
 * v3 回退逻辑:仅凭 base_vault 余额变化 + SOL native balance 推算价格。
 *
 * 解析路径:
 *   1. 完整路径:pool_base_vault + pool_quote_vault 都在 accountKeys
 *      → 直接读两个 vault 余额变化,精确算价格和方向
 *   2. CPI 回退路径:pool_base_vault 在但 pool_quote_vault 不在
 *      → 读 base_vault 余额变化 + SOL native balance 推算
 *      → 标记 source='cpi' 供下游判断
 *   3. 完全缺失:两个 vault 都不在 → 跳过(可能走别的 DEX)
 *
 * priceTick:仅当 pool 已知时才 emit(保证 PriceTracker 拿到的价格质量)
 */

const EventEmitter = require('events');
const bs58Lib = require('bs58');
const bs58 = bs58Lib.default || bs58Lib;
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const {
  priceDetailsFromUiReserves,
  constantProductAfterBaseUi,
} = require('../utils/pumpSwapPricing');

const monitor = getMonitor();
monitor.registerModule('DumpDetector', { staleMs: 120_000, label: 'Dump Detector' });

const WSOL_MINT = 'So11111111111111111111111111111111111111112';
const PUMP_AMM_PROGRAM_ID = config.programs.pumpAmm;

function encodeBase58(value) {
  if (value == null) return null;
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return bs58.encode(value);
  if (value instanceof Uint8Array) return bs58.encode(Buffer.from(value));
  return null;
}

/**
 * 从 uiTokenAmount 安全解析余额。优先用 amount/decimals(无精度丢失),
 * fallback 到 uiAmountString → uiAmount(可能对大数返回 null)。
 */
function safeTokenAmount(ui) {
  if (!ui) return 0;
  // 优先:amount (string) / 10^decimals - 精确无损失
  if (ui.amount != null && ui.decimals != null) {
    const raw = BigInt(ui.amount);
    const div = BigInt(10 ** ui.decimals);
    return Number(raw / div) + Number(raw % div) / Number(div);
  }
  // fallback
  const v = parseFloat(ui.uiAmountString || ui.uiAmount || '0');
  return Number.isFinite(v) ? v : 0;
}

class DumpDetector extends EventEmitter {
  constructor(tokenRegistry) {
    super();
    this.tokenRegistry = tokenRegistry;
    this.poolStateCache = null;
    this._poolStateRefreshAt = new Map();

    // v3.17.13: 短窗口累计砸盘追踪
    //   记录每个 mint 最近 N 秒内的所有卖出,用于检测 rug
    //   格式: mint → [{ sellSol, priceBefore, priceAfter, ts }, ...]
    this._recentSells = new Map();
    this._recentSellWindowMs = 10_000; // 追踪最近 10 秒

    // v3.18: 多 LaserStream region 会重复推同一笔 tx，按 signature 去重。
    this._processedSigs = new Map(); // signature -> expireAt
    this._sigDedupMs = parseInt(process.env.DUMP_DETECTOR_SIG_DEDUP_MS || '60000', 10);

    // 定期清理过期记录
    this._recentSellCleanup = setInterval(() => {
      const now = Date.now();
      const cutoff = now - this._recentSellWindowMs;
      for (const [mint, sells] of this._recentSells) {
        while (sells.length > 0 && sells[0].ts < cutoff) sells.shift();
        if (sells.length === 0) this._recentSells.delete(mint);
      }
      for (const [sig, expireAt] of this._processedSigs) {
        if (expireAt <= now) this._processedSigs.delete(sig);
      }
      for (const [poolAddress, requestedAt] of this._poolStateRefreshAt) {
        if (now - requestedAt >= 60_000) this._poolStateRefreshAt.delete(poolAddress);
      }
    }, 5_000);
    if (this._recentSellCleanup.unref) this._recentSellCleanup.unref();
  }

  shutdown() {
    if (this._recentSellCleanup) {
      clearInterval(this._recentSellCleanup);
      this._recentSellCleanup = null;
    }
    this._recentSells.clear();
    this._processedSigs.clear();
    this._poolStateRefreshAt.clear();
  }

  setPoolStateCache(cache) {
    this.poolStateCache = cache;
  }

  _requestPoolStateRefresh(poolAddress) {
    if (!poolAddress || !this.poolStateCache || typeof this.poolStateCache.refreshOne !== 'function') {
      return;
    }

    const now = Date.now();
    const lastRequestedAt = this._poolStateRefreshAt.get(poolAddress) || 0;
    if (now - lastRequestedAt < 1000) return;

    this._poolStateRefreshAt.set(poolAddress, now);
    Promise.resolve(this.poolStateCache.refreshOne(poolAddress, 0)).catch((err) => {
      monitor.recordError('DumpDetector', err, {
        phase: 'pool_state_on_demand',
        poolAddress,
      });
    });
  }

  /**
   * v3.17.13: 获取代币最近 N 秒内的累计砸盘统计
   * @returns {{ totalSellSol, cumImpactPct, sellCount, oldestTs } | null}
   */
  getRecentDumpStats(mint) {
    const sells = this._recentSells.get(mint);
    if (!sells || sells.length === 0) return null;
    const totalSellSol = sells.reduce((s, x) => s + x.sellSol, 0);
    const first = sells[0];
    const last = sells[sells.length - 1];
    let cumImpactPct = 0;
    if (first.priceBefore > 0 && last.priceAfter > 0) {
      cumImpactPct = ((first.priceBefore - last.priceAfter) / first.priceBefore) * 100;
    }
    return { totalSellSol, cumImpactPct, sellCount: sells.length, oldestTs: first.ts };
  }

  /**
   * v3.17.20: 返回最近窗口内**单笔最大**卖出(含其 impact),用于分析竞争对手
   *   "跟着多大的砸单买入"。竞争对手 BUY 落链通常在触发砸单后 1-2 slot 内,
   *   10 秒窗口足够覆盖。返回 null 表示该 mint 近期没有记录到卖单
   *   (可能竞争对手跟的是我们没解析到的砸单,或买入并非砸单驱动)。
   * @returns {{ maxSingleSellSol, maxSellImpactPct, totalSellSol, sellCount } | null}
   */
  getRecentMaxSell(mint) {
    const sells = this._recentSells.get(mint);
    if (!sells || sells.length === 0) return null;
    let maxSingleSellSol = 0;
    let maxSellImpactPct = 0;
    let maxSellSlot = null;
    let totalSellSol = 0;
    for (const s of sells) {
      totalSellSol += s.sellSol;
      if (s.sellSol > maxSingleSellSol) {
        maxSingleSellSol = s.sellSol;
        maxSellSlot = s.slot || null;
        if (s.priceBefore > 0 && s.priceAfter > 0) {
          maxSellImpactPct = ((s.priceBefore - s.priceAfter) / s.priceBefore) * 100;
        }
      }
    }
    return { maxSingleSellSol, maxSellImpactPct, maxSellSlot, totalSellSol, sellCount: sells.length };
  }

  handleTransaction(txMessage) {
    // v3.18: signature 去重 — 多 LS region 重推同一笔交易时跳过
    const _sig = txMessage?.transaction?.signature || txMessage?.signature;
    if (_sig) {
      const _sigStr = typeof _sig === 'string' ? _sig : bs58.encode(Uint8Array.from(_sig));
      if (this._processedSigs.has(_sigStr)) {
        monitor.inc('DumpDetector.dedupSkip', 1, 'DumpDetector');
        return;
      }
      this._processedSigs.set(_sigStr, Date.now() + this._sigDedupMs);
    }
    monitor.inc('DumpDetector.txParsed', 1, 'DumpDetector');
    monitor.beat('DumpDetector', 'parse');
    try {
      const parsed = this._parseTx(txMessage);
      if (!parsed) {
        monitor.inc('DumpDetector.parsedNull', 1, 'DumpDetector');
        return;
      }

      // emit priceTick (DumpDetector 只 emit "可信"价格--pool 已知的)
      // v3.17.17: 带 side + quoteAmount + poolQuoteAfter 让 RSI 能做 volume-weighted
      monitor.inc('DumpDetector.priceTicks', 1, 'DumpDetector');
      this.emit('priceTick', {
        mint: parsed.baseMint,
        price: parsed.priceAfter,
        ts: parsed.ts,
        slot: parsed.slot,
        signature: parsed.signature,
        poolAddress: parsed.poolAddress,
        side: parsed.side,                 // 'BUY' | 'SELL'
        solVolume: parsed.quoteAmount,     // 这笔交易的 SOL 体积
        poolQuoteAfter: parsed.poolQuoteAfter, // 池子当前 SOL
        poolBaseAfter: parsed.poolBaseAfter,
        baseDecimals: parsed.baseDecimals,
        rawPrice: parsed.rawPriceAfter,
        virtualQuoteReserveSol: parsed.virtualQuoteReserveSol,
        effectiveQuoteReserveSol: parsed.effectiveQuoteReserveSol,
      });

      // v3.17.20: emit swapParsed -- 给 CompetitorTracker 用。
      //   每一笔被监控代币上的 swap(无论 BUY/SELL)都带上 signer(钱包)、side、SOL 体积、价格。
      //   CompetitorTracker 只关心被追踪钱包的交易,其余直接忽略,零额外开销。
      this.emit('swapParsed', {
        mint: parsed.baseMint,
        symbol: parsed.symbol,
        signer: parsed.signer,             // 交易钱包(竞争对手地址比对用)
        side: parsed.side,                 // 'BUY' | 'SELL'
        solVolume: parsed.quoteAmount,     // 这笔 swap 的 SOL 体积
        price: parsed.priceAfter,
        priceBefore: parsed.priceBefore,
        priceChangePct: parsed.priceChangePct,
        ts: parsed.ts,
        slot: parsed.slot,
        signature: parsed.signature,
        poolAddress: parsed.poolAddress,
        poolQuoteAfter: parsed.poolQuoteAfter,
      });

      // 仅卖单进入下游判定
      if (parsed.side !== 'SELL') return;

      // ============ v3.26b: Aggregator 拆单校准（简化版，不依赖 poolState） ============
      // 根因: DFlow/Jupiter 等 Aggregator 把大额卖出拆成 Pump AMM + Meteora DLMM 等
      //   _parseFullVault/_parseCpiFallback 只看 Pump vault 变化，漏掉其他 DEX 部分
      //   但卖家钱包的 baseMint 余额减少量 = 全部卖出（跨所有 DEX）
      //   修复: 对比卖家 baseMint 总减少 vs vault 收到的量，差额用 priceBefore 估算 SOL
      {
        const _txMeta = txMessage?.transaction?.meta || txMessage?.meta;
        const _preBal = _txMeta?.preTokenBalances || [];
        const _postBal = _txMeta?.postTokenBalances || [];
        const _tokenInfo = this.tokenRegistry.getToken(parsed.baseMint);
        const sellerBaseDelta = this._calcSellerBaseDelta(_preBal, _postBal, parsed.baseMint, parsed.signer, _tokenInfo);
        const vaultBaseDelta = parsed._poolBaseDelta || 0;
        if (sellerBaseDelta > 0 && vaultBaseDelta > 0 && sellerBaseDelta > vaultBaseDelta * 1.15) {
          // 卖家卖出量 > vault 收到量的 15% → 有拆单
          // 差额部分的 SOL 用 priceBefore 估算
          const extraBase = sellerBaseDelta - vaultBaseDelta;
          const extraSol = extraBase * parsed.priceBefore; // priceBefore = quote/base
          const totalSellSol = parsed.quoteAmount + extraSol;
          // 安全限制: 校准后 sellSol 不超过原来的 3 倍, 不超过池子 SOL 的一半
          const maxSellSol = Math.min(parsed.quoteAmount * 3, (parsed.poolQuoteAfter || 100) * 0.5);
          if (totalSellSol > parsed.quoteAmount * 1.05 && totalSellSol <= maxSellSol) {
            const oldSellSol = parsed.quoteAmount;
            const oldImpact = -parsed.priceChangePct;
            parsed.quoteAmount = totalSellSol;
            // 重算 impact: 简化用 sellSol 比例放大,但上限 maxPriceImpactPct
            const newImpact = oldImpact * (totalSellSol / oldSellSol);
            const maxImpact = config.strategy.maxPriceImpactPct || 30;
            parsed.priceChangePct = -Math.min(newImpact, maxImpact);
            monitor.inc('DumpDetector.aggCalibration', 1, 'DumpDetector');
            console.log(
              `[DumpDetector] 🔀 AggCalib: ${parsed.symbol || parsed.baseMint.slice(0, 6)} ` +
              `sellSol ${oldSellSol.toFixed(1)} → ${totalSellSol.toFixed(1)} SOL ` +
              `impact ${oldImpact.toFixed(1)}% → ${Math.min(newImpact, maxImpact).toFixed(1)}% ` +
              `(sellerBase=${sellerBaseDelta.toFixed(0)} vaultBase=${vaultBaseDelta.toFixed(0)} ratio=${(sellerBaseDelta/vaultBaseDelta).toFixed(1)}x)`,
            );
          }
        }
      }

      const sellSol = parsed.quoteAmount; // 用户得到的 quote (SOL)
      const priceImpactPct = -parsed.priceChangePct; // 转为正数表示跌幅
      const poolQuoteAfter = parsed.poolQuoteAfter; // 池子 SOL 余额

      // v3.10: 三条过滤
      // 1. sellSol 下限(决心卖单)
      // 2. priceImpact 在区间 [min, max]:太小没反弹空间;太大说明池子已经空了/流动性危险
      // 3. 池子流动性下限:太小的池子进出滑点大,容易亏在 spread 上
      const passSize = sellSol >= config.strategy.minSellSol;
      // v3.17.38: CPI/balanceOnly 路径算不出 poolQuoteAfter 时用 tokenRegistry 兜底
      //   这些币进监控列表前已经过 FDV/LP 筛选,不可能流动性不足
      //   poolQuoteAfter=0 是解析路径限制,不是池子真的没 SOL
      // v3.27-fix: Pump.fun池子解析的poolQuoteAfter经常严重偏低
      //   (Pump AMM vault结构不同于传统AMM，CPI路径读到的不是真实SOL余额)
      //   当tokenRegistry的liquidity远大于poolQuoteAfter时，用tokenRegistry值
      let effectivePoolQuoteSol = poolQuoteAfter;
      if (parsed.baseMint) {
        const tokenInfo = this.tokenRegistry?.getToken(parsed.baseMint);
        if (tokenInfo?.liquidity) {
          const registrySol = tokenInfo.liquidity / 170;
          if (!effectivePoolQuoteSol || registrySol > effectivePoolQuoteSol * 2) {
            // tokenRegistry数据更可信（来自Birdeye/链上主动查询），且远大于解析值
            // 解析值偏低通常是CPI路径限制，不是池子真的没SOL
            effectivePoolQuoteSol = registrySol;
          }
        }
      }
      // v3.27: 新币允许更低的impact(竞对 impact<5% 94.4%胜率赚331 SOL)
      const newCoinMinImpact = parseFloat(process.env.NEW_COIN_MIN_IMPACT_PCT || '0');
      let effectiveMinImpact = config.strategy.minPriceImpactPct;
      if (newCoinMinImpact >= 0 && newCoinMinImpact < effectiveMinImpact && parsed.baseMint) {
        const _ti = this.tokenRegistry?.getToken(parsed.baseMint);
        if (_ti && _ti.added_at) {
          const _age = Date.now() - _ti.added_at;
          const _threshold = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '0');
          if (_age < _threshold) {
            effectiveMinImpact = newCoinMinImpact;
          }
        }
      }
      const passImpact = priceImpactPct >= effectiveMinImpact
                      && priceImpactPct <= config.strategy.maxPriceImpactPct;
      const passLiquidity = effectivePoolQuoteSol >= config.strategy.minPoolQuot