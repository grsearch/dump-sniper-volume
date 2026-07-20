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
  }

  setPoolStateCache(cache) {
    this.poolStateCache = cache;
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
      const passLiquidity = effectivePoolQuoteSol >= config.strategy.minPoolQuoteSol;
      const passAll = passSize && passImpact && passLiquidity;

      this.emit('sellAnalyzed', {
        mint: parsed.baseMint,
        symbol: parsed.symbol,
        sellSol,
        priceImpactPct,
        poolQuoteAfter: effectivePoolQuoteSol || poolQuoteAfter, // v3.17.38: 用有效值
        passSize,
        passImpact,
        passLiquidity,
        seller: parsed.signer,
        signature: parsed.signature,
        ts: parsed.ts,
        poolAddress: parsed.poolAddress,
        priceAfter: parsed.priceAfter,
        priceBefore: parsed.priceBefore,
      });

      // v3.17.15: 跨 slot RUG 检测已移除 - RUG 必须同 slot(相同 GAS)才算

      // v3.17.13: 记录所有卖出(不管是否通过过滤),用于累计砸盘检测
      const recentMint = parsed.baseMint;
      if (!this._recentSells.has(recentMint)) {
        this._recentSells.set(recentMint, []);
      }
      this._recentSells.get(recentMint).push({
        sellSol,
        priceBefore: parsed.priceBefore,
        priceAfter: parsed.priceAfter,
        ts: parsed.ts,
        slot: parsed.slot || null, // v3.17.20: 供 CompetitorTracker 算 dump→buy slot lag
      });

      if (passAll) {
        // v3.17.16 (FAST-PATH): 立即 emit dumpSignal,零延迟
        //
        // 上一版 (v3.17.15) 加了 500ms setTimeout 等"是否有协调出货",
        // 但 500ms = 1.25 slot,让所有单笔砸单都晚 1-2 个 slot 才发信号,
        // 导致 BUY 永远买在反弹之后,亏多赚少(实战已验证)。
        //
        // 新策略:
        //   - 单笔 passAll 的砸单:立即 emit(追求最快进 BUY 通道)
        //   - 同 slot 协调出货检测:放在 SignalEngine 层做(dedup map 已经能
        //     防止同一 mint 短时间内重复触发)
        //   - 仍然把这笔记到 slot 桶里供 AGGREGATED 路径使用,但 _checkSlotBucket
        //     里有 bucket.fired 的标记防止 single 已经 fire 后 AGGREGATED 再 fire
        if (process.env.DUMP_SIGNAL_DEBUG === 'true') {
          console.log(
            `[DumpDetector] 🚨 dump signal (fast-path): ${parsed.symbol || parsed.baseMint.slice(0, 6)} ` +
            `sellSol=${sellSol.toFixed(2)} impact=${priceImpactPct.toFixed(1)}% ` +
            `slot=${parsed.slot} sig=${parsed.signature?.slice(0, 12) || 'n/a'}..`,
          );
        }
        monitor.inc('DumpDetector.dumpSignals', 1, 'DumpDetector');
        this._emitSingleDumpSignal(parsed, sellSol, priceImpactPct, poolQuoteAfter);
        // 把这笔加入 slot 桶,但标记 fired=true(避免 AGGREGATED 路径重复 emit)
        this._accumulateSlotSell(parsed, /* alreadyFired */ true);
      } else {
        // v3.17.14: 同 slot 聚合检测
        // 同一 slot 内多笔小卖单可能累积成大砸盘,单笔检测不到
        // 每笔小单记录到 slot 桶,立即检查桶内总 SOL 和累积 impact
        this._accumulateSlotSell(parsed);
      }
    } catch (err) {
      monitor.inc('DumpDetector.parseErrors', 1, 'DumpDetector');
      monitor.recordError('DumpDetector', err, {
        signature: this._extractSignature(txMessage?.transaction),
      });
      {
      const fs = require('fs');
      fs.appendFileSync('/tmp/dd_errors.log', new Date().toISOString() + ' ' + err.message + '\n' + (err.stack || '').split('\n').slice(0,8).join('\n') + '\n---\n');
      console.error('[DumpDetector] parse error: ' + err.message);
    }
    }
  }

  /**
   * v3.17.14: 同 slot 卖单实时聚合
   * 同一 slot 内对同一 mint 的多笔小卖单累积为一次大砸盘检测
   * 实战:Bear 同 slot 7 笔小单 (8-16 SOL),单笔 impact 不够,但累积 95 SOL / 40%+
   *
   * 关键:每收到一笔就立即检查聚合结果,不等待!
   * 同 slot 交易在 LaserStream 里通常几十毫秒内连续到达,
   * 等待 2 秒 = 错过 5 个 slot = 来不及买入
   */
  _slotSells = new Map(); // key: `${slot}:${mint}` → { sells: [...], fired }
  _slotSellCleanup = null;

  /**
   * v3.17.16: 立即 emit 单笔 dumpSignal(零延迟)
   * 不再做"等 500ms 看是否协调出货"--协调出货检测靠:
   *   1) SignalEngine 层的 sellerTxDedupMs / sellerMintDedupMs(不会同一 mint 反复触发)
   *   2) AGGREGATED 路径(多卖家小单累计达到阈值时另发 _aggregated signal)
   *   3) RUG 信号(同 slot 5+ 卖家 → 强制卖出)
   */
  _emitSingleDumpSignal(parsed, sellSol, priceImpactPct, poolQuoteAfter) {
    // v3.17.36: 增加 10s 滚动窗口统计,跟 competitor_trades.trigger_sell_count 对齐
    const recentStats = this.getRecentMaxSell(parsed.baseMint);
    this.emit('dumpSignal', {
      mint: parsed.baseMint,
      symbol: parsed.symbol,
      sellSol,
      priceImpactPct,
      poolQuoteAfter,
      seller: parsed.signer,
      signature: parsed.signature,
      ts: parsed.ts,
      slot: parsed.slot,
      poolAddress: parsed.poolAddress,
      poolBaseVault: parsed.poolBaseVault,
      poolQuoteVault: parsed.poolQuoteVault,
      priceAfter: parsed.priceAfter,
      priceBefore: parsed.priceBefore,
      baseDecimals: parsed.baseDecimals,
      quoteDecimals: parsed.quoteDecimals,
      _sellCount10s: recentStats ? recentStats.sellCount : 1,
      _totalSellSol10s: recentStats ? recentStats.totalSellSol : sellSol,
    });
  }

  _accumulateSlotSell(parsed, alreadyFired = false) {
    const key = `${parsed.slot}:${parsed.baseMint}`;
    let bucket = this._slotSells.get(key);
    if (!bucket) {
      bucket = { sells: [], fired: false };
      this._slotSells.set(key, bucket);
    }

    // 仍然 push 到 bucket(用于 RUG 检测计数,即使 buy signal 已经 fired)
    bucket.sells.push({
      mint: parsed.baseMint,
      sellSol: parsed.quoteAmount,
      priceImpactPct: -parsed.priceChangePct,
      poolQuoteAfter: parsed.poolQuoteAfter,
      seller: parsed.signer,
      signature: parsed.signature,
      ts: parsed.ts,
      slot: parsed.slot,
      symbol: parsed.symbol,
      poolAddress: parsed.poolAddress,
      priceBefore: parsed.priceBefore,
      priceAfter: parsed.priceAfter,
      poolBaseVault: parsed.poolBaseVault,
      poolQuoteVault: parsed.poolQuoteVault,
      baseDecimals: parsed.baseDecimals,
      quoteDecimals: parsed.quoteDecimals,
    });

    if (alreadyFired) {
      // v3.17.16: 调用方已经发出 single signal,标记 fired 避免后续 AGGREGATED 二次发射
      bucket.fired = true;
    }

    if (bucket.fired) {
      // 已经 fired(无论本次 single 还是历史 AGGREGATED),不再检查 _checkSlotBucket(避免重复 buy signal)
      // 但 RUG 检测仍然要跑(独立 rugFired 标志保护)
      const totalSellSol = bucket.sells.reduce((s, x) => s + x.sellSol, 0);
      const sellers = [...new Set(bucket.sells.map(s => s.seller).filter(Boolean))];
      this._checkRug(bucket, totalSellSol, sellers, bucket.sells[bucket.sells.length - 1]);
    } else {
      // 立即检查聚合结果!(内部会跑 _checkRug)
      this._checkSlotBucket(key);
    }

    // 延迟清理过期桶(10 秒后清理,防内存泄漏)
    if (!this._slotSellCleanup) {
      this._slotSellCleanup = setTimeout(() => {
        this._slotSells.clear();
        this._slotSellCleanup = null;
      }, 10_000);
      if (this._slotSellCleanup.unref) this._slotSellCleanup.unref();
    }
  }

  _checkSlotBucket(key) {
    const bucket = this._slotSells.get(key);
    if (!bucket || bucket.sells.length < 2) return;

    // 聚合计算
    const totalSellSol = bucket.sells.reduce((sum, s) => sum + s.sellSol, 0);
    const lastSell = bucket.sells[bucket.sells.length - 1];
    const firstSell = bucket.sells[0];
    // 累积 impact:从第一笔的 priceBefore 到最后一笔的 priceAfter
    const priceBefore = firstSell.priceBefore;
    const priceAfter = lastSell.priceAfter;
    let cumulativeImpactPct = 0;
    if (priceBefore > 0 && priceAfter > 0) {
      cumulativeImpactPct = ((priceBefore - priceAfter) / priceBefore) * 100;
    }
    const poolQuoteAfter = lastSell.poolQuoteAfter;
    const sellers = [...new Set(bucket.sells.map(s => s.seller).filter(Boolean))];

    // 过滤
    const passSize = totalSellSol >= config.strategy.minSellSol;
    // v3.27: 新币允许更低的impact
    const newCoinMinImpact = parseFloat(process.env.NEW_COIN_MIN_IMPACT_PCT || '0');
    let effectiveAggMinImpact = config.strategy.minPriceImpactPct;
    if (newCoinMinImpact >= 0 && newCoinMinImpact < effectiveAggMinImpact) {
      const aggTokenInfo = this.tokenRegistry?.getToken(lastSell.mint);
      const aggTokenAgeMs = Date.now() - (aggTokenInfo?.added_at || 0);
      const newCoinThresholdMs = parseFloat(process.env.NEW_COIN_AGE_THRESHOLD_MS || '0');
      if (aggTokenInfo && aggTokenInfo.added_at && aggTokenAgeMs < newCoinThresholdMs) {
        effectiveAggMinImpact = newCoinMinImpact;
      }
    }
    const passImpact = cumulativeImpactPct >= effectiveAggMinImpact
                    && cumulativeImpactPct <= config.strategy.maxPriceImpactPct;
    // v3.27-fix: 同单笔路径的修复，Pump.fun解析值偏低用tokenRegistry
    let effectiveAggPoolQuoteSol = poolQuoteAfter;
    if (lastSell.mint) {
      const tokenInfo = this.tokenRegistry?.getToken(lastSell.mint);
      if (tokenInfo?.liquidity) {
        const registrySol = tokenInfo.liquidity / 170;
        if (!effectiveAggPoolQuoteSol || registrySol > effectiveAggPoolQuoteSol * 2) {
          effectiveAggPoolQuoteSol = registrySol;
        }
      }
    }
    const passLiquidity = effectiveAggPoolQuoteSol >= config.strategy.minPoolQuoteSol;

    if (!passSize || !passImpact || !passLiquidity) {
      // 还不够触发 AGGREGATED buy signal,但仍要检查 RUG(防止只数小卖单的场景漏掉)
      this._checkRug(bucket, totalSellSol, sellers, lastSell);
      return;
    }

    // 达标!立即触发聚合信号
    bucket.fired = true;
    monitor.inc('DumpDetector.dumpSignalsAggregated', 1, 'DumpDetector');
    console.log(
      `[DumpDetector] 🚨🚨 AGGREGATED dump signal: ${lastSell.symbol || '???'} ` +
      `${bucket.sells.length} sells in slot ${lastSell.slot}, ` +
      `totalSellSol=${totalSellSol.toFixed(1)} cumImpact=${cumulativeImpactPct.toFixed(1)}% ` +
      `poolAfter=${(poolQuoteAfter || 0).toFixed(0)} SOL sellers=${sellers.join(',')}`,
    );
    // v3.17.36: 10s 窗口统计
    const aggRecentStats = this.getRecentMaxSell(lastSell.mint || bucket.sells[0].mint);
    this.emit('dumpSignal', {
      mint: lastSell.mint || bucket.sells[0].mint,
      symbol: lastSell.symbol,
      sellSol: totalSellSol,
      priceImpactPct: cumulativeImpactPct,
      poolQuoteAfter,
      seller: sellers[0], // 主卖家(第一个)
      signature: lastSell.signature, // 用最后一笔的 sig
      ts: lastSell.ts,
      slot: lastSell.slot,
      poolAddress: lastSell.poolAddress,
      poolBaseVault: lastSell.poolBaseVault,
      poolQuoteVault: lastSell.poolQuoteVault,
      priceAfter: lastSell.priceAfter,
      priceBefore: firstSell.priceBefore,
      baseDecimals: lastSell.baseDecimals,
      quoteDecimals: lastSell.quoteDecimals,
      _aggregated: true, // 标记为聚合信号
      _sellCount: bucket.sells.length,
      _sellCount10s: aggRecentStats ? aggRecentStats.sellCount : bucket.sells.length,
      _totalSellSol10s: aggRecentStats ? aggRecentStats.totalSellSol : totalSellSol,
      _sellers: sellers,
    });

    // RUG 检测一并执行
    this._checkRug(bucket, totalSellSol, sellers, lastSell);
  }

  /**
   * v3.17.16: RUG 检测独立路径
   *   同 slot 5+ 笔卖出、合计 ≥ 5 SOL → 强制卖出持仓
   *   不依赖 bucket.fired 状态,无论 single 还是 AGGREGATED 路径 fire 过都仍然检查
   *   防止 RUG 在 single fire 之后才到的小卖单中被漏掉
   */
  _checkRug(bucket, totalSellSol, sellers, lastSell) {
    if (bucket.rugFired) return; // 已经发过 rug signal
    if (bucket.sells.length < 5 || totalSellSol < 5) return;
    bucket.rugFired = true;
    monitor.inc('DumpDetector.rugSignals', 1, 'DumpDetector');
    console.log(
      `[DumpDetector] 🚨🚨🚨 RUG PULL detected: ${lastSell.symbol || '???'} ` +
      `${bucket.sells.length} sells in slot ${lastSell.slot}, ` +
      `totalSellSol=${totalSellSol.toFixed(1)} SOL, ${sellers.length} sellers - forcing exit`,
    );
    this.emit('rugSignal', {
      mint: lastSell.mint || bucket.sells[0].mint,
      symbol: lastSell.symbol,
      sellSol: totalSellSol,
      sellCount: bucket.sells.length,
      sellers,
      slot: lastSell.slot,
      ts: lastSell.ts,
    });
  }

  /**
   * 解析交易,返回 { side, baseMint, quoteAmount, priceChangePct, ... } 或 null。
   *
   * 算法:
   *   1. 在 pre/postTokenBalances 里找属于监控代币的 mint
   *   2. 查 tokenRegistry.getToken(mint).pool_base_vault / pool_quote_vault
   *   3. 在 pre/postTokenBalances 的 accountIndex/owner 里精确定位这两个 vault 的变化
   *   4. 计算 baseBefore/baseAfter/quoteBefore/quoteAfter,得到价格和方向
   */
  _parseTx(txMessage) {
    const tx = txMessage.transaction;
    if (!tx) return null;
    const meta = tx.meta;
    if (!meta || meta.err) return null;

    // v3.17.7: 提取 slot 用于下游过期判断
    // yellowstone gRPC 把 slot 编码成 string,我们一路传到 SignalEngine
    const slotRaw = txMessage.slot;
    const slot = slotRaw != null
      ? (typeof slotRaw === 'string' ? Number(slotRaw) : slotRaw)
      : null;

    const signature = this._extractSignature(tx);
    const signer = this._extractSigner(tx);

    const preBalances = meta.preTokenBalances || [];
    const postBalances = meta.postTokenBalances || [];
    if (preBalances.length === 0 || postBalances.length === 0) return null;

    // 找出涉及的监控代币
    let baseMint = null;
    let baseDecimals = 6;
    for (const b of preBalances) {
      if (this.tokenRegistry.isActive(b.mint)) {
        baseMint = b.mint;
        baseDecimals = b.uiTokenAmount?.decimals ?? 6;
        break;
      }
    }
    if (!baseMint) {
      // 也可能在 post 里出现(极少见的 case,比如 base ATA 是这次新建的)
      for (const b of postBalances) {
        if (this.tokenRegistry.isActive(b.mint)) {
          baseMint = b.mint;
          baseDecimals = b.uiTokenAmount?.decimals ?? 6;
          break;
        }
      }
    }
    if (!baseMint) {
      // v3.17.14 debug: 检查为什么 BABYWOJAK 等活跃币的卖单没被识别
      const mintsInTx = [...new Set([...preBalances.map(b => b.mint), ...postBalances.map(b => b.mint)])];
      const watched = mintsInTx.filter(m => this.tokenRegistry.isActive(m));
      if (mintsInTx.length > 0 && watched.length === 0 && mintsInTx.some(m => m.includes('pump'))) {
        console.log(`[DumpDetector] DEBUG: tx with ${mintsInTx.length} mints but 0 watched: ${mintsInTx.slice(0,3).join(',')}..`);
      }
      return null;
    }

    const tokenInfo = this.tokenRegistry.getToken(baseMint);
    if (!tokenInfo) {
      monitor.inc('DumpDetector.noTokenInfo', 1, 'DumpDetector');
      return null;
    }

    // 必须有 pool base vault 信息才解析
    const poolBaseVault = tokenInfo.pool_base_vault;
    const poolQuoteVault = tokenInfo.pool_quote_vault;
    if (!poolBaseVault) {
      monitor.inc('DumpDetector.skippedNoPoolInfo', 1, 'DumpDetector');
      return null;
    }

    // accountKeys (静态 + loaded address)
    const staticKeys = tx.transaction?.message?.accountKeys || [];
    const loadedWritable = meta.loadedWritableAddresses || [];
    const loadedReadonly = meta.loadedReadonlyAddresses || [];
    const allKeys = [
      ...staticKeys.map((k) => encodeBase58(k)),
      ...loadedWritable.map((k) => encodeBase58(k)),
      ...loadedReadonly.map((k) => encodeBase58(k)),
    ];

    // 在 accountKeys 中找 vault 对应的 accountIndex
    const baseVaultIdx = allKeys.findIndex((k) => k === poolBaseVault);
    const quoteVaultIdx = poolQuoteVault
      ? allKeys.findIndex((k) => k === poolQuoteVault)
      : -1;

    // v3.17.26 DEBUG: removed (YOTS_MINT was undefined → ReferenceError crashed ALL parsing)

    // 两个 vault 都不在 → 尝试纯余额路径(Jupiter V4/V6 等深度聚合路由)
    if (baseVaultIdx < 0 && quoteVaultIdx < 0) {
      monitor.inc('DumpDetector.poolNotInTx', 1, 'DumpDetector');
      const balanceParsed = this._parseBalanceOnly(
        tx, meta, preBalances, postBalances,
        baseMint, baseDecimals, tokenInfo,
        signature, signer, slot, allKeys,
      );
      // v3.17.38: balanceOnly 已修复(v3.17.23),现在正式启用返回值
      //   之前 return null 导致 Jupiter/OKX 路由的卖单全部丢失
      if (balanceParsed) {
        monitor.inc('DumpDetector.balanceOnlyHit', 1, 'DumpDetector');
        return balanceParsed;
      }
      monitor.inc('DumpDetector.poolNotInTxAndBalanceMiss', 1, 'DumpDetector');
      return null;
    }

    // ---- 完整路径:base_vault + quote_vault 都在 ----
    if (baseVaultIdx >= 0 && quoteVaultIdx >= 0) {
      return this._parseFullVault(
        tx, meta, preBalances, postBalances,
        baseVaultIdx, quoteVaultIdx, baseMint, baseDecimals,
        tokenInfo, poolBaseVault, poolQuoteVault,
        signature, signer, slot, allKeys,
      );
    }

    // ---- CPI 回退路径:只有 base_vault 在(Jupiter/OKX/Flash 等 bot 路由) ----
    if (baseVaultIdx >= 0) {
      monitor.inc('DumpDetector.cpiFallback', 1, 'DumpDetector');
      return this._parseCpiFallback(
        tx, meta, preBalances, postBalances,
        baseVaultIdx, baseMint, baseDecimals,
        tokenInfo, poolBaseVault, poolQuoteVault,
        signature, signer, slot, allKeys,
      );
    }

    // quote_vault 在但 base_vault 不在 - 不太可能,但安全处理
    monitor.inc('DumpDetector.poolNotInTx', 1, 'DumpDetector');
    return null;
  }

  /**
   * 完整路径解析:base_vault + quote_vault 都在 accountKeys 中。
   * 直接读两个 vault 的 token balance 变化,精确算价格和方向。
   * 这是 v2 的原始逻辑,对 Pump AMM 直调交易。
   */
  _parseFullVault(
    tx, meta, preBalances, postBalances,
    baseVaultIdx, quoteVaultIdx, baseMint, baseDecimals,
    tokenInfo, poolBaseVault, poolQuoteVault,
    signature, signer, slot, allKeys,
  ) {
    const baseBefore = this._findBalance(preBalances, baseVaultIdx, baseMint);
    const baseAfter = this._findBalance(postBalances, baseVaultIdx, baseMint);
    const quoteBefore = this._findBalance(preBalances, quoteVaultIdx, WSOL_MINT);
    const quoteAfter = this._findBalance(postBalances, quoteVaultIdx, WSOL_MINT);

    if (
      baseBefore === null || baseAfter === null ||
      quoteBefore === null || quoteAfter === null
    ) {
      monitor.inc('DumpDetector.vaultBalanceMissing', 1, 'DumpDetector');
      return null;
    }

    const poolBaseDelta = baseAfter - baseBefore;
    const poolQuoteDelta = quoteAfter - quoteBefore;

    if (
      !Number.isFinite(baseBefore) || !Number.isFinite(baseAfter) ||
      !Number.isFinite(quoteBefore) || !Number.isFinite(quoteAfter) ||
      baseBefore <= 0 || baseAfter <= 0 ||
      quoteBefore <= 0 || quoteAfter <= 0
    ) {
      return null;
    }

    const priceBefore = quoteBefore / baseBefore;
    const priceAfter = quoteAfter / baseAfter;
    if (priceBefore <= 0 || priceAfter <= 0) return null;
    const priceChangePct = ((priceAfter - priceBefore) / priceBefore) * 100;

    let side;
    if (poolBaseDelta > 0 && poolQuoteDelta < 0) side = 'SELL';
    else if (poolBaseDelta < 0 && poolQuoteDelta > 0) side = 'BUY';
    else return null;

    const quoteAmount = Math.abs(poolQuoteDelta);

    return {
      signature,
      signer,
      ts: Date.now(),
      slot,
      side,
      baseMint,
      baseDecimals,
      quoteDecimals: 9,
      symbol: tokenInfo.symbol || null,
      quoteAmount,
      priceBefore,
      priceAfter,
      priceChangePct,
      poolAddress: tokenInfo.pool_address,
      poolBaseVault,
      poolQuoteVault,
      poolQuoteAfter: quoteAfter,
      poolBaseAfter: baseAfter,
      _poolBaseDelta: poolBaseDelta,
      source: 'direct',
    };
  }

  /**
   * CPI 回退路径解析:只有 base_vault 在 accountKeys 中。
   * 典型场景:Jupiter / OKX DEX Router / Flash / Trojan 等通过 CPI 调 Pump AMM,
   * 但 Jupiter 用中间 WSOL wrapping 账户路由,导致 pool_quote_vault 不在交易中。
   *
   * 算法:
   *   1. 读 base_vault 余额变化 → 确定 swap 方向和 base 数量
   *   2. 卖出时 base_vault 增加 → 找所有 WSOL token balance 减少(或 SOL native)的账户
   *   3. 用 base_vault 变化推算价格(需要参考 PoolStateCache 的实时价格)
   *
   * 关键限制:无法精确读到 quote_vault 的 SOL 变化,
   * 所以 quoteAmount 用 base_vault 变化 × 参考价格推算,
   * priceChangePct 用 base_vault 前后比例变化近似计算。
   */
  _parseCpiFallback(
    tx, meta, preBalances, postBalances,
    baseVaultIdx, baseMint, baseDecimals,
    tokenInfo, poolBaseVault, poolQuoteVault,
    signature, signer, slot, allKeys,
  ) {
    // 1. 读 base_vault 余额变化
    const baseBefore = this._findBalance(preBalances, baseVaultIdx, baseMint);
    const baseAfter = this._findBalance(postBalances, baseVaultIdx, baseMint);

    if (baseBefore === null || baseAfter === null) {
      monitor.inc('DumpDetector.cpiVaultBalanceMissing', 1, 'DumpDetector');
      return null;
    }

    if (!Number.isFinite(baseBefore) || !Number.isFinite(baseAfter) || baseBefore <= 0 || baseAfter <= 0) {
      return null;
    }

    const poolBaseDelta = baseAfter - baseBefore;

    // 2. 方向判定:base 增加 = 有人往池子卖代币 = SELL
    let side;
    if (poolBaseDelta > 0) side = 'SELL';
    else if (poolBaseDelta < 0) side = 'BUY';
    else return null; // 无变化,跳过

    // 3. 估算 quoteAmount(SOL)
    //    CPI 路由下 quote_vault 不在交易中,但可以估算:
    //    卖出 = 用户得到的 SOL ≈ 池子失去的 SOL
    //    用 AMM 常数乘积近似:
    //      quote_out = quote_before - (quote_before * base_before / base_after)
    //    这等价于: quoteAmount = quote_before * (1 - base_before / base_after)
    //    简化为: quoteAmount ≈ |baseDelta| * price_before
    //
    //    更好的方法:遍历 preTokenBalances 找所有 WSOL 账户的变化之和
    //    但 CPI 中间账户会混淆,所以我们用 base_vault 变化 + AMM 近似

    // 用 AMM 常数乘积近似报价
    // 需要池子的当前 quote/base reserve - 从 PoolStateCache 获取
    const poolState = this.poolStateCache
      ? this.poolStateCache.get(tokenInfo.pool_address)
      : null;

    let quoteAmount = 0;
    let priceBefore = 0;
    let priceAfter = 0;
    let priceChangePct = 0;
    let poolQuoteAfter = 0;

    if (poolState && poolState.poolQuoteAmount && poolState.poolBaseAmount) {
      // 有实时池子状态,用 AMM 常数乘积精确计算
      // BN → number(lamports → SOL, raw base → ui amount)
      const qBefore = poolState.poolQuoteAmount.toNumber
        ? poolState.poolQuoteAmount.toNumber() / 1e9
        : Number(poolState.poolQuoteAmount) / 1e9;
      const bBefore = poolState.poolBaseAmount.toNumber
        ? poolState.poolBaseAmount.toNumber() / Math.pow(10, baseDecimals)
        : Number(poolState.poolBaseAmount) / Math.pow(10, baseDecimals);

      if (qBefore > 0 && bBefore > 0) {
        priceBefore = qBefore / bBefore;
        const qAfter = (qBefore * bBefore) / baseAfter;
        quoteAmount = Math.abs(qBefore - qAfter);
        priceAfter = qAfter / baseAfter;
        priceChangePct = ((priceAfter - priceBefore) / priceBefore) * 100;
        poolQuoteAfter = qAfter;
      } else {
        priceBefore = 1;
        priceAfter = baseBefore / baseAfter;
        priceChangePct = ((priceAfter - priceBefore) / priceBefore) * 100;
        quoteAmount = 0;
        poolQuoteAfter = 0;
      }
    } else {
      // 没有实时池子状态(hotMints 改造后非热币的常态)
      // 用 base_vault 余额变化 + AMM 常数乘积近似计算 quoteAmount
      //
      // AMM 常数乘积:k = base × quote 不变
      //   price_after / price_before = (quote_after / base_after) / (quote_before / base_before)
      //                               = (base_before / base_after)2    (因为 k 恒定)
      //
      // quote_out = quote_before - quote_after
      //           = quote_before × (1 - base_before / base_after)
      //
      // 我们不知道 quote_before,但可以从 price 和 base 间接算:
      //   quote_before = price_before × base_before
      //   但归一化 priceBefore=1 时:
      //   quoteAmount ≈ |baseDelta| × priceBefore = |baseDelta| × 1(归一化)
      //
      // 更好的方法:用 base 余额比例直接算 priceChangePct 和近似 quoteAmount
      const baseRatio = baseBefore / baseAfter; // >1 for SELL (base increased)
      priceChangePct = (1 - baseRatio * baseRatio) * 100; // = (1 - (baseBefore/baseAfter)2) × 100
      priceBefore = 1; // 归一化
      priceAfter = baseRatio * baseRatio; // price_after/price_before = (base_before/base_after)2
      // quoteAmount 近似:|baseDelta| / baseBefore × 池子 quote 规模
      //   不知道池子规模,用 baseDelta 比例近似(偏保守)
      //   实际 quoteAmount ≈ |poolBaseDelta| × priceBefore = |poolBaseDelta| (归一化)
      //   但这不够准确 - 用另一个方法:从交易里找 SOL native balance 变化
      const solDelta = this._estimateCpiSolDelta(meta, signer, allKeys);
      quoteAmount = solDelta;
      // 补充:找 baseMint 余额减少最多的账户的 SOL 变化(Jupiter 路由时 signer 不是卖家)
      if (quoteAmount <= 0) {
        const bestSeller = this._findBestBaseSeller(preBalances, postBalances, baseMint, poolBaseVault, poolQuoteVault);
        if (bestSeller) {
          const sellerSolDelta = this._estimateCpiSolDelta(meta, bestSeller, allKeys);
          if (sellerSolDelta > 0) quoteAmount = sellerSolDelta;
        }
      }
      poolQuoteAfter = 0;
      monitor.inc('DumpDetector.cpiNoPoolState', 1, 'DumpDetector');
    }

    // CPI 路径只处理 SELL 且 quoteAmount > 0 的情况
    if (side === 'SELL' && quoteAmount <= 0) {
      // v3.17.26 DEBUG: removed (YOTS_MINT was undefined → ReferenceError)
      return null;
    }

    // v3.17.39: [REVERTED] CPI impact校正代码已回滚
    //   校正逻辑干扰了正常信号生成,导致信号量断崖下降
    //   保留 estimatedPoolSol 用 tokenRegistry 的修复(在 balanceOnly 路径)

    return {
      signature,
      signer,
      ts: Date.now(),
      slot,
      side,
      baseMint,
      baseDecimals,
      quoteDecimals: 9,
      symbol: tokenInfo.symbol || null,
      quoteAmount,
      priceBefore,
      priceAfter,
      priceChangePct,
      poolAddress: tokenInfo.pool_address,
      poolBaseVault,
      poolQuoteVault: poolQuoteVault || null,
      poolQuoteAfter,
      poolBaseAfter: baseAfter,
      _poolBaseDelta: poolBaseDelta,
      source: 'cpi',
    };
  }

  /**
   * 在 balances 数组里找指定 accountIndex + mint 的余额。
   * 返回 ui 数额(float),找不到返回 null。
   */
  _findBalance(balances, accountIndex, expectedMint) {
    for (const b of balances) {
      if (b.accountIndex !== accountIndex) continue;
      if (expectedMint && b.mint !== expectedMint) continue;
      const v = safeTokenAmount(b.uiTokenAmount);
      if (v <= 0 && b.uiTokenAmount?.amount !== '0') return null; // amount有值但解析0 → 异常
      return v;
    }
    return null;
  }

  _extractSignature(tx) {
    try {
      const sig = tx?.transaction?.signatures?.[0];
      return encodeBase58(sig);
    } catch (_) {
      return null;
    }
  }

  _extractSigner(tx) {
    try {
      const accountKeys = tx?.transaction?.message?.accountKeys || [];
      return encodeBase58(accountKeys[0]);
    } catch (_) {
      return null;
    }
  }

  /**
   * v3.17.21: 估算 CPI 路径中卖家得到的 SOL 数量
   * CPI 交易(Jupiter/OKX 等)没有 pool_quote_vault 在 accountKeys 里,
   * 但可以从交易元数据的 pre/postBalances 找 signer 的 SOL native balance 变化。
   *
   * 算法:
   *   1. 在 meta.preBalances / meta.postBalances 里找 signer 的 accountIndex
   *   2. SOL delta = post - pre(包含了 fee、中间账户等)
   *   3. 卖出时 SOL delta 为正(用户收到 SOL)
   *   4. 扣掉交易 fee(约 0.000005 SOL,可忽略)
   *
   * 注意:这个值是**用户钱包实际收到的 SOL**,是最准确的 quoteAmount。
   * 比 PoolStateCache + AMM 近似更准(因为 AMM 近似不考虑 fee_program 扣费等)。
   */
  /**
   * v3.17.23: 在 pre/postTokenBalances 中找 baseMint 余额减少最多的账户
   * 用于 Jupiter/OKX 等聚合路由场景--signer 不是真正的卖家
   */
  _findBestBaseSeller(preBalances, postBalances, baseMint, poolBaseVault, poolQuoteVault) {
    const balanceMap = new Map(); // owner → { before, after }

    for (const b of preBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.before = amount;
      balanceMap.set(owner, existing);
    }
    for (const b of postBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.after = amount;
      balanceMap.set(owner, existing);
    }

    let bestSeller = null;
    let bestTokensSold = 0;

    for (const [owner, bal] of balanceMap) {
      if (owner === poolBaseVault || owner === poolQuoteVault) continue;
      const delta = bal.after - bal.before;
      if (delta < -0.0001 && Math.abs(delta) > bestTokensSold) {
        bestSeller = owner;
        bestTokensSold = Math.abs(delta);
      }
    }

    return bestSeller;
  }

  _estimateCpiSolDelta(meta, signer, allKeys) {
    if (!meta || !signer) return 0;
    try {
      // 找 signer 在 allKeys 里的 index
      // allKeys 可能是 Buffer[] 或 string[],统一处理
      let signerIdx = -1;
      for (let i = 0; i < allKeys.length; i++) {
        const key = encodeBase58(allKeys[i]);
        if (key === signer) {
          signerIdx = i;
          break;
        }
      }
      if (signerIdx < 0) return 0;

      const preBalances = meta.preBalances || [];
      const postBalances = meta.postBalances || [];
      if (signerIdx >= preBalances.length || signerIdx >= postBalances.length) return 0;

      const preLamports = preBalances[signerIdx] || 0;
      const postLamports = postBalances[signerIdx] || 0;
      const deltaLamports = postLamports - preLamports;

      // 卖出时 delta 应该为正(用户收到 SOL)
      // 但 fee 会扣一些,所以小正值也可能是 fee 导致的
      // 只取正值,转为 SOL
      if (deltaLamports <= 0) return 0;
      return deltaLamports / 1e9;
    } catch (err) {
      return 0;
    }
  }

  /**
   * v3.17.21: 纯余额路径 - vault 账户不在 accountKeys 时的 fallback
   *
   * 场景:Jupiter V4/V6、OKX DEX Router 等深度聚合路由,
   * 它们通过多层 CPI 调 Pump AMM,但顶层 accountKeys 里
   * 既没有 pool_base_vault 也没有 pool_quote_vault。
   *
   * 算法:
   *   1. 在 pre/postTokenBalances 里找 signer 的 baseMint 余额变化
   *   2. signer base 余额减少 = SELL(有人在卖这个币)
   *   3. quoteAmount = signer SOL native balance 变化
   *   4. priceChangePct 从 AMM 近似(base 比例变化)
   *
   * 精度:quoteAmount 是用户实际收到的 SOL(最准),
   *       priceChangePct 是近似值(用 base reserve 比例估算)
   */
  _parseBalanceOnly(
    tx, meta, preBalances, postBalances,
    baseMint, baseDecimals, tokenInfo,
    signature, signer, slot, allKeys,
  ) {
    // v3.17.23: 纯余额路径 - vault 不在 accountKeys 时的 fallback
    //
    // 核心改进:不只看 signer,而是扫描所有账户的 baseMint 余额变化
    // 找余额减少最多的账户作为卖家。这样可以捕获:
    //   - Jupiter 聚合路由(signer 是 Jupiter 程序,不是原卖家)
    //   - OKX/Flash/Trojan 等 bot 路由
    //   - 多层 CPI 嵌套
    //
    // 算法:
    //   1. 构建所有账户的 baseMint 余额变化 map
    //   2. 找余额减少最多的账户(= 真正的卖家)
    //   3. 排除池子账户(baseVault/quoteVault 的 owner)
    //   4. quoteAmount = 卖家 SOL native balance 变化 或 AMM 近似
    //   5. priceChangePct 从 AMM 近似

    // 1. 构建所有账户的 baseMint pre/post 余额
    const balanceMap = new Map(); // owner → { before, after }

    for (const b of preBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.before = amount;
      balanceMap.set(owner, existing);
    }
    for (const b of postBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.after = amount;
      balanceMap.set(owner, existing);
    }

    if (balanceMap.size === 0) return null;

    // 2. 排除已知池子账户,找余额减少最多的账户
    const poolBaseVault = tokenInfo.pool_base_vault;
    const poolQuoteVault = tokenInfo.pool_quote_vault;

    let bestSeller = null;
    let bestTokensSold = 0;

    for (const [owner, bal] of balanceMap) {
      // 跳过池子 vault 账户(它们的 base 余额是增加的,不是卖家)
      if (owner === poolBaseVault || owner === poolQuoteVault) continue;

      const delta = bal.after - bal.before;
      if (delta < -0.0001 && Math.abs(delta) > bestTokensSold) {
        bestSeller = owner;
        bestTokensSold = Math.abs(delta);
      }
    }

    if (!bestSeller || bestTokensSold <= 0) return null;

    const tokensSold = bestTokensSold;
    const side = 'SELL';

    // 3. 算 quoteAmount
    let quoteAmount = 0;
    let priceBefore = 0;
    let priceAfter = 0;
    let priceChangePct = 0;
    let poolQuoteAfter = 0;

    // 先试卖家 SOL native balance 变化(最准)
    const solDelta = this._estimateCpiSolDelta(meta, bestSeller, allKeys);
    if (solDelta > 0) quoteAmount = solDelta;

    // 也试 signer 的 SOL 变化(Jupiter 路由时 signer 可能是用户钱包)
    if (quoteAmount <= 0 && signer && signer !== bestSeller) {
      const signerSolDelta = this._estimateCpiSolDelta(meta, signer, allKeys);
      if (signerSolDelta > 0) quoteAmount = signerSolDelta;
    }

    // 用 PoolStateCache 算价格影响和补充 quoteAmount
    const poolState = this.poolStateCache
      ? this.poolStateCache.get(tokenInfo.pool_address)
      : null;

    if (poolState && poolState.poolQuoteAmount && poolState.poolBaseAmount) {
      const qBefore = poolState.poolQuoteAmount.toNumber
        ? poolState.poolQuoteAmount.toNumber() / 1e9
        : Number(poolState.poolQuoteAmount) / 1e9;
      const bBefore = poolState.poolBaseAmount.toNumber
        ? poolState.poolBaseAmount.toNumber() / Math.pow(10, baseDecimals)
        : Number(poolState.poolBaseAmount) / Math.pow(10, baseDecimals);
      if (qBefore > 0 && bBefore > 0) {
        priceBefore = qBefore / bBefore;
        const bAfter = bBefore + tokensSold;
        const qAfter = (qBefore * bBefore) / bAfter;
        if (quoteAmount <= 0) quoteAmount = Math.abs(qBefore - qAfter);
        priceAfter = qAfter / bAfter;
        priceChangePct = ((priceAfter - priceBefore) / priceBefore) * 100;
        poolQuoteAfter = qAfter;
      }
    }

    // 如果 PoolStateCache 没数据 + SOL delta 也 0 → 无法确定卖出金额
    if (quoteAmount <= 0) return null;

    // 如果没有 PoolStateCache 数据,priceChangePct 也无法精确计算
    // v3.17.39: 用 tokenRegistry.liquidity 推算真实池子大小,替代硬编码 estimatedPoolSol=30
    //   旧 bug: 假设池子 30 SOL,实际 388 SOL → 5 SOL 砸单算出 17% impact(实际 1.3%)
    if (priceChangePct === 0 && tokensSold > 0 && quoteAmount > 0) {
      let estimatedPoolSol = 0;
      if (this.tokenRegistry) {
        const ti = this.tokenRegistry.getToken(baseMint);
        if (ti && ti.liquidity) {
          estimatedPoolSol = ti.liquidity / 170; // USD → SOL 粗估
        }
      }
      if (estimatedPoolSol <= 0) estimatedPoolSol = 30; // fallback
      // v3.17.39: AMM 非线性近似，而非简单除法
      //   简单除法: -(quoteAmount / poolSol) * 100 → 低估约一半
      //   例: 13SOL/199SOL 简单=6.5% 但 CPMM 实际≈13%
      //   CPMM 精确: Δq = quoteAmount, q = poolSol
      //   price_change = -(2*Δq/q) / (2 - Δq/q)  (一阶近似展开)
      //   更精确: price_change = 1 - (1 - Δq/q)² (AMM 不变量)
      const qRatio = quoteAmount / estimatedPoolSol; // <1 for normal trades
      if (qRatio > 0 && qRatio < 1) {
        priceChangePct = (1 - Math.pow(1 - qRatio, 2)) * -100; // CPMM 不变量
      } else {
        priceChangePct = -(quoteAmount / estimatedPoolSol) * 100; // fallback for edge cases
      }
    }

    // v3.17.39: [REVERTED] balanceOnly impact校正代码已回滚
    //   校正逻辑干扰了正常信号生成

    monitor.inc('DumpDetector.balanceOnlyParsed', 1, 'DumpDetector');

    return {
      signature,
      signer: bestSeller, // 用真正的卖家而非 tx signer
      ts: Date.now(),
      slot,
      side,
      baseMint,
      baseDecimals,
      quoteDecimals: 9,
      symbol: tokenInfo.symbol || null,
      quoteAmount,
      priceBefore,
      priceAfter,
      priceChangePct,
      poolAddress: tokenInfo.pool_address,
      poolBaseVault: poolBaseVault || null,
      poolQuoteVault: poolQuoteVault || null,
      poolQuoteAfter,
      poolBaseAfter: 0,
      source: 'balance_only',
    };
  }

  /**
   * v3.26: 计算卖家钱包在整个 TX 中的 baseMint 余额总减少量
   * 用于 Aggregator 拆单校准 — 一个 TX 里走了 Pump AMM + Meteora DLMM 等多步
   * vault 只反映 Pump AMM 那步，但卖家实际卖出了更多
   */
  _calcSellerBaseDelta(preBalances, postBalances, baseMint, signer, tokenInfo) {
    const poolBaseVault = tokenInfo?.pool_base_vault;
    const poolQuoteVault = tokenInfo?.pool_quote_vault;
    let totalDelta = 0;

    const balanceMap = new Map();
    for (const b of preBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      if (owner === poolBaseVault || owner === poolQuoteVault) continue;
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.before = amount;
      balanceMap.set(owner, existing);
    }
    for (const b of postBalances) {
      if (b.mint !== baseMint) continue;
      const owner = encodeBase58(b.owner);
      if (owner === poolBaseVault || owner === poolQuoteVault) continue;
      const amount = safeTokenAmount(b.uiTokenAmount);
      const existing = balanceMap.get(owner) || { before: 0, after: 0 };
      existing.after = amount;
      balanceMap.set(owner, existing);
    }

    for (const [owner, bal] of balanceMap) {
      const delta = bal.after - bal.before;
      if (delta < -0.0001) {
        totalDelta += Math.abs(delta);
      }
    }

    return totalDelta;
  }
}

module.exports = DumpDetector;
module.exports.PUMP_AMM_PROGRAM_ID = PUMP_AMM_PROGRAM_ID;
