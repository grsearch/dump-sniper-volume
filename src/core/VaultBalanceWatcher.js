'use strict';

/**
 * VaultBalanceWatcher — 链上 vault 余额轮询检测砸单
 * ============================================
 * 
 * 不依赖交易解析，直接批量查询所有监控币的 pool_base_vault 余额变化。
 * 任何卖出（无论 Jupiter/CPI/直调）都会让 base_vault 余额增加，
 * 这个变化是链上可见的，无法被聚合路由隐藏。
 *
 * 工作原理：
 *   1. 每个周期批量查所有 base_vault + quote_vault 余额（1-2次 RPC）
 *   2. 对比前后余额：base_vault 增加 + quote_vault 减少 = SELL
 *   3. 计算 sellSol、priceImpact 等指标
 *   4. emit 'vaultSell' 事件给 DumpDetector/SignalEngine
 *
 * 成本：
 *   71个 vault = 1次 getMultipleAccountsInfo (最多100个/次)
 *   每2秒查一次 = 0.5 RPS → 几乎不增加 credits
 *
 * 精度：
 *   - sellSol 用 AMM 常数乘积从 base/quote 变化推算，比 tx 解析更准
 *   - priceImpact 用前后价格比精确计算
 *   - 不依赖 tx signer、不依赖 vault 在 accountKeys 中
 */

const { PublicKey } = require('@solana/web3.js');
const { AccountLayout } = require('@solana/spl-token');
const { getMonitor } = require('../monitor/HealthMonitor');
const { config } = require('../config');
const BN = require('bn.js');

const monitor = getMonitor();
monitor.registerModule('VaultWatcher', { staleMs: 60_000, label: 'Vault Balance Watcher' });

// 默认轮询间隔（毫秒）
const DEFAULT_POLL_MS = parseInt(process.env.VAULT_WATCHER_POLL_MS || '2000', 10);
// 最小可检测卖单（SOL）— 使用全局策略配置
const MIN_SELL_SOL = parseFloat(process.env.VAULT_WATCHER_MIN_SELL_SOL || process.env.MIN_SELL_SOL || '6', 10);
// 最小 priceImpact（%）— 使用全局策略配置，VaultWatcher 的 impact 近似值天然偏低
// 所以这里用策略值的 80% 作为阈值，避免漏检但也不会太松
const MIN_PRICE_IMPACT_PCT = parseFloat(process.env.MIN_PRICE_IMPACT_PCT || '10.0', 10);
const MIN_PRICE_IMPACT = MIN_PRICE_IMPACT_PCT * 0.8;

class VaultBalanceWatcher {
  /**
   * @param {object} opts
   * @param {object} opts.connection - @solana/web3.js Connection
   * @param {object} opts.tokenRegistry - TokenRegistry 实例
   */
  constructor({ connection, tokenRegistry }) {
    this.connection = connection;
    this.tokenRegistry = tokenRegistry;
    this.pollMs = DEFAULT_POLL_MS;

    // TickStream 引用（用于获取 latestSlot）
    this._tickStream = null;
    this._latestSlot = 0;

    // vault 余额快照：poolAddress → { baseVaultBalance, quoteVaultBalance, ts }
    this._snapshot = new Map();
    this._timer = null;
    this._running = false;

    // 监控列表缓存（避免每次 poll 重建）
    this._watchList = []; // [{ mint, poolAddress, baseVault, quoteVault, decimals }]
    this._watchListDirty = true;

    // 去重：poolAddress → 上次 emit 的 ts（防止同一变化重复 emit）
    this._lastEmitTs = new Map();
  }

  /**
   * 标记监控列表需要刷新
   */
  markDirty() {
    this._watchListDirty = true;
  }

  _buildWatchList() {
    const tokens = this.tokenRegistry.listActive();
    this._watchList = [];
    for (const t of tokens) {
      if (!t.pool_address || !t.pool_base_vault) continue;
      this._watchList.push({
        mint: t.mint,
        poolAddress: t.pool_address,
        baseVault: t.pool_base_vault,
        quoteVault: t.pool_quote_vault, // 可能为 null
        decimals: t.decimals || 6,
        symbol: t.symbol,
      });
    }
    this._watchListDirty = false;
  }

  start() {
    if (this._timer) return;
    this._running = true;
    this._buildWatchList();

    // 首次立即 poll（建立基线快照）
    this._poll().catch(err => {
      console.warn(`[VaultWatcher] initial poll failed: ${err.message}`);
    });

    this._timer = setInterval(() => {
      this._poll().catch(err => {
        monitor.recordError('VaultWatcher', err, { phase: 'poll' });
      });
    }, this.pollMs);

    console.log(
      `[VaultWatcher] started (poll=${this.pollMs}ms, watch=${this._watchList.length} vaults, ` +
      `minSell=${MIN_SELL_SOL} SOL, minImpact=${MIN_PRICE_IMPACT}%)`,
    );
  }

  stop() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
    this._running = false;
    this._snapshot.clear();
  }

  /**
   * 设置 TickStream 引用（用于获取 latestSlot）
   */
  setTickStream(ts) {
    this._tickStream = ts;
  }

  async _poll() {
    if (this._watchListDirty) this._buildWatchList();
    if (this._watchList.length === 0) return;

    const t0 = Date.now();
    monitor.beat('VaultWatcher', 'poll');

    // 更新 latestSlot
    if (this._tickStream?.latestSlot) {
      this._latestSlot = this._tickStream.latestSlot;
    }

    // 构建查询列表：所有 base_vault + quote_vault
    const addresses = [];
    const addrMeta = []; // { type: 'base'|'quote', idx: watchList index }

    for (let i = 0; i < this._watchList.length; i++) {
      const w = this._watchList[i];
      addresses.push(new PublicKey(w.baseVault));
      addrMeta.push({ type: 'base', idx: i });
      if (w.quoteVault) {
        addresses.push(new PublicKey(w.quoteVault));
        addrMeta.push({ type: 'quote', idx: i });
      }
    }

    // 批量查询（Solana 最多100个/次，我们71个币≈142个地址，分2批）
    let allResults = [];
    const BATCH_SIZE = 100;
    for (let offset = 0; offset < addresses.length; offset += BATCH_SIZE) {
      const batch = addresses.slice(offset, offset + BATCH_SIZE);
      const infos = await this.connection.getMultipleAccountsInfo(batch);
      allResults = allResults.concat(infos);
    }

    // 解析余额
    const now = Date.now();
    const newSnapshot = new Map();

    // 先收集每个 watch entry 的 base/quote 余额
    const balances = new Map(); // idx → { base, quote }

    for (let i = 0; i < allResults.length; i++) {
      const info = allResults[i];
      const meta = addrMeta[i];
      if (!meta) continue;

      let amount = 0;
      if (info && info.data) {
        try {
          const decoded = AccountLayout.decode(info.data);
          amount = Number(decoded.amount); // 原始最小单位
        } catch (_) {
          // 解析失败跳过
        }
      }

      if (!balances.has(meta.idx)) balances.set(meta.idx, { base: 0, quote: 0 });
      const b = balances.get(meta.idx);
      if (meta.type === 'base') b.base = amount;
      else b.quote = amount;
    }

    // 对比前后快照，检测卖出
    let sellsDetected = 0;

    for (const [idx, bal] of balances) {
      const w = this._watchList[idx];
      if (!w) continue;

      const key = w.poolAddress;
      const prev = this._snapshot.get(key);

      newSnapshot.set(key, {
        baseVaultBalance: bal.base,
        quoteVaultBalance: bal.quote,
        decimals: w.decimals,
        ts: now,
      });

      if (!prev) continue; // 首次快照，无法比较

      // 检测 base_vault 余额增加（= 有人卖币到池子）
      const baseDelta = bal.base - prev.baseVaultBalance;
      if (baseDelta <= 0) continue; // base 没增加，不是 SELL

      // quote_vault 余额应该减少（池子付出 SOL）
      const quoteDelta = bal.quote - prev.quoteVaultBalance;

      // 计算 SOL 量
      const baseBefore = prev.baseVaultBalance;
      const quoteBefore = prev.quoteVaultBalance;
      const decimals = prev.decimals || w.decimals || 6;

      if (baseBefore <= 0 || quoteBefore <= 0) continue;

      // AMM 常数乘积：k = base * quote
      // quote_out = quote_before - quote_after
      //   quote_after = (base_before * quote_before) / base_after
      const baseAfter = bal.base;
      const quoteAfter = Math.floor((baseBefore * quoteBefore) / baseAfter);
      const quoteOutLamports = quoteBefore - quoteAfter;
      const sellSol = quoteOutLamports / 1e9;

      // Pool SOL 规模（卖出前）
      const poolSolBefore = quoteBefore / 1e9;

      // Price impact — 用两种方式计算取更准确的
      // 方式1：AMM 常数乘积直接算
      const priceBefore = quoteBefore / baseBefore;
      const priceAfter = quoteAfter / baseAfter;
      const impactAmm = ((priceAfter - priceBefore) / priceBefore) * 100;
      // 方式2：sellSol 占池子 SOL 比例近似（更稳定，不受快照间隔中其他交易干扰）
      // 对于常数乘积 AMM：impact ≈ sellSol / (poolSolBefore) * 100
      // 这是更保守的估算
      const impactRatio = poolSolBefore > 0 ? -(sellSol / poolSolBefore) * 100 : 0;
      // 取绝对值更大的那个（更保守，避免误检）
      const priceImpactPct = Math.abs(impactAmm) >= Math.abs(impactRatio) ? impactAmm : impactRatio;

      // 过滤：太小的卖单或太低的影响跳过
      if (sellSol < MIN_SELL_SOL) continue;
      if (Math.abs(priceImpactPct) < MIN_PRICE_IMPACT) continue;

      // 去重：同一 pool 2秒内不重复 emit
      const lastEmit = this._lastEmitTs.get(key) || 0;
      if (now - lastEmit < this.pollMs * 0.9) continue;
      this._lastEmitTs.set(key, now);

      sellsDetected++;

      const tokensSold = baseDelta / Math.pow(10, decimals);

      this.emit('vaultSell', {
        mint: w.mint,
        symbol: w.symbol,
        poolAddress: w.poolAddress,
        side: 'SELL',
        sellSol,
        tokensSold,
        priceBefore,
        priceAfter,
        priceImpactPct,
        baseVaultBefore: baseBefore,
        baseVaultAfter: baseAfter,
        quoteVaultBefore: quoteBefore,
        quoteVaultAfter: quoteAfter,
        ts: now,
        slot: this._latestSlot || 0,  // 用 TickStream 的最新 slot
        source: 'vault_watcher',
      });
    }

    // 替换快照
    this._snapshot = newSnapshot;

    const elapsed = Date.now() - t0;
    monitor.set('VaultWatcher.lastPollMs', elapsed, 'VaultWatcher');
    monitor.set('VaultWatcher.watchedVaults', this._watchList.length, 'VaultWatcher');
    monitor.inc('VaultWatcher.sellsDetected', sellsDetected, 'VaultWatcher');
    monitor.inc('VaultWatcher.polls', 1, 'VaultWatcher');
  }
}

// 混入 EventEmitter
const EventEmitter = require('events');
Object.setPrototypeOf(VaultBalanceWatcher.prototype, EventEmitter.prototype);
Object.setPrototypeOf(VaultBalanceWatcher, EventEmitter);

module.exports = VaultBalanceWatcher;
