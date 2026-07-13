#!/usr/bin/env node
'use strict';

/**
 * verify-pools.js (v3.17.18)
 * ==========================
 * 检查 tokenRegistry 里每个代币的 pool 地址是否仍然指向**当前活跃的 Pump AMM 池**。
 *
 * 背景:
 *   Pump.fun 代币的生命周期:
 *     1) bonding curve 阶段 — 使用 Pump bonding curve program
 *     2) 毕业 → 迁移到 Pump AMM (pAMMBay6...) — 这才是我们要监控的
 *
 *   `fill-pools.js` 在代币还在 bonding curve 时跑、或者在迁移期间跑,
 *   会把错误的池子地址写进 tokenRegistry,导致 LaserStream 订阅错池子的
 *   vault → DumpDetector 永远收不到 tx → **静默漏所有信号**。
 *
 *   这个脚本的工作:
 *     1) 对每个代币的 pool_address,用 RPC getAccountInfo 拿 account owner
 *     2) owner 必须是 Pump AMM program,否则标记为错
 *     3) 对 pool_base_vault, getAccountInfo 看 SPL Token Account 的 mint 字段
 *     4) mint 字段必须 == 代币本身 mint,否则标记为错
 *     5) 错的代币 → 默认只 report; 加 --fix 后重跑 PoolFinder 找正确地址
 *
 * 用法:
 *   node scripts/verify-pools.js                  # 只 report (推荐先跑)
 *   node scripts/verify-pools.js --fix            # report + 自动 fix 错的
 *   node scripts/verify-pools.js --fix --force    # 重新 verify 所有 (包括正确的)
 *   node scripts/verify-pools.js MINT1 MINT2      # 只 verify 指定 mint
 *
 * 建议: 加到 cron 每天跑一次 (只 report 模式),发现问题手动 --fix
 *   0 6 * * *  cd /path/to/dump-sniper && node scripts/verify-pools.js >> logs/verify-pools.log 2>&1
 */

require('dotenv').config();
const axios = require('axios');
const { PublicKey } = require('@solana/web3.js');
const TokenRegistry = require('../src/data/TokenRegistry');
const PoolFinder = require('../src/utils/poolFinder');
const { config } = require('../src/config');

const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const PUMP_BONDING_CURVE_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';

const RPC_URL = config.helius.rpcUrl;

/**
 * 批量 getAccountInfo (jsonParsed 模式让 SPL Token Account 自动解析出 mint)
 */
async function getAccountsInfo(addresses) {
  if (addresses.length === 0) return [];
  // getMultipleAccounts 单次最多 100 个
  const out = [];
  for (let i = 0; i < addresses.length; i += 100) {
    const batch = addresses.slice(i, i + 100);
    const body = {
      jsonrpc: '2.0',
      id: 1,
      method: 'getMultipleAccounts',
      params: [batch, { encoding: 'jsonParsed', commitment: 'confirmed' }],
    };
    try {
      const { data } = await axios.post(RPC_URL, body, { timeout: 15000 });
      if (data.error) throw new Error(JSON.stringify(data.error));
      for (let j = 0; j < batch.length; j++) {
        out.push({ address: batch[j], info: (data.result?.value || [])[j] });
      }
    } catch (err) {
      console.error(`[verify-pools] RPC batch failed: ${err.message}`);
      for (const addr of batch) out.push({ address: addr, info: null, error: err.message });
    }
  }
  return out;
}

/**
 * 校验一个池子三元组:
 *   - poolAddress 的 account owner 是否 = Pump AMM
 *   - poolBaseVault 是否 SPL Token Account 且 mint 字段 == 我们的 mint
 *   - poolQuoteVault 是否 SPL Token Account 且 mint 字段 == WSOL
 *
 * 返回:
 *   { ok: true }                          → 三元组都对
 *   { ok: false, reason: '...' }          → 至少一项错
 *   { ok: false, reason: '...', poolOwnerIsBondingCurve: true }  → 还在 bonding curve
 */
function verifyPoolTuple(token, accountInfos) {
  const { mint, pool_address, pool_base_vault, pool_quote_vault } = token;
  const WSOL = 'So11111111111111111111111111111111111111112';

  // 1) pool_address 必须是 Pump AMM PDA
  if (!pool_address) return { ok: false, reason: 'pool_address is null' };
  const pa = accountInfos.get(pool_address);
  if (!pa) return { ok: false, reason: 'pool_address: RPC returned no data' };
  if (!pa.info) return { ok: false, reason: 'pool_address: account not found on chain' };
  if (pa.info.owner === PUMP_BONDING_CURVE_PROGRAM) {
    return {
      ok: false,
      reason: `pool_address is BONDING CURVE account (owner=${pa.info.owner.slice(0, 8)}..)`,
      poolOwnerIsBondingCurve: true,
    };
  }
  if (pa.info.owner !== PUMP_AMM_PROGRAM) {
    return { ok: false, reason: `pool_address owner=${pa.info.owner} ≠ Pump AMM` };
  }

  // 2) pool_base_vault 必须是 SPL Token Account, mint 字段 == 代币 mint
  if (!pool_base_vault) return { ok: false, reason: 'pool_base_vault is null' };
  const bv = accountInfos.get(pool_base_vault);
  if (!bv) return { ok: false, reason: 'pool_base_vault: RPC returned no data' };
  if (!bv.info) return { ok: false, reason: 'pool_base_vault: account not found on chain' };
  if (bv.info.owner !== TOKEN_PROGRAM && bv.info.owner !== TOKEN_2022_PROGRAM) {
    return { ok: false, reason: `pool_base_vault owner=${bv.info.owner} ≠ SPL Token Program` };
  }
  const bvMint = bv.info.data?.parsed?.info?.mint;
  if (bvMint !== mint) {
    return { ok: false, reason: `pool_base_vault mint=${bvMint?.slice(0, 8)}.. ≠ token mint ${mint.slice(0, 8)}..` };
  }

  // 3) pool_quote_vault 必须是 SPL Token Account, mint 字段 == WSOL
  if (!pool_quote_vault) return { ok: false, reason: 'pool_quote_vault is null' };
  const qv = accountInfos.get(pool_quote_vault);
  if (!qv) return { ok: false, reason: 'pool_quote_vault: RPC returned no data' };
  if (!qv.info) return { ok: false, reason: 'pool_quote_vault: account not found on chain' };
  if (qv.info.owner !== TOKEN_PROGRAM && qv.info.owner !== TOKEN_2022_PROGRAM) {
    return { ok: false, reason: `pool_quote_vault owner=${qv.info.owner} ≠ SPL Token Program` };
  }
  const qvMint = qv.info.data?.parsed?.info?.mint;
  if (qvMint !== WSOL) {
    return { ok: false, reason: `pool_quote_vault mint=${qvMint?.slice(0, 8)}.. ≠ WSOL` };
  }

  return { ok: true };
}

async function fixToken(registry, finder, token, log) {
  log(`  → re-running PoolFinder for ${token.symbol || token.mint.slice(0, 8)}...`);
  try {
    const result = await finder.findPoolForMint(token.mint);
    if (!result) {
      log(`  ❌ PoolFinder still can't find Pump AMM pool — token may not be graduated yet`);
      return false;
    }
    if (result.poolAddress === token.pool_address && result.poolBaseVault === token.pool_base_vault) {
      log(`  ⚠️  PoolFinder returned same (wrong) pool — likely Helius indexed stale data`);
      return false;
    }
    registry.setPoolInfo(token.mint, result);
    log(`  ✅ Updated: pool=${result.poolAddress.slice(0, 6)}.. ` +
        `base=${result.poolBaseVault.slice(0, 6)}.. quote=${result.poolQuoteVault.slice(0, 6)}..`);
    return true;
  } catch (err) {
    log(`  ❌ PoolFinder error: ${err.message}`);
    return false;
  }
}

(async () => {
  const args = process.argv.slice(2);
  const doFix = args.includes('--fix');
  const force = args.includes('--force');
  const explicit = args.filter((a) => !a.startsWith('--'));

  const registry = new TokenRegistry();
  const finder = new PoolFinder({});

  let tokens;
  if (explicit.length > 0) {
    tokens = explicit.map((m) => registry.getToken(m)).filter(Boolean);
    if (tokens.length === 0) {
      console.log('No matching tokens found in registry');
      process.exit(1);
    }
  } else {
    tokens = registry.listAll();
  }

  if (tokens.length === 0) {
    console.log('No tokens in registry');
    process.exit(0);
  }

  console.log(`[verify-pools] checking ${tokens.length} tokens (mode: ${doFix ? 'FIX' : 'REPORT ONLY'})\n`);

  // 收集所有要查的 account 地址
  const addrsToFetch = new Set();
  for (const t of tokens) {
    if (t.pool_address) addrsToFetch.add(t.pool_address);
    if (t.pool_base_vault) addrsToFetch.add(t.pool_base_vault);
    if (t.pool_quote_vault) addrsToFetch.add(t.pool_quote_vault);
  }

  console.log(`[verify-pools] fetching ${addrsToFetch.size} accounts via RPC...`);
  const results = await getAccountsInfo([...addrsToFetch]);
  const accountInfos = new Map();
  for (const r of results) accountInfos.set(r.address, r);

  // 校验每个 token
  let okCount = 0;
  let bondingCurveCount = 0;
  let wrongPoolCount = 0;
  let missingCount = 0;
  const broken = [];

  for (const t of tokens) {
    const symbol = t.symbol || t.mint.slice(0, 8);

    // 跳过没填 pool 的(可能还没毕业,不算 broken)
    if (!t.pool_address && !t.pool_base_vault && !t.pool_quote_vault) {
      missingCount++;
      console.log(`  ⏳ ${symbol.padEnd(12)} no pool info (not graduated yet?) — skip`);
      continue;
    }

    const result = verifyPoolTuple(t, accountInfos);

    if (result.ok) {
      okCount++;
      if (force) {
        console.log(`  ✅ ${symbol.padEnd(12)} OK (pool=${t.pool_address.slice(0, 6)}..)`);
      }
    } else {
      if (result.poolOwnerIsBondingCurve) {
        bondingCurveCount++;
        console.log(`  🟡 ${symbol.padEnd(12)} STILL ON BONDING CURVE — ${result.reason}`);
      } else {
        wrongPoolCount++;
        console.log(`  ❌ ${symbol.padEnd(12)} WRONG — ${result.reason}`);
      }
      broken.push({ token: t, result });
    }
  }

  console.log('');
  console.log(`──────────────────────────────────────────`);
  console.log(`Total: ${tokens.length}`);
  console.log(`  ✅ OK:                     ${okCount}`);
  console.log(`  ⏳ no pool (not graduated): ${missingCount}`);
  console.log(`  🟡 still on bonding curve:  ${bondingCurveCount}`);
  console.log(`  ❌ WRONG pool address:      ${wrongPoolCount}`);
  console.log(`──────────────────────────────────────────`);

  if (broken.length === 0) {
    console.log('\n🎉 All pools are valid Pump AMM pools.');
    process.exit(0);
  }

  if (!doFix) {
    console.log(`\n⚠️  Found ${broken.length} tokens with broken pool info.`);
    console.log(`    Re-run with --fix to attempt repair.`);
    console.log(`    Tokens still on bonding curve will be skipped (not graduated yet).`);
    process.exit(1);
  }

  // --fix 模式
  console.log(`\n[verify-pools] attempting to fix ${broken.length} tokens...\n`);
  let fixed = 0;
  let failed = 0;
  for (const { token, result } of broken) {
    if (result.poolOwnerIsBondingCurve) {
      console.log(`  ⏭  ${token.symbol || token.mint.slice(0, 8)} — skip (still on bonding curve)`);
      continue;
    }
    const ok = await fixToken(registry, finder, token, (m) => console.log(m));
    if (ok) fixed++; else failed++;
    // 节流,避免 RPC 限速
    await new Promise(r => setTimeout(r, 500));
  }

  console.log('');
  console.log(`──────────────────────────────────────────`);
  console.log(`Fix result: ${fixed} fixed, ${failed} failed`);
  console.log(`──────────────────────────────────────────`);

  if (fixed > 0) {
    console.log('\n⚠️  IMPORTANT: Restart dump-sniper to load new pool info!');
    console.log('    sudo systemctl restart dump-sniper');
  }
  process.exit(failed > 0 ? 1 : 0);
})().catch(err => {
  console.error(`[verify-pools] fatal: ${err.message}`);
  console.error(err.stack);
  process.exit(2);
});
