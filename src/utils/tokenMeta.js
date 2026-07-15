'use strict';

const axios = require('axios');
const { config } = require('../config');

/**
 * 通过 Helius DAS API 获取代币基本信息（symbol, decimals, name）。
 */
async function fetchTokenAssetFromHelius(mint) {
  const url = config.helius.rpcUrl;
  const body = {
    jsonrpc: '2.0',
    id: 'getAsset',
    method: 'getAsset',
    params: { id: mint },
  };
  const { data } = await axios.post(url, body, { timeout: 8000 });
  if (data.error) throw new Error(`Helius getAsset error: ${JSON.stringify(data.error)}`);
  const asset = data.result;
  if (!asset) throw new Error('Helius returned empty asset');
  const meta = asset.content?.metadata || {};
  const tokenInfo = asset.token_info || {};
  return {
    mint,
    symbol: meta.symbol || tokenInfo.symbol || 'UNKNOWN',
    name: meta.name || 'Unknown',
    decimals: tokenInfo.decimals ?? 6,
    supply: tokenInfo.supply ?? null,
  };
}

/**
 * 通过 Birdeye 获取 FDV 和流动性等市场数据。
 */
async function fetchTokenMarketFromBirdeye(mint) {
  const url = `${config.birdeye.baseUrl}/defi/token_overview`;
  const headers = {
    'X-API-KEY': config.birdeye.apiKey,
    'x-chain': 'solana',
    accept: 'application/json',
  };
  const { data } = await axios.get(url, {
    headers,
    params: { address: mint },
    timeout: 8000,
  });
  if (!data?.success) throw new Error(`Birdeye token_overview failed: ${JSON.stringify(data)}`);
  const d = data.data || {};
  return {
    fdv: d.fdv ?? null,
    marketCap: d.mc ?? null,
    liquidity: d.liquidity ?? null,
    price: d.price ?? null,
    priceChange24h: d.priceChange24hPercent ?? null,
    volume24h: d.v24hUSD ?? null,
  };
}

/**
 * Fetch authority and creation metadata used to screen newly migrated tokens.
 * Birdeye has used both camelCase and snake_case fields over time, so normalize
 * the response here instead of leaking provider-specific names to callers.
 */
async function fetchTokenSecurityFromBirdeye(mint) {
  const url = `${config.birdeye.baseUrl}/defi/token_security`;
  const headers = {
    'X-API-KEY': config.birdeye.apiKey,
    'x-chain': 'solana',
    accept: 'application/json',
  };
  const { data } = await axios.get(url, {
    headers,
    params: { address: mint },
    timeout: 8000,
  });
  if (!data?.success || !data?.data) {
    throw new Error(`Birdeye token_security failed: ${JSON.stringify(data)}`);
  }

  const d = data.data;
  const normalizeAuthority = (value) => {
    if (value == null || value === false || value === '' || value === 'null') return null;
    return String(value);
  };
  return {
    mintAuthority: normalizeAuthority(d.mintAuthority ?? d.mint_authority),
    freezeAuthority: normalizeAuthority(d.freezeAuthority ?? d.freeze_authority),
    creationTime: d.creationTime ?? d.mintTime ?? d.creation_time ?? null,
    creationSlot: d.creationSlot ?? d.mintSlot ?? d.creation_slot ?? null,
  };
}

/**
 * 获取代币创建时间。
 * 优先用 Birdeye token_security，失败时回退到 Helius Enhanced Transactions API。
 * 返回 { creationTime: unix_seconds, creationSlot: number } 或 null。
 */
async function fetchTokenCreationTime(mint) {
  // 1. Try Birdeye first (most reliable, has exact creation time)
  const birdeyeResult = await _fetchCreationFromBirdeye(mint);
  if (birdeyeResult?.creationTime) return birdeyeResult;

  // 2. Fallback: Helius Enhanced Transactions API
  const heliusResult = await _fetchCreationFromHelius(mint);
  if (heliusResult?.creationTime) return heliusResult;

  return null;
}

/**
 * Birdeye token_security — 返回 Pump.fun 等平台的原始创建时间。
 */
async function _fetchCreationFromBirdeye(mint) {
  const url = `${config.birdeye.baseUrl}/defi/token_security`;
  const headers = {
    'X-API-KEY': config.birdeye.apiKey,
    'x-chain': 'solana',
    accept: 'application/json',
  };
  try {
    const { data } = await axios.get(url, {
      headers,
      params: { address: mint },
      timeout: 8000,
    });
    if (!data?.success || !data?.data) return null;
    const d = data.data;
    return {
      creationTime: d.creationTime ?? d.mintTime ?? null,
      creationSlot: d.creationSlot ?? d.mintSlot ?? null,
    };
  } catch (err) {
    return null;
  }
}

/**
 * Helius Enhanced Transactions API — 从交易的 INITIAL_TRANSACTION 类型
 * 或最早交易推算创建时间。只返回最近100笔中的最早记录，可能不准确。
 */
async function _fetchCreationFromHelius(mint) {
  try {
    const apiKey = config.helius.apiKey;
    if (!apiKey) return null;
    const url = `https://api.helius.xyz/v0/addresses/${mint}/transactions?api-key=${apiKey}`;
    const { data } = await axios.get(url, { timeout: 8000 });
    if (!Array.isArray(data) || data.length === 0) return null;

    // Look for INITIAL_TRANSACTION type first
    const initTx = data.find(t => t.type === 'INITIAL_TRANSACTION' || t.type === 'CREATE');
    if (initTx?.timestamp) {
      return { creationTime: initTx.timestamp, creationSlot: null };
    }

    // Otherwise use the earliest transaction in the batch (last element)
    const earliest = data[data.length - 1];
    if (earliest?.timestamp) {
      return { creationTime: earliest.timestamp, creationSlot: null };
    }
    return null;
  } catch (err) {
    return null;
  }
}

/**
 * 只调 Birdeye 获取市场数据（FDV/liquidity/price），不调 Helius getAsset。
 * 用于 Watchdog 定期刷新，symbol/name/decimals 用 DB 已有值，省掉 Helius API 调用。
 */
async function fetchTokenMarketOnly(mint) {
  let market = {};
  let birdeyeError = null;
  try {
    market = await fetchTokenMarketFromBirdeye(mint);
  } catch (err) {
    birdeyeError = err;
  }
  return { ...market, fetchedAt: Date.now(), _birdeyeError: birdeyeError };
}

/**
 * 综合调用：返回完整代币信息。Birdeye 失败时不阻塞，返回部分信息。
 */
async function fetchTokenFullInfo(mint) {
  const asset = await fetchTokenAssetFromHelius(mint);
  let market = {};
  let birdeyeError = null;
  try {
    market = await fetchTokenMarketFromBirdeye(mint);
  } catch (err) {
    console.warn(`[tokenMeta] Birdeye fetch failed for ${mint}: ${err.message}`);
    birdeyeError = err;
  }
  return { ...asset, ...market, fetchedAt: Date.now(), _birdeyeError: birdeyeError };
}

module.exports = {
  fetchTokenMarketOnly,
  fetchTokenAssetFromHelius,
  fetchTokenMarketFromBirdeye,
  fetchTokenSecurityFromBirdeye,
  fetchTokenCreationTime,
  fetchTokenFullInfo,
};
