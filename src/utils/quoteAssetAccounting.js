'use strict';

const LAMPORTS_PER_SOL = 1_000_000_000n;
const DEFAULT_WSOL_MINT = 'So11111111111111111111111111111111111111112';

function toLamports(value) {
  if (typeof value === 'bigint') return value;
  const n = Number(value);
  return Number.isFinite(n) ? BigInt(Math.trunc(n)) : 0n;
}

function rawTokenAmount(balance) {
  const raw = balance?.uiTokenAmount?.amount;
  if (raw == null || raw === '') return 0n;
  try {
    return BigInt(raw);
  } catch (_) {
    return 0n;
  }
}

function sumOwnedWsol(tokenBalances, wallet, wsolMint = DEFAULT_WSOL_MINT) {
  let total = 0n;
  for (const balance of tokenBalances || []) {
    if (balance?.owner !== wallet || balance?.mint !== wsolMint) continue;
    total += rawTokenAmount(balance);
  }
  return total;
}

function sumOwnedWsolAccountLamports(
  tokenBalances,
  nativeBalances,
  wallet,
  wsolMint = DEFAULT_WSOL_MINT,
) {
  const indexes = new Set();
  for (const balance of tokenBalances || []) {
    if (balance?.owner !== wallet || balance?.mint !== wsolMint) continue;
    if (Number.isInteger(balance.accountIndex)) indexes.add(balance.accountIndex);
  }
  let total = 0n;
  for (const index of indexes) total += toLamports(nativeBalances?.[index]);
  return total;
}

function accountKeyString(key) {
  if (!key) return '';
  if (typeof key === 'string') return key;
  if (key.pubkey) return accountKeyString(key.pubkey);
  if (typeof key.toBase58 === 'function') return key.toBase58();
  if (typeof key.toString === 'function') return key.toString();
  return '';
}

function findWalletIndex(tx, wallet) {
  const keys = transactionAccountKeys(tx);
  return keys.findIndex((key) => accountKeyString(key) === wallet);
}

function transactionAccountKeys(tx) {
  const message = tx?.transaction?.message;
  if (!message) return [];
  if (typeof message.getAccountKeys === 'function') {
    try {
      const resolved = message.getAccountKeys({
        accountKeysFromLookups: tx?.meta?.loadedAddresses,
      });
      if (resolved && Number.isInteger(resolved.length)) {
        return Array.from({ length: resolved.length }, (_, index) => resolved.get(index));
      }
    } catch (_) { /* fall through to JSON/RPC key lists */ }
  }
  const staticKeys = message.accountKeys || message.staticAccountKeys || [];
  const loaded = tx?.meta?.loadedAddresses;
  return [
    ...staticKeys,
    ...(loaded?.writable || []),
    ...(loaded?.readonly || []),
  ];
}

function sumTrackedWsol(
  tokenBalances,
  accountKeys,
  trackedAccounts,
  wallet,
  wsolMint = DEFAULT_WSOL_MINT,
) {
  const tracked = new Set(trackedAccounts || []);
  if (tracked.size === 0) return 0n;
  let total = 0n;
  for (const balance of tokenBalances || []) {
    if (balance?.mint !== wsolMint || balance?.owner === wallet) continue;
    const address = accountKeyString(accountKeys[balance.accountIndex]);
    if (!tracked.has(address)) continue;
    total += rawTokenAmount(balance);
  }
  return total;
}

function lamportsToSol(lamports) {
  return Number(lamports) / Number(LAMPORTS_PER_SOL);
}

/**
 * Compute quote-asset movement without treating SOL<->WSOL settlement as PnL.
 * Wallet-owned and verified transit WSOL use lamports as their raw token units.
 */
function computeWalletQuoteAssetMovement(
  tx,
  wallet,
  wsolMint = DEFAULT_WSOL_MINT,
  trackedWsolAccounts = [],
) {
  const meta = tx?.meta;
  if (!meta || !wallet) return null;

  const walletIndex = findWalletIndex(tx, wallet);
  const preNativeLamports = walletIndex >= 0
    ? toLamports(meta.preBalances?.[walletIndex])
    : 0n;
  const postNativeLamports = walletIndex >= 0
    ? toLamports(meta.postBalances?.[walletIndex])
    : 0n;
  const preWalletWsolLamports = sumOwnedWsol(
    meta.preTokenBalances,
    wallet,
    wsolMint,
  );
  const postWalletWsolLamports = sumOwnedWsol(
    meta.postTokenBalances,
    wallet,
    wsolMint,
  );
  const preWalletWsolAccountLamports = sumOwnedWsolAccountLamports(
    meta.preTokenBalances,
    meta.preBalances,
    wallet,
    wsolMint,
  );
  const postWalletWsolAccountLamports = sumOwnedWsolAccountLamports(
    meta.postTokenBalances,
    meta.postBalances,
    wallet,
    wsolMint,
  );
  const accountKeys = transactionAccountKeys(tx);
  const preTrackedWsolLamports = sumTrackedWsol(
    meta.preTokenBalances,
    accountKeys,
    trackedWsolAccounts,
    wallet,
    wsolMint,
  );
  const postTrackedWsolLamports = sumTrackedWsol(
    meta.postTokenBalances,
    accountKeys,
    trackedWsolAccounts,
    wallet,
    wsolMint,
  );

  const preQuoteLamports = preNativeLamports + preWalletWsolAccountLamports + preTrackedWsolLamports;
  const postQuoteLamports = postNativeLamports + postWalletWsolAccountLamports + postTrackedWsolLamports;
  const nativeDeltaLamports = postNativeLamports - preNativeLamports;
  const walletWsolDeltaLamports = postWalletWsolLamports - preWalletWsolLamports;
  const walletWsolAccountDeltaLamports =
    postWalletWsolAccountLamports - preWalletWsolAccountLamports;
  const walletWsolReserveDeltaLamports =
    walletWsolAccountDeltaLamports - walletWsolDeltaLamports;
  const trackedWsolDeltaLamports = postTrackedWsolLamports - preTrackedWsolLamports;
  const quoteDeltaLamports = postQuoteLamports - preQuoteLamports;

  return {
    walletIndex,
    preNativeLamports,
    postNativeLamports,
    preWalletWsolLamports,
    postWalletWsolLamports,
    preWalletWsolAccountLamports,
    postWalletWsolAccountLamports,
    preTrackedWsolLamports,
    postTrackedWsolLamports,
    preQuoteLamports,
    postQuoteLamports,
    nativeDeltaLamports,
    walletWsolDeltaLamports,
    walletWsolAccountDeltaLamports,
    walletWsolReserveDeltaLamports,
    trackedWsolDeltaLamports,
    quoteDeltaLamports,
    preNativeSol: lamportsToSol(preNativeLamports),
    postNativeSol: lamportsToSol(postNativeLamports),
    preWalletWsolSol: lamportsToSol(preWalletWsolLamports),
    postWalletWsolSol: lamportsToSol(postWalletWsolLamports),
    preWalletWsolAccountSol: lamportsToSol(preWalletWsolAccountLamports),
    postWalletWsolAccountSol: lamportsToSol(postWalletWsolAccountLamports),
    preTrackedWsolSol: lamportsToSol(preTrackedWsolLamports),
    postTrackedWsolSol: lamportsToSol(postTrackedWsolLamports),
    nativeDeltaSol: lamportsToSol(nativeDeltaLamports),
    walletWsolDeltaSol: lamportsToSol(walletWsolDeltaLamports),
    walletWsolAccountDeltaSol: lamportsToSol(walletWsolAccountDeltaLamports),
    walletWsolReserveDeltaSol: lamportsToSol(walletWsolReserveDeltaLamports),
    trackedWsolDeltaSol: lamportsToSol(trackedWsolDeltaLamports),
    quoteDeltaSol: lamportsToSol(quoteDeltaLamports),
  };
}

module.exports = {
  DEFAULT_WSOL_MINT,
  LAMPORTS_PER_SOL,
  computeWalletQuoteAssetMovement,
  lamportsToSol,
  rawTokenAmount,
  sumOwnedWsol,
  sumOwnedWsolAccountLamports,
  sumTrackedWsol,
  transactionAccountKeys,
};
