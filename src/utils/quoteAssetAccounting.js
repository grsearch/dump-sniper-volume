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

function findWalletIndex(tx, wallet) {
  const keys = transactionAccountKeys(tx);
  return keys.findIndex((key) => accountKeyString(key) === wallet);
}

function accountKeyString(key) {
  if (!key) return '';
  if (typeof key === 'string') return key;
  if (key.pubkey) return accountKeyString(key.pubkey);
  if (typeof key.toBase58 === 'function') return key.toBase58();
  if (typeof key.toString === 'function') return key.toString();
  return '';
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

function lamportsToSol(lamports) {
  return Number(lamports) / Number(LAMPORTS_PER_SOL);
}

function summarizeOwnedWsolAccounts(rows, wallet, wsolMint = DEFAULT_WSOL_MINT) {
  const accounts = [];
  let amountLamports = 0n;
  let accountLamports = 0n;
  for (const row of rows || []) {
    const info = row?.account?.data?.parsed?.info;
    if (!info || info.mint !== wsolMint || info.owner !== wallet) continue;
    const amount = toLamports(info.tokenAmount?.amount);
    const nativeLamports = toLamports(row.account?.lamports);
    const closeAuthority = info.closeAuthority || null;
    amountLamports += amount;
    accountLamports += nativeLamports;
    accounts.push({
      address: accountKeyString(row.pubkey),
      amountLamports: amount,
      amountSol: lamportsToSol(amount),
      accountLamports: nativeLamports,
      reclaimableRentLamports: nativeLamports > amount
        ? nativeLamports - amount
        : 0n,
      closeAuthority,
      closeable: !closeAuthority || closeAuthority === wallet,
    });
  }
  return {
    accounts,
    amountLamports,
    accountLamports,
    rentLamports: accountLamports > amountLamports
      ? accountLamports - amountLamports
      : 0n,
  };
}

function assessWalletWsolClose(account, wallet, wsolMint = DEFAULT_WSOL_MINT) {
  const mint = accountKeyString(account?.mint);
  const owner = accountKeyString(account?.owner);
  const closeAuthority = account?.closeAuthority
    ? accountKeyString(account.closeAuthority)
    : null;
  if (mint !== wsolMint) return { closeable: false, reason: 'mint_mismatch' };
  if (owner !== wallet) return { closeable: false, reason: 'owner_mismatch' };
  if (closeAuthority && closeAuthority !== wallet) {
    return { closeable: false, reason: 'close_authority_mismatch' };
  }
  return { closeable: true, reason: null };
}

/**
 * Compute wallet quote-asset movement without treating SOL<->WSOL wrapping as
 * PnL. Only native SOL and WSOL token accounts controlled by `wallet` count.
 * Token-account rent stays in the accounting basis solely so creating or
 * closing an account is an internal conversion instead of fake profit/loss.
 */
function computeWalletQuoteAssetMovement(
  tx,
  wallet,
  wsolMint = DEFAULT_WSOL_MINT,
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
  const preQuoteLamports = preNativeLamports + preWalletWsolAccountLamports;
  const postQuoteLamports = postNativeLamports + postWalletWsolAccountLamports;
  const nativeDeltaLamports = postNativeLamports - preNativeLamports;
  const walletWsolDeltaLamports = postWalletWsolLamports - preWalletWsolLamports;
  const walletWsolAccountDeltaLamports =
    postWalletWsolAccountLamports - preWalletWsolAccountLamports;
  const walletWsolReserveDeltaLamports =
    walletWsolAccountDeltaLamports - walletWsolDeltaLamports;
  const quoteDeltaLamports = postQuoteLamports - preQuoteLamports;

  return {
    walletIndex,
    preNativeLamports,
    postNativeLamports,
    preWalletWsolLamports,
    postWalletWsolLamports,
    preWalletWsolAccountLamports,
    postWalletWsolAccountLamports,
    preQuoteLamports,
    postQuoteLamports,
    nativeDeltaLamports,
    walletWsolDeltaLamports,
    walletWsolAccountDeltaLamports,
    walletWsolReserveDeltaLamports,
    quoteDeltaLamports,
    preNativeSol: lamportsToSol(preNativeLamports),
    postNativeSol: lamportsToSol(postNativeLamports),
    preWalletWsolSol: lamportsToSol(preWalletWsolLamports),
    postWalletWsolSol: lamportsToSol(postWalletWsolLamports),
    preWalletWsolAccountSol: lamportsToSol(preWalletWsolAccountLamports),
    postWalletWsolAccountSol: lamportsToSol(postWalletWsolAccountLamports),
    nativeDeltaSol: lamportsToSol(nativeDeltaLamports),
    walletWsolDeltaSol: lamportsToSol(walletWsolDeltaLamports),
    walletWsolAccountDeltaSol: lamportsToSol(walletWsolAccountDeltaLamports),
    walletWsolReserveDeltaSol: lamportsToSol(walletWsolReserveDeltaLamports),
    quoteDeltaSol: lamportsToSol(quoteDeltaLamports),
  };
}

/**
 * List positive WSOL changes on accounts not controlled by the wallet.
 * These rows are audit evidence only. They must never be added to wallet
 * equity unless a later, explicit settlement proves wallet ownership.
 */
function summarizeExternalWsolIncreases(
  tx,
  wallet,
  wsolMint = DEFAULT_WSOL_MINT,
) {
  const meta = tx?.meta;
  if (!meta || !wallet) return [];

  const keys = transactionAccountKeys(tx);
  const preByIndex = new Map();
  const postByIndex = new Map();
  for (const row of meta.preTokenBalances || []) {
    if (row?.mint === wsolMint && Number.isInteger(row.accountIndex)) {
      preByIndex.set(row.accountIndex, row);
    }
  }
  for (const row of meta.postTokenBalances || []) {
    if (row?.mint === wsolMint && Number.isInteger(row.accountIndex)) {
      postByIndex.set(row.accountIndex, row);
    }
  }

  const indexes = new Set([...preByIndex.keys(), ...postByIndex.keys()]);
  const increases = [];
  for (const accountIndex of indexes) {
    const pre = preByIndex.get(accountIndex);
    const post = postByIndex.get(accountIndex);
    const owner = post?.owner || pre?.owner || null;
    if (!owner || owner === wallet) continue;
    const deltaLamports = rawTokenAmount(post) - rawTokenAmount(pre);
    if (deltaLamports <= 0n) continue;
    increases.push({
      accountIndex,
      address: accountKeyString(keys[accountIndex]),
      owner,
      deltaLamports,
      deltaSol: lamportsToSol(deltaLamports),
    });
  }
  return increases.sort((a, b) => b.deltaSol - a.deltaSol);
}

module.exports = {
  DEFAULT_WSOL_MINT,
  LAMPORTS_PER_SOL,
  assessWalletWsolClose,
  computeWalletQuoteAssetMovement,
  lamportsToSol,
  rawTokenAmount,
  summarizeExternalWsolIncreases,
  summarizeOwnedWsolAccounts,
  sumOwnedWsol,
  sumOwnedWsolAccountLamports,
  transactionAccountKeys,
};
