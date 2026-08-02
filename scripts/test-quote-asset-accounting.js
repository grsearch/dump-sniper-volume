'use strict';

const assert = require('assert');
const {
  assessWalletWsolClose,
  computeWalletQuoteAssetMovement,
  summarizeOwnedWsolAccounts,
} = require('../src/utils/quoteAssetAccounting');

const WALLET = 'Gu3si111111111111111111111111111111111tatVF';
const WSOL = 'So11111111111111111111111111111111111111112';
const EXTERNAL = 'DmrQLy5nVJNnRrP8RimSuW8GJxvcjByizcYVzcyFEJFZ';
const EXTERNAL_OWNER = 'FtgZ6iPt4PjyHVyWRRhsooGVwA2U2vfDrTwtiStdqrXS';
const WSOL_RENT = 2_039_280;

function tokenBalance(amount, accountIndex, owner) {
  return {
    accountIndex,
    mint: WSOL,
    owner,
    uiTokenAmount: { amount: String(amount), decimals: 9 },
  };
}

function tx({
  preNative,
  postNative,
  preWsol = 0,
  postWsol = 0,
  preExternal = 0,
  postExternal = 0,
}) {
  return {
    transaction: { message: { accountKeys: [WALLET, 'WalletWsol', EXTERNAL] } },
    meta: {
      preBalances: [
        preNative,
        preWsol ? preWsol + WSOL_RENT : 0,
        preExternal ? preExternal + WSOL_RENT : 0,
      ],
      postBalances: [
        postNative,
        postWsol ? postWsol + WSOL_RENT : 0,
        postExternal ? postExternal + WSOL_RENT : 0,
      ],
      preTokenBalances: [
        ...(preWsol ? [tokenBalance(preWsol, 1, WALLET)] : []),
        ...(preExternal ? [tokenBalance(preExternal, 2, EXTERNAL_OWNER)] : []),
      ],
      postTokenBalances: [
        ...(postWsol ? [tokenBalance(postWsol, 1, WALLET)] : []),
        ...(postExternal ? [tokenBalance(postExternal, 2, EXTERNAL_OWNER)] : []),
      ],
    },
  };
}

function closeTo(actual, expected, label) {
  assert.ok(
    Math.abs(actual - expected) < 1e-12,
    `${label}: expected ${expected}, got ${actual}`,
  );
}

const wrapped = computeWalletQuoteAssetMovement(tx({
  preNative: 2_000_000_000,
  postNative: 997_955_720,
  postWsol: 1_000_000_000,
}), WALLET);
closeTo(wrapped.quoteDeltaSol, -0.000005, 'wrap only charges fee');

const soldIntoWsol = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 997_955_720,
  postWsol: 200_000_000,
}), WALLET);
closeTo(soldIntoWsol.quoteDeltaSol, 0.199995, 'sell into wallet WSOL is income');

const unwrapped = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 2_002_034_280,
  preWsol: 1_000_000_000,
}), WALLET);
closeTo(unwrapped.quoteDeltaSol, -0.000005, 'unwrap only charges fee');

const externalChanged = computeWalletQuoteAssetMovement(tx({
  preNative: 1_000_000_000,
  postNative: 999_995_000,
  postExternal: 200_000_000,
}), WALLET);
closeTo(
  externalChanged.quoteDeltaSol,
  -0.000005,
  'external WSOL is never attributed to the wallet',
);

function parsedAccount(address, owner, amount, closeAuthority = null) {
  return {
    pubkey: address,
    account: {
      lamports: amount + WSOL_RENT,
      data: {
        parsed: {
          info: {
            mint: WSOL,
            owner,
            closeAuthority,
            tokenAmount: { amount: String(amount), decimals: 9 },
          },
        },
      },
    },
  };
}

const owned = summarizeOwnedWsolAccounts([
  parsedAccount('WalletAta', WALLET, 100_000_000),
  parsedAccount('WalletAuxiliary', WALLET, 200_000_000, WALLET),
  parsedAccount('WalletRestricted', WALLET, 50_000_000, 'OtherCloseAuthority'),
  parsedAccount(EXTERNAL, EXTERNAL_OWNER, 400_000_000),
], WALLET, WSOL);
assert.strictEqual(owned.accounts.length, 3, 'all wallet-owned WSOL accounts are found');
assert.strictEqual(owned.amountLamports, 350_000_000n);
assert.strictEqual(owned.rentLamports, BigInt(WSOL_RENT * 3));
assert.strictEqual(owned.accounts.filter((row) => row.closeable).length, 2);
closeTo(Number(owned.amountLamports) / 1e9, 0.35, 'display total excludes rent');

assert.deepStrictEqual(
  assessWalletWsolClose({ mint: WSOL, owner: WALLET, closeAuthority: null }, WALLET, WSOL),
  { closeable: true, reason: null },
);
assert.strictEqual(
  assessWalletWsolClose({ mint: WSOL, owner: EXTERNAL_OWNER }, WALLET, WSOL).reason,
  'owner_mismatch',
);
assert.strictEqual(
  assessWalletWsolClose({ mint: 'OtherMint', owner: WALLET }, WALLET, WSOL).reason,
  'mint_mismatch',
);
assert.strictEqual(
  assessWalletWsolClose({
    mint: WSOL,
    owner: WALLET,
    closeAuthority: 'OtherCloseAuthority',
  }, WALLET, WSOL).reason,
  'close_authority_mismatch',
);

console.log('quote asset accounting tests passed');
