'use strict';

const { PublicKey } = require('@solana/web3.js');
const { getAccount, NATIVE_MINT, TOKEN_PROGRAM_ID } = require('@solana/spl-token');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { nextScheduledAt, shouldAutoUnwrap } = require('../utils/quoteAssetSchedule');

const monitor = getMonitor();
monitor.registerModule('QuoteAssetReconciler', {
  staleMs: config.quoteAssetReconciler.enabled
    ? 7 * 60 * 60_000
    : 365 * 24 * 60 * 60_000,
  label: 'Quote Asset Reconciler',
});

function lamportsToSol(value) {
  return Number(value || 0n) / 1e9;
}

class QuoteAssetReconciler {
  constructor({ executor, tradeLogger }) {
    this.executor = executor;
    this.tradeLogger = tradeLogger;
    this.settings = config.quoteAssetReconciler;
    this._scheduleTimer = null;
    this._retryTimer = null;
    this._running = false;
    this.snapshot = {
      enabled: !!this.settings.enabled,
      status: this.settings.enabled ? 'waiting' : 'disabled',
      nativeSol: null,
      walletWsolSol: null,
      jupiterPendingWsolSol: null,
      totalEquitySol: null,
      updatedAt: null,
      nextRunAt: null,
      walletWsolAccounts: 0,
      jupiterAccounts: [],
      lastAction: null,
      lastError: null,
    };
  }

  async start() {
    if (!this.settings.enabled) {
      monitor.beat('QuoteAssetReconciler', 'disabled');
      return;
    }
    // Startup is read-only. Automatic unwrapping only runs on the fixed CST schedule.
    try {
      await this.inspect({ reason: 'startup_snapshot', allowUnwrap: false });
    } catch (_) { /* inspect already records the error; trading may still start */ }
    this._scheduleNext();
  }

  stop() {
    if (this._scheduleTimer) clearTimeout(this._scheduleTimer);
    if (this._retryTimer) clearTimeout(this._retryTimer);
    this._scheduleTimer = null;
    this._retryTimer = null;
  }

  getSnapshot() {
    return {
      ...this.snapshot,
      jupiterAccounts: (this.snapshot.jupiterAccounts || []).map((x) => ({ ...x })),
    };
  }

  getConfirmedJupiterAccountAddresses() {
    return (this.snapshot.jupiterAccounts || [])
      .filter((account) => account.confirmed)
      .map((account) => account.address);
  }

  _scheduleNext() {
    if (!this.settings.enabled) return;
    if (this._scheduleTimer) clearTimeout(this._scheduleTimer);
    const nextAt = nextScheduledAt(
      Date.now(),
      this.settings.scheduleHoursCst,
      this.settings.timezoneOffsetMinutes,
    );
    this.snapshot.nextRunAt = nextAt;
    const delay = Math.max(1_000, nextAt - Date.now());
    this._scheduleTimer = setTimeout(() => {
      this._scheduleTimer = null;
      this.inspect({ reason: 'scheduled', allowUnwrap: true })
        .catch((err) => this._recordError(err, 'scheduled'))
        .finally(() => this._scheduleNext());
    }, delay);
    if (this._scheduleTimer.unref) this._scheduleTimer.unref();
  }

  _scheduleBusyRetry() {
    if (this._retryTimer) return;
    this._retryTimer = setTimeout(() => {
      this._retryTimer = null;
      this.inspect({ reason: 'busy_retry', allowUnwrap: true }).catch((err) => {
        this._recordError(err, 'busy_retry');
      });
    }, this.settings.busyRetryMs);
    if (this._retryTimer.unref) this._retryTimer.unref();
  }

  async inspect({ reason = 'manual', allowUnwrap = false } = {}) {
    if (!this.settings.enabled || this._running) return this.getSnapshot();
    if (!this.executor?.keypair || !this.executor?.rpc) {
      this.snapshot.status = 'wallet_unavailable';
      monitor.beat('QuoteAssetReconciler', 'wallet_unavailable');
      return this.getSnapshot();
    }

    if (allowUnwrap && this.executor.isExecutionBusy()) {
      this.snapshot.status = 'busy_retry_scheduled';
      this.snapshot.lastAction = 'skipped_while_trade_active';
      this._scheduleBusyRetry();
      monitor.inc('QuoteAssetReconciler.busySkips', 1, 'QuoteAssetReconciler');
      monitor.beat('QuoteAssetReconciler', 'busy_retry_scheduled');
      return this.getSnapshot();
    }

    this._running = true;
    try {
      const before = await this._readBalances();
      let action = 'inspect_only';
      let unwrapSignatures = [];
      let current = before;

      const closeableAccounts = before.walletWsolAccounts.filter(
        (account) => account.closeable,
      );
      const closeableLamports = closeableAccounts.reduce(
        (total, account) => total + account.amountLamports,
        0n,
      );
      const busyAfterRead = allowUnwrap && this.executor.isExecutionBusy();
      if (busyAfterRead) {
        action = 'unwrap_skipped_busy';
        this._scheduleBusyRetry();
        monitor.inc('QuoteAssetReconciler.busySkips', 1, 'QuoteAssetReconciler');
      } else if (shouldAutoUnwrap({
        allowUnwrap,
        busy: false,
        amountLamports: closeableLamports,
        minLamports: this.settings.autoUnwrapMinLamports,
        accountCount: closeableAccounts.length,
      })) {
        const result = await this.executor.closeWalletWsolAccounts(
          closeableAccounts.map((account) => account.address),
        );
        if (result?.busy) {
          this._scheduleBusyRetry();
          action = 'unwrap_skipped_busy';
        } else if (!result?.success) {
          throw new Error(result?.error || 'wallet WSOL unwrap failed');
        } else {
          unwrapSignatures = result.signatures || [];
          action = 'wallet_wsol_unwrapped';
          current = await this._readBalances();
          monitor.inc(
            'QuoteAssetReconciler.unwrapAccounts',
            closeableAccounts.length,
            'QuoteAssetReconciler',
          );
        }
      }

      this._updateSnapshot(current, action);
      this._logReconciliation({
        ts: Date.now(),
        reason,
        status: 'ok',
        nativeSol: current.nativeSol,
        walletWsolSol: current.walletWsolSol,
        walletWsolRentSol: current.walletWsolRentSol,
        jupiterPendingWsolSol: current.jupiterPendingWsolSol,
        totalEquitySol: current.totalEquitySol,
        walletWsolAccountCount: current.walletWsolAccounts.length,
        action,
        details: {
          unwrapSignatures,
          beforeWalletWsolSol: before.walletWsolSol,
          beforeWalletWsolRentSol: before.walletWsolRentSol,
          jupiterAccounts: current.jupiterAccounts.map((account) => ({
            ...account,
            amountLamports: account.amountLamports.toString(),
          })),
        },
      });
      monitor.beat('QuoteAssetReconciler', action);
      monitor.set('QuoteAssetReconciler.nativeSol', current.nativeSol, 'QuoteAssetReconciler');
      monitor.set('QuoteAssetReconciler.walletWsolSol', current.walletWsolSol, 'QuoteAssetReconciler');
      monitor.set('QuoteAssetReconciler.jupiterPendingWsolSol', current.jupiterPendingWsolSol, 'QuoteAssetReconciler');
      this._updateWalletWsolAlert(current);
      this._updateJupiterAlert(current);
      return this.getSnapshot();
    } catch (err) {
      this._recordError(err, reason);
      this._logReconciliation({
        ts: Date.now(),
        reason,
        status: 'error',
        action: 'none',
        details: { error: err.message },
      });
      throw err;
    } finally {
      this._running = false;
    }
  }

  async _readBalances() {
    const connection = this.executor.rpc;
    const wallet = this.executor.keypair.publicKey;
    const [nativeLamports, walletWsolResponse] = await Promise.all([
      connection.getBalance(wallet, 'confirmed'),
      connection.getParsedTokenAccountsByOwner(
        wallet,
        { mint: NATIVE_MINT },
        'confirmed',
      ),
    ]);

    const walletWsolAccounts = [];
    let walletWsolLamports = 0n;
    let walletWsolAccountLamports = 0n;
    for (const row of walletWsolResponse.value || []) {
      const info = row.account?.data?.parsed?.info;
      if (!info || info.mint !== NATIVE_MINT.toBase58() || info.owner !== wallet.toBase58()) {
        continue;
      }
      const amount = BigInt(info.tokenAmount?.amount || '0');
      walletWsolLamports += amount;
      const accountLamports = BigInt(row.account?.lamports || 0);
      walletWsolAccountLamports += accountLamports;
      const closeAuthority = info.closeAuthority || null;
      walletWsolAccounts.push({
        address: row.pubkey.toBase58(),
        amountLamports: amount,
        amountSol: lamportsToSol(amount),
        accountLamports,
        reclaimableRentLamports: accountLamports > amount
          ? accountLamports - amount
          : 0n,
        closeAuthority,
        closeable: !closeAuthority || closeAuthority === wallet.toBase58(),
      });
    }

    const jupiterAccounts = [];
    let jupiterPendingLamports = 0n;
    for (const configured of this.settings.jupiterEscrowAccounts) {
      const result = await this._readJupiterAccount(configured);
      jupiterAccounts.push(result);
      if (result.confirmed) jupiterPendingLamports += result.amountLamports;
    }

    const native = BigInt(nativeLamports || 0);
    return {
      nativeLamports: native,
      walletWsolLamports,
      walletWsolAccountLamports,
      jupiterPendingLamports,
      nativeSol: lamportsToSol(native),
      walletWsolSol: lamportsToSol(walletWsolLamports),
      walletWsolRentSol: lamportsToSol(
        walletWsolAccountLamports > walletWsolLamports
          ? walletWsolAccountLamports - walletWsolLamports
          : 0n,
      ),
      jupiterPendingWsolSol: lamportsToSol(jupiterPendingLamports),
      totalEquitySol: lamportsToSol(
        native + walletWsolAccountLamports + jupiterPendingLamports,
      ),
      walletWsolAccounts,
      jupiterAccounts,
    };
  }

  async _readJupiterAccount(configured) {
    const base = {
      address: configured.address,
      expectedOwner: configured.owner || null,
      amountLamports: 0n,
      amountSol: 0,
      confirmed: false,
      status: 'unavailable',
    };
    try {
      const tokenAccount = await getAccount(
        this.executor.rpc,
        new PublicKey(configured.address),
        'confirmed',
        TOKEN_PROGRAM_ID,
      );
      const actualOwner = tokenAccount.owner.toBase58();
      const actualMint = tokenAccount.mint.toBase58();
      const ownerMatches = !!configured.owner && actualOwner === configured.owner;
      const mintMatches = actualMint === NATIVE_MINT.toBase58();
      const amountLamports = BigInt(tokenAccount.amount.toString());
      return {
        ...base,
        actualOwner,
        actualMint,
        amountLamports,
        amountSol: lamportsToSol(amountLamports),
        confirmed: ownerMatches && mintMatches,
        status: !mintMatches
          ? 'mint_mismatch'
          : !configured.owner
            ? 'owner_not_configured'
            : !ownerMatches
              ? 'owner_mismatch'
              : 'confirmed',
      };
    } catch (err) {
      return { ...base, status: 'rpc_error', error: err.message };
    }
  }

  _updateSnapshot(current, action) {
    this.snapshot = {
      ...this.snapshot,
      enabled: true,
      status: 'ok',
      nativeSol: current.nativeSol,
      walletWsolSol: current.walletWsolSol,
      walletWsolRentSol: current.walletWsolRentSol,
      jupiterPendingWsolSol: current.jupiterPendingWsolSol,
      totalEquitySol: current.totalEquitySol,
      walletWsolAccounts: current.walletWsolAccounts.length,
      jupiterAccounts: current.jupiterAccounts.map((account) => ({
        ...account,
        amountLamports: account.amountLamports.toString(),
      })),
      updatedAt: Date.now(),
      lastAction: action,
      lastError: null,
    };
  }

  _updateJupiterAlert(current) {
    const invalid = current.jupiterAccounts.filter((account) => account.status !== 'confirmed');
    if (invalid.length > 0) {
      monitor.fireAlert(
        'quote_asset.jupiter_account_unverified',
        'warn',
        `${invalid.length} configured Jupiter WSOL account(s) could not be verified`,
        { accounts: invalid.map((x) => ({ address: x.address, status: x.status })) },
      );
    } else {
      monitor.clearAlert('quote_asset.jupiter_account_unverified');
    }

    if (current.jupiterPendingWsolSol >= this.settings.jupiterEscrowAlertMinSol) {
      monitor.fireAlert(
        'quote_asset.jupiter_pending_wsol',
        'warn',
        `Jupiter pending WSOL ${current.jupiterPendingWsolSol.toFixed(6)} SOL requires settlement`,
        { amountSol: current.jupiterPendingWsolSol, autoSettle: false },
      );
    } else {
      monitor.clearAlert('quote_asset.jupiter_pending_wsol');
    }
  }

  _updateWalletWsolAlert(current) {
    const uncloseable = current.walletWsolAccounts.filter(
      (account) => !account.closeable,
    );
    if (uncloseable.length > 0) {
      monitor.fireAlert(
        'quote_asset.wallet_wsol_uncloseable',
        'warn',
        `${uncloseable.length} wallet WSOL account(s) have an unexpected close authority`,
        {
          accounts: uncloseable.map((account) => ({
            address: account.address,
            amountSol: account.amountSol,
            closeAuthority: account.closeAuthority,
          })),
        },
      );
    } else {
      monitor.clearAlert('quote_asset.wallet_wsol_uncloseable');
    }
  }

  _logReconciliation(row) {
    try {
      this.tradeLogger?.logQuoteAssetReconciliation(row);
    } catch (err) {
      monitor.recordError('QuoteAssetReconciler', err, {
        phase: 'reconciliation_audit',
        reason: row.reason,
      });
    }
  }

  _recordError(err, reason) {
    this.snapshot.status = 'error';
    this.snapshot.lastError = err.message;
    this.snapshot.updatedAt = Date.now();
    monitor.recordError('QuoteAssetReconciler', err, { reason });
  }
}

module.exports = {
  QuoteAssetReconciler,
  nextScheduledAt,
};
