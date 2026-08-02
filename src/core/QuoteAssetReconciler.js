'use strict';

const { NATIVE_MINT } = require('@solana/spl-token');
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');
const { summarizeOwnedWsolAccounts } = require('../utils/quoteAssetAccounting');
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
      totalWalletQuoteSol: null,
      // Backward-compatible API alias. It has the same wallet-only value.
      totalEquitySol: null,
      updatedAt: null,
      nextRunAt: null,
      walletWsolAccounts: 0,
      lastAction: null,
      lastError: null,
    };
  }

  async start() {
    this._clearLegacyExternalAlerts();
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
    return { ...this.snapshot };
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
        totalEquitySol: current.totalEquitySol,
        walletWsolAccountCount: current.walletWsolAccounts.length,
        action,
        details: {
          unwrapSignatures,
          beforeWalletWsolSol: before.walletWsolSol,
          beforeWalletWsolRentSol: before.walletWsolRentSol,
        },
      });
      monitor.beat('QuoteAssetReconciler', action);
      monitor.set('QuoteAssetReconciler.nativeSol', current.nativeSol, 'QuoteAssetReconciler');
      monitor.set('QuoteAssetReconciler.walletWsolSol', current.walletWsolSol, 'QuoteAssetReconciler');
      this._updateWalletWsolAlert(current);
      this._clearLegacyExternalAlerts();
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

    const walletAddress = wallet.toBase58();
    const ownedWsol = summarizeOwnedWsolAccounts(
      walletWsolResponse.value,
      walletAddress,
      NATIVE_MINT.toBase58(),
    );

    const native = BigInt(nativeLamports || 0);
    const totalWalletQuoteLamports = native + ownedWsol.amountLamports;
    return {
      nativeLamports: native,
      walletWsolLamports: ownedWsol.amountLamports,
      walletWsolAccountLamports: ownedWsol.accountLamports,
      nativeSol: lamportsToSol(native),
      walletWsolSol: lamportsToSol(ownedWsol.amountLamports),
      walletWsolRentSol: lamportsToSol(ownedWsol.rentLamports),
      totalWalletQuoteSol: lamportsToSol(totalWalletQuoteLamports),
      totalEquitySol: lamportsToSol(totalWalletQuoteLamports),
      walletWsolAccounts: ownedWsol.accounts,
    };
  }

  _updateSnapshot(current, action) {
    this.snapshot = {
      ...this.snapshot,
      enabled: true,
      status: 'ok',
      nativeSol: current.nativeSol,
      walletWsolSol: current.walletWsolSol,
      walletWsolRentSol: current.walletWsolRentSol,
      totalWalletQuoteSol: current.totalWalletQuoteSol,
      totalEquitySol: current.totalEquitySol,
      walletWsolAccounts: current.walletWsolAccounts.length,
      updatedAt: Date.now(),
      lastAction: action,
      lastError: null,
    };
  }

  _clearLegacyExternalAlerts() {
    monitor.clearAlert('quote_asset.jupiter_account_unverified');
    monitor.clearAlert('quote_asset.jupiter_pending_wsol');
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
