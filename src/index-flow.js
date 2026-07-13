'use strict';

const SignalEngineBase = require('./core/SignalEngine');
const DumpDetector = require('./core/DumpDetector');
const OrderFlowTracker = require('./core/OrderFlowTracker');

function boolEnv(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === '') return fallback;
  return String(raw).toLowerCase() === 'true' || raw === '1';
}

const orderFlowEnabled = boolEnv('ORDER_FLOW_ENABLED', true);
const replaceDumpSignal = boolEnv('ORDER_FLOW_REPLACE_DUMP_SIGNAL', true);

if (orderFlowEnabled) {
  const tracker = new OrderFlowTracker();
  let signalEngine = null;

  class FlowSignalEngine extends SignalEngineBase {
    constructor(...args) {
      super(...args);
      signalEngine = this;
    }
  }

  require.cache[require.resolve('./core/SignalEngine')].exports = FlowSignalEngine;

  tracker.on('flowReversalSignal', (signal) => {
    if (!signalEngine) {
      console.warn('[OrderFlow] signal skipped: SignalEngine not ready');
      return;
    }
    Promise.resolve(signalEngine.handleDumpSignal(signal)).catch((err) => {
      console.error('[OrderFlow] SignalEngine rejected with error:', err && err.message ? err.message : err);
    });
  });

  const originalEmit = DumpDetector.prototype.emit;
  DumpDetector.prototype.emit = function patchedDumpDetectorEmit(event, payload, ...args) {
    if (event === 'swapParsed') {
      try {
        tracker.handleSwap(payload);
      } catch (err) {
        console.warn('[OrderFlow] handleSwap failed:', err && err.message ? err.message : err);
      }
    }

    if (event === 'dumpSignal' && replaceDumpSignal) {
      try {
        tracker.noteSuppressedDumpSignal(payload);
      } catch (_) {}
      return true;
    }

    return originalEmit.call(this, event, payload, ...args);
  };

  console.log(
    `[OrderFlow] enabled; legacy dumpSignal ${replaceDumpSignal ? 'suppressed' : 'allowed'}; ` +
      'set ORDER_FLOW_ENABLED=false or run npm run start:legacy to disable',
  );
}

require('./index');
