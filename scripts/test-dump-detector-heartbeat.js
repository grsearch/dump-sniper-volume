'use strict';

const assert = require('assert');
const Module = require('module');

const beats = [];
const counters = new Map();
const monitor = {
  registerModule() {},
  beat(moduleName, context) {
    beats.push({ moduleName, context });
  },
  inc() {},
  set(name, value) {
    counters.set(name, value);
  },
  recordError() {},
};

const originalLoad = Module._load;
Module._load = function loadWithStubs(request, parent, isMain) {
  if (request === 'bs58') return { default: { encode: (value) => String(value) } };
  if (request === '../config') {
    return {
      config: {
        programs: { pumpAmm: 'PumpAmm111111111111111111111111111111111' },
        strategy: {},
      },
    };
  }
  if (request === '../monitor/HealthMonitor') return { getMonitor: () => monitor };
  return originalLoad.call(this, request, parent, isMain);
};
const DumpDetector = require('../src/core/DumpDetector');
Module._load = originalLoad;

const detector = new DumpDetector({});
assert.deepStrictEqual(beats.at(-1), {
  moduleName: 'DumpDetector',
  context: 'idle:no_input',
});

detector._runMaintenance(10_000);
assert.strictEqual(counters.get('DumpDetector.secondsSinceInput'), -1);
assert.strictEqual(beats.at(-1).context, 'idle:no_input');

detector._lastInputAt = 7_000;
detector._runMaintenance(10_000);
assert.strictEqual(counters.get('DumpDetector.secondsSinceInput'), 3);
assert.strictEqual(beats.at(-1).context, 'idle:3s_since_input');

detector._parseTx = () => null;
detector.handleTransaction({ signature: 'heartbeat-test-signature' });
assert.deepStrictEqual(
  beats.slice(-3).map((entry) => entry.context),
  ['tx:received', 'parse:start', 'parse:done'],
);
assert(detector._lastParseCompletedAt > 0);

detector.handleTransaction({ signature: 'heartbeat-test-signature' });
assert.deepStrictEqual(
  beats.slice(-2).map((entry) => entry.context),
  ['tx:received', 'tx:duplicate'],
);

detector.shutdown();
console.log('DumpDetector heartbeat tests passed');
