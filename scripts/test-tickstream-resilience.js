'use strict';

const assert = require('assert');
const Module = require('module');

process.env.HELIUS_LASERSTREAM_ENDPOINTS = 'https://example.invalid';
process.env.LS_JUPITER_ENABLED = '0';

const heartbeats = [];
const monitor = {
  registerModule() {},
  beat(moduleName, context) {
    heartbeats.push({ moduleName, context });
  },
  set() {},
  inc() {},
  recordError() {},
};

const originalLoad = Module._load;
Module._load = function loadWithGrpcStub(request, parent, isMain) {
  if (request === '@triton-one/yellowstone-grpc') {
    return {
      default: class FakeClient {},
      CommitmentLevel: { PROCESSED: 0 },
      SubscribeRequest: null,
      SubscribeRequestFilterTransactions: null,
    };
  }
  if (request === '../monitor/HealthMonitor') {
    return { getMonitor: () => monitor };
  }
  return originalLoad.call(this, request, parent, isMain);
};
const TickStream = require('../src/core/TickStream');
Module._load = originalLoad;

async function main() {
  const stream = new TickStream();
  let starts = 0;
  const fakeRegion = {
    label: 'LS-TEST',
    filterMode: 'pumpAmm',
    shouldRun: false,
    async start(mints) {
      starts++;
      this.shouldRun = true;
      this.mints = Array.from(mints);
    },
    async stop() {
      this.shouldRun = false;
    },
  };
  stream.regions = [fakeRegion];
  stream._startSlotSubscriber = () => {};
  stream._startShredStream = () => {};
  stream._stopShredStream = () => {};
  stream._startWorker = () => {};

  await stream.start([]);
  assert.strictEqual(starts, 1, 'regions must start even when the initial watchlist is empty');
  assert.strictEqual(fakeRegion.shouldRun, true);
  assert.deepStrictEqual(fakeRegion.mints, []);

  stream._onRegionProgress(12345, 'LS-TEST');
  assert.strictEqual(stream._latestLsSlot, 12345);

  stream._latestSlotFromSlotUpdate = 0;
  stream._checkRegionLagHealth({
    now: 100_000,
    lagThresholdSlots: 100,
    lagSustainedMs: 10_000,
    lagReconnectCooldownMs: 30_000,
    sampleMs: 5_000,
  });
  assert.deepStrictEqual(
    heartbeats.at(-1),
    { moduleName: 'TickStream', context: 'health:0_mints:0/1_connected' },
    'the health timer must keep TickStream alive while no mints are watched',
  );

  await stream.stop();

  const subscriptionStream = new TickStream();
  let pumpRebuilds = 0;
  let jupiterRebuilds = 0;
  subscriptionStream.regions = [
    {
      filterMode: 'pumpAmm',
      async rebuild() {
        pumpRebuilds++;
      },
    },
    {
      filterMode: 'jupiter',
      async rebuild() {
        jupiterRebuilds++;
      },
    },
  ];
  subscriptionStream.watchedMints = new Set(['mint-a']);
  await subscriptionStream._performRebuild();
  assert.strictEqual(pumpRebuilds, 1, 'mint-filtered regions must rebuild');
  assert.strictEqual(jupiterRebuilds, 0, 'global Jupiter streams must not rebuild');

  subscriptionStream._lastSubscriptionChangeAt = 123;
  await subscriptionStream.updateSubscription(['mint-a']);
  assert.strictEqual(
    subscriptionStream._lastSubscriptionChangeAt,
    123,
    'an unchanged subscription must not restart the memory grace window',
  );
  assert.strictEqual(
    subscriptionStream._rebuildTimer,
    null,
    'an unchanged subscription must not schedule a stream rebuild',
  );

  const lagStream = new TickStream();
  const reconnects = [];
  const endpoint = 'https://lag-test.invalid';
  lagStream.regions = [
    {
      label: 'JUP-LAG',
      endpoint,
      filterMode: 'jupiter',
      shouldRun: true,
      connected: true,
      _connectedAt: 1_000,
      _lastReceivedSlot: 900,
      reconnectAttempts: 4,
      _scheduleReconnect() { reconnects.push(this.label); },
    },
    {
      label: 'LS-LAG',
      endpoint,
      filterMode: 'pumpAmm',
      shouldRun: true,
      reconnectAttempts: 4,
      _scheduleReconnect() { reconnects.push(this.label); },
    },
  ];
  lagStream._latestSlotFromSlotUpdate = 1_100;
  const lagOptions = {
    now: 100_000,
    lagThresholdSlots: 100,
    lagSustainedMs: 10_000,
    lagReconnectCooldownMs: 30_000,
    sampleMs: 5_000,
  };
  lagStream._checkRegionLagHealth(lagOptions);
  assert.deepStrictEqual(reconnects, [], 'a single lag sample must not reconnect');
  lagStream._checkRegionLagHealth(lagOptions);
  assert.deepStrictEqual(
    reconnects.sort(),
    ['JUP-LAG', 'LS-LAG'],
    'sustained canary lag must reconnect all streams on the same endpoint',
  );
  console.log('TickStream resilience tests passed');
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
