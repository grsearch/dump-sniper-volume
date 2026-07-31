'use strict';

const assert = require('assert');
const Module = require('module');

process.env.HELIUS_LASERSTREAM_ENDPOINTS = 'https://example.invalid';
process.env.LS_JUPITER_ENABLED = '0';

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

  await stream.stop();

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
