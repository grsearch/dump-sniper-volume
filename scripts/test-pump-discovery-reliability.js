'use strict';

const assert = require('assert');
const WebSocket = require('ws');
const PumpGraduationDiscovery = require('../src/core/PumpGraduationDiscovery');

function makeDiscovery(rpcRequest) {
  return new PumpGraduationDiscovery({
    tokenRegistry: {
      getToken: () => null,
    },
    rpcUrl: 'https://example.invalid',
    rpcRequest,
    settings: {
      enabled: true,
      pollLimit: 2,
      pollMaxPages: 3,
      startupLookbackSec: 120,
      wsRecoverySilenceMs: 30_000,
    },
  });
}

async function testPollPagination() {
  const nowSec = Math.floor(Date.now() / 1000);
  const pages = {
    first: [
      { signature: 's5', blockTime: nowSec },
      { signature: 's4', blockTime: nowSec },
    ],
    s4: [
      { signature: 's3', blockTime: nowSec },
      { signature: 's2', blockTime: nowSec },
    ],
    s2: [
      { signature: 's1', blockTime: nowSec },
    ],
  };
  const discovery = makeDiscovery(async (method, params) => {
    assert.strictEqual(method, 'getSignaturesForAddress');
    return pages[params[1].before || 'first'];
  });
  discovery.startupCutoffMs = Date.now() - 120_000;

  const result = await discovery._fetchPollRows();
  assert.deepStrictEqual(result.rows.map((row) => row.signature), ['s5', 's4', 's3', 's2', 's1']);
  assert.strictEqual(result.pages, 3);
}

async function testSeenBoundaryStopsOlderRows() {
  const nowSec = Math.floor(Date.now() / 1000);
  const discovery = makeDiscovery(async (_method, params) => {
    if (!params[1].before) {
      return [
        { signature: 'new2', blockTime: nowSec },
        { signature: 'new1', blockTime: nowSec },
      ];
    }
    return [
      { signature: 'known', blockTime: nowSec },
      { signature: 'older-unseen', blockTime: nowSec },
    ];
  });
  discovery.startupCutoffMs = Date.now() - 120_000;
  discovery.seenSignatures.set('known', Date.now());

  const result = await discovery._fetchPollRows();
  assert.deepStrictEqual(result.rows.map((row) => row.signature), ['new2', 'new1']);
  assert.strictEqual(result.pages, 2);
}

function testSilentWebSocketRecovery() {
  const discovery = makeDiscovery(async () => []);
  let terminated = 0;
  discovery.ws = {
    readyState: WebSocket.OPEN,
    terminate: () => { terminated++; },
  };
  discovery.wsSubscriptionId = 42;
  discovery.lastWsMigrationAt = Date.now() - 60_000;

  discovery._recoverSilentWebSocket(1);
  assert.strictEqual(terminated, 1);
  assert.strictEqual(discovery.wsSubscriptionId, null);
}

async function main() {
  await testPollPagination();
  await testSeenBoundaryStopsOlderRows();
  testSilentWebSocketRecovery();
  console.log('pump discovery reliability tests passed');
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
