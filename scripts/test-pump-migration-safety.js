'use strict';

const assert = require('assert');
const {
  PumpMigrationSafetyScanner,
  anchorDiscriminator,
} = require('../src/core/PumpMigrationSafetyScanner');
const {
  PUMP_AMM_PROGRAM_ID,
} = require('../src/utils/pumpMigrationParser');

const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const MINT = 'Mint111111111111111111111111111111111111111';
const OTHER_MINT = 'OtherMint11111111111111111111111111111111111';
const POOL = 'Pool111111111111111111111111111111111111111';
const BASE_VAULT = 'BaseVault11111111111111111111111111111111111';
const QUOTE_VAULT = 'QuoteVault1111111111111111111111111111111111';
const POOL_AUTHORITY_ATA = 'PoolAuthorityAta11111111111111111111111111111';
const SOURCE = 'SourceAta11111111111111111111111111111111111';
const DESTINATION = 'DestAta111111111111111111111111111111111111';
const USER = 'User111111111111111111111111111111111111111';
const POOL_AUTHORITY = 'PoolAuthority11111111111111111111111111111111';

function parsedTransfer({
  mint = MINT,
  source = SOURCE,
  destination = DESTINATION,
  amount = '60000000000000',
  decimals = 6,
} = {}) {
  return {
    programId: TOKEN_PROGRAM_ID,
    parsed: {
      type: 'transferChecked',
      info: {
        mint,
        source,
        destination,
        tokenAmount: { amount, decimals },
      },
    },
  };
}

function tokenBalances(accountKeys, mint = MINT) {
  return [
    {
      accountIndex: accountKeys.indexOf(SOURCE),
      mint,
      owner: USER,
      uiTokenAmount: { amount: '0', decimals: 6 },
    },
    {
      accountIndex: accountKeys.indexOf(DESTINATION),
      mint,
      owner: USER,
      uiTokenAmount: { amount: '0', decimals: 6 },
    },
    {
      accountIndex: accountKeys.indexOf(BASE_VAULT),
      mint,
      owner: POOL,
      uiTokenAmount: { amount: '0', decimals: 6 },
    },
    {
      accountIndex: accountKeys.indexOf(POOL_AUTHORITY_ATA),
      mint,
      owner: POOL,
      uiTokenAmount: { amount: '0', decimals: 6 },
    },
  ];
}

function balanceRow(accountKeys, account, owner, amount, mint = MINT, decimals = 6) {
  return {
    accountIndex: accountKeys.indexOf(account),
    mint,
    owner,
    uiTokenAmount: { amount: String(amount), decimals },
  };
}

function transaction(signature, instructions, opts = {}) {
  const accountKeys = opts.accountKeys || [
    USER,
    MINT,
    SOURCE,
    DESTINATION,
    BASE_VAULT,
    POOL_AUTHORITY_ATA,
    POOL,
    PUMP_AMM_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
  ];
  const balances = tokenBalances(accountKeys, opts.balanceMint || MINT)
    .filter((row) => row.accountIndex >= 0);
  return {
    meta: {
      err: null,
      innerInstructions: opts.innerInstructions || [],
      preTokenBalances: opts.preTokenBalances || balances,
      postTokenBalances: opts.postTokenBalances || balances,
      logMessages: opts.logMessages || [],
    },
    transaction: {
      signatures: [signature],
      message: {
        accountKeys,
        instructions,
      },
    },
  };
}

function block(...transactions) {
  return { transactions };
}

function migration(overrides = {}) {
  return {
    mint: MINT,
    poolAddress: POOL,
    poolBaseVault: BASE_VAULT,
    poolQuoteVault: QUOTE_VAULT,
    migrationUser: USER,
    migrationPoolAuthority: POOL_AUTHORITY,
    migrationTokenAccounts: [SOURCE, POOL_AUTHORITY_ATA, BASE_VAULT],
    signature: 'migration-signature',
    slot: 100,
    detectionSlot: 105,
    ...overrides,
  };
}

function makeScanner(blocks, overrides = {}) {
  const calls = [];
  const rpcRequest = async (method, params) => {
    calls.push({ method, params });
    if (method === 'getTokenSupply') {
      return {
        value: {
          amount: '1000000000000000',
          decimals: 6,
          uiAmountString: '1000000000',
        },
      };
    }
    if (method === 'getBlocks') return Object.keys(blocks).map(Number).sort((a, b) => a - b);
    if (method === 'getBlock') return blocks[params[0]] || null;
    throw new Error(`unexpected RPC method ${method}`);
  };
  return {
    calls,
    scanner: new PumpMigrationSafetyScanner({
      rpcRequest,
      settings: {
        auditEnabled: true,
        auditPreSlots: 10,
        auditLargeTransferTokens: 100_000_000,
        auditLargeTransferSupplyPct: 5,
        auditRpcConcurrency: 3,
        auditRpcRetries: 1,
        auditBlockCacheMs: 120_000,
        auditBlockCacheMaxSlots: 32,
        auditFailClosed: true,
        ...overrides,
      },
    }),
  };
}

(async () => {
  {
    const { scanner, calls } = makeScanner({
      100: block(transaction('migration-signature', [
        parsedTransfer({
          source: SOURCE,
          destination: POOL_AUTHORITY_ATA,
          amount: '800000000000000',
        }),
        parsedTransfer({
          source: POOL_AUTHORITY_ATA,
          destination: BASE_VAULT,
          amount: '800000000000000',
        }),
      ])),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, true, 'normal Pump AMM base-vault funding must be allowed');
    const getBlocks = calls.find((call) => call.method === 'getBlocks');
    assert.deepStrictEqual(
      getBlocks.params.slice(0, 2),
      [90, 105],
      'audit must cover Migrate-10 through the detection slot',
    );
  }

  {
    const accountKeys = [
      USER,
      MINT,
      SOURCE,
      DESTINATION,
      BASE_VAULT,
      POOL_AUTHORITY_ATA,
      POOL,
      POOL_AUTHORITY,
      PUMP_AMM_PROGRAM_ID,
      TOKEN_PROGRAM_ID,
    ];
    const preTokenBalances = [
      balanceRow(accountKeys, SOURCE, USER, '800000000000000'),
      balanceRow(accountKeys, POOL_AUTHORITY_ATA, POOL_AUTHORITY, '0'),
      balanceRow(accountKeys, BASE_VAULT, POOL_AUTHORITY, '0'),
    ];
    const postTokenBalances = [
      balanceRow(accountKeys, SOURCE, USER, '907700000000000'),
      balanceRow(accountKeys, POOL_AUTHORITY_ATA, POOL_AUTHORITY, '206900000000000'),
      balanceRow(accountKeys, BASE_VAULT, POOL_AUTHORITY, '206900000000000'),
    ];
    const { scanner } = makeScanner({
      100: block(transaction('migration-signature', [], {
        accountKeys,
        preTokenBalances,
        postTokenBalances,
      })),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(
      result.allowed,
      true,
      'migration caller residual tokens and official pool funding must not look like a buy',
    );
  }

  {
    const { scanner } = makeScanner({
      95: block(transaction('large-before-migration', [
        parsedTransfer({ amount: '60000000000000' }),
      ])),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false);
    assert.strictEqual(result.reasonCode, 'large_transfer');
    assert.strictEqual(Math.round(result.evidence.amountUi), 60_000_000);
    assert.strictEqual(Math.round(result.evidence.supplyPct), 6);
  }

  {
    const accountKeys = [
      USER,
      MINT,
      SOURCE,
      DESTINATION,
      BASE_VAULT,
      POOL_AUTHORITY_ATA,
      POOL,
      PUMP_AMM_PROGRAM_ID,
      TOKEN_PROGRAM_ID,
    ];
    const { scanner } = makeScanner({
      100: block(
        transaction('migration-signature', []),
        transaction('same-slot-nonstandard-pump-buy', [], {
          accountKeys,
          preTokenBalances: [
            balanceRow(accountKeys, DESTINATION, USER, '0'),
          ],
          postTokenBalances: [
            balanceRow(accountKeys, DESTINATION, USER, '60000000000000'),
          ],
        }),
      ),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false);
    assert.strictEqual(result.reasonCode, 'large_transfer');
    assert.strictEqual(result.evidence.detection, 'token_balance_delta');
    assert.strictEqual(Math.round(result.evidence.amountUi), 60_000_000);
    assert.strictEqual(
      Math.round(result.evidence.supplyPct),
      6,
      'raw balance deltas must be divided by token decimals',
    );
  }

  {
    const accountKeys = [
      USER,
      MINT,
      SOURCE,
      DESTINATION,
      BASE_VAULT,
      POOL_AUTHORITY_ATA,
      POOL,
      PUMP_AMM_PROGRAM_ID,
      TOKEN_PROGRAM_ID,
    ];
    const { scanner } = makeScanner({
      100: block(transaction('migration-signature', [], {
        accountKeys,
        preTokenBalances: [
          balanceRow(accountKeys, DESTINATION, 'ExternalBuyer1111111111111111111111111111111', '0'),
        ],
        postTokenBalances: [
          balanceRow(
            accountKeys,
            DESTINATION,
            'ExternalBuyer1111111111111111111111111111111',
            '1000000',
          ),
        ],
      })),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false);
    assert.strictEqual(result.reasonCode, 'same_tx_buy_migrate');
    assert.strictEqual(result.evidence.detection, 'token_balance_delta');
    assert.strictEqual(result.evidence.amountUi, 1);
  }

  {
    const accountKeys = [
      USER,
      MINT,
      SOURCE,
      DESTINATION,
      BASE_VAULT,
      POOL_AUTHORITY_ATA,
      POOL,
      PUMP_AMM_PROGRAM_ID,
      TOKEN_PROGRAM_ID,
    ];
    const { scanner } = makeScanner({
      99: block(transaction('small-nonstandard-buy', [], {
        accountKeys,
        preTokenBalances: [
          balanceRow(accountKeys, DESTINATION, USER, '0'),
        ],
        postTokenBalances: [
          balanceRow(accountKeys, DESTINATION, USER, '33270000000000'),
        ],
      })),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(
      result.allowed,
      true,
      'ordinary window balance deltas below 100M and 5% must remain allowed',
    );
  }

  {
    const mintTo = {
      programId: TOKEN_PROGRAM_ID,
      parsed: {
        type: 'mintTo',
        info: { mint: MINT, account: DESTINATION, amount: '1' },
      },
    };
    const { scanner } = makeScanner({
      94: block(transaction('large-first', [
        parsedTransfer({ amount: '120000000000000' }),
      ])),
      101: block(transaction('mint-to-later', [mintTo])),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false);
    assert.strictEqual(result.reasonCode, 'mint_to', 'MintTo must have the highest priority');
  }

  {
    const buyInstruction = {
      programId: PUMP_AMM_PROGRAM_ID,
      accounts: [POOL, MINT, USER],
      data: anchorDiscriminator('buy_exact_quote_in'),
    };
    const { scanner } = makeScanner({
      100: block(transaction('migration-signature', [
        parsedTransfer({
          source: SOURCE,
          destination: BASE_VAULT,
          amount: '800000000000000',
        }),
        buyInstruction,
        parsedTransfer({
          source: BASE_VAULT,
          destination: DESTINATION,
          amount: '1000000000000',
        }),
      ])),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false);
    assert.strictEqual(result.reasonCode, 'same_tx_buy_migrate');
  }

  {
    const unrelatedMintTo = {
      programId: TOKEN_PROGRAM_ID,
      parsed: {
        type: 'mintTo',
        info: { mint: OTHER_MINT, account: DESTINATION, amount: '900000000000000' },
      },
    };
    const { scanner } = makeScanner({
      100: block(transaction('migration-signature', [unrelatedMintTo], {
        balanceMint: OTHER_MINT,
      })),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, true, 'LP or unrelated mint issuance must be ignored');
  }

  {
    const { scanner } = makeScanner({
      99: block(transaction('unrelated-transfer', [
        parsedTransfer({
          mint: OTHER_MINT,
          amount: '900000000000000',
        }),
      ], { balanceMint: OTHER_MINT })),
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, true, 'another mint must not affect this candidate');
  }

  {
    const scanner = new PumpMigrationSafetyScanner({
      settings: {
        auditEnabled: true,
        auditRpcRetries: 1,
        auditFailClosed: true,
      },
      rpcRequest: async (method) => {
        if (method === 'getTokenSupply') {
          return {
            value: {
              amount: '1000000000000000',
              decimals: 6,
              uiAmountString: '1000000000',
            },
          };
        }
        throw new Error('RPC unavailable');
      },
    });
    const result = await scanner.audit(migration());
    assert.strictEqual(result.allowed, false, 'incomplete audit must fail closed by default');
    assert.strictEqual(result.reasonCode, 'audit_incomplete');
  }

  console.log('Pump migration safety audit tests passed');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
