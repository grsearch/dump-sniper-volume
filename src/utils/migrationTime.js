'use strict';

function normalizeUnixMs(value) {
  if (value == null || value === '') return null;

  if (value instanceof Date) {
    const ts = value.getTime();
    return Number.isFinite(ts) && ts > 0 ? Math.trunc(ts) : null;
  }

  if (typeof value === 'string' && !/^\d+(?:\.\d+)?$/.test(value.trim())) {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  return numeric > 10_000_000_000
    ? Math.trunc(numeric)
    : Math.trunc(numeric * 1000);
}

function firstPresent(payload, keys) {
  for (const key of keys) {
    if (payload?.[key] != null && payload[key] !== '') return payload[key];
  }
  return null;
}

function extractMigrationInfo(payload = {}, defaultSource = 'webhook_payload') {
  const migrationTime = normalizeUnixMs(firstPresent(payload, [
    'migrationTime',
    'migration_time',
    'migrationTimestamp',
    'migration_timestamp',
    'migratedAt',
    'migrated_at',
    'pairCreatedAt',
    'pair_created_at',
  ]));
  const migrationSlotValue = firstPresent(payload, [
    'migrationSlot',
    'migration_slot',
    'slot',
  ]);
  const migrationSlot = Number.isFinite(Number(migrationSlotValue))
    ? Number(migrationSlotValue)
    : null;
  const migrationSignature = firstPresent(payload, [
    'migrationSignature',
    'migration_signature',
    'signature',
    'txHash',
    'tx_hash',
  ]);
  const migrationTimeSource = firstPresent(payload, [
    'migrationTimeSource',
    'migration_time_source',
  ]) || (migrationTime ? defaultSource : null);

  return {
    migrationTime,
    migrationTimeSource,
    migrationSlot,
    migrationSignature: migrationSignature ? String(migrationSignature) : null,
  };
}

module.exports = {
  normalizeUnixMs,
  extractMigrationInfo,
};
