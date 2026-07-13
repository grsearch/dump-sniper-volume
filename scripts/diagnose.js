#!/usr/bin/env node
'use strict';

/**
 * diagnose.js (v3.17.18)
 * ======================
 * 综合诊断 — 在 dump-sniper 跑着的状态下运行,定位「为什么 slot 延迟越来越大」
 * 或「为什么没信号触发」的根因。不需要停服务。
 *
 * 6 项诊断:
 *   1. Dashboard /api/health — heartbeats 是否 STALE,counter 趋势
 *   2. Helius RPC 延迟 — getSlot / getLatestBlockhash / getMultipleAccounts
 *   3. LaserStream region TCP ping — 每个 region 的网络往返延迟
 *   4. Node.js event loop lag — 主线程是否被阻塞
 *   5. SQLite 写入延迟 — WAL 模式下持续写是否积压
 *   6. 最近 N 笔交易的 slot lag 趋势 — 阵发性 vs 单调上升
 *
 * 用法:
 *   node scripts/diagnose.js              # 完整诊断
 *   node scripts/diagnose.js --quick      # 跳过 RPC + TCP ping (秒级返回)
 *   node scripts/diagnose.js --json       # JSON 输出供机器解析
 */

require('dotenv').config();
const axios = require('axios');
const net = require('net');
const { config } = require('../src/config');

const args = process.argv.slice(2);
const QUICK = args.includes('--quick');
const JSON_MODE = args.includes('--json');

const RPC_URL = config.helius.rpcUrl;
const DASHBOARD = process.env.DASHBOARD_URL || `http://localhost:${config.server?.port || 3001}`;
const DB_PATH = config.storage?.dbPath || './data/sniper.db';

// ─────────────────────────────────────────────────────────
// 工具
// ─────────────────────────────────────────────────────────

function pad(s, n) { return String(s).padEnd(n); }
function color(s, code) { return JSON_MODE ? s : `\x1b[${code}m${s}\x1b[0m`; }
const red = (s) => color(s, 31);
const green = (s) => color(s, 32);
const yellow = (s) => color(s, 33);
const blue = (s) => color(s, 34);
const dim = (s) => color(s, 90);

const findings = []; // 结构化结论,用于 JSON 输出和最终总结
const note = (severity, msg) => findings.push({ severity, msg });

const out = (line = '') => { if (!JSON_MODE) console.log(line); };
const section = (n, title) => {
  if (!JSON_MODE) { out(''); out(blue(`━━━ ${n}. ${title} ━━━`)); }
};

async function rpcCall(method, params = []) {
  const t0 = Date.now();
  try {
    const { data } = await axios.post(RPC_URL,
      { jsonrpc: '2.0', id: 1, method, params },
      { timeout: 10000 });
    const dt = Date.now() - t0;
    if (data.error) return { ok: false, ms: dt, err: JSON.stringify(data.error) };
    return { ok: true, ms: dt, result: data.result };
  } catch (err) {
    return { ok: false, ms: Date.now() - t0, err: err.message };
  }
}

async function tcpPing(host, port, timeoutMs = 3000) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const sock = new net.Socket();
    let done = false;
    const finish = (err) => {
      if (done) return;
      done = true;
      sock.destroy();
      resolve({ ok: !err, ms: Date.now() - t0, err: err?.message });
    };
    sock.setTimeout(timeoutMs);
    sock.once('connect', () => finish(null));
    sock.once('error', (e) => finish(e));
    sock.once('timeout', () => finish(new Error('timeout')));
    sock.connect(port, host);
  });
}

async function measureEventLoopLag(samples = 5) {
  const lags = [];
  for (let i = 0; i < samples; i++) {
    const t0 = process.hrtime.bigint();
    await new Promise(r => setImmediate(r));
    const dt = Number(process.hrtime.bigint() - t0) / 1e6;
    lags.push(dt);
  }
  lags.sort((a, b) => a - b);
  return {
    min: lags[0],
    median: lags[Math.floor(lags.length / 2)],
    max: lags[lags.length - 1],
  };
}

// ─────────────────────────────────────────────────────────
// 1. Dashboard 健康
// ─────────────────────────────────────────────────────────
async function diagDashboard() {
  section(1, 'Dashboard /api/health');

  let health;
  try {
    const { data } = await axios.get(`${DASHBOARD}/api/health`, { timeout: 5000 });
    health = data;
  } catch (err) {
    out(red(`✗ 无法连接 dashboard: ${err.message}`));
    out(dim(`  → 服务可能没在跑,或 DASHBOARD_URL 错(默认 ${DASHBOARD})`));
    note('critical', `dashboard unreachable: ${err.message}`);
    return null;
  }

  out(`Status:  ${health.status === 'OK' ? green(health.status) : red(health.status)}`);
  out(`Uptime:  ${Math.round(health.uptime_s / 60)} 分钟`);
  out(`Memory:  ${health.mem_mb} MB`);

  // 心跳
  out('');
  out(dim('Heartbeats:'));
  const heartbeats = health.heartbeats || {};
  let staleCount = 0;
  for (const [name, h] of Object.entries(heartbeats)) {
    const tag = h.status === 'OK' ? green('OK   ') :
                h.status === 'STALE' ? red('STALE') : yellow('NEVER');
    const elapsed = h.elapsed_ms != null ? `${Math.round(h.elapsed_ms/1000)}s ago` : 'never';
    out(`  ${tag}  ${pad(name, 36)}  last beat ${elapsed}`);
    if (h.status === 'STALE') {
      staleCount++;
      note('critical', `heartbeat STALE: ${name} (${elapsed})`);
    }
  }
  if (staleCount > 0) {
    out(red(`  → ${staleCount} 个模块心跳 STALE,这是 slot 延迟主因`));
  }

  // Active alerts
  if (health.active_alerts && health.active_alerts.length > 0) {
    out('');
    out(red('Active alerts:'));
    for (const a of health.active_alerts) {
      out(`  [${a.severity}] ${a.name}: ${a.message || ''}`);
      note(a.severity === 'CRITICAL' ? 'critical' : 'warning', `alert: ${a.name}`);
    }
  }

  // Counter 趋势 — 看每分钟速率是不是在下降
  const snaps = health.snapshots_recent || [];
  if (snaps.length >= 4) {
    out('');
    out(dim('Counter 速率趋势 (最近 6 分钟,看是不是在下降):'));
    const interesting = [
      'TickStream.LS.firstSeen',
      'TickStream.SS.firstSeen',
      'DumpDetector.priceTicks',
      'DumpDetector.dumpSignals',
      'Executor.buyAttempts',
      'Executor.buySuccess',
      'Executor.buyFail',
      'SignalEngine.rejectedExpiredSlot',
      'PoolStateCache.refreshFail',
    ];
    out('  ' + pad('counter', 38) + pad('first', 10) + pad('mid', 10) + pad('last', 10) + 'trend');
    const mid = Math.floor(snaps.length / 2);
    for (const c of interesting) {
      const v0 = snaps[0].counters?.[c] ?? 0;
      const vm = snaps[mid].counters?.[c] ?? 0;
      const vl = snaps[snaps.length - 1].counters?.[c] ?? 0;
      if (v0 === 0 && vl === 0) continue;
      const rate1 = (vm - v0) / mid; // per-snapshot
      const rate2 = (vl - vm) / (snaps.length - mid);
      const trend = rate2 > rate1 * 1.2 ? green('↑') :
                    rate2 < rate1 * 0.5 ? red('↓ ' + Math.round((1 - rate2/rate1)*100) + '%') :
                    dim('→');
      out(`  ${pad(c, 38)}${pad(v0, 10)}${pad(vm, 10)}${pad(vl, 10)}${trend}`);
      // priceTicks 显著下降 = LaserStream 在退化
      if (c === 'DumpDetector.priceTicks' && rate1 > 0 && rate2 < rate1 * 0.3) {
        note('critical', `DumpDetector.priceTicks rate dropped ${Math.round((1 - rate2/rate1)*100)}% — LaserStream degrading`);
      }
      // rejectedExpiredSlot 暴涨 = MAX_SIGNAL_SLOT_GAP 太严 或 LaserStream 落后
      if (c === 'SignalEngine.rejectedExpiredSlot' && rate2 > 5) {
        note('warning', `EXPIRED_SLOT rejections high (${Math.round(rate2*2)}/min) — LaserStream lagging or slot gap too tight`);
      }
    }
  } else {
    out(dim('  数据不足 (snapshots < 4),服务可能刚启动'));
  }

  // 内存增长
  if (snaps.length >= 2) {
    const memStart = snaps[0].memMB;
    const memEnd = snaps[snaps.length - 1].memMB;
    const growth = memEnd - memStart;
    const dt_min = (snaps[snaps.length-1].ts - snaps[0].ts) / 60000;
    out('');
    out(`Memory: ${memStart} → ${memEnd} MB (${growth >= 0 ? '+' : ''}${growth} in ${dt_min.toFixed(1)} min)`);
    if (growth > 50 && dt_min < 10) {
      out(red('  ⚠️ 内存增长过快 → 可能引发 GC 暂停 → slot 延迟'));
      note('warning', `memory growth ${growth}MB in ${dt_min.toFixed(1)}min`);
    }
  }

  return health;
}

// ─────────────────────────────────────────────────────────
// 2. Helius RPC 延迟
// ─────────────────────────────────────────────────────────
async function diagRpc() {
  if (QUICK) return null;
  section(2, 'Helius RPC 延迟');

  // 5 次 getSlot
  const slotResults = [];
  for (let i = 0; i < 5; i++) {
    slotResults.push(await rpcCall('getSlot'));
    await new Promise(r => setTimeout(r, 100));
  }
  const ok = slotResults.filter(r => r.ok);
  if (ok.length === 0) {
    out(red(`✗ getSlot 全部失败: ${slotResults[0]?.err}`));
    note('critical', `RPC unreachable: ${slotResults[0]?.err}`);
    return null;
  }
  const slotMs = ok.map(r => r.ms).sort((a,b) => a-b);
  const med = slotMs[Math.floor(slotMs.length/2)];
  const max = slotMs[slotMs.length-1];
  const tag = med < 100 ? green('OK') : med < 300 ? yellow('SLOW') : red('VERY SLOW');
  out(`getSlot           : median=${med}ms  max=${max}ms  ${tag}`);
  if (med > 300) note('critical', `Helius RPC getSlot median=${med}ms (should be <100ms)`);

  // getLatestBlockhash
  const bh = await rpcCall('getLatestBlockhash');
  const bhTag = !bh.ok ? red('FAIL') : bh.ms < 200 ? green('OK') : bh.ms < 500 ? yellow('SLOW') : red('VERY SLOW');
  out(`getLatestBlockhash: ${bh.ms}ms  ${bhTag}`);
  if (bh.ok && bh.ms > 500) note('warning', `getLatestBlockhash slow: ${bh.ms}ms`);

  // getMultipleAccounts (PoolStateCache 关键路径)
  const gma = await rpcCall('getMultipleAccounts',
    [['So11111111111111111111111111111111111111112'], { encoding: 'jsonParsed' }]);
  const gmaTag = !gma.ok ? red('FAIL') : gma.ms < 200 ? green('OK') : gma.ms < 500 ? yellow('SLOW') : red('VERY SLOW');
  out(`getMultipleAccounts: ${gma.ms}ms  ${gmaTag}`);
  if (gma.ok && gma.ms > 500) note('critical', `getMultipleAccounts slow: ${gma.ms}ms — PoolStateCache will miss`);

  // Helius latest slot vs 我们的差距
  if (ok.length > 0) {
    const heliusSlot = ok[ok.length - 1].result;
    try {
      const health = await axios.get(`${DASHBOARD}/api/health`, { timeout: 3000 });
      // tickStream.latestSlot 没直接暴露,但 dashboard 端可能有
      // 这里能拿到最近 BUY 的 dump_slot,但需要 health 没暴露,先跳过
      out(dim(`  Helius getSlot 当前: ${heliusSlot}`));
    } catch (_) {}
  }
}

// ─────────────────────────────────────────────────────────
// 3. LaserStream Region TCP ping
// ─────────────────────────────────────────────────────────
async function diagLaserStream() {
  if (QUICK) return null;
  section(3, 'LaserStream Region TCP ping');

  // 从 env 推断启用的 region
  const endpoints = (process.env.HELIUS_LASERSTREAM_ENDPOINTS || 'fra,tyo,ewr')
    .split(',').map(s => s.trim()).filter(Boolean);

  const regionHosts = {
    fra: 'laserstream-mainnet-fra.helius-rpc.com',
    tyo: 'laserstream-mainnet-tyo.helius-rpc.com',
    ewr: 'laserstream-mainnet-ewr.helius-rpc.com',
    ams: 'laserstream-mainnet-ams.helius-rpc.com',
    sgp: 'laserstream-mainnet-sgp.helius-rpc.com',
    pit: 'laserstream-mainnet-pit.helius-rpc.com',
  };

  for (const region of endpoints) {
    const host = regionHosts[region];
    if (!host) {
      out(`  ${pad(region, 8)} ${yellow('(unknown region, skipping)')}`);
      continue;
    }
    // 3 次取 median
    const pings = [];
    for (let i = 0; i < 3; i++) {
      pings.push(await tcpPing(host, 443));
    }
    const okPings = pings.filter(p => p.ok).map(p => p.ms).sort((a,b) => a-b);
    if (okPings.length === 0) {
      out(`  ${pad(region, 8)} ${red('UNREACHABLE')} (${host})`);
      note('critical', `LaserStream region ${region} unreachable`);
    } else {
      const med = okPings[Math.floor(okPings.length/2)];
      const tag = med < 30 ? green(`${med}ms`) :
                  med < 80 ? yellow(`${med}ms`) :
                  red(`${med}ms`);
      out(`  ${pad(region, 8)} ${tag.padEnd(20)} ${dim('(' + host + ')')}`);
      if (med > 80) note('warning', `LaserStream ${region} TCP latency ${med}ms — consider switching region`);
    }
  }
  out(dim('  TCP ping 只测网络往返,不代表实际 gRPC 推送延迟,只用于排除明显网络问题'));
}

// ─────────────────────────────────────────────────────────
// 4. Event Loop Lag
// ─────────────────────────────────────────────────────────
async function diagEventLoop() {
  section(4, 'Node.js Event Loop Lag (此 diagnose 脚本自身)');
  const lag = await measureEventLoopLag(10);
  const tag = (ms) => ms < 5 ? green(`${ms.toFixed(2)}ms`) :
                     ms < 50 ? yellow(`${ms.toFixed(2)}ms`) :
                     red(`${ms.toFixed(2)}ms`);
  out(`Event loop lag (本进程): min=${tag(lag.min)} median=${tag(lag.median)} max=${tag(lag.max)}`);
  out(dim('  注意:此处测的是 diagnose.js 自己的 lag,不是 dump-sniper 主进程的'));
  out(dim('  要测主进程,需要在主进程内置 lag monitor (待加)'));
  if (lag.median > 50) {
    note('warning', `diagnose process event loop lag high (${lag.median.toFixed(0)}ms) — system busy`);
  }
}

// ─────────────────────────────────────────────────────────
// 5. SQLite 写入延迟
// ─────────────────────────────────────────────────────────
async function diagSqlite() {
  if (QUICK) return null;
  section(5, 'SQLite 写入延迟');

  let Database;
  try { Database = require('better-sqlite3'); }
  catch (_) { out(yellow('  better-sqlite3 不可用,跳过')); return; }

  const fs = require('fs');
  if (!fs.existsSync(DB_PATH)) {
    out(yellow(`  DB 文件不存在: ${DB_PATH},跳过`));
    return;
  }

  // 用只读模式打开,不影响主进程
  let db;
  try {
    db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
  } catch (err) {
    out(red(`  无法打开 DB: ${err.message}`));
    return;
  }

  try {
    // 测一个简单查询的延迟
    const t0 = Date.now();
    const row = db.prepare('SELECT COUNT(*) as n FROM positions').get();
    const dt = Date.now() - t0;
    out(`COUNT(positions) : ${dt}ms (${row.n} rows)`);

    const t1 = Date.now();
    db.prepare('SELECT COUNT(*) FROM trades').get();
    out(`COUNT(trades)    : ${Date.now() - t1}ms`);

    const t2 = Date.now();
    db.prepare('SELECT COUNT(*) FROM signals').get();
    out(`COUNT(signals)   : ${Date.now() - t2}ms`);

    // 看 WAL 文件大小
    const wal = DB_PATH + '-wal';
    if (fs.existsSync(wal)) {
      const sz = fs.statSync(wal).size;
      const tag = sz < 10 * 1024 * 1024 ? green('OK') :
                  sz < 100 * 1024 * 1024 ? yellow('LARGE') : red('VERY LARGE');
      out(`WAL file size   : ${(sz / 1024 / 1024).toFixed(1)} MB  ${tag}`);
      if (sz > 50 * 1024 * 1024) {
        out(red('  ⚠️ WAL 文件过大,可能 checkpoint 滞后 → SQLite 写入变慢 → 主线程阻塞'));
        note('warning', `WAL too large (${(sz/1024/1024).toFixed(0)}MB) — checkpoint lagging`);
      }
    }

    // DB 文件大小
    const sz = fs.statSync(DB_PATH).size;
    out(`DB file size    : ${(sz / 1024 / 1024).toFixed(1)} MB`);
  } finally {
    db.close();
  }
}

// ─────────────────────────────────────────────────────────
// 6. 最近 trades 的 slot lag 趋势
// ─────────────────────────────────────────────────────────
async function diagSlotLag() {
  section(6, '最近 BUY 的 slot lag 趋势');

  let Database;
  try { Database = require('better-sqlite3'); }
  catch (_) { out(yellow('  better-sqlite3 不可用,跳过')); return; }

  const fs = require('fs');
  if (!fs.existsSync(DB_PATH)) {
    out(yellow(`  DB 文件不存在: ${DB_PATH},跳过`));
    return;
  }

  const db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
  try {
    // 看 positions 表有没有 buy_slot 字段
    const cols = db.prepare("PRAGMA table_info(positions)").all().map(r => r.name);
    if (!cols.includes('buy_slot')) {
      out(yellow('  positions 表没有 buy_slot 字段,无法分析 slot lag'));
      out(dim('  → 需要等 v3.17.18 之后产生新数据 (旧仓位不会回填)'));
      return;
    }

    // 拿最近 50 个 BUY,带 dump_slot (从 signals 表 join,如果有)
    // 简化:只看 buy_slot 和 opened_at 的相对趋势
    const rows = db.prepare(`
      SELECT symbol, opened_at, buy_slot, exit_reason
      FROM positions
      WHERE buy_slot > 0
      ORDER BY opened_at DESC
      LIMIT 50
    `).all();

    if (rows.length === 0) {
      out(yellow('  没有带 buy_slot 的 position'));
      return;
    }

    // 按时间正序
    rows.reverse();

    // 我们没有 dump_slot 单独存,但可以用 signals 表 join
    // 简化:看 buy_slot 是不是单调递增 (健康) 还是阵发性 (异常)
    out(`找到 ${rows.length} 笔有 slot 数据的 BUY (从旧到新):`);
    out('');
    out(dim('  time              symbol         buy_slot     exit_reason'));
    for (const r of rows.slice(-20)) {  // 只显示最近 20 笔
      const t = new Date(r.opened_at).toISOString().slice(11, 19);
      out(`  ${pad(t, 18)}${pad(r.symbol || '?', 15)}${pad(r.buy_slot, 13)}${r.exit_reason || '(open)'}`);
    }

    // 如果 signals 表也有 slot 信息,可以算 dump_slot → buy_slot 差
    // (这部分需要 SignalEngine 写入 signals 表时记 slot,看下有没有)
    const sigCols = db.prepare("PRAGMA table_info(signals)").all().map(r => r.name);
    // 简化:不算精确 lag,只看 buy_slot 趋势

    // 算延迟 — 同一个 BUY 的 latest_slot vs buy_slot 差不存,先跳过
    out('');
    out(dim('  💡 完整 slot lag 需要 dump_slot 和 buy_slot 比对'));
    out(dim('     当前 SignalEngine 没把 dump tx slot 写到 signals 表'));
    out(dim('     需要额外改动才能精确算 lag (我可以做)'));
  } finally {
    db.close();
  }
}

// ─────────────────────────────────────────────────────────
// 总结
// ─────────────────────────────────────────────────────────
function printSummary() {
  if (JSON_MODE) {
    console.log(JSON.stringify({ findings, ts: Date.now() }, null, 2));
    return;
  }
  out('');
  out(blue('━━━ 诊断结论 ━━━'));
  if (findings.length === 0) {
    out(green('✓ 所有诊断项正常'));
    return;
  }
  const critical = findings.filter(f => f.severity === 'critical');
  const warning = findings.filter(f => f.severity === 'warning');

  if (critical.length > 0) {
    out(red(`\n${critical.length} CRITICAL:`));
    for (const f of critical) out(red(`  ✗ ${f.msg}`));
  }
  if (warning.length > 0) {
    out(yellow(`\n${warning.length} WARNING:`));
    for (const f of warning) out(yellow(`  ⚠ ${f.msg}`));
  }

  // 给行动建议
  out('');
  out(blue('行动建议:'));
  const hasRpcSlow = findings.some(f => /RPC|getSlot|getMultipleAccounts/i.test(f.msg));
  const hasLsSlow = findings.some(f => /LaserStream|priceTicks/i.test(f.msg));
  const hasMem = findings.some(f => /memory/i.test(f.msg));
  const hasStale = findings.some(f => /STALE/i.test(f.msg));
  const hasExpired = findings.some(f => /EXPIRED_SLOT/i.test(f.msg));

  if (hasRpcSlow && hasLsSlow) {
    out('  → Helius 服务整体退化,联系 Helius support 或临时降级监控代币数');
  } else if (hasRpcSlow) {
    out('  → 单 Helius RPC 慢,可能限速。检查 https://dashboard.helius.dev 配额');
  } else if (hasLsSlow) {
    out('  → LaserStream 推送慢,看 Step 3 的 region ping 是否能换区');
  }
  if (hasMem) {
    out('  → 重启 dump-sniper 清内存,然后用 npm run analyze 看哪个 counter 涨太快');
  }
  if (hasStale) {
    out('  → 心跳 STALE 的模块需要重启服务才能恢复');
  }
  if (hasExpired) {
    out('  → 检查 MAX_SIGNAL_SLOT_GAP 配置 (.env),或 LaserStream 落后于真实链');
  }
}

// ─────────────────────────────────────────────────────────
// 主流程
// ─────────────────────────────────────────────────────────
(async () => {
  if (!JSON_MODE) {
    out(blue('═══ Dump Sniper Diagnose v3.17.18 ═══'));
    out(dim(`Time: ${new Date().toISOString()}`));
    out(dim(`Dashboard: ${DASHBOARD}`));
    out(dim(`RPC: ${RPC_URL?.slice(0, 60)}..`));
    out(dim(`DB: ${DB_PATH}`));
  }

  await diagDashboard();
  await diagRpc();
  await diagLaserStream();
  await diagEventLoop();
  await diagSqlite();
  await diagSlotLag();

  printSummary();
  process.exit(0);
})().catch(err => {
  console.error(red(`[diagnose] fatal: ${err.message}`));
  console.error(err.stack);
  process.exit(2);
});
