'use strict';

/**
 * TickStream (v3.17: 多 region 订阅)
 * ===================================
 * 同时订阅多个 Helius LaserStream gRPC region（例如 FRA + AMS + EWR），
 * 谁先收到砸单 tx 就触发下游 — 用 signature LRU 去重。
 *
 * 为什么多 region：
 *   实测 LaserStream 推送延迟 116ms ~ 1528ms（13x 差异），其中 1.2~1.8s 的尾延迟主要来自
 *   "砸单方 tx 发到了离你订阅 region 远的 leader" → shred 传播 + Helius 节点接收都要时间。
 *   多 region 订阅取最快到达的那一份，能把那部分尾延迟压平。
 *
 * 关键设计：
 *   - 每个 region 独立一个 Client/stream，各自重连，互不影响
 *   - 监控列表变化时重建**所有** stream（保持简单，频次不高 — 一般添 token 是稀疏事件）
 *   - LRU signature 去重：最多 2000 项，5 分钟 TTL（覆盖最慢 region 的延迟范围）
 *   - 向后兼容：env 只配单一 endpoint 时退化成单 region 行为
 *
 * v1.1 历史修复保留：
 *   - 监控列表为空时不订阅（避免误订全网 Pump 流量）
 *   - accountInclude=[mints] + accountRequired=[PUMP_AMM_PROGRAM]
 *   - 监控列表变化时重建 stream
 *   - 自动重连 + 指数退避
 */

const Client = require('@triton-one/yellowstone-grpc').default;
const yellowstoneGrpc = require('@triton-one/yellowstone-grpc');
const { CommitmentLevel } = yellowstoneGrpc;
// v3.17.12: ShredStream UDP 数据源
let ShredListener;
try {
  ShredListener = require('shredstream').ShredListener;
} catch (_) {
  ShredListener = null; // SDK not installed → ShredStream disabled
}
// v3.17.6: @triton-one/yellowstone-grpc v1.4+ 要求 stream.write 收到 protobuf message
// 实例，而不是 plain JS object。新 napi-rs 路径下 plain object 会被静默拒收
// （TCP 连接 OK、subscribe 调用不报错、stream.write 不报错，但 server 端拒绝
//  序列化 → 永远收不到 data → "NEVER_BEAT" 告警）。
// SubscribeRequest.create() / SubscribeRequestFilterTransactions.create() 能把
// plain object 转成正确的 protobuf message。我们 defensive 导入：
//   - 优先用 .create()（新版 SDK）
//   - fallback 到 plain object（老版 SDK 兼容）
const SubscribeRequest = yellowstoneGrpc.SubscribeRequest || null;
const SubscribeRequestFilterTransactions =
  yellowstoneGrpc.SubscribeRequestFilterTransactions || null;
const EventEmitter = require('events');
const bs58Lib = require('bs58');
const bs58 = bs58Lib.default || bs58Lib;
const { config } = require('../config');
const { getMonitor } = require('../monitor/HealthMonitor');

const PUMP_AMM_PROGRAM_ID = config.programs.pumpAmm; // string

// v3.17.38: Pumpfun AMM v2 — constant product AMM (different program than v1 Pump AMM)
//   Coins migrated to v2 AMM are invisible to v1-only LaserStream subscription
const PUMP_AMM_V2_PROGRAM_ID = 'Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1';

// v3.17.24: Jupiter program IDs for Jupiter LS subscription
// Jupiter program is always in staticAccountKeys (not ALT), so LS accountInclude can match
const JUPITER_PROGRAM_IDS = [
  'JUP6LkbZbjS1jKKwapdHNy74cnZidQ6Ep5qJtREpsGS', // Jupiter V6 Aggregator
  'JUP4Fb2cqiRUcKKFCJYa6tVmKgFqA5bXfzFYsFCLvh7', // Jupiter V4 Aggregator
];

const monitor = getMonitor();
monitor.registerModule('TickStream', { staleMs: 90_000, label: 'LaserStream gRPC' });

// LRU + TTL signature 去重
// - 容量 2000：每秒砸盘信号数通常 < 10/s，5 分钟 = 300s → 最多 3000 项，2000 已经够
// - TTL 5 分钟：覆盖最慢 region 的尾延迟（实测 < 2s）+ 余量
const DEDUP_TTL_MS = 5 * 60_000;
const DEDUP_MAX = 2000;

class SignatureDedup {
  constructor() {
    this.map = new Map(); // signature → expireAt
  }
  /** 第一次见返回 true（应处理），重复返回 false（应丢弃） */
  shouldProcess(sig) {
    if (!sig) return true; // 没 signature 时不去重（保守）
    const now = Date.now();
    const existing = this.map.get(sig);
    if (existing && existing > now) {
      return false; // 重复
    }
    this.map.set(sig, now + DEDUP_TTL_MS);
    if (this.map.size > DEDUP_MAX) {
      this._evict(now);
    }
    return true;
  }
  _evict(now) {
    // 先清过期
    for (const [k, exp] of this.map) {
      if (exp <= now) this.map.delete(k);
      if (this.map.size <= DEDUP_MAX * 0.9) return;
    }
    // 还超容量 → 删最早写入的（Map 按插入顺序）
    while (this.map.size > DEDUP_MAX * 0.9) {
      const firstKey = this.map.keys().next().value;
      if (firstKey === undefined) break;
      this.map.delete(firstKey);
    }
  }
  size() {
    return this.map.size;
  }
}

/**
 * 单个 region 的连接实例。
 * 内部管理重连、订阅、生命周期。tx 来了上抛给 TickStream 由 dedup 统一过滤。
 */
class RegionStream {
  constructor({ endpoint, token, label, onTx, onConnected, onSlot, filterMode = 'pumpAmm' }) {
    this.endpoint = endpoint;
    this.token = token;
    this.label = label;
    this.onTx = onTx;
    this.onConnected = onConnected;
    this.onSlot = onSlot; // v3.17.29: slot update 回调(从 SubscribeRequest slots filter 来)
    // v3.17.24: filterMode
    //   'pumpAmm'  — accountInclude=mints, accountRequired=PUMP_AMM (default, current behavior)
    //   'jupiter'  — accountInclude=JUP_programs, accountRequired=[] (Jupiter route trades)
    this.filterMode = filterMode;

    this.client = null;
    this.stream = null;
    this.connected = false;
    this.reconnectAttempts = 0;
    this.shouldRun = false;
    this._reconnectScheduled = false;  // v3.17.21: 防抖标志,防止 error+end 双触发排两个重连
    this._currentMints = [];
    // v3.17.26: replay — 记录本 region 最后收到的 slot，重连时 fromSlot 从这里开始
    this._lastReceivedSlot = 0;
  }

  async start(mints) {
    this.shouldRun = true;
    this._currentMints = Array.from(mints);
    // v3.17.24: Jupiter filter mode 不需要 mints（用 JUP_program_ids）
    if (this._currentMints.length === 0 && this.filterMode !== 'jupiter') {
      console.log(`[TickStream:${this.label}] no mints to watch, idle`);
      return;
    }
    await this._connect();
  }

  async stop() {
    this.shouldRun = false;
    await this._closeStream();
  }

  async rebuild(mints) {
    this._currentMints = Array.from(mints);
    await this._closeStream();
    await new Promise((r) => setTimeout(r, 500));
    // v3.17.24: Jupiter filter mode 允许 mints 为空
    if (this.shouldRun && (this._currentMints.length > 0 || this.filterMode === 'jupiter')) {
      await this._connect();
    }
  }

  async _closeStream() {
    // v3.17.21: 彻底释放 gRPC 连接,每步独立 try/catch
    //   - cancel() 而非 end(): end() 对卡住的流无效,cancel() 才强制释放
    //   - removeAllListeners(): 释放事件监听闭包引用,允许 GC
    //   - client.close() 优先,兜底 _connectedGrpcClient.close()
    if (this.stream) {
      // v3.17.26: destroy() 替代 cancel() — destroy 触发 _destroy() → nativeStream.close() 释放 Rust 内存
      // cancel() 在 ClientDuplexStream 上不存在（被静默吞异常），导致 Rust 端 DuplexStream 从未被 close()
      // 这是 RSS 从 ~200MB 爬到 2GB OOM 的根因
      //
      // 重要：先 removeAllListeners() 再 destroy()，否则 destroy() 触发的
      // 'close'/'error' 事件会走到 _handleEnd/_handleError → _scheduleReconnect，
      // 和 _closeStream 调用方的重连冲突（双重重连）
      try { this.stream.removeAllListeners(); } catch (_) {}
      try { this.stream.destroy(); } catch (_) {}
      this.stream = null;
    }
    if (this.client) {
      try {
        if (typeof this.client.close === 'function') this.client.close();
        else if (this.client._connectedGrpcClient) this.client._connectedGrpcClient.close();
      } catch (_) {}
      try { if (this.client.removeAllListeners) this.client.removeAllListeners(); } catch (_) {}
      this.client = null;
    }
    this.connected = false;
  }

  async _connect() {
    // v3.17.24: Jupiter filter mode 不需要 mints
    if (this._currentMints.length === 0 && this.filterMode !== 'jupiter') return;
    try {
      this.client = new Client(
        this.endpoint,
        this.token,
        {
          'grpc.max_receive_message_length': 64 * 1024 * 1024,
          // v3.17.28: gRPC keepalive — 根治低流量间隙连接被静默断开
          'grpc.keepalive_time_ms': 30000,
          'grpc.keepalive_timeout_ms': 5000,
          'grpc.keepalive_permit_without_calls': 1,
          'grpc.http2.max_pings_without_data': 0,
          'grpc.http2.min_time_between_pings_ms': 15000,
        },
      );
      // v5 API: must call connect() before subscribe()
      if (typeof this.client.connect === 'function') {
        await this.client.connect();
      }
      console.log(`[TickStream:${this.label}] connect() done, calling subscribe()...`);
      this.stream = await this.client.subscribe();
      console.log(`[TickStream:${this.label}] subscribe() returned, setting up handlers...`);

      this.stream.on('data', (msg) => this._handleMessage(msg));
      this.stream.on('error', (err) => this._handleError(err));
      this.stream.on('end', () => this._handleEnd());
      this.stream.on('close', () => this._handleEnd());

      await this._sendSubscribeRequest();
      this.connected = true;
      this.reconnectAttempts = 0;
      monitor.inc(`TickStream.${this.label}.connectsTotal`, 1, 'TickStream');
      monitor.beat('TickStream', `${this.label}:connected:${this._currentMints.length}_mints`);
      console.log(
        `[TickStream:${this.label}] connected${this.filterMode === 'jupiter' ? ' (Jupiter mode)' : ''}, watching ${this._currentMints.length} mints`,
      );
      if (this.onConnected) this.onConnected(this.label);
    } catch (err) {
      monitor.recordError('TickStream', err, { phase: 'connect', region: this.label });
      console.error(`[TickStream:${this.label}] connect failed: ${err.message}`);
      this._scheduleReconnect();
    }
  }

  async _sendSubscribeRequest() {
    const mints = this._currentMints;
    // v3.17.24: Jupiter filter mode 不依赖 mints（用 JUP_program_ids），允许 mints 为空
    if (mints.length === 0 && this.filterMode !== 'jupiter') return;

    // v3.17.24: 支持 filterMode='jupiter' 订阅 Jupiter 路由交易
    //   Jupiter program 总在 staticAccountKeys（不在 ALT），LS 能匹配
    //   LS 推送的消息包含完整 meta（loadedAddresses + preTokenBalances）
    //   本地用 preTokenBalances 过滤是否涉及监控 mint
    //   数据量：Jupiter ~20 tx/s × 3 region = 60 tx/s，完全可控
    let filterPlain;
    if (this.filterMode === 'jupiter') {
      filterPlain = {
        vote: false,
        failed: false,
        accountInclude: JUPITER_PROGRAM_IDS,
        accountExclude: [],
        accountRequired: [],
      };
    } else {
      // pumpAmm 模式 — v1 和 v2 用独立 filter key（Helius LS 不支持单 filter 多 accountRequired）
      //   v3.17.38: 新增 v2 (Ce6TQ) 独立订阅，之前只有 v1 (pAMMBay)
      filterPlain = {
        vote: false,
        failed: false,
        accountInclude: mints,
        accountExclude: [],
        accountRequired: [PUMP_AMM_PROGRAM_ID],
      };
      const v2Filter = {
        vote: false,
        failed: false,
        accountInclude: mints,
        accountExclude: [],
        accountRequired: [PUMP_AMM_V2_PROGRAM_ID],
      };
    }
    const filter = SubscribeRequestFilterTransactions
      ? SubscribeRequestFilterTransactions.create(filterPlain)
      : filterPlain;

    // v3.17.24: Jupiter 订阅用独立的 filter key
    const filterKey = this.filterMode === 'jupiter' ? 'jupiterTrades' : 'pumpAmmTrades';
    const requestPlain = {
      transactions: { [filterKey]: filter },
      slots: {
        // v3.17.29: 订阅 slot 推进事件,独立于 tx 数据流
        // 用于维护"真实系统当下 slot",不受 LS 服务端对特定 mint 迟到推送的污染
        // filterByCommitment=true → 只推 PROCESSED commitment 的 slot,不推 confirmed/finalized,减半数据量
        // interslotUpdates=false → 不推 slot 内部子状态变化,只推 slot 切换,数据量再降
        // ⚠️ 2026-05-27: Helius LaserStream 不支持在 transactions 订阅中混 slots filter
        //   导致 subscribe stream receive failed → 重连风暴。先回退为空,后续改用独立连接
        // systemSlot: { filterByCommitment: true, interslotUpdates: false },
      },
      accounts: {},
      blocks: {},
      blocksMeta: {},
      entry: {},
      transactionsStatus: {},
      accountsDataSlice: [],
      commitment: CommitmentLevel.PROCESSED,
    };

    // v3.17.38: 如果是 pumpAmm 模式，额外加 v2 AMM filter
    if (this.filterMode !== 'jupiter' && typeof v2Filter !== 'undefined') {
      const v2FilterObj = SubscribeRequestFilterTransactions
        ? SubscribeRequestFilterTransactions.create(v2Filter)
        : v2Filter;
      requestPlain.transactions['pumpAmmV2Trades'] = v2FilterObj;
    }

    // v3.17.40: replay 限制回看窗口
    //   Bug: currentSlot=0 时 cap 不生效 → LS 从旧 slot 开始推所有历史
    //   修复: currentSlot=0 时跳过 fromSlot（让 LS 从当前 slot 开始推）
    //   只有 currentSlot 有值且 gap 在合理范围内才设 fromSlot
    const maxReplaySlots = parseInt(process.env.MAX_REPLAY_SLOTS || '150', 10); // ~60s
    // v3.17.40: 优先用 SlotSub 真实 slot 算 replay gap
    // 原 bug:优先 _latestSlotFromTx,若 tx 流已延迟它也是旧值 → gap 算偏小 →
    // 不触发 cap → 走正常 replay → 拉一堆旧数据 → off-heap 内存暴涨 + tx 积压
    // 修复:用 _latestSlotFromSlotUpdate(SlotSub 独立连接,不被 tx 延迟污染)算 gap 才准
    const currentSlot = this._latestSlotFromSlotUpdate || this._latestSlotFromTx || this._latestSlot || 0;
    if (this._lastReceivedSlot > 0 && currentSlot > 0) {
      const gap = currentSlot - this._lastReceivedSlot;
      if (gap > maxReplaySlots) {
        // 断线太久，只补最后 60 秒
        const replayFrom = currentSlot - maxReplaySlots;
        requestPlain.fromSlot = replayFrom;
        console.warn(
          `[TickStream:${this.label}] replay capped: gap=${gap} > ${maxReplaySlots}, fromSlot=${replayFrom} (skipping ${gap - maxReplaySlots} stale slots)`,
        );
        monitor.inc(`TickStream.${this.label}.replayCapped`, 1, 'TickStream');
      } else {
        // 正常 replay，gap 在合理范围内
        requestPlain.fromSlot = this._lastReceivedSlot;
        console.log(
          `[TickStream:${this.label}] replay fromSlot=${this._lastReceivedSlot} (gap=${gap}, current=${currentSlot})`,
        );
      }
    } else if (this._lastReceivedSlot > 0 && currentSlot === 0) {
      // currentSlot 未知（启动初期/重连时 SS 还没数据）
      // 不设 fromSlot → LS 从当前 slot 开始推，避免 replay 历史风暴
      console.warn(
        `[TickStream:${this.label}] replay skipped: currentSlot unknown, not setting fromSlot (avoids stale replay)`,
      );
      monitor.inc(`TickStream.${this.label}.replaySkipped_noCurrentSlot`, 1, 'TickStream');
    }

    const request = SubscribeRequest
      ? SubscribeRequest.create(requestPlain)
      : requestPlain;

    return new Promise((resolve, reject) => {
      this.stream.write(request, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  _handleMessage(msg) {
    // v3.17.29: 处理 slot update 消息(SubscribeRequest 里订阅了 slots)
    // slot update 消息结构: { slot: { slot: <num>, parent, status }, filters: ['systemSlot'] }
    // 通过 onSlot 回调上抛给 TickStream 维护 _latestSlot
    if (msg.slot && typeof msg.slot === 'object' && msg.slot.slot != null) {
      const slotRaw = msg.slot.slot;
      const slot = typeof slotRaw === 'string' ? Number(slotRaw) : slotRaw;
      if (Number.isFinite(slot)) {
        monitor.inc(`TickStream.${this.label}.slotUpdatesReceived`, 1, 'TickStream');
        if (this.onSlot) this.onSlot(slot, this.label);
      }
      return; // slot update 消息不走 tx 路径
    }

    if (!msg.transaction) return;
    // v3.17.27: 修 replay — slot 在 msg.transaction.slot 而不是 msg.slot
    //   LaserStream gRPC 消息结构: {transaction: {slot, transaction: {...}}, ...}
    //   msg.slot 是 undefined，实际 slot 在 msg.transaction.slot
    const msgSlot = msg.slot ?? msg.transaction?.slot;
    if (msgSlot != null) {
      const slot = typeof msgSlot === 'string' ? Number(msgSlot) : msgSlot;
      if (Number.isFinite(slot) && slot > this._lastReceivedSlot) {
        this._lastReceivedSlot = slot;
      }
    }
    // v3.17.26 DEBUG: removed YOTS debug check (no longer needed)
    monitor.inc(`TickStream.${this.label}.txReceived`, 1, 'TickStream');
    monitor.beat('TickStream', `${this.label}:tx`);
    this.onTx(msg.transaction, this.label);
  }

  _handleError(err) {
    // v3.17.21: 不再各自 _closeStream,关闭统一交给 _scheduleReconnect 里做
    monitor.inc(`TickStream.${this.label}.streamErrors`, 1, 'TickStream');
    monitor.recordError('TickStream', err, { phase: 'stream', region: this.label });
    console.error(`[TickStream:${this.label}] stream error: ${err.message || err}`);
    this.connected = false;
    this._scheduleReconnect();
  }

  _handleEnd() {
    // v3.17.21: 同上,不再各自 _closeStream
    if (!this.shouldRun) return;
    monitor.inc(`TickStream.${this.label}.streamEnded`, 1, 'TickStream');
    console.warn(`[TickStream:${this.label}] stream ended`);
    this.connected = false;
    this._scheduleReconnect();
  }

  _scheduleReconnect() {
    // v3.17.21: 防抖 — error+end 可能短时间双触发,只排一个重连
    if (this._reconnectScheduled) return;
    if (!this.shouldRun || (this._currentMints.length === 0 && this.filterMode !== 'jupiter')) return;
    this._reconnectScheduled = true;
    monitor.inc(`TickStream.${this.label}.reconnects`, 1, 'TickStream');
    const delay = Math.min(30_000, 1000 * Math.pow(2, this.reconnectAttempts));
    this.reconnectAttempts += 1;
    console.log(
      `[TickStream:${this.label}] reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`,
    );
    setTimeout(async () => {
      this._reconnectScheduled = false;  // 回调开头就重置,下次真断线还能正常排程
      if (!this.shouldRun) return;
      await this._closeStream();         // 重连前先彻底关旧的
      this._connect();
    }, delay);
  }
}

/** 提取 tx signature（base58）—— 用于多 region 去重。 */
function extractSignature(txMessage) {
  try {
    // LaserStream gRPC: txMessage = {transaction: {signature, isVote, transaction, meta, index}, slot}
    // ShredStream: txMessage = {slot, transaction: {signatures: [Buffer]}}
    // 优先从 LaserStream 的 transaction.signature 取，然后 fallback 到 transaction.signatures[0]
    let sig = txMessage?.transaction?.signature;
    if (!sig) sig = txMessage?.transaction?.signatures?.[0];
    if (!sig) sig = txMessage?.signature; // 兜底
    if (!sig) return null;
    if (typeof sig === 'string') return sig;
    if (Buffer.isBuffer(sig)) return bs58.encode(sig);
    if (sig instanceof Uint8Array) return bs58.encode(Buffer.from(sig));
    // protobuf 对象兜底
    try {
      const buf = Buffer.from(sig);
      if (buf.length > 0) return bs58.encode(buf);
    } catch (_) {}
    return null;
  } catch (_) {
    return null;
  }
}

class TickStream extends EventEmitter {
  constructor() {
    super();
    this.watchedMints = new Set();
    this.shouldRun = false;
    // v3.17.7: 最新观察到的 slot（任何 region 都更新，dedup 去重不影响）
    //   用于 SignalEngine 判断"砸盘信号 vs 当前最新 slot"差距，过滤陈旧信号
    this._latestSlot = 0;
    // v3.17.29: slot → wall-clock 参考点
    // 每次收到 slot update 就刷新,downstream 用它做 slotToWallClockMs 换算
    // refWallClockMs - (refSlot - slot) * 400 ≈ slot 的真实落链时刻
    this._slotRefWallClockMs = 0;
    this._slotRefSlot = 0;
    // v3.17.29: 区分 tx-driven slot vs slot-update-driven slot(诊断用)
    this._latestSlotFromSlotUpdate = 0;
    this._latestSlotFromTx = 0;
    // v3.17.41: LS-only slot (不被 SS 数据污染), 用于 laggyReconnect 检测
    this._latestLsSlot = 0;

    this.regions = [];
    this.dedup = new SignatureDedup();
    this._sigFirstRegion = new Map(); // v3.17.12: sig → { region, ts }
    this._rebuildTimer = null;
    this._rebuildInProgress = false;
    this._rebuildQueued = false;

    // v3.17.25: Reader-Worker 分离 — reader 回调只入队，worker 异步消费
    //   目的：让 gRPC reader 永不阻塞，避免突破 Helius 450-slot 阈值被切流
    this._msgQueue = [];           // 有界队列：{ txMessage, region }
    this._queueMax = 5000;         // 上限（满了丢最老的，绝不无限堆积）
    this._queueDropped = 0;        // 丢弃计数（监控用）
    this._workerRunning = false;   // worker 循环是否活跃

    // v3.17.13: SS 领先速度统计
    this._ssLeadSamples = []; // 最近 N 个样本 (ms, 正数=SS 快)
    this._ssLeadCounters = {
      ssFirstCount: 0,
      lsFirstCount: 0,
      ahFirstCount: 0,
      ssMatchedCount: 0,
      ssOrphanCount: 0,
    };
    this._ssLeadStatsTimer = null;

    // ---- Helius LaserStream regions ----
    const laserEndpoints = config.helius.laserstreamEndpoints || [];
    if (laserEndpoints.length === 0) {
      throw new Error(
        '[TickStream] no LaserStream endpoints configured. ' +
          'Set HELIUS_LASERSTREAM_ENDPOINTS (comma-separated) or HELIUS_LASERSTREAM_ENDPOINT.',
      );
    }
    laserEndpoints.forEach((ep, idx) => {
      const label = this._labelForEndpoint(ep, idx, 'LS');
      this.regions.push(
        new RegionStream({
          endpoint: ep,
          token: config.helius.laserstreamToken,
          label,
          onTx: (txMessage, region) => this._enqueue(txMessage, region),
          onConnected: (region) => this.emit('regionConnected', region),
          // v3.17.29: LS 同时推 slot update,用于维护真实当下 slot
          onSlot: (slot, region) => this._onLsSlot(slot, region),
        }),
      );
    });

    // ---- AllenHark gRPC regions ----
    // AllenHark 也用 Yellowstone Geyser 协议，同 @triton-one/yellowstone-grpc 客户端
    // 作为额外数据源，IP 白名单制（无需 token 或用单独 token）
    const ahEndpoints = config.allenhark.grpcEndpoints || [];
    ahEndpoints.forEach((ep, idx) => {
      const label = this._labelForEndpoint(ep, idx, 'AH');
      this.regions.push(
        new RegionStream({
          endpoint: ep,
          token: config.allenhark.grpcToken || undefined,
          label,
          onTx: (txMessage, region) => this._enqueue(txMessage, region),
          onConnected: (region) => this.emit('regionConnected', region),
        }),
      );
    });

    // ---- v3.17.24: Jupiter LaserStream regions ----
    // 单独订阅 Jupiter program 的交易，给 Jupiter 路由交易加第二源
    // 过滤：accountInclude=[JUP_programs], accountRequired=[]
    // 本地用 preTokenBalances/postTokenBalances 过滤监控 mint
    // 数据量：~20 tx/s × region数，可控
    const jupiterEnabled = parseInt(process.env.LS_JUPITER_ENABLED || '1', 10);
    if (jupiterEnabled) {
      laserEndpoints.forEach((ep, idx) => {
        const label = this._labelForEndpoint(ep, idx, 'JUP');
        this.regions.push(
          new RegionStream({
            endpoint: ep,
            token: config.helius.laserstreamToken,
            label,
            filterMode: 'jupiter',
            onTx: (txMessage, region) => this._enqueueJupiter(txMessage, region),
            onConnected: (region) => this.emit('regionConnected', region),
          }),
        );
      });
    }

    console.log(
      `[TickStream] initialized with ${this.regions.length} region(s): ` +
        this.regions.map((r) => r.label).join(', '),
    );

    // ---- v3.17.29: 独立 SlotSubscriber ----
    // Helius 不允许在 transactions 订阅中混 slots filter(实测导致 subscribe stream receive failed)
    // 所以开一条独立连接，只订阅 slot updates，几乎不消耗配额(slots updates 体积极小)
    // 用第一个 LS endpoint 作为连接目标(同一个 endpoint，不同订阅)
    this._slotSubscriber = null;
    this._slotSubscriberRunning = false;
    if (laserEndpoints.length > 0) {
      this._slotSubscriberEndpoint = laserEndpoints[0];
      console.log(
        `[TickStream:SlotSub] will subscribe slot updates via ${this._labelForEndpoint(laserEndpoints[0], 0, 'LS')}`,
      );
    }

    // ---- v3.17.12: ShredStream UDP ----
    // ShredStream 推送的是已解码的完整交易，不经过 gRPC
    // 和 LaserStream 并行跑，谁先到用谁（同 signature dedup）
    this.shredStreamPort = parseInt(process.env.SHREDSTREAM_PORT || '0', 10);
    this._shredListener = null;
    this._shredStreamRunning = false;
    if (this.shredStreamPort > 0 && ShredListener) {
      console.log(`[TickStream:SS] ShredStream enabled on UDP port ${this.shredStreamPort}`);
    } else if (this.shredStreamPort > 0 && !ShredListener) {
      console.warn('[TickStream:SS] SHREDSTREAM_PORT set but shredstream SDK not installed');
    }

    // v3.17.17: 注入 tokenRegistry 用于 SS pre-warm 的 base_vault → mint 反查
    //   不在 constructor 参数里 (保持向后兼容);用 setTokenRegistry() 注入
    this._tokenRegistry = null;
  }

  /** v3.17.17: 注入 tokenRegistry,SS pre-warm 用它做 base_vault 反查 */
  setTokenRegistry(tokenRegistry) {
    this._tokenRegistry = tokenRegistry;
  }

  _labelForEndpoint(endpoint, idx, prefix = '') {
    const m = endpoint.match(/(?:^|[\.\/\:\-])(fra|ams|ewr|slc|tyo|sgp|lax|lon|pitt)\b/i);
    if (m) return (prefix ? prefix + '-' : '') + m[1].toUpperCase();
    try {
      const host = endpoint.replace(/^https?:\/\//, '').split(/[:/]/)[0];
      const first = host.split('.')[0];
      return (prefix ? prefix + '-' : '') + (first || `R${idx}`).toUpperCase().slice(0, 6);
    } catch (_) {
      return (prefix ? prefix + '-' : '') + `R${idx}`;
    }
  }

  async start(initialMints = []) {
    this.shouldRun = true;
    initialMints.forEach((m) => this.watchedMints.add(m));
    if (this.watchedMints.size === 0) {
      console.log('[TickStream] no tokens to watch yet, idle');
      return;
    }
    await Promise.all(this.regions.map((r) => r.start(this.watchedMints)));
    // v3.17.29: 启动独立 SlotSubscriber
    this._startSlotSubscriber();
    // v3.17.12: Start ShredStream
    this._startShredStream();
    // v3.17.25: 启动 worker 循环（消费 _msgQueue）
    this._startWorker();
    // v3.17.13: 启动 SS lead stats 定时打印（每 60s）+ sigFirstRegion 清理
    this._ssLeadStatsTimer = setInterval(() => {
      this._cleanupSigFirstRegion();
      this._printSsLeadStats();
    }, 60_000);

    // v3.17.41: LS 延迟自动重连
    // 故障表现: _latestSlotFromTx 整体落后 SlotSub 几百 slots (实测 943)
    // 正常抖动: 0-50 slots。真故障: >300 slots。用 300 阈值清晰区分
    this._laggySec = 0;
    console.log('[TickStream] 🔧 laggyReconnect timer starting (5s interval, threshold=100 slots, 10s sustained)');
    this._laggyReconnectTimer = setInterval(() => {
      const lsSlot = this._latestLsSlot || 0;
      const slotSlot = this._latestSlotFromSlotUpdate || 0;
      // v3.22: 修复 lsSlot=0 时 lag 永远为0的 bug
      // 当 SlotSub 有数据但 LS 完全没收到 tx 时，lag = slotSlot（表示 LS 严重滞后）
      const lag = slotSlot > 0 ? (lsSlot > 0 ? slotSlot - lsSlot : slotSlot) : 0;
      monitor.set('TickStream.txStreamLag', lag, 'TickStream');
      if (slotSlot > 0 && lag > 100) {
        this._laggySec = (this._laggySec || 0) + 5;
        console.log(`[TickStream] LS lag detected: ${lag} slots (SlotUpdate=${slotSlot}, LsSlot=${lsSlot}), sustained=${this._laggySec}s`);
        if (this._laggySec >= 10) {
          console.error(`[TickStream] ⚠️ LS lag ${lag} slots for ${this._laggySec}s — reconnecting ALL tx regions`);
          monitor.inc('TickStream.laggyReconnect', 1, 'TickStream');
          // 重连所有 tx region (SlotSub 不动, 它是健康的)
          for (const r of this.regions) {
            try {
              if (typeof r._scheduleReconnect === 'function') {
                r.reconnectAttempts = 0;
                r._scheduleReconnect();
              }
            } catch (e) {
              console.warn(`[TickStream] reconnect region ${r.label} failed: ${e.message}`);
            }
          }
          this._laggySec = 0;
        }
      } else {
        this._laggySec = 0;
      }
    }, 5000);
  }

  async stop() {
    this.shouldRun = false;
    this._slotSubscriberRunning = false; // v3.17.29: 停 SlotSubscriber
    this._stopShredStream();
    if (this._ssLeadStatsTimer) {
      clearInterval(this._ssLeadStatsTimer);
      this._ssLeadStatsTimer = null;
    }
    if (this._laggyReconnectTimer) {
      clearInterval(this._laggyReconnectTimer);
      this._laggyReconnectTimer = null;
    }
    await Promise.all(this.regions.map((r) => r.stop()));
  }

  async updateSubscription(mints) {
    this.watchedMints = new Set(mints);
    if (this._rebuildTimer) clearTimeout(this._rebuildTimer);
    this._rebuildTimer = setTimeout(() => {
      this._rebuildTimer = null;
      this._performRebuild().catch((err) => {
        monitor.recordError('TickStream', err, { phase: 'rebuild' });
        console.error(`[TickStream] rebuild failed: ${err.message}`);
      });
    }, 2000);
  }

  async _performRebuild() {
    if (this._rebuildInProgress) {
      this._rebuildQueued = true;
      return;
    }
    this._rebuildInProgress = true;
    try {
      do {
        this._rebuildQueued = false;
        const targetMints = new Set(this.watchedMints);
        console.log(
          `[TickStream] subscription change → rebuilding all ${this.regions.length} region(s) ` +
            `(${targetMints.size} mints)`,
        );
        await Promise.all(this.regions.map((r) => r.rebuild(targetMints)));
      } while (this._rebuildQueued);
    } finally {
      this._rebuildInProgress = false;
    }
  }

  /** 任一 region 收到 tx 时调用。signature 去重后才 emit 给下游。 */
  /**
   * v3.17.24: 处理 Jupiter LS 订阅收到的交易
   * 用 preTokenBalances/postTokenBalances 本地过滤监控 mint
   * 通过的交给 _handleRegionTx 统一处理（和 Pump AMM LS / SS 竞速）
   */
  _handleJupiterTx(txMessage, region) {
    // 快速本地过滤：检查 preTokenBalances/postTokenBalances 是否涉及监控 mint
    // 这个过滤在 reader 回调里做（很快，纯内存 Set 查找），过滤后再入队
    const tx = txMessage?.transaction || txMessage;
    const meta = tx?.meta;
    if (!meta) return; // 没有meta，无法过滤

    const preBalances = meta.preTokenBalances || [];
    const postBalances = meta.postTokenBalances || [];
    const allBalances = preBalances.length > 0 ? preBalances : postBalances;
    if (allBalances.length === 0) return;

    // 检查是否有监控 mint
    let hasWatchedMint = false;
    for (const b of allBalances) {
      if (this.watchedMints.has(b.mint)) {
        hasWatchedMint = true;
        break;
      }
    }
    if (!hasWatchedMint) {
      monitor.inc('TickStream.jupiterFilteredOut', 1, 'TickStream');
      return;
    }

    // 通过 → 入队（v3.17.25: 不再直接调 _handleRegionTx）
    monitor.inc('TickStream.jupiterMintMatch', 1, 'TickStream');
    this._enqueue(txMessage, region);
  }

  // ─── v3.17.25: Reader-Worker 分离 ────────────────────────────
  //
  // reader 回调(LS/AH/JUP/SS)只入队，不直接处理。
  // 独立 worker 用 setImmediate 持续消费队列，让出事件循环。
  // 目的：gRPC reader 永不阻塞 → 不突破 Helius 450-slot 阈值 → 不被切流。

  /**
   * 入队 —— reader 回调调这个，不做任何处理
   * 有界队列：满了丢最老的（背压，绝不无限堆积）
   */
  _enqueue(txMessage, region) {
    if (this._msgQueue.length >= this._queueMax) {
      this._msgQueue.shift(); // 满了丢最老的
      this._queueDropped++;
      monitor.inc('TickStream.queueDropped', 1, 'TickStream');
    }
    this._msgQueue.push({ txMessage, region });
  }

  /**
   * Jupiter 过滤后再入队的包装（reader 回调入口）
   */
  _enqueueJupiter(txMessage, region) {
    this._handleJupiterTx(txMessage, region); // 内部会过滤后调 _enqueue
  }

  /**
   * 启动 worker 循环 — 在 start() 里调用
   * 用 setImmediate 持续抽干队列，每轮最多处理 100 条，让出事件循环
   */
  _startWorker() {
    if (this._workerRunning) return;
    this._workerRunning = true;

    const loop = () => {
      if (!this.shouldRun) {
        this._workerRunning = false;
        return;
      }

      let processed = 0;
      while (this._msgQueue.length > 0 && processed < 100) {
        const { txMessage, region } = this._msgQueue.shift();
        try {
          this._handleRegionTx(txMessage, region);
        } catch (e) {
          // 单条失败不影响后续
          monitor.inc('TickStream.workerErrors', 1, 'TickStream');
        }
        processed++;
      }

      // 更新监控指标
      monitor.set('TickStream.queueLength', this._msgQueue.length, 'TickStream');
      monitor.set('TickStream.queueDroppedTotal', this._queueDropped, 'TickStream');

      // 队列空了或处理了一批，让出事件循环后继续
      setImmediate(loop);
    };

    loop();
  }

  /**
   * v3.17.29: 处理来自 LS slots subscription 的 slot update
   * - 更新 _latestSlot(永远以这个为准,不再被 tx 自带 slot 污染)
   * - 刷新 slot → wall-clock 参考点
   * - tx 自带 slot 仅在 _latestSlotFromSlotUpdate 落后超过 50 时做 fallback
   */
  _onLsSlot(slot, region) {
    if (!Number.isFinite(slot) || slot <= 0) return;

    if (slot > this._latestSlotFromSlotUpdate) {
      this._latestSlotFromSlotUpdate = slot;
      // 刷新参考点:此 slot 在墙钟"现在"
      this._slotRefSlot = slot;
      this._slotRefWallClockMs = Date.now();
    }

    // _latestSlot 取 slot update 和 tx 自带 slot 的最大值
    // 但优先信任 slot update,因为它独立于 tx 数据流,不会被服务端迟到推送污染
    if (slot > this._latestSlot) {
      this._latestSlot = slot;
      monitor.set('TickStream.latestSlot', this._latestSlot, 'TickStream');
    }
    monitor.set('TickStream.latestSlotFromSlotUpdate', this._latestSlotFromSlotUpdate, 'TickStream');
  }

  /**
   * v3.17.29: 把 slot 换算成对应的 wall-clock 毫秒
   * 基于最新参考点: refWallClockMs - (refSlot - slot) * 400
   * 400 = Solana 平均 slot time (ms)
   * 若参考点未建立(LS 还没推 slot update),返回 null
   */
  slotToWallClockMs(slot) {
    if (!this._slotRefSlot || !this._slotRefWallClockMs) return null;
    return this._slotRefWallClockMs - (this._slotRefSlot - slot) * 400;
  }

  /**
   * v3.17.29: 启动独立 SlotSubscriber 连接
   * 只订阅 slot updates，不订阅 transactions
   * 用第一个 LS endpoint，与交易流完全解耦
   */
  _startSlotSubscriber() {
    if (!this._slotSubscriberEndpoint || this._slotSubscriberRunning) return;
    this._slotSubscriberRunning = true;

    const label = 'SlotSub';
    const connectAndSubscribe = async () => {
      while (this._slotSubscriberRunning && this.shouldRun) {
        try {
          const client = new Client(
            this._slotSubscriberEndpoint,
            config.helius.laserstreamToken,
            {
              'grpc.max_receive_message_length': 4 * 1024 * 1024, // slot updates 很小,4MB 足够
              'grpc.keepalive_time_ms': 30000,
              'grpc.keepalive_timeout_ms': 5000,
              'grpc.keepalive_permit_without_calls': 1,
              'grpc.http2.max_pings_without_data': 0,
              'grpc.http2.min_time_between_pings_ms': 15000,
            },
          );
          if (typeof client.connect === 'function') await client.connect();
          const stream = await client.subscribe();

          // 只订阅 slots，不订阅 transactions
          const requestPlain = {
            transactions: {},
            slots: {
              slotUpdates: { filterByCommitment: true, interslotUpdates: false },
            },
            accounts: {},
            blocks: {},
            blocksMeta: {},
            entry: {},
            transactionsStatus: {},
            accountsDataSlice: [],
            commitment: CommitmentLevel.PROCESSED,
          };
          const request = SubscribeRequest
            ? SubscribeRequest.create(requestPlain)
            : requestPlain;

          await new Promise((resolve, reject) => {
            stream.write(request, (err) => {
              if (err) reject(err);
              else resolve();
            });
          });

          console.log(`[TickStream:${label}] connected, receiving slot updates`);
          monitor.beat('TickStream', `${label}:connected`);

          // v3.17.29: 用事件模式读取(和 RegionStream 一致,不用 for await)
          let _slotSubMsgCount = 0;
          const onMessage = (msg) => {
            if (!this._slotSubscriberRunning) { stream.destroy(); return; }
            _slotSubMsgCount++;
            // debug: log first 3 messages to see actual format
            if (_slotSubMsgCount <= 3) {
              const keys = Object.keys(msg);
              const slotType = typeof msg.slot;
              const slotVal = msg.slot;
              console.log(`[TickStream:${label}] DEBUG msg #${_slotSubMsgCount}: keys=${keys.join(',')}, slot type=${slotType}, slot=${JSON.stringify(slotVal)?.slice(0,100)}`);
            }
            // 检查 slot update 消息: msg.slot 是对象 {slot, parent, status}
            // v3.17.41-fix: 同时支持 msg.slot 为 number (直接 slot 值)
            if (msg.slot != null) {
              let slot;
              if (typeof msg.slot === 'object' && msg.slot.slot != null) {
                slot = typeof msg.slot.slot === 'string' ? Number(msg.slot.slot) : msg.slot.slot;
              } else if (typeof msg.slot === 'number' || typeof msg.slot === 'string') {
                slot = typeof msg.slot === 'string' ? Number(msg.slot) : msg.slot;
              }
              if (Number.isFinite(slot) && slot > 0) {
                monitor.inc('TickStream.SlotSub.slotUpdatesReceived', 1, 'TickStream');
                this._onLsSlot(slot, label);
              }
            }
          };

          const onError = (err) => {
            if (!this._slotSubscriberRunning) return;
            monitor.recordError('TickStream', err, { phase: 'slotSubscriber' });
            console.error(`[TickStream:${label}] stream error: ${err.message || err}`);
            stream.removeAllListeners();
          };

          const onEnd = () => {
            if (!this._slotSubscriberRunning) return;
            console.warn(`[TickStream:${label}] stream ended, reconnecting in 5s...`);
            stream.removeAllListeners();
          };

          stream.on('data', onMessage);
          stream.on('error', onError);
          stream.on('end', onEnd);
          stream.on('close', onEnd);

          // 等待 stream 结束或 _slotSubscriberRunning 变 false
          await new Promise((resolve) => {
            const check = setInterval(() => {
              if (!this._slotSubscriberRunning) {
                clearInterval(check);
                stream.destroy();
                resolve();
              }
            }, 1000);
            stream.on('close', () => { clearInterval(check); resolve(); });
            stream.on('end', () => { clearInterval(check); resolve(); });
          });

          if (this._slotSubscriberRunning) {
            console.warn(`[TickStream:${label}] reconnecting in 5s...`);
          }
        } catch (err) {
          if (!this._slotSubscriberRunning) break;
          monitor.recordError('TickStream', err, { phase: 'slotSubscriber' });
          console.error(`[TickStream:${label}] error: ${err.message}, reconnecting in 5s...`);
        }

        // 重连间隔
        if (this._slotSubscriberRunning) {
          await new Promise((r) => setTimeout(r, 5000));
        }
      }
      console.log(`[TickStream:${label}] stopped`);
    };

    connectAndSubscribe().catch((err) => {
      console.error(`[TickStream:${label}] fatal: ${err.message}`);
    });
  }

  _handleRegionTx(txMessage, region) {
    const sig = extractSignature(txMessage);
    const isFirst = this.dedup.shouldProcess(sig);

    // v3.17.7→v3.17.29: tx 自带 slot 仅在 slot-update-driven 路径落后时做 fallback
    // 正常情况下 _latestSlot 由 _onLsSlot 维护,tx 自带 slot 不再无条件更新
    // 只有当 LS slots subscription 还没建立(_latestSlotFromSlotUpdate=0)时才用 tx slot
    const slotRaw = txMessage?.slot;
    if (slotRaw != null) {
      const slot = typeof slotRaw === 'string' ? Number(slotRaw) : slotRaw;
      if (Number.isFinite(slot)) {
        if (slot > this._latestSlotFromTx) {
          this._latestSlotFromTx = slot;
          monitor.set('TickStream.latestSlotFromTx', this._latestSlotFromTx, 'TickStream');
        }
        // v3.17.41: 记录 LS-only slot (不受 SS 影响), 用于 laggyReconnect 延迟检测
        if (slot > this._latestLsSlot) {
          this._latestLsSlot = slot;
        }
        // fallback: 只在 slot-update 路径未建立时才让 tx slot 推进 _latestSlot
        if (this._latestSlotFromSlotUpdate === 0 && slot > this._latestSlot) {
          this._latestSlot = slot;
        }
      }
    }

    if (!isFirst) {
      monitor.inc(`TickStream.${region}.dedup_dup`, 1, 'TickStream');
      monitor.inc('TickStream.dedupDups', 1, 'TickStream');
      // v3.17.13: dedup 命中,说明这个 sig 之前已经被另一个 region 看到了
      if (sig) {
        const firstInfo = this._sigFirstRegion.get(sig);
        if (firstInfo) {
          const leadMs = Date.now() - firstInfo.ts;
          this._recordRegionPair(firstInfo.region, region, leadMs);
          this._sigFirstRegion.delete(sig);
        }
      }
      return;
    }
    monitor.inc(`TickStream.${region}.dedup_first`, 1, 'TickStream');
    monitor.inc('TickStream.txReceived', 1, 'TickStream');
    monitor.beat('TickStream', `tx_first:${region}`);
    monitor.set('TickStream.dedupSize', this.dedup.size(), 'TickStream');
    monitor.set('TickStream.latestSlot', this._latestSlot, 'TickStream');
    if (sig) {
      this._sigFirstRegion.set(sig, { region, ts: Date.now() });
      if (region === 'SS') this._ssLeadCounters.ssFirstCount++;
      else if (region.startsWith('AH')) this._ssLeadCounters.ahFirstCount++;
      else this._ssLeadCounters.lsFirstCount++;
    } else {
      // sig is null — could not extract signature
    }
    this.emit('transaction', txMessage, { firstRegion: region });
  }

  /**
   * v3.17.13: 记录两个 region 之间的到达时间差
   */
  _recordRegionPair(firstRegion, secondRegion, leadMs) {
    let signedLead = null;
    if (firstRegion === 'SS' && !secondRegion.startsWith('AH') && secondRegion !== 'SS') {
      signedLead = leadMs;
      this._ssLeadCounters.ssMatchedCount++;
    } else if (secondRegion === 'SS' && !firstRegion.startsWith('AH') && firstRegion !== 'SS') {
      signedLead = -leadMs;
      this._ssLeadCounters.ssMatchedCount++;
    }
    if (signedLead !== null) {
      this._ssLeadSamples.push(signedLead);
      if (this._ssLeadSamples.length > 500) {
        this._ssLeadSamples.splice(0, this._ssLeadSamples.length - 500);
      }
      monitor.inc('TickStream.SS_LS_pairs', 1, 'TickStream');
    }
  }

  /**
   * v3.17.13: 清理过老的 _sigFirstRegion 条目
   */
  _cleanupSigFirstRegion() {
    const now = Date.now();
    const cutoff = now - 30_000;
    let orphanCount = 0;
    let ssOrphan = 0;
    for (const [sig, info] of this._sigFirstRegion) {
      if (info.ts < cutoff) {
        if (info.region === 'SS') ssOrphan++;
        this._sigFirstRegion.delete(sig);
        orphanCount++;
      }
    }
    if (ssOrphan > 0) this._ssLeadCounters.ssOrphanCount += ssOrphan;
    if (orphanCount > 0) {
      monitor.inc('TickStream.sigFirstRegion_cleaned', orphanCount, 'TickStream');
    }
  }

  /**
   * v3.17.13: 每 60 秒打印一次 SS lead 统计
   */
  _printSsLeadStats() {
    const samples = this._ssLeadSamples.slice();
    const c = this._ssLeadCounters;
    const totalFirst = c.ssFirstCount + c.lsFirstCount + c.ahFirstCount;

    if (samples.length === 0 && totalFirst === 0) {
      return;
    }

    const sorted = samples.slice().sort((a, b) => a - b);
    const n = sorted.length;
    const median = n > 0 ? sorted[Math.floor(n / 2)] : 0;
    const p95 = n > 0 ? sorted[Math.min(n - 1, Math.floor(n * 0.95))] : 0;
    const p05 = n > 0 ? sorted[Math.max(0, Math.floor(n * 0.05))] : 0;
    const mean = n > 0 ? Math.round(samples.reduce((s, v) => s + v, 0) / n) : 0;
    const ssWinCount = samples.filter((v) => v > 0).length;
    const lsWinCount = samples.filter((v) => v < 0).length;
    const ssWinPct = n > 0 ? Math.round((ssWinCount / n) * 100) : 0;

    monitor.set('TickStream.SS_lead_median_ms', median, 'TickStream');
    monitor.set('TickStream.SS_lead_p95_ms', p95, 'TickStream');
    monitor.set('TickStream.SS_lead_mean_ms', mean, 'TickStream');
    monitor.set('TickStream.SS_win_pct', ssWinPct, 'TickStream');
    monitor.set('TickStream.SS_samples', n, 'TickStream');
    monitor.set('TickStream.SS_first_count', c.ssFirstCount, 'TickStream');
    monitor.set('TickStream.LS_first_count', c.lsFirstCount, 'TickStream');
    monitor.set('TickStream.SS_orphan_count', c.ssOrphanCount, 'TickStream');

    console.log(
      `[TickStream:SS_STATS] over last hour (n=${n}): ` +
      `SS 领先 median=${median}ms p95=${p95}ms p05=${p05}ms mean=${mean}ms | ` +
      `SS 先到=${ssWinCount}/${n} (${ssWinPct}%) | ` +
      `LS 反而先到=${lsWinCount}/${n} | ` +
      `first 计数 SS=${c.ssFirstCount} LS=${c.lsFirstCount} AH=${c.ahFirstCount} | ` +
      `SS 孤儿(LS 漏掉)=${c.ssOrphanCount}`,
    );
  }

  // ─── v3.17.12: ShredStream UDP 数据源 ───────────────────────
  // ShredStream 推送原始交易（serialized VersionedTransaction），
  // 我们需要从中提取 Pump AMM 相关交易，构造和 LaserStream 兼容的 txMessage。
  // 关键：ShredStream 不做 gRPC 过滤，需要我们自己匹配 mint。

  _startShredStream() {
    if (this.shredStreamPort <= 0 || !ShredListener) return;
    if (this._shredStreamRunning) return;

    // v3.17.17: 检查 Linux UDP buffer 是否调够 (默认 256KB,SDK 推荐 25MB)
    // 不调够会在高负载下丢 shred — 实测「时不时漏 1 个 slot」就是这个症状
    this._checkUdpBuffer();

    try {
      this._shredListener = ShredListener.bind(this.shredStreamPort);
      this._shredStreamRunning = true;
      console.log(`[TickStream:SS] bound to UDP ${this.shredStreamPort}`);

      // 启动异步消费循环
      this._shredLoop().catch((err) => {
        if (this._shredStreamRunning) {
          console.error(`[TickStream:SS] loop error: ${err.message}`);
          monitor.recordError('TickStream', err, { phase: 'shredstream_loop' });
        }
      });
    } catch (err) {
      console.error(`[TickStream:SS] bind failed: ${err.message}`);
      monitor.recordError('TickStream', err, { phase: 'shredstream_bind' });
    }
  }

  /**
   * v3.17.17: 检查 Linux UDP rmem_max 是否调够
   * shredstream.com 文档要求 ≥ 25MB,否则在高 shred 流量下丢包
   */
  _checkUdpBuffer() {
    if (process.platform !== 'linux') return; // 只 Linux 有这个问题
    try {
      const fs = require('fs');
      const max = parseInt(fs.readFileSync('/proc/sys/net/core/rmem_max', 'utf8').trim(), 10);
      const REQUIRED = 26_214_400; // 25 MB
      if (max < REQUIRED) {
        console.warn(
          `[TickStream:SS] ⚠️  net.core.rmem_max=${max} (${(max/1024/1024).toFixed(1)} MB) < 25 MB. ` +
          `Will likely drop shreds under load. Fix:\n` +
          `    sudo sysctl -w net.core.rmem_max=26214400\n` +
          `    sudo sysctl -w net.core.rmem_default=26214400\n` +
          `    echo 'net.core.rmem_max=26214400' | sudo tee -a /etc/sysctl.conf\n` +
          `    echo 'net.core.rmem_default=26214400' | sudo tee -a /etc/sysctl.conf`,
        );
      } else {
        console.log(`[TickStream:SS] ✓ net.core.rmem_max=${(max/1024/1024).toFixed(1)} MB (≥ 25 MB required)`);
      }
    } catch (err) {
      // /proc 读取失败 (容器或非 Linux),跳过检查
    }
  }

  _stopShredStream() {
    this._shredStreamRunning = false;
    if (this._shredListener) {
      try {
        this._shredListener.close();
      } catch (_) {}
      this._shredListener = null;
    }
  }

  async _shredLoop() {
    const { VersionedTransaction, PublicKey } = require('@solana/web3.js');
    const PUMP_AMM = new Set([PUMP_AMM_PROGRAM_ID, PUMP_AMM_V2_PROGRAM_ID]);

    // v3.17.12: byte scan 预过滤 — 跳过 95%+ 无关交易，省掉昂贵的 deserialize
    // v3.17.38: 同时扫描 v1 (pAMMBay) 和 v2 (Ce6TQ) 的 program ID bytes
    const PUMP_AMM_BYTES = new PublicKey(PUMP_AMM_PROGRAM_ID).toBuffer();
    const PUMP_AMM_V2_BYTES = new PublicKey(PUMP_AMM_V2_PROGRAM_ID).toBuffer();

    // v3.17.17: Pump AMM sell instruction discriminator (sha256("global:sell")[0..8])
    //   来源: https://deepwiki.com/pump-fun/pump-public-docs/4.2-program-instructions
    //   sell instruction data 布局:
    //     [0..8]   sell discriminator
    //     [8..16]  base_amount_in (u64 LE)   — 卖出的代币数量(base unit)
    //     [16..24] min_quote_amount_out (u64 LE) — 至少收回的 SOL(lamports)
    //   account index 7 = pool_base_token_account (= tokenRegistry 里的 pool_base_vault)
    //   我们用 account[7] 反查 mint(避免依赖 base_mint 在 account[3] 的索引)
    const SELL_DISC = Buffer.from([51, 230, 133, 164, 1, 127, 131, 173]);
    // 用 base_vault 反向查 mint(每次 watchedMints 变化时重建)
    let baseVaultToMint = this._rebuildBaseVaultMap();
    let lastVaultMapBuildTs = Date.now();

    for await (const batch of this._shredListener) {
      if (!this._shredStreamRunning) break;

      const slot = Number(batch.slot);
      if (Number.isFinite(slot)) {
        if (slot > this._latestSlotFromTx) {
          this._latestSlotFromTx = slot;
        }
        // v3.17.40: SS slot 始终推进 _latestSlot — SS 是独立数据源，不受 LS 断线影响
        //   LS 断线后 _latestSlot 冻结导致 SignalEngine slot gap 误杀
        //   只在 SS slot > _latestSlot 时更新（不回退）
        if (slot > this._latestSlot) {
          this._latestSlot = slot;
        }
      }

      if (!batch.transactions || batch.transactions.length === 0) continue;

      // 每 5s 重建一次 base_vault → mint 映射(应对新加代币)
      if (Date.now() - lastVaultMapBuildTs > 5000) {
        baseVaultToMint = this._rebuildBaseVaultMap();
        lastVaultMapBuildTs = Date.now();
      }

      let ssMatch = 0;
      for (const rawTx of batch.transactions) {
        try {
          // 快速 byte scan：检查 raw tx 是否包含 Pump AMM program ID (v1 or v2)
          const buf = Buffer.isBuffer(rawTx) ? rawTx : Buffer.from(rawTx);
          if (!buf.includes(PUMP_AMM_BYTES) && !buf.includes(PUMP_AMM_V2_BYTES)) continue;

          const tx = VersionedTransaction.deserialize(new Uint8Array(rawTx));

          // v3.17.12→v3.34: 保留 hasWatchedMint 过滤 — 只有已监控的 mint 进入 _enqueue
          // 避免全量 Pump AMM 交易涌入队列导致性能问题
          // 新 mint 发现走 _tryEmitPrewarm 里的 newMintDiscovered 事件（独立路径）
          const accountKeys = tx.message.staticAccountKeys || [];
          let hasWatchedMint = false;
          for (const key of accountKeys) {
            if (this.watchedMints.has(key.toBase58())) {
              hasWatchedMint = true;
              break;
            }
          }
          if (!hasWatchedMint) {
            // v3.34: 未知 mint — 仍然走 _tryEmitPrewarm 触发 newMintDiscovered
            // 但不进入 _enqueue（避免全量交易涌入 DumpDetector）
            // 先确认是 Pump AMM 交易
            let hasPumpAmmNew = false;
            for (const key of accountKeys) {
              if (PUMP_AMM.has(key.toBase58())) {
                hasPumpAmmNew = true;
                break;
              }
            }
            if (hasPumpAmmNew) {
              const sigBytes = tx.signatures[0];
              const sig = sigBytes ? bs58.encode(Buffer.from(sigBytes)) : null;
              this._tryEmitPrewarm(tx, accountKeys, sig, slot, SELL_DISC, baseVaultToMint);
            }
            continue;
          }

          // 二次确认 Pump AMM（防误判）
          let hasPumpAmm = false;
          for (const key of accountKeys) {
            if (PUMP_AMM.has(key.toBase58())) {
              hasPumpAmm = true;
              break;
            }
          }
          if (!hasPumpAmm) continue;

          // 提取 signature
          const sigBytes = tx.signatures[0];
          const sig = sigBytes ? bs58.encode(Buffer.from(sigBytes)) : null;

          // ============================================================
          // v3.17.17: SS Pre-warm — 在入队之前就扫 sell instruction
          // 即使这笔 tx 后来被 LaserStream 也推送(dedup_dup),pre-warm 已经
          // 提前 50-200ms 启动了 pool state RPC,Executor.buy 时 cache 就是 hot 的
          // v3.17.25: pre-warm 仍在 reader 回调里做（要抢时间），dedup 移到 worker
          // ============================================================
          this._tryEmitPrewarm(tx, accountKeys, sig, slot, SELL_DISC, baseVaultToMint);

          // v3.17.25: 不在 reader 里做 dedup/统计/_sigFirstRegion 了，全部移到 worker 里
          // 只构造 txMessage 入队

          // 构造和 LaserStream 兼容的 txMessage
          const signatureBuffers = tx.signatures.map((s) => Buffer.from(s));
          const accountKeyBuffers = accountKeys.map((k) => k.toBuffer());

          const txMessage = {
            slot,
            transaction: {
              signatures: signatureBuffers,
              message: {
                accountKeys: accountKeyBuffers,
                instructions: tx.message.compiledInstructions?.map((ix) => ({
                  programIdIndex: ix.programIdIndex,
                  accounts: Array.from(ix.accountKeyIndexes),
                  data: Buffer.from(ix.data),
                })) || [],
              },
            },
            meta: {
              err: null,
              logMessages: null,
            },
          };

          // v3.17.25: SS 也走入队，不直接 emit
          // 统计和 _sigFirstRegion 由 worker 里 _handleRegionTx 统一做
          this._enqueue(txMessage, 'SS');
          ssMatch++;  // reader 层只统计 pumpTxs 匹配数
        } catch (_) {
          // deserialize 失败（shred 可能不完整），跳过
        }
      }

      if (ssMatch > 0) {
        monitor.inc('TickStream.SS.pumpTxs', ssMatch, 'TickStream');
      }
    }
  }

  // ─── v3.17.17: SS Pre-warm 支持 ─────────────────────────────────
  //
  // 用 SS 推过来的 raw tx 解析 sell instruction,在 LaserStream 把完整 meta
  // 推过来之前 50-200ms 启动 pool state cache 刷新。这样真正 BUY 触发时,
  // cache 已经是 hot 的,省 80-150ms RPC。
  //
  // 重要约束:
  //   - SS 不走 dedup (即使 LaserStream 也会推同一笔,我们要的就是先抢先 refresh)
  //   - SS 信号本身**不触发买入**,只 pre-warm。真正 BUY 决策仍走 LaserStream + meta
  //   - 解析失败/不是 sell 不 emit。误 emit 的代价是 1 次 RPC,可接受

  /**
   * 用 tokenRegistry 重建 pool_base_vault → mint 的映射
   * SS 收到 sell 指令时用 instruction account[7] (pool_base_token_account) 反查 mint
   */
  _rebuildBaseVaultMap() {
    const map = new Map();
    if (!this._tokenRegistry) return map;
    try {
      for (const t of this._tokenRegistry.listActive()) {
        if (t.pool_base_vault) {
          map.set(t.pool_base_vault, {
            mint: t.mint,
            symbol: t.symbol,
            poolAddress: t.pool_address,
            decimals: t.decimals || 6,
          });
        }
      }
    } catch (_) {}
    return map;
  }

  /**
   * 扫一笔 SS tx 找 Pump AMM sell instruction;若涉及监控 mint,emit prewarmSignal
   *
   * @param {VersionedTransaction} tx
   * @param {PublicKey[]} accountKeys
   * @param {string|null} sig
   * @param {number} slot
   * @param {Buffer} SELL_DISC sell discriminator bytes
   * @param {Map<string, {mint, symbol, poolAddress, decimals}>} baseVaultToMint
   */
  _tryEmitPrewarm(tx, accountKeys, sig, slot, SELL_DISC, baseVaultToMint) {
    try {
      const compiledIxs = tx.message.compiledInstructions || [];
      if (compiledIxs.length === 0) return;

      // 找 Pump AMM program 在 accountKeys 中的索引 (v1 or v2)
      let pumpProgramIdx = -1;
      for (let i = 0; i < accountKeys.length; i++) {
        const key = accountKeys[i].toBase58();
        if (key === PUMP_AMM_PROGRAM_ID || key === PUMP_AMM_V2_PROGRAM_ID) {
          pumpProgramIdx = i;
          break;
        }
      }
      if (pumpProgramIdx < 0) return;

      // 遍历指令找 Pump AMM 的 sell instruction
      for (const ix of compiledIxs) {
        if (ix.programIdIndex !== pumpProgramIdx) continue;
        const data = Buffer.from(ix.data);
        if (data.length < 24) continue; // sell instruction 至少 8(disc) + 8(base_in) + 8(min_quote_out)
        // 比较 discriminator
        let isSell = true;
        for (let i = 0; i < 8; i++) {
          if (data[i] !== SELL_DISC[i]) { isSell = false; break; }
        }
        if (!isSell) continue;

        // 解析 args
        const baseAmountIn = data.readBigUInt64LE(8);
        const minQuoteOut = data.readBigUInt64LE(16);
        const minQuoteOutSol = Number(minQuoteOut) / 1e9; // lamports → SOL

        // sell instruction account 布局(Pump AMM IDL):
        //   [0] pool, [1] user, [2] global_config, [3] base_mint, [4] quote_mint,
        //   [5] user_base_ata, [6] user_quote_ata,
        //   [7] pool_base_token_account, [8] pool_quote_token_account, ...
        // 用 account[7] 反查我们的 tokenRegistry
        const accIdxs = Array.from(ix.accountKeyIndexes);
        if (accIdxs.length < 8) continue;
        const poolBaseAtaIdx = accIdxs[7];
        if (poolBaseAtaIdx >= accountKeys.length) continue;
        const poolBaseAta = accountKeys[poolBaseAtaIdx].toBase58();

        const tokInfo = baseVaultToMint.get(poolBaseAta);
        if (!tokInfo) {
          // v3.34: 未知 mint — 从 sell instruction 的 account[3] 提取 base_mint
          // emit newMintDiscovered 事件让 index.js 自动添加到 tokenRegistry
          const baseMintIdx = accIdxs[3];
          if (baseMintIdx != null && baseMintIdx < accountKeys.length) {
            const newMint = accountKeys[baseMintIdx].toBase58();
            if (!this.watchedMints.has(newMint)) {
              // 从 account[7] 也保存 pool_base_vault
              const newPoolBaseVault = poolBaseAta;
              // account[0] 是 pool address
              const poolIdx = accIdxs[0];
              const newPoolAddress = (poolIdx != null && poolIdx < accountKeys.length)
                ? accountKeys[poolIdx].toBase58() : null;
              // account[8] 是 pool_quote_token_account
              const poolQuoteAtaIdx = accIdxs[8];
              const newPoolQuoteVault = (poolQuoteAtaIdx != null && poolQuoteAtaIdx < accountKeys.length)
                ? accountKeys[poolQuoteAtaIdx].toBase58() : null;

              if (minQuoteOutSol >= prewarmMinQuoteSol) {
                monitor.inc('TickStream.SS.newMintDiscovered', 1, 'TickStream');
                this.emit('newMintDiscovered', {
                  mint: newMint,
                  poolAddress: newPoolAddress,
                  poolBaseVault: newPoolBaseVault,
                  poolQuoteVault: newPoolQuoteVault,
                  minQuoteOutSol,
                  baseAmountIn: baseAmountIn.toString(),
                  slot,
                  signature: sig,
                  ts: Date.now(),
                });
                if (process.env.SS_PREWARM_DEBUG === 'true') {
                  console.log(
                    `[TickStream:SS] 🆕 newMint: ${newMint.slice(0, 8)}.. ` +
                    `pool=${newPoolAddress?.slice(0, 6)}.. min_quote=${minQuoteOutSol.toFixed(3)} SOL slot=${slot}`,
                  );
                }
              }
            }
          }
          continue;
        }

        // 阈值:min_quote_out 太小的不 pre-warm(普通小卖单,RPC 不值)
        // 注意 min_quote_out 通常是用户设的滑点下限,真实 quote 会大一些
        // 这里用 0.5 SOL 作为下限(对应至少 ~1-2 SOL 实际卖出)
        const prewarmMinQuoteSol = parseFloat(process.env.SS_PREWARM_MIN_QUOTE_SOL || '0.5');
        if (minQuoteOutSol < prewarmMinQuoteSol) continue;

        monitor.inc('TickStream.SS.prewarmEmitted', 1, 'TickStream');
        // emit prewarm 信号(同步,下游 listener 必须立即响应)
        this.emit('prewarmSignal', {
          source: 'SS',
          slot,
          signature: sig,
          mint: tokInfo.mint,
          symbol: tokInfo.symbol,
          poolAddress: tokInfo.poolAddress,
          minQuoteOutSol,
          baseAmountIn: baseAmountIn.toString(),
          ts: Date.now(),
        });

        if (process.env.SS_PREWARM_DEBUG === 'true') {
          console.log(
            `[TickStream:SS] 🔥 prewarm: ${tokInfo.symbol || tokInfo.mint.slice(0, 6)} ` +
            `min_quote_out=${minQuoteOutSol.toFixed(3)} SOL slot=${slot} sig=${sig?.slice(0, 8)}..`,
          );
        }

        // 一笔 tx 一般只有一个 Pump AMM sell instruction,但保险起见允许多个
      }
    } catch (err) {
      // 解析失败不影响主流程
      monitor.inc('TickStream.SS.prewarmParseFail', 1, 'TickStream');
    }
  }

  /** v3.17.7: 暴露 latestSlot 给 SignalEngine 做过期判断 */
  get latestSlot() {
    return this._latestSlot;
  }
}

module.exports = TickStream;
