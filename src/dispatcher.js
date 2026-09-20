import { EndpointWorker } from './worker.js';
import { saveState, loadState } from './state.js';

const DEFAULTS = {
  maxAttempts: 4,      // 最多 4 次尝试（1 次首发 + 3 次重试）
  baseDelayMs: 100,    // 退避基数 100ms
  backoffFactor: 2,    // 退避倍数 2
  jitterRatio: 0.2,    // 抖动上限为退避时长的 20%
  timeoutMs: 2000,     // 单次请求超时
  ratePerSecond: 5,    // 每个 endpoint 每秒最多 5 次请求
  burst: undefined,    // 令牌桶容量，默认等于 ratePerSecond
  stateFile: 'state.json',
};

export class Dispatcher {
  constructor(options = {}) {
    this.options = { ...DEFAULTS, ...options };
    this.endpoints = new Map();      // endpointId -> { id, url, secret }
    this.workers = new Map();        // endpointId -> EndpointWorker
    this.seen = new Set();           // 所有见过的 "endpointId:eventId"，用于幂等
    this.deliveredKeys = new Set();  // 已成功投递的 key，重启后防重投
    this.deadLetters = [];           // 死信
    this.pendingItems = new Map();   // 尚未到达终态的事件，停机时持久化
    this.stats = { delivered: 0, duplicated: 0, dead: 0, retried: 0 };
    this.accepting = true;
  }

  static keyOf(item) {
    return `${item.endpointId}:${item.eventId}`;
  }

  registerEndpoint(endpoint) {
    this.endpoints.set(endpoint.id, { ...endpoint });
  }

  // 入队。重复 (endpointId, eventId) 返回 duplicated，不发请求。
  enqueue({ endpointId, eventId, payload }) {
    if (!this.accepting) return { status: 'rejected', reason: 'dispatcher is stopping' };
    const key = `${endpointId}:${eventId}`;
    if (this.seen.has(key)) {
      this.stats.duplicated += 1;
      return { status: 'duplicated' };
    }
    const endpoint = this.endpoints.get(endpointId);
    if (!endpoint) throw new Error(`unknown endpoint: ${endpointId}`);
    this.seen.add(key);
    const item = { endpointId, eventId, payload };
    this.pendingItems.set(key, item);
    this.#workerFor(endpoint).enqueue(item);
    return { status: 'queued' };
  }

  #workerFor(endpoint) {
    let worker = this.workers.get(endpoint.id);
    if (!worker) {
      worker = new EndpointWorker({
        endpoint,
        options: this.options,
        hooks: {
          onDelivered: (item) => {
            const key = Dispatcher.keyOf(item);
            this.stats.delivered += 1;
            this.deliveredKeys.add(key);
            this.pendingItems.delete(key);
          },
          onDead: (item, reason) => {
            const key = Dispatcher.keyOf(item);
            this.stats.dead += 1;
            this.deadLetters.push({ ...item, reason, deadAt: new Date().toISOString() });
            this.pendingItems.delete(key);
          },
          onRetried: () => {
            this.stats.retried += 1;
          },
        },
      });
      this.workers.set(endpoint.id, worker);
    }
    return worker;
  }

  // 等待所有 worker 排空（或按 stop 语义停下）。
  async idle() {
    await Promise.all([...this.workers.values()].map((worker) => worker.idle()));
  }

  async persist() {
    await saveState(this.options.stateFile, {
      version: 1,
      savedAt: new Date().toISOString(),
      endpoints: [...this.endpoints.values()],
      pending: [...this.pendingItems.values()],
      dead: this.deadLetters,
      deliveredKeys: [...this.deliveredKeys],
      stats: this.stats,
    });
  }

  // 从 state.json 恢复：重建幂等集合、死信，并把未完成事件重新入队（保持原顺序）。
  async restore() {
    const state = await loadState(this.options.stateFile);
    if (!state) return false;
    for (const endpoint of state.endpoints ?? []) this.registerEndpoint(endpoint);
    for (const key of state.deliveredKeys ?? []) {
      this.deliveredKeys.add(key);
      this.seen.add(key);
    }
    for (const dead of state.dead ?? []) {
      this.deadLetters.push(dead);
      this.seen.add(Dispatcher.keyOf(dead));
    }
    if (state.stats) Object.assign(this.stats, state.stats);
    for (const item of state.pending ?? []) {
      const key = Dispatcher.keyOf(item);
      this.seen.add(key);
      this.pendingItems.set(key, item);
      const endpoint = this.endpoints.get(item.endpointId);
      if (endpoint) this.#workerFor(endpoint).enqueue(item);
    }
    return true;
  }

  // 优雅停机：拒收新事件，把已入队事件处理到终态（drain=true），
  // 然后把未完成事件、死信、已投递集合写入 state.json。
  async stop({ drain = true } = {}) {
    this.accepting = false;
    for (const worker of this.workers.values()) worker.requestStop(drain);
    await this.idle();
    await this.persist();
  }
}
