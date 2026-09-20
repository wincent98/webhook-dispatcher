import { computeDelay } from './backoff.js';
import { postJson } from './http-client.js';
import { RateLimiter } from './rate-limiter.js';
import { sign } from './sign.js';
import { loadStateFile, saveStateFile } from './state-store.js';

const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 并发模型：每个 endpointId 一条独立的异步泵（pump），串行处理自己的队列；
// 不同 endpoint 的泵互不 await，由事件循环天然并发。
export class Dispatcher {
  constructor(options = {}) {
    this.secret = options.secret ?? 'webhook-dispatcher-secret';
    this.statePath = options.statePath ?? null;
    this.maxAttempts = options.maxAttempts ?? 4;
    this.baseDelayMs = options.baseDelayMs ?? 100;
    this.backoffMultiplier = options.backoffMultiplier ?? 2;
    this.jitterRatio = options.jitterRatio ?? 0.2;
    this.rateMax = options.rateMax ?? 5; // 每 endpoint 每窗口最多请求数
    this.rateWindowMs = options.rateWindowMs ?? 1000;
    this.requestTimeoutMs = options.requestTimeoutMs ?? 5000;
    this.sleep = options.sleep ?? defaultSleep;
    this.random = options.random ?? Math.random;
    this.autoStart = options.autoStart ?? true; // false 时只入队不派发（用于崩溃恢复场景）

    this.endpoints = new Map(); // endpointId -> { endpointId, url }
    this.queues = new Map(); // endpointId -> event[]
    this.pumps = new Map(); // endpointId -> Promise
    this.limiters = new Map(); // endpointId -> RateLimiter
    this.knownKeys = new Set(); // "endpointId eventId"，在途/已成功的组合，幂等判重用
    this.deadLetters = [];
    this.counters = { delivered: 0, duplicated: 0, dead: 0, retried: 0 };
    this.stopping = false;
  }

  registerEndpoint(endpointId, url) {
    this.endpoints.set(endpointId, { endpointId, url });
  }

  _key(endpointId, eventId) {
    return `${endpointId} ${eventId}`;
  }

  // 返回 'queued' | 'duplicate'。重复入队的 (endpointId, eventId) 只计数，不发请求。
  enqueue(endpointId, eventId, payload = {}) {
    if (this.stopping) throw new Error('dispatcher is shutting down, not accepting new events');
    if (!this.endpoints.has(endpointId)) throw new Error(`unknown endpoint: ${endpointId}`);
    const key = this._key(endpointId, eventId);
    if (this.knownKeys.has(key)) {
      this.counters.duplicated += 1;
      return 'duplicate';
    }
    this.knownKeys.add(key);
    const event = { endpointId, eventId, payload };
    if (!this.queues.has(endpointId)) this.queues.set(endpointId, []);
    this.queues.get(endpointId).push(event);
    if (this.autoStart) this._ensurePump(endpointId);
    return 'queued';
  }

  // 从 state.json 恢复：端点、死信、未完成的 pending 事件。
  start() {
    if (this.statePath) {
      const state = loadStateFile(this.statePath);
      if (state) this._restore(state);
    }
    for (const [endpointId, queue] of this.queues) {
      if (queue.length > 0) this._ensurePump(endpointId);
    }
  }

  _restore(state) {
    for (const ep of state.endpoints ?? []) {
      if (!this.endpoints.has(ep.endpointId)) this.endpoints.set(ep.endpointId, ep);
    }
    for (const ev of state.pending ?? []) {
      const key = this._key(ev.endpointId, ev.eventId);
      if (this.knownKeys.has(key)) continue;
      this.knownKeys.add(key);
      if (!this.queues.has(ev.endpointId)) this.queues.set(ev.endpointId, []);
      this.queues.get(ev.endpointId).push(ev);
    }
    for (const dead of state.dead ?? []) this.deadLetters.push(dead);
  }

  _ensurePump(endpointId) {
    if (this.pumps.has(endpointId)) return;
    const pump = this._pump(endpointId).finally(() => this.pumps.delete(endpointId));
    this.pumps.set(endpointId, pump);
  }

  // 同一 endpoint 严格串行：队列头部事件到达终态后才 shift 并处理下一个。
  async _pump(endpointId) {
    const queue = this.queues.get(endpointId);
    const endpoint = this.endpoints.get(endpointId);
    const limiter = this._limiter(endpointId);
    while (queue.length > 0) {
      const event = queue[0];
      await this._deliver(endpoint, event, limiter);
      queue.shift();
    }
  }

  _limiter(endpointId) {
    if (!this.limiters.has(endpointId)) {
      this.limiters.set(
        endpointId,
        new RateLimiter({ max: this.rateMax, windowMs: this.rateWindowMs, sleep: this.sleep }),
      );
    }
    return this.limiters.get(endpointId);
  }

  async _deliver(endpoint, event, limiter) {
    const body = JSON.stringify({
      eventId: event.eventId,
      endpointId: event.endpointId,
      payload: event.payload,
    });
    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      await limiter.acquire(); // 限流作用在每一次 HTTP 请求上（含重试）
      const timestamp = Date.now().toString();
      const headers = {
        'X-Signature': sign(this.secret, timestamp, body),
        'X-Timestamp': timestamp,
      };
      const result = await postJson(endpoint.url, body, headers, this.requestTimeoutMs);
      if (result.ok && result.status >= 200 && result.status < 300) {
        this.counters.delivered += 1;
        return;
      }
      // 5xx / 429 / 网络错误 / 超时 -> 可重试；其余 4xx -> 永久失败直接进死信
      const retryable = result.ok
        ? result.status === 429 || result.status >= 500
        : result.retryable;
      if (!retryable) {
        this._dead(endpoint, event, `permanent failure: http ${result.status ?? result.error}`, attempt);
        return;
      }
      if (attempt < this.maxAttempts) {
        this.counters.retried += 1;
        const delay = computeDelay(
          attempt,
          { baseDelayMs: this.baseDelayMs, multiplier: this.backoffMultiplier, jitterRatio: this.jitterRatio },
          this.random,
        );
        await this.sleep(delay);
      }
    }
    this._dead(endpoint, event, `max attempts (${this.maxAttempts}) exceeded`, this.maxAttempts);
  }

  _dead(endpoint, event, reason, attempts) {
    this.counters.dead += 1;
    this.deadLetters.push({
      endpointId: endpoint.endpointId,
      eventId: event.eventId,
      payload: event.payload,
      reason,
      attempts,
      deadAt: new Date().toISOString(),
    });
  }

  // 持久化未完成事件与死信（原子写入）。
  saveState() {
    if (!this.statePath) return;
    const pending = [];
    for (const queue of this.queues.values()) {
      for (const event of queue) pending.push(event);
    }
    saveStateFile(this.statePath, {
      version: 1,
      savedAt: new Date().toISOString(),
      endpoints: [...this.endpoints.values()],
      pending,
      dead: this.deadLetters,
    });
  }

  // 优雅停机：停止接收新事件，等待所有在途事件到达终态，然后落盘。
  async shutdown() {
    this.stopping = true;
    while (this.pumps.size > 0) {
      await Promise.allSettled([...this.pumps.values()]);
    }
    this.saveState();
    return this.report();
  }

  report() {
    return { ...this.counters, deadLetters: this.deadLetters };
  }
}
