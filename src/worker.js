import { TokenBucket } from './rate-limiter.js';
import { computeBackoff, sleep } from './backoff.js';
import { signPayload } from './sign.js';
import { postJson } from './http-client.js';

// 每个 endpointId 一个 worker：单条串行流水线。
// 同一 endpoint 的事件严格 FIFO，前一个到达终态（成功/死信）后才处理下一个。
export class EndpointWorker {
  constructor({ endpoint, options, hooks }) {
    this.endpoint = endpoint;
    this.options = options;
    this.hooks = hooks;
    this.queue = [];
    this.limiter = new TokenBucket({
      capacity: options.burst ?? options.ratePerSecond,
      refillPerSecond: options.ratePerSecond,
    });
    this.running = false;
    this.stopRequested = false;
    this.drain = true;
    this.idleResolvers = [];
  }

  enqueue(item) {
    this.queue.push(item);
    if (!this.running) {
      this.running = true;
      void this.#run();
    }
  }

  requestStop(drain) {
    this.stopRequested = true;
    this.drain = drain;
  }

  idle() {
    if (!this.running) return Promise.resolve();
    return new Promise((resolve) => this.idleResolvers.push(resolve));
  }

  async #run() {
    try {
      while (this.queue.length > 0) {
        // drain=false 时完成当前在途事件后停下，其余留在队列里等待持久化。
        if (this.stopRequested && !this.drain) break;
        const item = this.queue[0];
        try {
          await this.#deliver(item);
        } catch (err) {
          this.hooks.onDead?.(item, `internal error: ${err.message}`);
        }
        this.queue.shift();
      }
    } finally {
      this.running = false;
      const resolvers = this.idleResolvers.splice(0);
      for (const resolve of resolvers) resolve();
    }
  }

  async #deliver(item) {
    const { maxAttempts, baseDelayMs, backoffFactor, jitterRatio, timeoutMs } = this.options;
    const body = JSON.stringify({
      eventId: item.eventId,
      endpointId: item.endpointId,
      payload: item.payload,
    });
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      if (attempt > 1) {
        this.hooks.onRetried?.(item, attempt);
        await sleep(computeBackoff({ retry: attempt - 1, baseMs: baseDelayMs, factor: backoffFactor, jitterRatio }));
      }
      // 限流与顺序共存的关键：串行循环里 await 令牌，只延迟、不重排、不丢弃。
      await this.limiter.acquire();
      const timestamp = Date.now().toString();
      const result = await postJson({
        url: this.endpoint.url,
        body,
        timeoutMs,
        headers: {
          'X-Signature': signPayload(this.endpoint.secret, timestamp, body),
          'X-Timestamp': timestamp,
        },
      });
      if (result.ok && result.status >= 200 && result.status < 300) {
        this.hooks.onDelivered?.(item, attempt);
        return;
      }
      // 5xx / 429 / 网络错误可重试；其余 4xx 为永久失败，直接进死信。
      const retryable = !result.ok || result.status === 429 || result.status >= 500;
      if (!retryable) {
        this.hooks.onDead?.(item, `permanent failure: HTTP ${result.status}`);
        return;
      }
    }
    this.hooks.onDead?.(item, `exhausted ${maxAttempts} attempts`);
  }
}
