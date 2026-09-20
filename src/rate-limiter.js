// 令牌桶限流器：容量 capacity，按 refillPerSecond 匀速补充。
// 每个 endpoint 的 worker 串行调用 acquire()，因此不存在并发竞争，也不需要锁。
export class TokenBucket {
  constructor({ capacity, refillPerSecond }) {
    this.capacity = capacity;
    this.refillPerMs = refillPerSecond / 1000;
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  #refill() {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    if (elapsed > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerMs);
      this.lastRefill = now;
    }
  }

  // 等待直到拿到一个令牌；只等待、不丢弃。
  async acquire() {
    for (;;) {
      this.#refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const waitMs = Math.max(1, Math.ceil((1 - this.tokens) / this.refillPerMs));
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}
