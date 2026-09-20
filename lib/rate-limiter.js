// 滑动窗口限流器：每个 endpoint 在 windowMs 内最多 max 次请求。
// 超限时 sleep 等待窗口滑动，绝不丢弃请求。
export class RateLimiter {
  constructor({ max, windowMs, sleep, now = Date.now }) {
    this.max = max;
    this.windowMs = windowMs;
    this.sleep = sleep;
    this.now = now;
    this.stamps = [];
  }

  async acquire() {
    for (;;) {
      const nowTs = this.now();
      this.stamps = this.stamps.filter((t) => nowTs - t < this.windowMs);
      if (this.stamps.length < this.max) {
        this.stamps.push(nowTs);
        return;
      }
      const waitMs = this.windowMs - (nowTs - this.stamps[0]) + 1;
      await this.sleep(waitMs);
    }
  }
}
