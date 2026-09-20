// 指数退避 + 抖动：delay = base * multiplier^(attempt-1)，再加不超过 20% 的随机抖动。
// random 可注入，测试里传 () => 0 得到确定性的退避序列。
export function computeDelay(attempt, { baseDelayMs, multiplier, jitterRatio }, random = Math.random) {
  const base = baseDelayMs * multiplier ** (attempt - 1);
  return Math.round(base + random() * jitterRatio * base);
}
