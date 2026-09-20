// 指数退避计算：delay = baseMs * factor^(retry-1)，再加不超过 delay * jitterRatio 的正向抖动。
export function computeBackoff({ retry, baseMs = 100, factor = 2, jitterRatio = 0.2, random = Math.random }) {
  const delay = baseMs * factor ** (retry - 1);
  return delay + delay * jitterRatio * random();
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
