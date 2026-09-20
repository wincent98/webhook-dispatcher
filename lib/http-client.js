import http from 'node:http';
import https from 'node:https';

// 发一次 POST JSON。永远 resolve（不 reject），把结果分类交给上层：
//   { ok: true, status }                 —— 拿到了 HTTP 响应
//   { ok: false, retryable, error }      —— 网络错误 / 超时（可重试）或 URL 非法（不可重试）
export function postJson(url, body, headers, timeoutMs) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch {
      resolve({ ok: false, retryable: false, error: `invalid url: ${url}` });
      return;
    }
    const mod = parsed.protocol === 'https:' ? https : http;
    const req = mod.request(
      parsed,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...headers,
        },
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve({ ok: true, status: res.statusCode }));
        res.on('error', (err) => resolve({ ok: false, retryable: true, error: err.message }));
      },
    );
    req.setTimeout(timeoutMs, () => req.destroy(new Error(`request timeout after ${timeoutMs}ms`)));
    req.on('error', (err) => resolve({ ok: false, retryable: true, error: err.message }));
    req.write(body);
    req.end();
  });
}
