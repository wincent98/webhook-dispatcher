import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';

// 发送一次 POST JSON 请求。永不 reject：网络错误/超时统一解析为 { ok: false }，
// 由调用方按“可重试失败”处理。
export function postJson({ url, body, headers = {}, timeoutMs = 2000 }) {
  return new Promise((resolve) => {
    let target;
    try {
      target = new URL(url);
    } catch (err) {
      resolve({ ok: false, error: err });
      return;
    }
    const doRequest = target.protocol === 'https:' ? httpsRequest : httpRequest;
    const req = doRequest(
      {
        method: 'POST',
        hostname: target.hostname,
        port: target.port || (target.protocol === 'https:' ? 443 : 80),
        path: target.pathname + target.search,
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          ...headers,
        },
        timeout: timeoutMs,
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve({ ok: true, status: res.statusCode }));
        res.on('error', (err) => resolve({ ok: false, error: err }));
      },
    );
    req.on('timeout', () => req.destroy(new Error(`request timeout after ${timeoutMs}ms`)));
    req.on('error', (err) => resolve({ ok: false, error: err }));
    req.write(body);
    req.end();
  });
}
