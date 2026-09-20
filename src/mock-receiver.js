import { createServer } from 'node:http';

// 本地模拟接收端，用路径控制行为，覆盖四种失败分支：
//   /ok          立即 200
//   /delayed     延迟 150ms 后 200（用于停机恢复测试）
//   /flaky       前 2 次 500，之后 200（重试后成功）
//   /always500   永远 500（重试耗尽进死信）
//   /notfound    永远 404（永久失败，不重试）
//   /ratelimited 第 1 次 429，之后 200（429 可重试）
//   /slow        slowDelayMs 后才响应（触发客户端超时）
export function createMockReceiver({ slowDelayMs = 3000 } = {}) {
  const received = [];
  const hits = new Map();
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      const path = req.url;
      const count = (hits.get(path) ?? 0) + 1;
      hits.set(path, count);
      received.push({ url: path, headers: req.headers, body, at: Date.now() });
      if (path.startsWith('/ok')) {
        res.writeHead(200).end('ok');
      } else if (path.startsWith('/delayed')) {
        setTimeout(() => { if (!res.destroyed) res.writeHead(200).end('ok'); }, 150).unref();
      } else if (path.startsWith('/flaky')) {
        if (count <= 2) res.writeHead(500).end('err');
        else res.writeHead(200).end('ok');
      } else if (path.startsWith('/always500')) {
        res.writeHead(500).end('err');
      } else if (path.startsWith('/notfound')) {
        res.writeHead(404).end('not found');
      } else if (path.startsWith('/ratelimited')) {
        if (count === 1) res.writeHead(429).end('too many');
        else res.writeHead(200).end('ok');
      } else if (path.startsWith('/slow')) {
        setTimeout(() => { if (!res.destroyed) res.writeHead(200).end('slow ok'); }, slowDelayMs).unref();
      } else {
        res.writeHead(200).end('ok');
      }
    });
  });
  return {
    server,
    received,
    hits,
    listen: () => new Promise((resolve) => server.listen(0, '127.0.0.1', resolve)),
    port: () => server.address().port,
    close: () => new Promise((resolve) => {
      server.closeAllConnections?.();
      server.close(resolve);
    }),
  };
}
