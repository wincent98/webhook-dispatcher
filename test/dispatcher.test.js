import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { mkdtempSync, readFileSync } from 'node:fs';
import http from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { Dispatcher } from '../lib/dispatcher.js';

const SECRET = 'test-secret';

// 启一个本地接收端，handler(eventId, headers, count) 返回 [status, delayMs]
async function startReceiver(handler) {
  const received = [];
  const counts = new Map();
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      const eventId = JSON.parse(body).eventId;
      const count = (counts.get(eventId) ?? 0) + 1;
      counts.set(eventId, count);
      received.push({ eventId, headers: req.headers, body });
      const [status, delayMs = 0] = handler(eventId, req.headers, count);
      setTimeout(() => {
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end('{}');
      }, delayMs);
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}/hook`,
    received,
    counts,
    close: () => server.close(),
  };
}

function makeDispatcher(receiver, extra = {}) {
  const d = new Dispatcher({ secret: SECRET, requestTimeoutMs: 300, ...extra });
  d.registerEndpoint('ep', receiver.url);
  return d;
}

test('幂等命中：重复入队的 (endpointId, eventId) 不重复发请求', async (t) => {
  const receiver = await startReceiver(() => [200]);
  t.after(() => receiver.close());
  const d = makeDispatcher(receiver);

  assert.equal(d.enqueue('ep', 'e1', { a: 1 }), 'queued');
  assert.equal(d.enqueue('ep', 'e1', { a: 1 }), 'duplicate');
  assert.equal(d.enqueue('ep', 'e1', { a: 2 }), 'duplicate');
  const report = await d.shutdown();

  assert.equal(report.delivered, 1);
  assert.equal(report.duplicated, 2);
  assert.equal(receiver.received.length, 1);
});

test('顺序保证：同一 endpoint 严格按入队顺序投递', async (t) => {
  const receiver = await startReceiver(() => [200, 15]); // 每个请求都拖 15ms，放大乱序概率
  t.after(() => receiver.close());
  const d = makeDispatcher(receiver);

  for (const id of ['e1', 'e2', 'e3', 'e4', 'e5']) d.enqueue('ep', id, {});
  await d.shutdown();

  assert.deepEqual(receiver.received.map((r) => r.eventId), ['e1', 'e2', 'e3', 'e4', 'e5']);
});

test('退避重试：500 时按 100/200/400ms 退避，共 4 次尝试后进死信', async (t) => {
  const receiver = await startReceiver(() => [500]);
  t.after(() => receiver.close());
  const sleeps = [];
  const d = makeDispatcher(receiver, {
    sleep: async (ms) => sleeps.push(ms),
    random: () => 0, // 去掉抖动，验证纯退避序列
  });

  d.enqueue('ep', 'e1', {});
  const report = await d.shutdown();

  assert.equal(receiver.counts.get('e1'), 4); // 最多 4 次尝试
  assert.deepEqual(sleeps, [100, 200, 400]); // 基数 100ms、倍数 2
  assert.equal(report.retried, 3);
  assert.equal(report.dead, 1);
  assert.equal(report.delivered, 0);
});

test('4xx（非 429）永久失败：不重试直接进死信，429 会重试', async (t) => {
  const receiver = await startReceiver((eventId, _headers, count) => {
    if (eventId === 'e404') return [404];
    if (eventId === 'e429' && count === 1) return [429];
    return [200];
  });
  t.after(() => receiver.close());
  const d = makeDispatcher(receiver, { sleep: async () => {} });

  d.enqueue('ep', 'e404', {});
  d.enqueue('ep', 'e429', {});
  const report = await d.shutdown();

  assert.equal(receiver.counts.get('e404'), 1); // 不重试
  assert.equal(receiver.counts.get('e429'), 2); // 重试后成功
  assert.equal(report.dead, 1);
  assert.equal(report.delivered, 1);
  assert.equal(report.retried, 1);
});

test('限流：超过速率的请求等待而不是丢弃', async (t) => {
  const receiver = await startReceiver(() => [200]);
  t.after(() => receiver.close());
  const d = makeDispatcher(receiver, { rateMax: 2, rateWindowMs: 200 });

  const started = Date.now();
  for (let i = 1; i <= 6; i += 1) d.enqueue('ep', `e${i}`, {});
  const report = await d.shutdown();
  const elapsed = Date.now() - started;

  assert.equal(report.delivered, 6); // 一个都不丢
  assert.equal(receiver.received.length, 6);
  // 6 个请求、每 200ms 窗口 2 个 -> 至少等到第 3 个窗口（约 400ms）
  assert.ok(elapsed >= 350, `expected throttling to take >=350ms, took ${elapsed}ms`);
});

test('停机恢复：state.json 中的未完成事件在下次启动后继续投递', async (t) => {
  const receiver = await startReceiver(() => [200]);
  t.after(() => receiver.close());
  const dir = mkdtempSync(path.join(tmpdir(), 'webhook-dispatcher-'));
  const statePath = path.join(dir, 'state.json');

  // 模拟崩溃前：事件已入队但还没开始派发
  const d1 = makeDispatcher(receiver, { statePath, autoStart: false });
  d1.enqueue('ep', 'e1', {});
  d1.enqueue('ep', 'e2', {});
  d1.enqueue('ep', 'e3', {});
  d1.saveState();

  const saved = JSON.parse(readFileSync(statePath, 'utf8'));
  assert.equal(saved.pending.length, 3);

  // 重启后从 state.json 恢复
  const d2 = new Dispatcher({ secret: SECRET, statePath, requestTimeoutMs: 300 });
  await d2.start();
  assert.equal(d2.enqueue('ep', 'e2', {}), 'duplicate'); // 恢复的组合仍被幂等识别
  const report = await d2.shutdown();

  assert.equal(report.delivered, 3);
  assert.equal(report.duplicated, 1);
  assert.deepEqual(receiver.received.map((r) => r.eventId), ['e1', 'e2', 'e3']);
});

test('签名：每个请求带合法的 X-Signature 与 X-Timestamp', async (t) => {
  const receiver = await startReceiver(() => [200]);
  t.after(() => receiver.close());
  const d = makeDispatcher(receiver);

  d.enqueue('ep', 'e1', { hello: 'world' });
  await d.shutdown();

  assert.equal(receiver.received.length, 1);
  const { headers, body } = receiver.received[0];
  const expected = createHmac('sha256', SECRET)
    .update(`${headers['x-timestamp']}.${body}`)
    .digest('hex');
  assert.equal(headers['x-signature'], expected);
  assert.ok(Number(headers['x-timestamp']) > 0);
});
