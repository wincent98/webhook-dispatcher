import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHmac } from 'node:crypto';
import { Dispatcher } from '../src/dispatcher.js';
import { createMockReceiver } from '../src/mock-receiver.js';

async function waitFor(cond, { timeout = 5000, interval = 10 } = {}) {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeout) throw new Error('waitFor timeout');
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

async function setup(t, options = {}) {
  const receiver = createMockReceiver();
  await receiver.listen();
  const dir = await mkdtemp(join(tmpdir(), 'webhook-dispatcher-test-'));
  const dispatcher = new Dispatcher({
    stateFile: join(dir, 'state.json'),
    baseDelayMs: 10,
    timeoutMs: 300,
    ratePerSecond: 1000,
    ...options,
  });
  t.after(async () => {
    dispatcher.accepting = false;
    await receiver.close();
  });
  return { receiver, dispatcher, dir, base: `http://127.0.0.1:${receiver.port()}` };
}

test('幂等命中: 重复入队相同 (endpointId, eventId) 只投递一次', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/ok`, secret: 's' });

  const first = dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: { n: 1 } });
  const second = dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: { n: 1 } });
  const third = dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: { n: 1 } });

  assert.equal(first.status, 'queued');
  assert.equal(second.status, 'duplicated');
  assert.equal(third.status, 'duplicated');

  await dispatcher.idle();
  assert.equal(receiver.received.length, 1);
  assert.equal(dispatcher.stats.delivered, 1);
  assert.equal(dispatcher.stats.duplicated, 2);
});

test('顺序保证: 同一 endpoint 严格按入队顺序投递', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/ok`, secret: 's' });

  const count = 8;
  for (let i = 0; i < count; i += 1) {
    dispatcher.enqueue({ endpointId: 'ep1', eventId: `e${i}`, payload: { seq: i } });
  }
  await dispatcher.idle();

  assert.equal(receiver.received.length, count);
  const seqs = receiver.received.map((r) => JSON.parse(r.body).payload.seq);
  assert.deepEqual(seqs, [...Array(count).keys()]);
});

test('退避重试: 持续 500 时按指数退避重试, 最多 4 次后进入死信', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/always500`, secret: 's' });

  dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: {} });
  await dispatcher.idle();

  // 恰好 4 次请求: 1 次首发 + 3 次重试
  assert.equal(receiver.hits.get('/always500'), 4);
  assert.equal(dispatcher.stats.retried, 3);
  assert.equal(dispatcher.stats.dead, 1);
  assert.equal(dispatcher.stats.delivered, 0);

  // 退避间隔递增: base 10ms -> 至少 10/20/40ms (抖动只会加大)
  const times = receiver.received.map((r) => r.at);
  const gaps = [times[1] - times[0], times[2] - times[1], times[3] - times[2]];
  assert.ok(gaps[0] >= 8, `gap1=${gaps[0]}`);
  assert.ok(gaps[1] > gaps[0], `gap2=${gaps[1]} should exceed gap1=${gaps[0]}`);
  assert.ok(gaps[2] > gaps[1], `gap3=${gaps[2]} should exceed gap2=${gaps[1]}`);
});

test('4xx(除429) 为永久失败: 不重试直接进死信', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/notfound`, secret: 's' });

  dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: {} });
  await dispatcher.idle();

  assert.equal(receiver.hits.get('/notfound'), 1);
  assert.equal(dispatcher.stats.retried, 0);
  assert.equal(dispatcher.stats.dead, 1);
  assert.match(dispatcher.deadLetters[0].reason, /404/);
});

test('429 可重试: 第一次 429 后重试成功', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/ratelimited`, secret: 's' });

  dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: {} });
  await dispatcher.idle();

  assert.equal(receiver.hits.get('/ratelimited'), 2);
  assert.equal(dispatcher.stats.delivered, 1);
  assert.equal(dispatcher.stats.retried, 1);
});

test('限流: 超过速率的请求被延迟而不是丢弃, 且顺序不变', async (t) => {
  // 容量 3, 每秒 100 个令牌: 8 个事件 -> 前 3 个立即发出, 后 5 个每个至少等 ~10ms
  const { receiver, dispatcher, base } = await setup(t, { ratePerSecond: 100, burst: 3 });
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/ok`, secret: 's' });

  const count = 8;
  for (let i = 0; i < count; i += 1) {
    dispatcher.enqueue({ endpointId: 'ep1', eventId: `e${i}`, payload: { seq: i } });
  }
  await dispatcher.idle();

  assert.equal(receiver.received.length, count);
  const elapsed = receiver.received.at(-1).at - receiver.received[0].at;
  assert.ok(elapsed >= 40, `elapsed=${elapsed}ms, expected >= 40ms (5 throttled requests at 100/s)`);
  assert.ok(elapsed < 3000, `elapsed=${elapsed}ms, suspiciously slow`);
  const seqs = receiver.received.map((r) => JSON.parse(r.body).payload.seq);
  assert.deepEqual(seqs, [...Array(count).keys()]);
});

test('停机恢复: 停机时把未完成事件写入 state.json, 重启后恢复继续投递且不重复', async (t) => {
  const receiver = createMockReceiver();
  await receiver.listen();
  t.after(() => receiver.close());
  const dir = await mkdtemp(join(tmpdir(), 'webhook-dispatcher-test-'));
  const stateFile = join(dir, 'state.json');
  const base = `http://127.0.0.1:${receiver.port()}`;
  const options = { stateFile, baseDelayMs: 10, timeoutMs: 1000, ratePerSecond: 1000 };

  // 第一阶段: 4 个事件发到 /delayed (每个 150ms), 等第 2 个事件的请求到达
  // 接收端 (即已在途、必定会完成) 后立刻非排空停机
  const d1 = new Dispatcher(options);
  d1.registerEndpoint({ id: 'ep1', url: `${base}/delayed`, secret: 's' });
  for (let i = 0; i < 4; i += 1) {
    d1.enqueue({ endpointId: 'ep1', eventId: `e${i}`, payload: { seq: i } });
  }
  await waitFor(() => receiver.received.length === 2);
  await d1.stop({ drain: false });

  // state.json 应包含 2 个已完成 + 2 个未完成
  const state = JSON.parse(await readFile(stateFile, 'utf8'));
  assert.equal(state.deliveredKeys.length, 2);
  assert.equal(state.pending.length, 2);
  assert.deepEqual(state.pending.map((item) => item.eventId), ['e2', 'e3']);
  assert.equal(receiver.received.length, 2);

  // 第二阶段: 新进程语义 -- 全新 Dispatcher 从 state.json 恢复
  const d2 = new Dispatcher(options);
  const restored = await d2.restore();
  assert.equal(restored, true);
  await d2.idle();

  // 4 个事件全部恰好投递一次, 顺序保持
  assert.equal(receiver.received.length, 4);
  const eventIds = receiver.received.map((r) => JSON.parse(r.body).eventId);
  assert.deepEqual(eventIds, ['e0', 'e1', 'e2', 'e3']);
  assert.equal(new Set(eventIds).size, 4);

  // 恢复后重复入队已投递事件仍是幂等命中
  const dup = d2.enqueue({ endpointId: 'ep1', eventId: 'e0', payload: { seq: 0 } });
  assert.equal(dup.status, 'duplicated');
  await d2.stop();
});

test('签名: 每个请求带正确的 X-Signature 和 X-Timestamp 头', async (t) => {
  const { receiver, dispatcher, base } = await setup(t);
  dispatcher.registerEndpoint({ id: 'ep1', url: `${base}/ok`, secret: 'topsecret' });

  dispatcher.enqueue({ endpointId: 'ep1', eventId: 'e1', payload: { hello: 'world' } });
  await dispatcher.idle();

  assert.equal(receiver.received.length, 1);
  const { headers, body } = receiver.received[0];
  const expected = createHmac('sha256', 'topsecret')
    .update(`${headers['x-timestamp']}.${body}`)
    .digest('hex');
  assert.equal(headers['x-signature'], expected);
  assert.ok(Number(headers['x-timestamp']) > 0);
});
