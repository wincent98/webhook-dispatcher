#!/usr/bin/env node
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Dispatcher } from './src/dispatcher.js';
import { createMockReceiver } from './src/mock-receiver.js';

const args = process.argv.slice(2);

if (args.includes('--demo')) {
  await runDemo();
} else {
  console.log(`webhook-dispatcher

用法:
  node index.js --demo    运行内置演示场景（本地模拟接收端，覆盖 500/429/404/超时）
  node --test             运行测试
`);
}

async function runDemo() {
  console.log('== webhook-dispatcher demo ==');
  const receiver = createMockReceiver();
  await receiver.listen();
  const port = receiver.port();
  console.log(`mock receiver: http://127.0.0.1:${port}`);

  const dir = await mkdtemp(join(tmpdir(), 'webhook-dispatcher-demo-'));
  const dispatcher = new Dispatcher({
    stateFile: join(dir, 'state.json'),
    timeoutMs: 500, // /slow 端点 3s 才响应，必然触发超时分支
  });

  let stopping = false;
  const onSignal = async (signal) => {
    if (stopping) return;
    stopping = true;
    console.log(`\nreceived ${signal}, draining...`);
    await dispatcher.stop();
    await receiver.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void onSignal('SIGINT'));
  process.on('SIGTERM', () => void onSignal('SIGTERM'));

  const base = `http://127.0.0.1:${port}`;
  const endpoints = [
    { id: 'ep-ok', url: `${base}/ok`, secret: 'secret-ok' },
    { id: 'ep-flaky', url: `${base}/flaky`, secret: 'secret-flaky' },
    { id: 'ep-always500', url: `${base}/always500`, secret: 'secret-500' },
    { id: 'ep-notfound', url: `${base}/notfound`, secret: 'secret-404' },
    { id: 'ep-ratelimited', url: `${base}/ratelimited`, secret: 'secret-429' },
    { id: 'ep-slow', url: `${base}/slow`, secret: 'secret-slow' },
  ];
  for (const endpoint of endpoints) dispatcher.registerEndpoint(endpoint);

  // ep-ok: 3 个事件，顺序投递，全部成功
  for (let i = 1; i <= 3; i += 1) {
    dispatcher.enqueue({ endpointId: 'ep-ok', eventId: `ok-${i}`, payload: { seq: i } });
  }
  // ep-flaky: 前 2 次 500，第 3 次成功 -> delivered, retried +2
  dispatcher.enqueue({ endpointId: 'ep-flaky', eventId: 'flaky-1', payload: {} });
  // ep-always500: 4 次全失败 -> dead, retried +3
  dispatcher.enqueue({ endpointId: 'ep-always500', eventId: 'a500-1', payload: {} });
  // ep-notfound: 404 永久失败 -> dead, 不重试
  dispatcher.enqueue({ endpointId: 'ep-notfound', eventId: 'nf-1', payload: {} });
  // ep-ratelimited: 第 1 次 429，第 2 次成功 -> delivered, retried +1
  dispatcher.enqueue({ endpointId: 'ep-ratelimited', eventId: 'rl-1', payload: {} });
  // ep-slow: 4 次全部超时 -> dead, retried +3
  dispatcher.enqueue({ endpointId: 'ep-slow', eventId: 'slow-1', payload: {} });
  // 幂等命中：重复入队相同 (endpointId, eventId)
  const dup = dispatcher.enqueue({ endpointId: 'ep-ok', eventId: 'ok-1', payload: { seq: 1 } });
  console.log(`duplicate enqueue of ep-ok/ok-1 -> ${dup.status}`);

  await dispatcher.idle();
  await dispatcher.stop();

  const expected = { delivered: 5, duplicated: 1, dead: 3, retried: 9 };
  const actual = dispatcher.stats;
  const ok = Object.keys(expected).every((key) => actual[key] === expected[key]);

  console.log('\n== 投递报告 ==');
  console.log('  counter      actual  expected');
  for (const key of Object.keys(expected)) {
    const mark = actual[key] === expected[key] ? 'ok' : 'MISMATCH';
    console.log(`  ${key.padEnd(11)} ${String(actual[key]).padStart(6)}  ${String(expected[key]).padStart(8)}  ${mark}`);
  }
  console.log(`  dead letters: ${dispatcher.deadLetters.map((d) => `${d.endpointId}/${d.eventId} (${d.reason})`).join('; ')}`);
  console.log(ok ? '\nRESULT: PASS' : '\nRESULT: FAIL');

  await receiver.close();
  await rm(dir, { recursive: true, force: true });
  process.exitCode = ok ? 0 : 1;
}
