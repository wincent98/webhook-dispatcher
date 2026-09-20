#!/usr/bin/env node
import http from 'node:http';
import { createHmac } from 'node:crypto';
import { Dispatcher } from './lib/dispatcher.js';

const DEMO_SECRET = 'demo-secret';
const STATE_PATH = new URL('./state.json', import.meta.url).pathname;

// 模拟接收端：按 eventId 的脚本制造 200 / 500 / 429 / 404 / 超时，并校验签名。
function startReceiver() {
  const hits = new Map(); // eventId -> 收到次数
  let invalidSignatures = 0;
  const scripts = {
    'evt-001': 'ok',
    'evt-002': 'ok',
    'evt-003': 'flaky-429', // 第一次 429，之后 200
    'evt-004': 'always-500',
    'evt-005': 'always-404',
    'evt-006': 'flaky-timeout', // 第一次超时，之后 200
    'evt-007': 'always-500',
    'evt-008': 'ok',
  };
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      const timestamp = req.headers['x-timestamp'] ?? '';
      const expected = createHmac('sha256', DEMO_SECRET)
        .update(`${timestamp}.${body}`)
        .digest('hex');
      if (expected !== req.headers['x-signature']) invalidSignatures += 1;

      let eventId = 'unknown';
      try {
        eventId = JSON.parse(body).eventId;
      } catch {
        // 忽略畸形 body
      }
      const count = (hits.get(eventId) ?? 0) + 1;
      hits.set(eventId, count);
      const script = scripts[eventId] ?? 'ok';
      const respond = (status) => {
        res.writeHead(status, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status }));
      };
      if (script === 'always-500') return respond(500);
      if (script === 'always-404') return respond(404);
      if (script === 'flaky-429' && count === 1) return respond(429);
      if (script === 'flaky-timeout' && count === 1) {
        setTimeout(() => respond(200), 900); // 客户端超时 400ms，必然超时
        return;
      }
      respond(200);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({ server, port: server.address().port, hits, invalidSignatures: () => invalidSignatures });
    });
  });
}

async function runDemo() {
  const receiver = await startReceiver();
  const url = `http://127.0.0.1:${receiver.port}/webhook`;
  console.log(`[demo] mock receiver listening on ${url}`);

  const dispatcher = new Dispatcher({
    secret: DEMO_SECRET,
    statePath: STATE_PATH,
    requestTimeoutMs: 400,
  });
  for (const ep of ['ep-alpha', 'ep-beta', 'ep-gamma', 'ep-delta', 'ep-epsilon', 'ep-zeta']) {
    dispatcher.registerEndpoint(ep, url);
  }

  dispatcher.enqueue('ep-alpha', 'evt-001', { msg: 'plain ok' });
  dispatcher.enqueue('ep-alpha', 'evt-002', { msg: 'plain ok' });
  dispatcher.enqueue('ep-alpha', 'evt-002', { msg: 'duplicate enqueue' }); // 幂等命中
  dispatcher.enqueue('ep-beta', 'evt-003', { msg: '429 then ok' });
  dispatcher.enqueue('ep-gamma', 'evt-004', { msg: 'always 500 -> DLQ' });
  dispatcher.enqueue('ep-delta', 'evt-005', { msg: '404 -> DLQ, no retry' });
  dispatcher.enqueue('ep-epsilon', 'evt-006', { msg: 'timeout then ok' });
  dispatcher.enqueue('ep-zeta', 'evt-007', { msg: 'always 500 -> DLQ' });
  dispatcher.enqueue('ep-zeta', 'evt-008', { msg: 'must still be delivered after evt-007 died' });

  const report = await dispatcher.shutdown(); // 优雅停机：排空后写 state.json
  receiver.server.close();

  const expected = { delivered: 5, duplicated: 1, dead: 3, retried: 8 };
  console.log('\n=== Delivery Report ===');
  let failed = false;
  for (const key of ['delivered', 'duplicated', 'dead', 'retried']) {
    const ok = report[key] === expected[key];
    if (!ok) failed = true;
    console.log(`  ${key.padEnd(10)} ${report[key]}  (expected ${expected[key]})  ${ok ? 'OK' : 'MISMATCH'}`);
  }
  const invalid = receiver.invalidSignatures();
  console.log(`  signatures invalid: ${invalid} ${invalid === 0 ? 'OK' : 'MISMATCH'}`);
  if (invalid !== 0) failed = true;
  console.log('\n  dead letters:');
  for (const d of report.deadLetters) {
    console.log(`    - ${d.endpointId}/${d.eventId}: ${d.reason} (attempts=${d.attempts})`);
  }
  console.log(`\n[demo] state written to ${STATE_PATH}`);
  console.log(failed ? '[demo] FAILED' : '[demo] PASSED');
  process.exitCode = failed ? 1 : 0;
}

if (process.argv.includes('--demo')) {
  runDemo().catch((err) => {
    console.error('[demo] crashed:', err);
    process.exitCode = 1;
  });
} else {
  console.log('webhook-dispatcher');
  console.log('  node index.js --demo   run the built-in demo scenario');
  console.log('  node --test            run the test suite');
}
