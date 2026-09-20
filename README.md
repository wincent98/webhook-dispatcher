# webhook-dispatcher

内部事件到商户回调地址的投递器。仅使用 Node.js 22 标准库，零第三方依赖。

## 使用

```bash
node index.js --demo   # 运行内置演示（本地模拟接收端，覆盖 500/429/404/超时）
node --test            # 运行测试
```

## 模块划分

- `index.js` — CLI 入口与演示场景，注册 SIGINT/SIGTERM 优雅停机
- `src/dispatcher.js` — 门面：幂等去重、endpoint 注册、统计、停机与状态恢复
- `src/worker.js` — 每个 endpointId 一个串行 worker：FIFO、重试分类、限流、签名
- `src/rate-limiter.js` — 令牌桶限流器（等待式，不丢弃）
- `src/backoff.js` — 指数退避 + 抖动
- `src/sign.js` — HMAC-SHA256 签名
- `src/http-client.js` — 带超时的 POST 客户端，网络错误统一为可重试失败
- `src/state.js` — state.json 原子读写（临时文件 + rename）
- `src/mock-receiver.js` — 演示/测试用的本地模拟接收端
- `test/dispatcher.test.js` — node:test 测试

## 语义

- 至少一次投递；同一 `(endpointId, eventId)` 最多成功一次，重复入队返回 `duplicated`
- 同一 endpointId 严格 FIFO；不同 endpointId 并发互不阻塞
- 失败重试：基数 100ms、倍数 2、最多 4 次尝试、抖动 ≤20%；5xx/429/网络错误可重试，其余 4xx 直接死信
- 每个 endpointId 每秒最多 5 次请求（令牌桶，等待不限丢）
- 请求头：`X-Signature = hex(HMAC-SHA256(secret, timestamp + "." + body))`，`X-Timestamp`
- 停机：拒收新事件，在途事件处理到终态，`state.json` 持久化未完成事件与死信，下次启动 `restore()` 恢复
