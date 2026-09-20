# webhook-dispatcher

仅使用 Node.js 22 标准库实现的 webhook 投递器：至少一次投递、(endpointId, eventId) 幂等、
同 endpoint 严格顺序、指数退避重试 + 死信队列、每 endpoint 滑动窗口限流、HMAC-SHA256 签名、
优雅停机与 state.json 恢复。

## 使用

```bash
node index.js --demo   # 跑内置演示场景（自带本地模拟接收端，覆盖 500/429/404/超时）
node --test            # 跑测试套件
```

## 模块划分

- `index.js` — 入口与演示场景（本地 mock 接收端 + 投递报告 + 退出码校验）
- `lib/dispatcher.js` — 核心：幂等判重、每 endpoint 串行泵、重试/死信、优雅停机、状态恢复
- `lib/rate-limiter.js` — 滑动窗口限流器（超限等待，不丢弃）
- `lib/backoff.js` — 指数退避 + 抖动计算
- `lib/http-client.js` — 基于 node:http/https 的 POST 客户端（含超时，错误分类）
- `lib/sign.js` — X-Signature / X-Timestamp 签名
- `lib/state-store.js` — state.json 原子读写（tmp + rename）
