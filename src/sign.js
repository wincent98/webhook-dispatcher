import { createHmac } from 'node:crypto';

// X-Signature = hex(HMAC-SHA256(secret, timestamp + "." + body))
export function signPayload(secret, timestamp, body) {
  return createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
}
