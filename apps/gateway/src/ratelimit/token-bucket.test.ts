import { describe, expect, it } from 'vitest';
import { consumeToken, InMemoryRateLimiter } from './token-bucket.js';

const opts = { refillPerSec: 1, burst: 3 };

describe('token bucket', () => {
  it('starts full: allows a burst of 3, then refuses with a retry time', () => {
    let state = null as ReturnType<typeof consumeToken>['state'] | null;
    const results = [];
    for (let i = 0; i < 4; i++) {
      const r = consumeToken(state, 1_000, opts);
      state = r.state;
      results.push(r.result);
    }
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, false]);
    expect(results[3]?.retryAfterMs).toBe(1_000);
  });

  it('refills over time but never above the burst size', () => {
    let r = consumeToken(null, 0, opts);
    r = consumeToken(r.state, 0, opts);
    r = consumeToken(r.state, 0, opts); // 0 tokens left
    r = consumeToken(r.state, 0, opts);
    expect(r.result.allowed).toBe(false);

    const later = consumeToken(r.state, 2_000, opts); // +2 tokens
    expect(later.result.allowed).toBe(true);
    expect(later.state.tokens).toBeCloseTo(1, 5);

    const muchLater = consumeToken(r.state, 3_600_000, opts); // capped at burst 3
    expect(muchLater.state.tokens).toBeCloseTo(2, 5); // 3 (cap) - 1 taken
  });

  it('retryAfterMs shrinks as the bucket refills', () => {
    const empty = { tokens: 0, updatedAtMs: 0 };
    expect(consumeToken(empty, 0, opts).result.retryAfterMs).toBe(1_000);
    expect(consumeToken(empty, 600, opts).result.retryAfterMs).toBe(400);
  });

  it('keeps separate buckets per key', async () => {
    const limiter = new InMemoryRateLimiter({ refillPerSec: 1, burst: 1 }, () => 0);
    expect((await limiter.take('a')).allowed).toBe(true);
    expect((await limiter.take('a')).allowed).toBe(false);
    expect((await limiter.take('b')).allowed).toBe(true);
  });
});
