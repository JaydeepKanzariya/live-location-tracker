import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { RedisDedupeStore } from '../location/dedupe.js';
import { consumeToken, RedisRateLimiter } from '../ratelimit/token-bucket.js';
import { RedisSessionStore } from '../session/session-store.js';

let container: StartedRedisContainer;
let redis: Redis;

beforeAll(async () => {
  container = await new RedisContainer('redis:7-alpine').start();
  redis = new Redis(container.getConnectionUrl());
});
afterAll(async () => {
  redis?.disconnect();
  await container?.stop();
});

const user = { sub: 'alice-sub', displayName: 'Alice' };
const record = { user, idToken: 'idt', refreshTokenEnc: 'enc' };

describe('RedisSessionStore (real Redis)', () => {
  it('creates, reads and destroys a session', async () => {
    const store = new RedisSessionStore(redis, { idleTtlMs: 60_000, maxAgeMs: 600_000, secret: 's' });
    const id = await store.create(record);
    expect((await store.get(id))?.user).toEqual(user);
    await store.destroy(id);
    expect(await store.get(id)).toBeNull();
  });

  it('never stores the raw session id in a Redis key', async () => {
    const store = new RedisSessionStore(redis, { idleTtlMs: 60_000, maxAgeMs: 600_000, secret: 's' });
    const id = await store.create(record);
    const keys = await redis.keys('sess:*');
    expect(keys.some((k) => k.includes(id))).toBe(false);
    expect(keys).toContain(`sess:${store.roomId(id)}`);
  });

  it('slides the idle timeout on get() but not on peek()', async () => {
    const store = new RedisSessionStore(redis, { idleTtlMs: 2_000, maxAgeMs: 600_000, secret: 's' });
    const id = await store.create(record);
    const key = `sess:${store.roomId(id)}`;
    await redis.pexpire(key, 300); // pretend it has been idle for a while
    await store.peek(id);
    expect(await redis.pttl(key)).toBeLessThanOrEqual(300);
    await store.get(id);
    expect(await redis.pttl(key)).toBeGreaterThan(1_500);
  });

  it('ends the session at the absolute max age even if it is used constantly', async () => {
    let now = 1_000_000;
    const store = new RedisSessionStore(redis, {
      idleTtlMs: 30 * 60_000,
      maxAgeMs: 8 * 3_600_000,
      secret: 's',
      now: () => now,
    });
    const id = await store.create(record);
    now += 7 * 3_600_000 + 59 * 60_000; // 7h59m later, still active
    expect(await store.get(id)).not.toBeNull();
    // The idle timer must never reach past the absolute limit.
    expect(await redis.pttl(`sess:${store.roomId(id)}`)).toBeLessThanOrEqual(60_000);
    now += 2 * 60_000; // past 8 h
    expect(await store.get(id)).toBeNull();
    expect(await store.peek(id)).toBeNull();
  });

  it('treats a corrupt entry as logged out', async () => {
    const store = new RedisSessionStore(redis, { idleTtlMs: 60_000, maxAgeMs: 600_000, secret: 's' });
    const id = await store.create(record);
    await redis.set(`sess:${store.roomId(id)}`, '{not json');
    expect(await store.get(id)).toBeNull();
  });

  it('pending logins can be read exactly once', async () => {
    const store = new RedisSessionStore(redis, { idleTtlMs: 60_000, maxAgeMs: 600_000, secret: 's' });
    await store.savePending('state-1', { codeVerifier: 'v', nonce: 'n', returnTo: '/x' });
    expect(await store.takePending('state-1')).toEqual({ codeVerifier: 'v', nonce: 'n', returnTo: '/x' });
    expect(await store.takePending('state-1')).toBeNull();
    expect(await store.takePending('never-saved')).toBeNull();
  });
});

describe('RedisDedupeStore (real Redis)', () => {
  it('lets exactly one of many concurrent callers claim an id', async () => {
    const dedupe = new RedisDedupeStore(redis, 60);
    const id = crypto.randomUUID();
    const results = await Promise.all(Array.from({ length: 20 }, () => dedupe.claim(id)));
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('allows a claim again after release, and expires on its own', async () => {
    const dedupe = new RedisDedupeStore(redis, 1);
    const id = crypto.randomUUID();
    expect(await dedupe.claim(id)).toBe(true);
    await dedupe.release(id);
    expect(await dedupe.claim(id)).toBe(true);
    await new Promise((r) => setTimeout(r, 1_300));
    expect(await dedupe.claim(id)).toBe(true); // TTL ran out
  });
});

describe('RedisRateLimiter (real Redis)', () => {
  it('matches the pure token-bucket maths: burst of 3, then a retry time close to 1 s', async () => {
    const limiter = new RedisRateLimiter(redis, { refillPerSec: 1, burst: 3 }, `rl:test:${Date.now()}:`);
    const results = [];
    for (let i = 0; i < 4; i++) results.push(await limiter.take('u'));
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, false]);
    expect(results[3]?.retryAfterMs).toBeGreaterThan(800);
    expect(results[3]?.retryAfterMs).toBeLessThanOrEqual(1_000);
    expect(consumeToken(null, 0, { refillPerSec: 1, burst: 3 }).result.allowed).toBe(true);
  });

  it('is atomic: 50 concurrent takes let through exactly the burst', async () => {
    const limiter = new RedisRateLimiter(redis, { refillPerSec: 0.001, burst: 5 }, `rl:race:${Date.now()}:`);
    const results = await Promise.all(Array.from({ length: 50 }, () => limiter.take('u')));
    expect(results.filter((r) => r.allowed)).toHaveLength(5);
  });

  it('refills over real time', async () => {
    const limiter = new RedisRateLimiter(redis, { refillPerSec: 5, burst: 1 }, `rl:refill:${Date.now()}:`);
    expect((await limiter.take('u')).allowed).toBe(true);
    expect((await limiter.take('u')).allowed).toBe(false);
    await new Promise((r) => setTimeout(r, 400)); // 5/s -> 2 tokens worth, capped at 1
    expect((await limiter.take('u')).allowed).toBe(true);
  });

  it('keeps separate buckets per user', async () => {
    const limiter = new RedisRateLimiter(redis, { refillPerSec: 0.001, burst: 1 }, `rl:sep:${Date.now()}:`);
    expect((await limiter.take('a')).allowed).toBe(true);
    expect((await limiter.take('a')).allowed).toBe(false);
    expect((await limiter.take('b')).allowed).toBe(true);
  });
});
