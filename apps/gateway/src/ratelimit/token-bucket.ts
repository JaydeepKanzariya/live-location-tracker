import type { Redis } from 'ioredis';

export interface RateLimitResult {
  allowed: boolean;
  /** Only set when allowed is false: how long until one token is available. */
  retryAfterMs?: number;
}

export interface RateLimiter {
  take(key: string): Promise<RateLimitResult>;
}

export interface TokenBucketOptions {
  /** Tokens added per second (plan: 1). */
  refillPerSec: number;
  /** Bucket size, i.e. the burst allowed (plan: 3). */
  burst: number;
}

/**
 * The bucket maths in one pure function. The Redis Lua script below is the same algorithm,
 * so this is what the unit tests check and what the integration test compares against.
 */
export function consumeToken(
  state: { tokens: number; updatedAtMs: number } | null,
  nowMs: number,
  { refillPerSec, burst }: TokenBucketOptions,
): { state: { tokens: number; updatedAtMs: number }; result: RateLimitResult } {
  const prev = state ?? { tokens: burst, updatedAtMs: nowMs };
  const elapsedSec = Math.max(0, nowMs - prev.updatedAtMs) / 1000;
  const tokens = Math.min(burst, prev.tokens + elapsedSec * refillPerSec);
  if (tokens >= 1) {
    return { state: { tokens: tokens - 1, updatedAtMs: nowMs }, result: { allowed: true } };
  }
  const retryAfterMs = Math.max(1, Math.ceil(((1 - tokens) / refillPerSec) * 1000));
  return { state: { tokens, updatedAtMs: nowMs }, result: { allowed: false, retryAfterMs } };
}

export class InMemoryRateLimiter implements RateLimiter {
  private readonly buckets = new Map<string, { tokens: number; updatedAtMs: number }>();

  constructor(
    private readonly options: TokenBucketOptions,
    private readonly now: () => number = Date.now,
  ) {}

  async take(key: string): Promise<RateLimitResult> {
    const { state, result } = consumeToken(this.buckets.get(key) ?? null, this.now(), this.options);
    this.buckets.set(key, state);
    return result;
  }
}

// Runs atomically inside Redis, so several gateway pods share one bucket per user.
// Time comes from the Redis server (TIME), so pod clock drift cannot change the rate.
const TAKE_SCRIPT = `
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end
local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(burst, tokens + elapsed * rate)
local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.max(1, math.ceil(((1 - tokens) / rate) * 1000))
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, retry}
`;

export class RedisRateLimiter implements RateLimiter {
  constructor(
    private readonly redis: Redis,
    private readonly options: TokenBucketOptions,
    private readonly keyPrefix = 'rl:loc:',
  ) {}

  async take(key: string): Promise<RateLimitResult> {
    const [allowed, retry] = (await this.redis.eval(
      TAKE_SCRIPT,
      1,
      this.keyPrefix + key,
      String(this.options.refillPerSec),
      String(this.options.burst),
    )) as [number, number];
    return allowed === 1 ? { allowed: true } : { allowed: false, retryAfterMs: retry };
  }
}
