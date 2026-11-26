import type { Redis } from 'ioredis';

/**
 * Remembers recently seen eventIds so a retried update is accepted only once.
 * `claim` is atomic: exactly one caller gets `true` for a given eventId.
 */
export interface DedupeStore {
  claim(eventId: string): Promise<boolean>;
  /** Forget the id again, used when the update was NOT produced (so a retry can succeed). */
  release(eventId: string): Promise<void>;
}

export class RedisDedupeStore implements DedupeStore {
  constructor(
    private readonly redis: Redis,
    private readonly ttlSec: number,
    private readonly keyPrefix = 'loc:dedupe:',
  ) {}

  async claim(eventId: string): Promise<boolean> {
    // SET key 1 NX EX ttl -> "OK" for the first caller, null for everyone after
    const res = await this.redis.set(this.keyPrefix + eventId, '1', 'EX', this.ttlSec, 'NX');
    return res === 'OK';
  }

  async release(eventId: string): Promise<void> {
    await this.redis.del(this.keyPrefix + eventId);
  }
}

export class InMemoryDedupeStore implements DedupeStore {
  private readonly seen = new Set<string>();

  async claim(eventId: string): Promise<boolean> {
    if (this.seen.has(eventId)) return false;
    this.seen.add(eventId);
    return true;
  }

  async release(eventId: string): Promise<void> {
    this.seen.delete(eventId);
  }
}
