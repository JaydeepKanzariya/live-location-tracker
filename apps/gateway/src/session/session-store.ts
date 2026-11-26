import { SessionUserSchema, type SessionUser } from '@llt/contracts';
import type { Redis } from 'ioredis';
import { z } from 'zod';
import { newSessionId, sessionKeyId } from '../crypto.js';

export interface SessionRecord {
  user: SessionUser;
  /** Kept only as `id_token_hint` for RP-initiated logout. Never sent to the browser. */
  idToken: string | null;
  /** AES-GCM encrypted (see crypto.ts). Never sent to the browser. */
  refreshTokenEnc: string | null;
  createdAt: number;
}

export interface PendingLogin {
  codeVerifier: string;
  nonce: string;
  returnTo: string;
}

export interface SessionStore {
  /** Creates a session with a brand-new id (so a pre-login id can never be reused: no fixation). */
  create(record: Omit<SessionRecord, 'createdAt'>): Promise<string>;
  /** Loads a session and slides the idle timeout. Null if missing, idle-expired or past max age. */
  get(sessionId: string): Promise<SessionRecord | null>;
  /** Like get, but does NOT extend the idle timeout. Used by the expiry sweep. */
  peek(sessionId: string): Promise<SessionRecord | null>;
  destroy(sessionId: string): Promise<void>;
  /** Identifier safe to use as a Socket.IO room name (not the cookie value). */
  roomId(sessionId: string): string;
  savePending(state: string, pending: PendingLogin): Promise<void>;
  /** One-time read: the pending login is deleted as it is returned. */
  takePending(state: string): Promise<PendingLogin | null>;
  ping(): Promise<boolean>;
}

export interface SessionStoreOptions {
  idleTtlMs: number;
  maxAgeMs: number;
  /** Used to derive Redis key ids from session ids (see sessionKeyId). */
  secret: string;
  pendingTtlMs?: number;
  now?: () => number;
}

const StoredSessionSchema = z.object({
  user: SessionUserSchema,
  idToken: z.string().nullable(),
  refreshTokenEnc: z.string().nullable(),
  createdAt: z.number().int(),
});

const PendingSchema = z.object({
  codeVerifier: z.string(),
  nonce: z.string(),
  returnTo: z.string(),
});

export class RedisSessionStore implements SessionStore {
  private readonly now: () => number;
  private readonly pendingTtlMs: number;

  constructor(
    private readonly redis: Redis,
    private readonly options: SessionStoreOptions,
  ) {
    this.now = options.now ?? Date.now;
    this.pendingTtlMs = options.pendingTtlMs ?? 600_000;
  }

  roomId(sessionId: string): string {
    return sessionKeyId(sessionId, this.options.secret);
  }

  private key(sessionId: string): string {
    return `sess:${this.roomId(sessionId)}`;
  }

  async create(record: Omit<SessionRecord, 'createdAt'>): Promise<string> {
    const id = newSessionId();
    const full: SessionRecord = { ...record, createdAt: this.now() };
    const ttl = Math.min(this.options.idleTtlMs, this.options.maxAgeMs);
    await this.redis.set(this.key(id), JSON.stringify(full), 'PX', ttl);
    return id;
  }

  private async load(sessionId: string, slide: boolean): Promise<SessionRecord | null> {
    const key = this.key(sessionId);
    const raw = await this.redis.get(key);
    if (!raw) return null;

    let record: SessionRecord;
    try {
      record = StoredSessionSchema.parse(JSON.parse(raw));
    } catch {
      await this.redis.del(key); // corrupt entry: treat as logged out
      return null;
    }

    const remainingAbsolute = record.createdAt + this.options.maxAgeMs - this.now();
    if (remainingAbsolute <= 0) {
      await this.redis.del(key);
      return null;
    }
    if (slide) {
      // Idle timeout slides forward, but never past the absolute 8 h limit.
      await this.redis.pexpire(key, Math.min(this.options.idleTtlMs, remainingAbsolute));
    }
    return record;
  }

  get(sessionId: string): Promise<SessionRecord | null> {
    return this.load(sessionId, true);
  }

  peek(sessionId: string): Promise<SessionRecord | null> {
    return this.load(sessionId, false);
  }

  async destroy(sessionId: string): Promise<void> {
    await this.redis.del(this.key(sessionId));
  }

  async savePending(state: string, pending: PendingLogin): Promise<void> {
    await this.redis.set(`oidc:pending:${state}`, JSON.stringify(pending), 'PX', this.pendingTtlMs);
  }

  async takePending(state: string): Promise<PendingLogin | null> {
    const raw = await this.redis.getdel(`oidc:pending:${state}`);
    if (!raw) return null;
    const parsed = PendingSchema.safeParse(JSON.parse(raw));
    return parsed.success ? parsed.data : null;
  }

  async ping(): Promise<boolean> {
    return (await this.redis.ping()) === 'PONG';
  }
}
