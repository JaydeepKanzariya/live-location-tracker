import { LocationUpdatedEventSchema, type PresenceEntry } from '@llt/contracts';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  APP_URL,
  ORIGIN,
  TestEnv,
  cookieHeader,
  once,
  sendUpdate,
  sleep,
  type Pod,
} from './harness.js';

const env = new TestEnv();
let pod: Pod;

beforeAll(async () => {
  await env.start();
  pod = await env.addPod('gateway-1', {}, { sweepIntervalMs: 200 });
});
afterAll(() => env.stop());

const recordsBefore = () => env.records.length;

describe('Phase 1 exit checks', () => {
  describe('socket handshake auth', () => {
    it('rejects a socket with no session cookie', async () => {
      const s = env.socket(pod, {});
      const err = (await once(s, 'connect_error')) as Error;
      expect(err.message).toBe('unauthorized');
      expect(s.connected).toBe(false);
    });

    it('rejects a socket whose cookie names a session that does not exist', async () => {
      const s = env.socket(pod, { cookie: 'sid=not-a-real-session' });
      expect(((await once(s, 'connect_error')) as Error).message).toBe('unauthorized');
    });

    it('rejects a valid session when the Origin is not on the allow-list (cross-site hijack)', async () => {
      const cookie = await env.login(pod, 'alice');
      const s = env.socket(pod, { cookie, origin: 'https://evil.example' });
      await once(s, 'connect_error');
      expect(s.connected).toBe(false);
    });

    it('rejects a valid session when there is no Origin header at all', async () => {
      const cookie = await env.login(pod, 'alice');
      const s = env.socket(pod, { cookie, origin: null });
      await once(s, 'connect_error');
      expect(s.connected).toBe(false);
    });

    it('accepts a valid session and tells the client who it is, from the server-side session', async () => {
      const cookie = await env.login(pod, 'alice');
      const s = env.socket(pod, { cookie });
      const ready = (await once(s, 'session:ready')) as {
        user: { sub: string; displayName: string };
        settings: { heartbeatMs: number };
      };
      expect(ready.user).toMatchObject({ sub: 'alice-sub', displayName: 'Alice' });
      expect(ready.settings.heartbeatMs).toBe(15_000);
    });
  });

  describe('location:update', () => {
    it('SPOOFING: a userId in the payload is rejected; a valid update is always attributed to the session user', async () => {
      const cookie = await env.login(pod, 'alice');
      const s = await env.connected(pod, cookie);
      const before = recordsBefore();

      expect(await sendUpdate(s, { userId: 'bob-sub' })).toEqual({ ok: false, code: 'INVALID_PAYLOAD' });
      expect(recordsBefore()).toBe(before);

      expect(await sendUpdate(s)).toMatchObject({ ok: true, partition: expect.any(Number) });
      await sleep(50);
      const event = LocationUpdatedEventSchema.parse(JSON.parse(env.records.at(-1)!.value!));
      expect(event.userId).toBe('alice-sub');
      expect(env.records.at(-1)!.key).toBe('alice-sub');
    });

    it('DUPLICATE: the same eventId sent twice produces exactly one record', async () => {
      const cookie = await env.login(pod, 'carol');
      const s = await env.connected(pod, cookie);
      const eventId = crypto.randomUUID();
      const before = recordsBefore();

      const first = await sendUpdate(s, { eventId });
      const second = await sendUpdate(s, { eventId });
      expect(first).toMatchObject({ ok: true, partition: expect.any(Number) });
      expect(second).toEqual({ ok: true, duplicate: true });
      await sleep(50);
      expect(recordsBefore() - before).toBe(1);
    });

    it('DUPLICATE (race): the same eventId from several sockets at the same moment produces one record', async () => {
      const cookie = await env.login(pod, 'dave');
      const sockets = await Promise.all([1, 2, 3, 4].map(() => env.connected(pod, cookie)));
      const eventId = crypto.randomUUID();
      const before = recordsBefore();

      const acks = await Promise.all(sockets.map((s) => sendUpdate(s, { eventId })));
      await sleep(50);
      expect(recordsBefore() - before).toBe(1);
      expect(acks.filter((a) => a.ok && 'duplicate' in a)).toHaveLength(3);
    });

    it('RATE LIMIT: 5 updates in a burst -> 3 accepted, 2 RATE_LIMITED with retryAfterMs', async () => {
      const cookie = await env.login(pod, 'erin');
      const s = await env.connected(pod, cookie);
      const before = recordsBefore();

      const acks = [];
      for (let i = 0; i < 5; i++) acks.push(await sendUpdate(s));
      const limited = acks.filter((a) => !a.ok);
      expect(acks.filter((a) => a.ok)).toHaveLength(3);
      expect(limited).toHaveLength(2);
      for (const a of limited) {
        expect(a).toMatchObject({ ok: false, code: 'RATE_LIMITED' });
        expect((a as { retryAfterMs: number }).retryAfterMs).toBeGreaterThan(0);
      }
      await sleep(50);
      expect(recordsBefore() - before).toBe(3);
    });

    it('RATE LIMIT: the bucket is shared by all of one user\'s sockets and tabs', async () => {
      const cookie = await env.login(pod, 'frank');
      const [a, b] = await Promise.all([env.connected(pod, cookie), env.connected(pod, cookie)]);
      const results = [];
      for (let i = 0; i < 3; i++) results.push(await sendUpdate(a));
      for (let i = 0; i < 2; i++) results.push(await sendUpdate(b));
      expect(results.filter((r) => r.ok)).toHaveLength(3);
    });

    it.each([
      ['latitude 91', { lat: 91 }],
      ['longitude 181', { lng: 181 }],
      ['string coordinates', { lat: '23', lng: '72' }],
      ['missing eventId', { eventId: undefined }],
      ['unknown key', { admin: true }],
    ])('INVALID: %s is rejected and nothing is produced', async (_name, override) => {
      const cookie = await env.login(pod, `grace${_name.length}`);
      const s = await env.connected(pod, cookie);
      const before = recordsBefore();
      expect(await sendUpdate(s, override)).toEqual({ ok: false, code: 'INVALID_PAYLOAD' });
      await sleep(30);
      expect(recordsBefore()).toBe(before);
    });

    it('INVALID: a 10 KB payload closes the socket and nothing is produced', async () => {
      const cookie = await env.login(pod, 'heidi');
      const s = await env.connected(pod, cookie);
      const before = recordsBefore();
      const closed = once(s, 'disconnect');
      s.emit('location:update', { eventId: crypto.randomUUID(), pad: 'x'.repeat(10_240) } as never);
      await closed;
      expect(recordsBefore()).toBe(before);
    });
  });

  describe('sessions', () => {
    it('SESSION EXPIRY on event: after the session disappears, the next update gets SESSION_EXPIRED and the socket is closed', async () => {
      const cookie = await env.login(pod, 'ivan');
      const s = await env.connected(pod, cookie);
      expect(await sendUpdate(s)).toMatchObject({ ok: true });

      const expired = once(s, 'auth:expired');
      const closed = once(s, 'disconnect');
      await env.redis.del(...(await env.redis.keys('sess:*')));
      expect(await sendUpdate(s)).toEqual({ ok: false, code: 'SESSION_EXPIRED' });
      await expired;
      await closed;
      expect(s.connected).toBe(false);
    });

    it('SESSION EXPIRY on sweep: an idle socket is told auth:expired without sending anything', async () => {
      const cookie = await env.login(pod, 'judy');
      const s = await env.connected(pod, cookie);
      const expired = once(s, 'auth:expired', 3_000);
      const closed = once(s, 'disconnect', 3_000);
      // Drop only Judy's session: find the key whose payload belongs to her.
      for (const key of await env.redis.keys('sess:*')) {
        if ((await env.redis.get(key))?.includes('judy-sub')) await env.redis.del(key);
      }
      await expired;
      await closed;
    });

    it('SESSION EXPIRY on handshake: the idle timeout ends a session, and a new login works', async () => {
      const short = await env.addPod('gateway-short', { SESSION_IDLE_TTL_MS: '1000' });
      const cookie = await env.login(short, 'ken');
      await sleep(1_400);
      const s = env.socket(short, { cookie });
      expect(((await once(s, 'connect_error')) as Error).message).toBe('unauthorized');

      const again = await env.login(short, 'ken');
      await env.connected(short, again);
    });

    it('LOGOUT: POST /auth/logout ends the session, closes its sockets, and returns the IdP logout URL', async () => {
      const cookie = await env.login(pod, 'liam');
      const s = await env.connected(pod, cookie);
      const expired = once(s, 'auth:expired');
      const closed = once(s, 'disconnect');

      const res = await fetch(`${pod.baseUrl}/auth/logout`, {
        method: 'POST',
        headers: { cookie, origin: ORIGIN },
      });
      expect(res.status).toBe(200);
      expect(((await res.json()) as { redirectTo: string }).redirectTo).toContain('http://idp.test/logout');
      await expired;
      await closed;

      const me = await fetch(`${pod.baseUrl}/auth/me`, { headers: { cookie } });
      expect(me.status).toBe(401);
      const s2 = env.socket(pod, { cookie });
      expect(((await once(s2, 'connect_error')) as Error).message).toBe('unauthorized');
    });

    it('LOGOUT: a POST from another site (bad Origin) is refused and the session survives', async () => {
      const cookie = await env.login(pod, 'mia');
      const res = await fetch(`${pod.baseUrl}/auth/logout`, {
        method: 'POST',
        headers: { cookie, origin: 'https://evil.example' },
      });
      expect(res.status).toBe(403);
      expect((await fetch(`${pod.baseUrl}/auth/me`, { headers: { cookie } })).status).toBe(200);

      const noOrigin = await fetch(`${pod.baseUrl}/auth/logout`, { method: 'POST', headers: { cookie } });
      expect(noOrigin.status).toBe(403);
    });
  });

  describe('login flow', () => {
    it('sets a hardened session cookie and keeps tokens out of the browser', async () => {
      const start = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
      expect(start.status).toBe(302);
      const state = new URL(start.headers.get('location')!).searchParams.get('state')!;
      const cb = await fetch(`${pod.baseUrl}/auth/callback?code=nina&state=${state}`, {
        redirect: 'manual',
        headers: { cookie: cookieHeader(start.headers.getSetCookie()) },
      });
      expect(cb.status).toBe(302);
      const sessionCookie = cb.headers.getSetCookie().find((c) => c.startsWith('sid='))!;
      expect(sessionCookie).toMatch(/HttpOnly/i);
      expect(sessionCookie).toMatch(/SameSite=Lax/i);
      expect(sessionCookie).toMatch(/Path=\//);
      expect(sessionCookie).not.toMatch(/Domain=/i);
      expect(sessionCookie).not.toContain('id-token');
      expect(sessionCookie).not.toContain('refresh-token');

      // The refresh token is stored encrypted, never as plain text.
      const stored = (await env.redis.keys('sess:*')).length
        ? (await Promise.all((await env.redis.keys('sess:*')).map((k) => env.redis.get(k)))).join()
        : '';
      expect(stored).toContain('nina-sub');
      expect(stored).not.toContain('refresh-token-for-nina');
    });

    it('refuses a callback whose state cookie does not match (login CSRF)', async () => {
      const victim = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
      const attacker = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
      const attackerState = new URL(attacker.headers.get('location')!).searchParams.get('state')!;
      // The victim's browser (their state cookie) finishes the ATTACKER's login.
      const cb = await fetch(`${pod.baseUrl}/auth/callback?code=oscar&state=${attackerState}`, {
        redirect: 'manual',
        headers: { cookie: cookieHeader(victim.headers.getSetCookie()) },
      });
      expect(cb.headers.get('location')).toContain('error=login_failed');
      expect(cb.headers.getSetCookie().some((c) => c.startsWith('sid='))).toBe(false);
    });

    it('a login callback can only be used once', async () => {
      const start = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
      const state = new URL(start.headers.get('location')!).searchParams.get('state')!;
      const cookie = cookieHeader(start.headers.getSetCookie());
      const url = `${pod.baseUrl}/auth/callback?code=pat&state=${state}`;
      const first = await fetch(url, { redirect: 'manual', headers: { cookie } });
      const replay = await fetch(url, { redirect: 'manual', headers: { cookie } });
      expect(first.headers.getSetCookie().some((c) => c.startsWith('sid='))).toBe(true);
      expect(replay.headers.get('location')).toContain('error=login_expired');
    });

    it('a failed code exchange redirects with an error and creates no session', async () => {
      const start = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
      const state = new URL(start.headers.get('location')!).searchParams.get('state')!;
      const cb = await fetch(`${pod.baseUrl}/auth/callback?code=bad&state=${state}`, {
        redirect: 'manual',
        headers: { cookie: cookieHeader(start.headers.getSetCookie()) },
      });
      expect(cb.headers.get('location')).toContain('error=login_failed');
      expect(cb.headers.getSetCookie().some((c) => c.startsWith('sid='))).toBe(false);
    });

    it('only redirects back to paths on this site (no open redirect)', async () => {
      for (const [returnTo, expected] of [
        ['/map', '/map'],
        ['https://evil.example/x', '/'],
        ['//evil.example', '/'],
      ] as const) {
        const start = await fetch(`${pod.baseUrl}/auth/login?returnTo=${encodeURIComponent(returnTo)}`, {
          redirect: 'manual',
        });
        const state = new URL(start.headers.get('location')!).searchParams.get('state')!;
        const cb = await fetch(`${pod.baseUrl}/auth/callback?code=quinn&state=${state}`, {
          redirect: 'manual',
          headers: { cookie: cookieHeader(start.headers.getSetCookie()) },
        });
        expect(cb.headers.get('location')).toBe(expected);
      }
    });
  });

  describe('broadcast', () => {
    it('an update from alice reaches bob, but not a socket that is not logged in', async () => {
      const [aliceCookie, bobCookie] = await Promise.all([env.login(pod, 'rita'), env.login(pod, 'sam')]);
      const rita = await env.connected(pod, aliceCookie);
      const sam = await env.connected(pod, bobCookie);
      const stranger = env.socket(pod, {});
      stranger.on('location:batch', () => {
        throw new Error('an unauthenticated socket received data');
      });

      const batch = once(sam, 'location:batch') as Promise<PresenceEntry[]>;
      await sendUpdate(rita, { lat: 12.5, lng: 77.5 });
      const entries = await batch;
      expect(entries).toHaveLength(1);
      expect(entries[0]).toMatchObject({ userId: 'rita-sub', displayName: 'Rita', lat: 12.5, lng: 77.5 });
      await sleep(100);
    });

    it('two gateway pods: a user on pod 2 sees updates from a user on pod 1 (each pod has its own consumer group)', async () => {
      const pod1 = await env.addPod('gateway-a');
      const pod2 = await env.addPod('gateway-b');
      const [c1, c2] = await Promise.all([env.login(pod1, 'tess'), env.login(pod2, 'uma')]);
      const tess = await env.connected(pod1, c1);
      const uma = await env.connected(pod2, c2);

      const onPod2 = once(uma, 'location:batch') as Promise<PresenceEntry[]>;
      await sendUpdate(tess, { lat: 1.5, lng: 2.5 });
      expect((await onPod2)[0]).toMatchObject({ userId: 'tess-sub', lat: 1.5 });
    });
  });

  it('health endpoints answer', async () => {
    expect((await fetch(`${pod.baseUrl}/healthz`)).status).toBe(200);
    const ready = await fetch(`${pod.baseUrl}/readyz`);
    expect(ready.status).toBe(200);
  });

  it('sends security headers', async () => {
    const res = await fetch(`${pod.baseUrl}/healthz`);
    expect(res.headers.get('x-content-type-options')).toBe('nosniff');
    expect(res.headers.get('permissions-policy')).toBe('geolocation=(self)');
    expect(APP_URL).toBeTruthy();
  });
});
