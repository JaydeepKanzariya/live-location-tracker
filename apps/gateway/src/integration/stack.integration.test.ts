/**
 * Full-stack checks against the REAL services from infra/compose (Kafka and Keycloak), plus a
 * throwaway Redis from Testcontainers. Start the stack first: npm run v1:infra:up
 *
 * Run: npm run test:stack -w @llt/gateway
 */
import { LocationUpdatedEventSchema, type PresenceEntry } from '@llt/contracts';
import { KafkaEventBus } from '@llt/kafka';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { OpenIdClientProvider } from '../auth/oidc.js';
import { ORIGIN, TestEnv, once, sendUpdate, sleep, type Pod } from './harness.js';

const KEYCLOAK = process.env['OIDC_ISSUER_URL'] ?? 'http://localhost:8080/realms/tracker';
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const DRIVER = (process.env['KAFKA_DRIVER'] ?? 'confluent') as 'confluent' | 'kafkajs';

const bus = new KafkaEventBus({ brokers: BROKERS, clientId: 'gateway-stack-test', driver: DRIVER });
const env = new TestEnv({
  bus,
  oidc: new OpenIdClientProvider({
    issuerUrl: KEYCLOAK,
    clientId: 'location-tracker',
    clientSecret: 'location-tracker-secret',
    scopes: 'openid profile email',
    // Must equal a redirect URI registered in the realm. We never visit it: the test reads the
    // Location header and replays the callback against the gateway's random test port.
    redirectUri: 'http://localhost:3000/auth/callback',
    postLogoutRedirectUri: 'http://localhost:3000',
    allowInsecure: true,
  }),
});
let pod: Pod;

beforeAll(async () => {
  await env.start();
  pod = await env.addPod('gateway-stack', { OIDC_ISSUER_URL: KEYCLOAK }, { flushIntervalMs: 50 });
  // A consumer needs a few seconds to join its group. Publish probes until the recorder sees one,
  // so the tests below never race the group join.
  const deadline = Date.now() + 60_000;
  while (!env.records.some((r) => r.key === 'probe')) {
    if (Date.now() > deadline) throw new Error('Kafka consumer never received a probe record');
    await bus.publish({ topic: 'location-updates', key: 'probe', value: '{"probe":true}' });
    await sleep(1_000);
  }
});
afterAll(async () => {
  await env.stop();
  await bus.disconnect();
});

/** A very small cookie jar, enough to be a "browser" for Keycloak's login page. */
class Jar {
  private cookies = new Map<string, string>();
  absorb(res: Response) {
    for (const c of res.headers.getSetCookie()) {
      const [pair] = c.split(';');
      const [name, ...rest] = pair!.split('=');
      const value = rest.join('=');
      if (value === '' || /max-age=0/i.test(c)) this.cookies.delete(name!);
      else this.cookies.set(name!, value);
    }
  }
  header() {
    return [...this.cookies].map(([k, v]) => `${k}=${v}`).join('; ');
  }
}

/** Does what a browser does: gateway -> Keycloak login form -> submit -> back to the gateway. */
async function keycloakLogin(username: string, password: string) {
  const gwJar = new Jar();
  const kcJar = new Jar();

  const start = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
  expect(start.status).toBe(302);
  gwJar.absorb(start);
  const authUrl = start.headers.get('location')!;
  expect(authUrl).toContain('/protocol/openid-connect/auth');
  expect(authUrl).toContain('code_challenge_method=S256'); // PKCE

  const page = await fetch(authUrl, { redirect: 'manual' });
  kcJar.absorb(page);
  const html = await page.text();
  const form = /<form[^>]*id="kc-form-login"[^>]*>/.exec(html)?.[0];
  if (!form) throw new Error(`no Keycloak login form (status ${page.status})`);
  const action = /action="([^"]+)"/.exec(form)![1]!.replaceAll('&amp;', '&');

  const submit = await fetch(action, {
    method: 'POST',
    redirect: 'manual',
    headers: { 'content-type': 'application/x-www-form-urlencoded', cookie: kcJar.header() },
    body: new URLSearchParams({ username, password, credentialId: '' }),
  });
  kcJar.absorb(submit);
  return { gwJar, kcJar, authUrl, submit };
}

describe('real Keycloak login', () => {
  it('logs alice in through Keycloak, creates a session, and opens an authenticated socket', async () => {
    const { gwJar, submit } = await keycloakLogin('alice', 'alice');
    expect(submit.status).toBe(302);
    const back = new URL(submit.headers.get('location')!);
    expect(back.origin + back.pathname).toBe('http://localhost:3000/auth/callback');

    // The gateway code-for-token exchange: verifies PKCE, state, nonce and the ID token.
    const cb = await fetch(`${pod.baseUrl}/auth/callback${back.search}`, {
      redirect: 'manual',
      headers: { cookie: gwJar.header() },
    });
    expect(cb.status).toBe(302);
    expect(cb.headers.get('location')).toBe('/');
    gwJar.absorb(cb);
    const sid = gwJar.header();
    expect(sid).toContain('sid=');

    const me = await fetch(`${pod.baseUrl}/auth/me`, { headers: { cookie: sid } });
    expect(me.status).toBe(200);
    const { user } = (await me.json()) as { user: { sub: string; displayName: string; email: string } };
    expect(user.displayName).toBe('Alice Rider');
    expect(user.email).toBe('alice@example.com');
    expect(user.sub).toMatch(/^[0-9a-f-]{36}$/); // Keycloak's stable user id

    const socket = env.socket(pod, { cookie: sid.split('; ').find((c) => c.startsWith('sid='))! });
    const ready = (await once(socket, 'session:ready')) as { user: { sub: string } };
    expect(ready.user.sub).toBe(user.sub);
  });

  it('rejects a wrong password at Keycloak: no callback, no session', async () => {
    const { submit } = await keycloakLogin('alice', 'wrong-password');
    expect(submit.status).toBe(200); // Keycloak re-shows the login form
    expect(submit.headers.get('location')).toBeNull();
    expect(await submit.text()).toContain('Invalid username or password');
  });

  it('logout ends the gateway session AND the Keycloak single-sign-on session', async () => {
    const { gwJar, kcJar, submit } = await keycloakLogin('bob', 'bob');
    const back = new URL(submit.headers.get('location')!);
    const cb = await fetch(`${pod.baseUrl}/auth/callback${back.search}`, {
      redirect: 'manual',
      headers: { cookie: gwJar.header() },
    });
    gwJar.absorb(cb);
    const sid = gwJar.header().split('; ').find((c) => c.startsWith('sid='))!;

    const socket = env.socket(pod, { cookie: sid });
    await once(socket, 'session:ready');
    const expired = once(socket, 'auth:expired');

    const out = await fetch(`${pod.baseUrl}/auth/logout`, {
      method: 'POST',
      headers: { cookie: sid, origin: ORIGIN },
    });
    const { redirectTo } = (await out.json()) as { redirectTo: string };
    expect(redirectTo).toContain('/protocol/openid-connect/logout');
    expect(redirectTo).toContain('id_token_hint=');
    await expired;
    expect((await fetch(`${pod.baseUrl}/auth/me`, { headers: { cookie: sid } })).status).toBe(401);

    // Visit Keycloak's logout URL like the browser would.
    const kcLogout = await fetch(redirectTo, { redirect: 'manual', headers: { cookie: kcJar.header() } });
    kcJar.absorb(kcLogout);
    expect([200, 302]).toContain(kcLogout.status);

    // A new login attempt must now ask for credentials again (no silent SSO re-login).
    const again = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
    const authPage = await fetch(again.headers.get('location')!, {
      redirect: 'manual',
      headers: { cookie: kcJar.header() },
    });
    expect(authPage.status).toBe(200);
    expect(await authPage.text()).toContain('kc-form-login');
  });
});

describe('real Kafka (idempotent producer via the configured driver)', () => {
  async function loggedInSocket(user: 'alice' | 'bob') {
    const { gwJar, submit } = await keycloakLogin(user, user);
    const back = new URL(submit.headers.get('location')!);
    const cb = await fetch(`${pod.baseUrl}/auth/callback${back.search}`, {
      redirect: 'manual',
      headers: { cookie: gwJar.header() },
    });
    gwJar.absorb(cb);
    const sid = gwJar.header().split('; ').find((c) => c.startsWith('sid='))!;
    const socket = env.socket(pod, { cookie: sid });
    const ready = (await once(socket, 'session:ready')) as { user: { sub: string } };
    return { socket, sub: ready.user.sub };
  }

  it('DUPLICATE: the same eventId twice leaves exactly one record on the Kafka topic', async () => {
    const { socket, sub } = await loggedInSocket('alice');
    const eventId = crypto.randomUUID();

    const first = await sendUpdate(socket, { eventId });
    const second = await sendUpdate(socket, { eventId });
    expect(first).toMatchObject({ ok: true, partition: expect.any(Number), offset: expect.any(String) });
    expect(second).toEqual({ ok: true, duplicate: true });

    await sleep(3_000); // let the recorder consumer catch up
    const matching = env.records.filter((r) => r.value?.includes(eventId));
    expect(matching).toHaveLength(1);
    expect(matching[0]!.key).toBe(sub);
    const event = LocationUpdatedEventSchema.parse(JSON.parse(matching[0]!.value!));
    expect(event.userId).toBe(sub); // the Keycloak sub, set by the gateway
    // The ack points at the real record.
    expect({ partition: matching[0]!.partition, offset: matching[0]!.offset }).toEqual({
      partition: (first as { partition: number }).partition,
      offset: (first as { offset: string }).offset,
    });
  });

  it('SPOOFING: a payload userId never reaches Kafka', async () => {
    const { socket } = await loggedInSocket('alice');
    const eventId = crypto.randomUUID();
    expect(await sendUpdate(socket, { eventId, userId: 'someone-else' })).toEqual({
      ok: false,
      code: 'INVALID_PAYLOAD',
    });
    await sleep(2_000);
    expect(env.records.filter((r) => r.value?.includes(eventId))).toHaveLength(0);
  });

  it('BROADCAST: bob sees alice move, delivered through Kafka and the broadcaster consumer', async () => {
    const [alice, bob] = [await loggedInSocket('alice'), await loggedInSocket('bob')];
    const batch = once(bob.socket, 'location:batch', 15_000) as Promise<PresenceEntry[]>;
    await sleep(500);
    await sendUpdate(alice.socket, { lat: 23.5, lng: 72.9 });
    const entries = await batch;
    expect(entries.find((e) => e.userId === alice.sub)).toMatchObject({ lat: 23.5, lng: 72.9 });
  });
});
