import { GatewayEnvSchema, loadConfig } from '@llt/config';
import type { ClientToServerEvents, LocationUpdateAck, ServerToClientEvents } from '@llt/contracts';
import { InMemoryEventBus, type ConsumedMessage } from '@llt/kafka';
import { createLogger } from '@llt/observability';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { io as connect, type Socket } from 'socket.io-client';
import { buildGateway, type Gateway } from '../app.js';
import type { LoginResult, LoginStart, OidcProvider } from '../auth/oidc.js';
import { userFromClaims } from '../auth/oidc.js';

export const ORIGIN = 'http://localhost:5173';
export const APP_URL = 'http://localhost:3000';

type ClientSocket = Socket<ServerToClientEvents, ClientToServerEvents>;

/**
 * Stand-in identity provider. `startLogin` hands out a real random state; `finishLogin` takes
 * the user from the `code` query value (code=alice -> user "alice"). It lets the tests exercise
 * OUR code (state cookie, one-time pending login, sessions, cookies) without a Keycloak.
 * The real Keycloak login is covered by scripts/keycloak-login-check.mjs.
 */
export class FakeOidc implements OidcProvider {
  async startLogin(): Promise<LoginStart> {
    const state = crypto.randomUUID();
    return {
      url: `http://idp.test/authorize?state=${state}`,
      state,
      codeVerifier: 'verifier-' + state,
      nonce: 'nonce-' + state,
    };
  }

  async finishLogin(
    callbackUrl: URL,
    expected: { state: string; codeVerifier: string; nonce: string },
  ): Promise<LoginResult> {
    if (callbackUrl.searchParams.get('state') !== expected.state) throw new Error('state mismatch');
    const code = callbackUrl.searchParams.get('code');
    if (!code || code === 'bad') throw new Error('invalid code');
    return {
      user: userFromClaims({ sub: `${code}-sub`, name: code[0]!.toUpperCase() + code.slice(1) }),
      idToken: `id-token-for-${code}`,
      refreshToken: `refresh-token-for-${code}`,
    };
  }

  async logoutUrl(idToken: string | null): Promise<string | null> {
    return `http://idp.test/logout?id_token_hint=${idToken ?? ''}`;
  }
}

export interface Pod {
  gateway: Gateway;
  port: number;
  baseUrl: string;
}

export class TestEnv {
  redisContainer!: StartedRedisContainer;
  redis!: Redis;
  bus = new InMemoryEventBus(3);
  /** Every record that reached the location-updates topic (a stand-in for "the Kafka topic"). */
  records: ConsumedMessage[] = [];
  pods: Pod[] = [];
  private sockets: ClientSocket[] = [];
  private extraRedis: Redis[] = [];

  async start(): Promise<void> {
    this.redisContainer = await new RedisContainer('redis:7-alpine').start();
    this.redis = new Redis(this.redisContainer.getConnectionUrl());
    await this.bus.connect();
    await this.bus.subscribe({ groupId: 'test-recorder', topics: ['location-updates'] }, async (m) => {
      this.records.push(m);
    });
  }

  async addPod(
    instanceId: string,
    env: Record<string, string> = {},
    options: { sweepIntervalMs?: number; flushIntervalMs?: number } = {},
  ): Promise<Pod> {
    const config = loadConfig(GatewayEnvSchema, {
      NODE_ENV: 'test',
      INSTANCE_ID: instanceId,
      APP_BASE_URL: APP_URL,
      CORS_ALLOWED_ORIGINS: `${ORIGIN},${APP_URL}`,
      REDIS_URL: this.redisContainer.getConnectionUrl(),
      ...env,
    });
    const redis = new Redis(this.redisContainer.getConnectionUrl());
    this.extraRedis.push(redis);
    const gateway = await buildGateway({
      config,
      redis,
      bus: this.bus,
      oidc: new FakeOidc(),
      logger: createLogger({ service: `test:${instanceId}`, level: 'silent' }),
      sweepIntervalMs: options.sweepIntervalMs ?? 60_000,
      flushIntervalMs: options.flushIntervalMs ?? 50,
    });
    const port = await gateway.start(0, '127.0.0.1');
    const pod = { gateway, port, baseUrl: `http://127.0.0.1:${port}` };
    this.pods.push(pod);
    return pod;
  }

  /** Runs the whole login over HTTP and returns the session cookie header value. */
  async login(pod: Pod, who: string): Promise<string> {
    const start = await fetch(`${pod.baseUrl}/auth/login`, { redirect: 'manual' });
    const state = new URL(start.headers.get('location')!).searchParams.get('state')!;
    const stateCookie = cookieHeader(start.headers.getSetCookie());
    const cb = await fetch(`${pod.baseUrl}/auth/callback?code=${who}&state=${state}`, {
      redirect: 'manual',
      headers: { cookie: stateCookie },
    });
    const session = cookieHeader(cb.headers.getSetCookie()).split('; ').find((c) => c.startsWith('sid='));
    if (!session) throw new Error(`login as ${who} failed: ${cb.status} ${cb.headers.get('location')}`);
    return session;
  }

  socket(
    pod: Pod,
    opts: { cookie?: string; origin?: string | null } = {},
  ): ClientSocket {
    const extraHeaders: Record<string, string> = {};
    if (opts.cookie) extraHeaders['cookie'] = opts.cookie;
    if (opts.origin !== null) extraHeaders['origin'] = opts.origin ?? ORIGIN;
    const s: ClientSocket = connect(pod.baseUrl, {
      transports: ['websocket'],
      extraHeaders,
      reconnection: false,
      forceNew: true,
    });
    this.sockets.push(s);
    return s;
  }

  async connected(pod: Pod, cookie: string): Promise<ClientSocket> {
    const s = this.socket(pod, { cookie });
    await once(s, 'connect');
    return s;
  }

  async stop(): Promise<void> {
    for (const s of this.sockets) s.close();
    for (const pod of this.pods) await pod.gateway.stop().catch(() => undefined);
    for (const r of this.extraRedis) r.disconnect();
    this.redis?.disconnect();
    await this.redisContainer?.stop();
  }
}

export function cookieHeader(setCookies: string[]): string {
  return setCookies.map((c) => c.split(';')[0]!).join('; ');
}

export function once(socket: ClientSocket, event: string, timeoutMs = 5_000): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for "${event}"`)), timeoutMs);
    // The events used here are a mix of reserved and custom ones; the client types are loose enough.
    (socket as Socket).once(event, (...args: unknown[]) => {
      clearTimeout(timer);
      resolve(args[0]);
    });
  });
}

export function sendUpdate(
  socket: ClientSocket,
  overrides: Record<string, unknown> = {},
): Promise<LocationUpdateAck> {
  const payload = {
    eventId: crypto.randomUUID(),
    lat: 23.0225,
    lng: 72.5714,
    accuracyM: 10,
    headingDeg: null,
    speedMps: null,
    occurredAt: Date.now(),
    source: 'gps',
    ...overrides,
  };
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('no ack received')), 5_000);
    (socket as Socket).emit('location:update', payload, (ack: LocationUpdateAck) => {
      clearTimeout(timer);
      resolve(ack);
    });
  });
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
