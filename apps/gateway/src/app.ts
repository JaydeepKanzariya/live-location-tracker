import type { GatewayEnv } from '@llt/config';
import type { EventBus } from '@llt/kafka';
import fastifyCookie from '@fastify/cookie';
import fastifyHelmet from '@fastify/helmet';
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import type { Logger } from 'pino';
import { registerAuthRoutes, type CookieNames } from './auth/routes.js';
import type { OidcProvider } from './auth/oidc.js';
import { Broadcaster } from './broadcaster.js';
import { buildAllowedOrigins } from './http/origin.js';
import { RedisDedupeStore, type DedupeStore } from './location/dedupe.js';
import { createLocationPipeline } from './location/pipeline.js';
import { RedisRateLimiter, type RateLimiter } from './ratelimit/token-bucket.js';
import { RedisSessionStore, type SessionStore } from './session/session-store.js';
import { attachSocketServer, type GatewayServer } from './socket/socket-server.js';

export interface GatewayDeps {
  config: GatewayEnv;
  redis: Redis;
  bus: EventBus;
  oidc: OidcProvider;
  logger: Logger;
  /** Overridable for tests; default to the Redis implementations. */
  store?: SessionStore;
  dedupe?: DedupeStore;
  limiter?: RateLimiter;
  /** Defaults: 5 s expiry sweep, 150 ms broadcast batching. Tests shorten them. */
  sweepIntervalMs?: number;
  flushIntervalMs?: number;
}

export interface Gateway {
  app: FastifyInstance;
  io: GatewayServer;
  broadcaster: Broadcaster;
  store: SessionStore;
  /** Starts the Kafka consumer and begins listening. Returns the bound port. */
  start(port?: number, host?: string): Promise<number>;
  /** Graceful shutdown: stop accepting, drain sockets, flush, disconnect. */
  stop(): Promise<void>;
}

export function cookieNamesFor(config: GatewayEnv): { names: CookieNames; secure: boolean } {
  const secure = config.NODE_ENV === 'production';
  // The __Host- prefix makes browsers insist on Secure + Path=/ + no Domain, but it only works
  // over https. Local development is plain http, so it uses plain names there.
  return secure
    ? { secure, names: { session: '__Host-sid', loginState: '__Host-oidc-state' } }
    : { secure, names: { session: 'sid', loginState: 'oidc-state' } };
}

export async function buildGateway(deps: GatewayDeps): Promise<Gateway> {
  const { config, redis, bus, oidc, logger } = deps;
  const { names: cookieNames, secure } = cookieNamesFor(config);
  const allowedOrigins = buildAllowedOrigins(config.CORS_ALLOWED_ORIGINS, config.APP_BASE_URL);

  const store =
    deps.store ??
    new RedisSessionStore(redis, {
      idleTtlMs: config.SESSION_IDLE_TTL_MS,
      maxAgeMs: config.SESSION_MAX_AGE_MS,
      secret: config.SESSION_SECRET,
    });
  const dedupe = deps.dedupe ?? new RedisDedupeStore(redis, config.LOCATION_DEDUPE_TTL_SEC);
  // Plan: 1 update/s sustained, bursts of 3.
  const limiter = deps.limiter ?? new RedisRateLimiter(redis, { refillPerSec: 1, burst: 3 });

  // NOTE: request logging stays off on purpose: /auth/callback URLs carry a one-time OIDC code.
  // Fastify 5.12 deprecates this option (replacement: logController); revisit before Fastify 6.
  const app = Fastify({ loggerInstance: logger as FastifyBaseLogger, disableRequestLogging: true });
  await app.register(fastifyHelmet);
  await app.register(fastifyCookie);
  app.addHook('onRequest', async (_req, reply) => {
    reply.header('Permissions-Policy', 'geolocation=(self)');
  });

  const handleLocationUpdate = createLocationPipeline({
    bus,
    dedupe,
    limiter,
    topic: config.KAFKA_TOPIC,
    maxClockSkewMs: config.LOCATION_MAX_CLOCK_SKEW_MS,
    logger,
  });

  const sockets = attachSocketServer({
    httpServer: app.server,
    store,
    logger,
    allowedOrigins,
    sessionCookieName: cookieNames.session,
    settings: {
      minIntervalMs: config.LOCATION_MIN_INTERVAL_MS,
      heartbeatMs: config.LOCATION_HEARTBEAT_MS,
      staleFadeMs: config.LOCATION_STALE_FADE_MS,
      staleAfterMs: config.LOCATION_STALE_AFTER_MS,
      clientRemoveMs: config.LOCATION_CLIENT_REMOVE_MS,
    },
    handleLocationUpdate,
    sweepIntervalMs: deps.sweepIntervalMs ?? 5_000,
  });

  registerAuthRoutes(app, {
    store,
    oidc,
    logger,
    appBaseUrl: config.APP_BASE_URL,
    allowedOrigins,
    cookieNames,
    secureCookies: secure,
    sessionMaxAgeMs: config.SESSION_MAX_AGE_MS,
    encryptionKey: config.SESSION_ENCRYPTION_KEY,
    onSessionEnded: sockets.endSession,
  });

  let draining = false;
  app.get('/healthz', async () => ({ status: 'ok' }));
  app.get('/readyz', async (_req, reply) => {
    if (draining) return reply.code(503).send({ status: 'draining' });
    const [redisOk, kafkaOk] = await Promise.all([
      store.ping().catch(() => false),
      bus.healthCheck().catch(() => false),
    ]);
    const ready = redisOk && kafkaOk;
    return reply.code(ready ? 200 : 503).send({ status: ready ? 'ready' : 'not_ready', redisOk, kafkaOk });
  });

  const broadcaster = new Broadcaster({
    bus,
    io: sockets.io,
    topic: config.KAFKA_TOPIC,
    groupId: `${config.KAFKA_BROADCASTER_GROUP}-${config.INSTANCE_ID}`,
    flushIntervalMs: deps.flushIntervalMs ?? 150,
    logger,
  });

  return {
    app,
    io: sockets.io,
    broadcaster,
    store,
    async start(port = config.PORT, host = '0.0.0.0') {
      await broadcaster.start();
      await app.listen({ port, host });
      const address = app.server.address();
      return typeof address === 'object' && address ? address.port : port;
    },
    async stop() {
      draining = true;
      sockets.stop();
      await broadcaster.stop();
      sockets.io.emit('server:draining', {});
      await sockets.io.close(); // also closes the underlying http server
      await app.close().catch(() => undefined);
    },
  };
}
