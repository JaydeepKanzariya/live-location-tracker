// Gateway process: HTTP (OIDC login, static frontend, small JSON API) + Socket.IO + Kafka producer
// + Kafka broadcaster consumer.
//
//   npm run dev
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { Server as SocketIOServer } from 'socket.io';
import { config } from './config.js';
import { createLogger } from './logger.js';
import { sessionMiddleware, requireAuth } from './auth/session.js';
import { createAuthRouter } from './auth/oidc.js';
import { ensureTopic } from './kafka/client.js';
import { createLocationProducer } from './kafka/producer.js';
import { PresenceStore } from './realtime/presence.js';
import { attachSocketGateway } from './realtime/socket-gateway.js';
import { createBroadcaster } from './realtime/broadcaster.js';
import { getPool, closePool } from './db/pool.js';
import { getUserHistory } from './db/history-repo.js';

const log = createLogger('server');
const publicDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public');

const producer = createLocationProducer();
const presence = new PresenceStore();

// ---------- HTTP ----------
const app = express();
if (config.isProduction) app.set('trust proxy', 1); // secure cookies behind Render/Railway/NGINX TLS
app.disable('x-powered-by');
app.use(sessionMiddleware);

app.use('/auth', createAuthRouter());

app.get('/api/me', requireAuth, (req, res) => res.json({ user: req.session.user }));

app.get('/api/history', requireAuth, async (req, res, next) => {
  if (!getPool()) return res.json({ enabled: false, points: [] });
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 200, 1), 1000);
    const points = await getUserHistory(req.session.user.id, limit);
    res.json({ enabled: true, points });
  } catch (err) {
    next(err);
  }
});

app.get('/healthz', (req, res) => {
  const healthy = producer.isConnected();
  res.status(healthy ? 200 : 503).json({
    ok: healthy,
    instance: config.instanceId,
    kafkaProducer: healthy ? 'connected' : 'disconnected',
    usersOnMap: presence.users.size,
  });
});

app.use(express.static(publicDir));

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  log.error('request failed', { path: req.path, error: err.message });
  if (req.path.startsWith('/api/')) return res.status(500).json({ error: 'internal error' });
  res.status(500).send(`<p>${err.message.replace(/[<>&]/g, '')}</p><p><a href="/">Back</a></p>`);
});

// ---------- Socket.IO ----------
const server = http.createServer(app);
const io = new SocketIOServer(server, {
  // Same-origin by default. Browsers send the session cookie on the handshake automatically.
  pingInterval: 10_000,
  pingTimeout: 5_000,
  maxHttpBufferSize: 10_000, // a location payload is ~200 bytes; refuse anything absurd
});
const gateway = attachSocketGateway({ io, producer, presence, sessionMiddleware });
const broadcaster = createBroadcaster({ io, presence });

// ---------- Startup / shutdown ----------
async function main() {
  await ensureTopic();
  await producer.connect();
  await broadcaster.start();
  server.listen(config.port, () => {
    log.info('gateway listening', { url: config.appBaseUrl, instance: config.instanceId });
  });
}

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info('shutting down', { signal });
  gateway.clearTimers();
  io.close();
  await Promise.allSettled([broadcaster.stop(), producer.disconnect(), closePool()]);
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  log.error('failed to start - is Kafka running? (docker compose up -d)', { error: err.message });
  process.exit(1);
});
