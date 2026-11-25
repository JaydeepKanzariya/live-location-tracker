// Runs the real Socket.IO gateway + real express-session, with an in-memory stand-in for the
// Kafka producer. Verifies auth on the handshake, validation, dedupe, rate limiting and offline events.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { Server } from 'socket.io';
import { io as connect } from 'socket.io-client';
import { sessionMiddleware } from '../src/auth/session.js';
import { attachSocketGateway } from '../src/realtime/socket-gateway.js';
import { PresenceStore } from '../src/realtime/presence.js';

const published = [];
const fakeProducer = {
  async publish(event) {
    published.push(event);
    return { partition: 0, offset: String(published.length - 1) };
  },
};

let server;
let ioServer;
let baseUrl;

before(async () => {
  const app = express();
  app.use(sessionMiddleware);
  // Test-only stand-in for /auth/callback: puts an "OIDC user" into the session.
  app.get('/test-login/:sub', (req, res) => {
    req.session.user = { id: req.params.sub, name: `User ${req.params.sub}`, email: null };
    req.session.save(() => res.sendStatus(204));
  });
  server = http.createServer(app);
  ioServer = new Server(server);
  attachSocketGateway({ io: ioServer, producer: fakeProducer, presence: new PresenceStore(), sessionMiddleware });
  await new Promise((r) => server.listen(0, r));
  baseUrl = `http://localhost:${server.address().port}`;
});

after(async () => {
  ioServer.close();
  await new Promise((r) => server.close(r));
});

async function login(sub) {
  const res = await fetch(`${baseUrl}/test-login/${sub}`);
  return res.headers.get('set-cookie').split(';')[0];
}

function openSocket(cookie) {
  return new Promise((resolve, reject) => {
    const socket = connect(baseUrl, { transports: ['websocket'], extraHeaders: cookie ? { cookie } : {}, reconnection: false });
    socket.once('session:ready', (data) => resolve({ socket, data }));
    socket.once('connect_error', (err) => reject(err));
  });
}

const location = (extra = {}) => ({ eventId: randomUUID(), lat: 19.07, lng: 72.87, accuracy: 10, clientTs: Date.now(), ...extra });

test('socket without a session is rejected', async () => {
  await assert.rejects(openSocket(null), /unauthorized/);
});

test('authenticated socket publishes events tagged with the session user id', async () => {
  const cookie = await login('alice-sub');
  const { socket, data } = await openSocket(cookie);
  assert.equal(data.user.id, 'alice-sub');

  const payload = location({ userId: 'spoofed' });
  const ack = await socket.emitWithAck('location:update', payload);
  assert.equal(ack.ok, true);
  assert.equal(published.at(-1).userId, 'alice-sub', 'user id comes from the session, not the payload');

  // Same eventId again -> duplicate, not published twice.
  const before = published.length;
  const dup = await socket.emitWithAck('location:update', payload);
  assert.deepEqual([dup.ok, dup.duplicate], [true, true]);
  assert.equal(published.length, before);

  // New event immediately -> rate limited.
  const limited = await socket.emitWithAck('location:update', location());
  assert.equal(limited.ok, false);
  assert.match(limited.error, /rate limited/);

  // Invalid data -> rejected with a reason, nothing published.
  const bad = await socket.emitWithAck('location:update', location({ lat: 500 }));
  assert.equal(bad.ok, false);
  assert.equal(published.length, before);

  // Stop sharing -> offline event.
  socket.emit('location:stop');
  await new Promise((r) => setTimeout(r, 100));
  assert.equal(published.at(-1).type, 'user.offline');
  assert.equal(published.at(-1).reason, 'stopped-sharing');
  socket.disconnect();
});
