// Socket.IO gateway: the ONLY entry point for browser location updates.
// It authenticates, validates, rate-limits and de-duplicates, then hands the event to Kafka.
// It deliberately does NOT broadcast directly - broadcasting happens when the event comes back
// out of Kafka (see broadcaster.js), so Kafka is the single source of truth for every update.
import { config } from '../config.js';
import { createLogger } from '../logger.js';
import { validateClientLocation, buildLocationEvent, buildOfflineEvent } from '../events/location-event.js';
import { RecentIds } from '../events/recent-ids.js';

const log = createLogger('gateway');
const OFFLINE_GRACE_MS = 5_000; // survive a page reload / brief network blip without the marker flickering

export const AUTHENTICATED_ROOM = 'authenticated';
const userRoom = (userId) => `user:${userId}`;

export function attachSocketGateway({ io, producer, presence, sessionMiddleware }) {
  const recentIds = new RecentIds(200);
  const lastAcceptedAt = new Map(); // userId -> ms, per USER (not per socket) so many tabs can't bypass it
  const pendingOffline = new Map(); // userId -> timeout

  // Run express-session on the handshake request so socket.request.session is populated from the cookie.
  io.engine.use(sessionMiddleware);

  // Reject the connection outright unless the session holds an OIDC-authenticated user.
  io.use((socket, next) => {
    const session = socket.request.session;
    if (!session?.user) {
      log.warn('socket rejected: no authenticated session', { socketId: socket.id });
      return next(new Error('unauthorized'));
    }
    socket.data.user = session.user;
    socket.data.sessionExpiresAt = new Date(session.cookie.expires ?? Date.now() + config.session.maxAgeMs).getTime();
    next();
  });

  const publishOffline = async (user, reason) => {
    try {
      await producer.publish(buildOfflineEvent(user, reason));
      log.info('user offline published', { userId: user.id, reason });
    } catch (err) {
      // Not fatal: the stale sweep removes the user from the map anyway.
      log.warn('failed to publish offline event', { userId: user.id, error: err.message });
    }
  };

  io.on('connection', (socket) => {
    const user = socket.data.user;
    socket.join([AUTHENTICATED_ROOM, userRoom(user.id)]);
    clearTimeout(pendingOffline.get(user.id));
    pendingOffline.delete(user.id);
    log.info('socket connected', { userId: user.id, socketId: socket.id });

    socket.emit('session:ready', {
      user,
      settings: { minIntervalMs: config.location.minIntervalMs, staleAfterMs: config.location.staleAfterMs },
    });
    socket.emit('presence:snapshot', presence.snapshot());

    const sessionExpired = () => {
      if (Date.now() < socket.data.sessionExpiresAt) return false;
      socket.emit('auth:expired');
      socket.disconnect(true);
      return true;
    };

    socket.on('location:update', async (payload, ack) => {
      const reply = typeof ack === 'function' ? ack : (res) => !res.ok && socket.emit('location:error', res);
      if (sessionExpired()) return reply({ ok: false, error: 'session expired' });

      // 1. Validate shape and ranges. User id comes from the session, never from the payload.
      const result = validateClientLocation(payload, { maxClockSkewMs: config.location.maxClockSkewMs });
      if (!result.ok) {
        log.warn('invalid location rejected', { userId: user.id, error: result.error });
        return reply({ ok: false, error: result.error });
      }
      const location = result.value;

      // 2. Duplicate? (client re-sent the same eventId because an ack got lost)
      if (recentIds.seen(user.id, location.eventId)) {
        return reply({ ok: true, duplicate: true, eventId: location.eventId });
      }

      // 3. Rate limit per user.
      const now = Date.now();
      const previous = lastAcceptedAt.get(user.id) ?? 0;
      if (now - previous < config.location.minIntervalMs) {
        recentIds.release(user.id, location.eventId);
        return reply({ ok: false, error: 'rate limited', retryAfterMs: config.location.minIntervalMs - (now - previous) });
      }
      lastAcceptedAt.set(user.id, now);

      // 4. Publish to Kafka. The ack tells the browser where the event was stored.
      const event = buildLocationEvent(location, user, { gatewayInstance: config.instanceId, now });
      try {
        const { partition, offset } = await producer.publish(event);
        socket.data.hasShared = true;
        reply({ ok: true, eventId: event.eventId, partition, offset });
      } catch (err) {
        recentIds.release(user.id, location.eventId);
        lastAcceptedAt.set(user.id, previous);
        log.error('kafka publish failed', { userId: user.id, error: err.message });
        reply({ ok: false, error: 'location service temporarily unavailable', retryable: true });
      }
    });

    socket.on('location:stop', async () => {
      if (!socket.data.hasShared) return;
      socket.data.hasShared = false;
      await publishOffline(user, 'stopped-sharing');
    });

    socket.on('disconnect', (reason) => {
      log.info('socket disconnected', { userId: user.id, socketId: socket.id, reason });
      if (!socket.data.hasShared) return;

      // Only mark the user offline if they have no other tab open after a short grace period.
      clearTimeout(pendingOffline.get(user.id));
      pendingOffline.set(
        user.id,
        setTimeout(async () => {
          pendingOffline.delete(user.id);
          const remaining = await io.in(userRoom(user.id)).fetchSockets();
          if (remaining.length === 0) {
            recentIds.forget(user.id);
            lastAcceptedAt.delete(user.id);
            await publishOffline(user, 'disconnected');
          }
        }, OFFLINE_GRACE_MS),
      );
    });
  });

  return {
    clearTimers() {
      for (const t of pendingOffline.values()) clearTimeout(t);
    },
  };
}
