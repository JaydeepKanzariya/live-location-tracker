import type { Server as HttpServer } from 'node:http';
import {
  type ClientToServerEvents,
  type LocationUpdateAck,
  type ServerToClientEvents,
  type SessionSettings,
  type SessionUser,
} from '@llt/contracts';
import { hashUserId } from '@llt/observability';
import { parse as parseCookie } from 'cookie';
import type { Logger } from 'pino';
import { Server, type Socket } from 'socket.io';
import { isAllowedOrigin } from '../http/origin.js';
import type { SessionStore } from '../session/session-store.js';

export interface SocketData {
  user: SessionUser;
  sessionId: string;
}

export type GatewayServer = Server<ClientToServerEvents, ServerToClientEvents, object, SocketData>;
type GatewaySocket = Socket<ClientToServerEvents, ServerToClientEvents, object, SocketData>;

export const AUTHENTICATED_ROOM = 'authenticated';

export interface SocketServerDeps {
  httpServer: HttpServer;
  store: SessionStore;
  logger: Logger;
  allowedOrigins: Set<string>;
  sessionCookieName: string;
  settings: SessionSettings;
  /** The location:update pipeline (validation, dedupe, rate limit, produce). */
  handleLocationUpdate: (user: SessionUser, payload: unknown) => Promise<LocationUpdateAck>;
  /** How often connected sockets are re-checked for an expired session. */
  sweepIntervalMs: number;
}

export function attachSocketServer(deps: SocketServerDeps): {
  io: GatewayServer;
  /** Tells every socket of one session that it is over, then closes them (logout). */
  endSession: (roomId: string) => void;
  stop: () => void;
} {
  const { store, logger } = deps;

  const io: GatewayServer = new Server(deps.httpServer, {
    // The biggest valid update is a few hundred bytes. This closes the socket on anything larger.
    maxHttpBufferSize: 4096,
    transports: ['websocket', 'polling'],
    // Browsers send cookies on cross-site WebSocket upgrades, so we check Origin ourselves.
    allowRequest: (req, callback) => {
      const ok = isAllowedOrigin(req.headers.origin, deps.allowedOrigins);
      if (!ok) logger.warn({ origin: req.headers.origin ?? null }, 'socket refused: bad origin');
      callback(ok ? null : 'origin not allowed', ok);
    },
    cors: { origin: [...deps.allowedOrigins], credentials: true },
  });

  const expire = (socket: GatewaySocket) => {
    socket.emit('auth:expired', {});
    socket.disconnect(true);
  };

  // Handshake auth: the cookie must name a live session, or the connection is refused.
  io.use(async (socket, next) => {
    try {
      const cookies = parseCookie(socket.handshake.headers.cookie ?? '');
      const sessionId = cookies[deps.sessionCookieName];
      const record = sessionId ? await store.get(sessionId) : null;
      if (!sessionId || !record) return next(new Error('unauthorized'));
      // Identity comes from the server-side session and nowhere else.
      socket.data.user = record.user;
      socket.data.sessionId = sessionId;
      next();
    } catch (err) {
      logger.error({ err: err instanceof Error ? err.message : String(err) }, 'handshake failed');
      next(new Error('unavailable'));
    }
  });

  io.on('connection', (socket) => {
    const { user, sessionId } = socket.data;
    void socket.join([AUTHENTICATED_ROOM, `user:${user.sub}`, `sid:${store.roomId(sessionId)}`]);
    logger.info({ user: hashUserId(user.sub) }, 'socket connected');

    socket.emit('session:ready', { user, settings: deps.settings });
    // Redis-backed presence arrives with the presence-projector (Phase 2); until then the
    // snapshot is empty and clients rely on location:batch.
    socket.emit('presence:snapshot', []);

    socket.on('location:update', async (payload, ack) => {
      const reply = typeof ack === 'function' ? ack : () => undefined;
      try {
        // Every event re-checks the session (slides the idle timeout while the user is active).
        if (!(await store.get(socket.data.sessionId))) {
          reply({ ok: false, code: 'SESSION_EXPIRED' });
          expire(socket);
          return;
        }
        reply(await deps.handleLocationUpdate(socket.data.user, payload));
      } catch (err) {
        logger.error({ err: err instanceof Error ? err.message : String(err) }, 'update crashed');
        reply({ ok: false, code: 'UNAVAILABLE' });
      }
    });

    socket.on('disconnect', () => {
      logger.debug({ user: hashUserId(user.sub) }, 'socket disconnected');
    });
  });

  // Sessions can end while a socket is idle (8 h limit, logout elsewhere, Redis flush).
  // `peek` is used on purpose: the sweep must not keep idle sessions alive.
  let sweeping = false;
  const sweep = setInterval(() => {
    if (sweeping) return;
    sweeping = true;
    void (async () => {
      try {
        const bySession = new Map<string, GatewaySocket[]>();
        for (const socket of io.sockets.sockets.values()) {
          const list = bySession.get(socket.data.sessionId) ?? [];
          list.push(socket);
          bySession.set(socket.data.sessionId, list);
        }
        for (const [sessionId, sockets] of bySession) {
          if (!(await store.peek(sessionId))) sockets.forEach(expire);
        }
      } catch (err) {
        logger.error({ err: err instanceof Error ? err.message : String(err) }, 'sweep failed');
      } finally {
        sweeping = false;
      }
    })();
  }, deps.sweepIntervalMs);
  sweep.unref();

  return {
    io,
    endSession: (roomId) => {
      const room = `sid:${roomId}`;
      io.to(room).emit('auth:expired', {});
      io.in(room).disconnectSockets(true);
    },
    stop: () => clearInterval(sweep),
  };
}
