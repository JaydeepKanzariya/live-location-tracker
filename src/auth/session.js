// One session middleware instance shared by Express (HTTP) AND Socket.IO (WebSocket handshake).
// That sharing is what links a socket to the logged-in OIDC user.
import session from 'express-session';
import { config } from '../config.js';

export const sessionMiddleware = session({
  name: 'llt.sid',
  secret: config.session.secret,
  resave: false,
  saveUninitialized: false,
  rolling: false,
  // NOTE: default MemoryStore - fine for one instance / a demo. With several gateway instances use a
  // shared store (connect-redis, connect-pg-simple) so any instance can read any session.
  cookie: {
    httpOnly: true, // JS in the page cannot read the session cookie
    sameSite: 'lax', // sent on the top-level redirect back from the identity provider
    secure: config.isProduction,
    maxAge: config.session.maxAgeMs,
  },
});

/** Express guard for JSON APIs. */
export function requireAuth(req, res, next) {
  if (req.session?.user) return next();
  res.status(401).json({ error: 'not authenticated' });
}
