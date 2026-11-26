import { hashUserId } from '@llt/observability';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { Logger } from 'pino';
import { encryptSecret } from '../crypto.js';
import { isAllowedOrigin, safeReturnTo } from '../http/origin.js';
import type { SessionRecord, SessionStore } from '../session/session-store.js';
import type { OidcProvider } from './oidc.js';

export interface CookieNames {
  session: string;
  loginState: string;
}

export interface AuthRoutesDeps {
  store: SessionStore;
  oidc: OidcProvider;
  logger: Logger;
  appBaseUrl: string;
  allowedOrigins: Set<string>;
  cookieNames: CookieNames;
  secureCookies: boolean;
  sessionMaxAgeMs: number;
  encryptionKey: string;
  /** Called after logout so the socket layer can tell that session's sockets and close them. */
  onSessionEnded: (roomId: string) => void;
}

/** Reads the session for an HTTP request, or null. Slides the idle timeout. */
export async function sessionFromRequest(
  request: FastifyRequest,
  store: SessionStore,
  cookieName: string,
): Promise<{ id: string; record: SessionRecord } | null> {
  const id = request.cookies[cookieName];
  if (!id) return null;
  const record = await store.get(id);
  return record ? { id, record } : null;
}

export function registerAuthRoutes(app: FastifyInstance, deps: AuthRoutesDeps): void {
  const { store, oidc, logger, cookieNames } = deps;

  const baseCookie = {
    httpOnly: true, // page JavaScript cannot read it
    secure: deps.secureCookies,
    sameSite: 'lax' as const, // still sent on the top-level redirect back from the IdP
    path: '/', // and no Domain attribute: required for the __Host- prefix
  };

  // Step 1: send the browser to the identity provider.
  app.get('/auth/login', async (request: FastifyRequest, reply: FastifyReply) => {
    const { returnTo } = request.query as { returnTo?: string };
    try {
      const login = await oidc.startLogin();
      await store.savePending(login.state, {
        codeVerifier: login.codeVerifier,
        nonce: login.nonce,
        returnTo: safeReturnTo(returnTo),
      });
      // Binds this login attempt to THIS browser. Without it, an attacker could start a login
      // themselves and trick a victim into finishing it ("login CSRF").
      reply.setCookie(cookieNames.loginState, login.state, { ...baseCookie, maxAge: 600 });
      return reply.redirect(login.url);
    } catch (err) {
      logger.error({ err: errMessage(err) }, 'could not start login');
      return reply.code(503).send({ error: 'identity_provider_unavailable' });
    }
  });

  // Step 2: the IdP redirects back with ?code=...&state=...
  app.get('/auth/callback', async (request: FastifyRequest, reply: FastifyReply) => {
    const fail = (reason: string) => {
      reply.clearCookie(cookieNames.loginState, baseCookie);
      return reply.redirect(`/?error=${reason}`);
    };

    const query = request.query as { state?: string; error?: string };
    if (query.error || !query.state) return fail('login_failed');
    if (request.cookies[cookieNames.loginState] !== query.state) return fail('login_failed');

    const pending = await store.takePending(query.state); // one-time use
    if (!pending) return fail('login_expired');

    try {
      const callbackUrl = new URL(request.url, deps.appBaseUrl);
      const result = await oidc.finishLogin(callbackUrl, {
        state: query.state,
        codeVerifier: pending.codeVerifier,
        nonce: pending.nonce,
      });

      // A brand-new session id is issued here, after login (prevents session fixation).
      const sessionId = await store.create({
        user: result.user,
        idToken: result.idToken,
        refreshTokenEnc: result.refreshToken
          ? encryptSecret(result.refreshToken, deps.encryptionKey)
          : null,
      });
      reply.clearCookie(cookieNames.loginState, baseCookie);
      reply.setCookie(cookieNames.session, sessionId, {
        ...baseCookie,
        maxAge: Math.floor(deps.sessionMaxAgeMs / 1000),
      });
      logger.info({ user: hashUserId(result.user.sub) }, 'user logged in');
      return reply.redirect(pending.returnTo);
    } catch (err) {
      logger.warn({ err: errMessage(err) }, 'login callback failed');
      return fail('login_failed');
    }
  });

  // Who am I? 401 when there is no valid session. The web app uses this to decide what to show.
  app.get('/auth/me', async (request: FastifyRequest, reply: FastifyReply) => {
    const session = await sessionFromRequest(request, store, cookieNames.session);
    if (!session) return reply.code(401).send({ error: 'unauthenticated' });
    reply.header('Cache-Control', 'no-store');
    return { user: session.record.user };
  });

  // POST + Origin check: a cross-site page cannot log users out (CSRF).
  app.post('/auth/logout', async (request: FastifyRequest, reply: FastifyReply) => {
    if (!isAllowedOrigin(request.headers.origin, deps.allowedOrigins)) {
      return reply.code(403).send({ error: 'forbidden_origin' });
    }
    const id = request.cookies[cookieNames.session];
    let idToken: string | null = null;
    if (id) {
      const record = await store.peek(id);
      idToken = record?.idToken ?? null;
      await store.destroy(id);
      deps.onSessionEnded(store.roomId(id));
      if (record) logger.info({ user: hashUserId(record.user.sub) }, 'user logged out');
    }
    reply.clearCookie(cookieNames.session, baseCookie);

    let redirectTo = '/';
    try {
      redirectTo = (await oidc.logoutUrl(idToken)) ?? '/';
    } catch (err) {
      logger.warn({ err: errMessage(err) }, 'could not build provider logout url');
    }
    return { redirectTo };
  });
}

function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
