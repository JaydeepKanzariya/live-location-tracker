// OpenID Connect login using the Authorization Code flow + PKCE (openid-client v6).
// Works with any OIDC provider: Keycloak (docker-compose default), Auth0, Google, Okta, Entra ID...
import { Router } from 'express';
import * as oidc from 'openid-client';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('auth');
const redirectUri = new URL('/auth/callback', config.appBaseUrl).href;

// Discovery (fetching /.well-known/openid-configuration) is done lazily and cached.
// If the provider isn't up yet (Keycloak boots slowly) we simply retry on the next login.
let discoveryPromise;
function getOidcConfig() {
  if (!discoveryPromise) {
    const options = config.oidc.allowInsecureIssuer ? { execute: [oidc.allowInsecureRequests] } : undefined;
    discoveryPromise = oidc
      .discovery(new URL(config.oidc.issuerUrl), config.oidc.clientId, config.oidc.clientSecret, undefined, options)
      .then((cfg) => {
        log.info('OIDC provider discovered', { issuer: cfg.serverMetadata().issuer });
        return cfg;
      })
      .catch((err) => {
        discoveryPromise = undefined;
        throw err;
      });
  }
  return discoveryPromise;
}

/** Map ID-token claims to the user object we keep in the session. `sub` is the stable user id. */
function userFromClaims(claims) {
  return {
    id: claims.sub,
    name: claims.name || claims.preferred_username || claims.email || claims.sub,
    email: claims.email ?? null,
    picture: claims.picture ?? null,
  };
}

// Promisified session helpers.
const regenerate = (req) => new Promise((resolve, reject) => req.session.regenerate((e) => (e ? reject(e) : resolve())));
const save = (req) => new Promise((resolve, reject) => req.session.save((e) => (e ? reject(e) : resolve())));
const destroy = (req) => new Promise((resolve) => req.session.destroy(() => resolve()));

export function createAuthRouter() {
  const router = Router();

  // Step 1: send the browser to the identity provider.
  router.get('/login', async (req, res, next) => {
    try {
      const cfg = await getOidcConfig();
      const codeVerifier = oidc.randomPKCECodeVerifier();
      const codeChallenge = await oidc.calculatePKCECodeChallenge(codeVerifier);
      const state = oidc.randomState();
      const nonce = oidc.randomNonce();

      // Remember the one-time values so the callback can prove the response belongs to this browser.
      req.session.oidc = { codeVerifier, state, nonce };
      await save(req);

      const url = oidc.buildAuthorizationUrl(cfg, {
        redirect_uri: redirectUri,
        scope: config.oidc.scopes,
        code_challenge: codeChallenge,
        code_challenge_method: 'S256',
        state,
        nonce,
      });
      res.redirect(url.href);
    } catch (err) {
      log.error('login redirect failed', { error: err.message });
      next(new Error('Identity provider is not reachable. Is Keycloak running? (docker compose up -d)'));
    }
  });

  // Step 2: provider redirects back with ?code=...&state=... ; exchange the code for tokens.
  router.get('/callback', async (req, res) => {
    const pending = req.session.oidc;
    if (!pending) return res.redirect('/?error=login_expired');

    try {
      const cfg = await getOidcConfig();
      const currentUrl = new URL(req.originalUrl, config.appBaseUrl);
      const tokens = await oidc.authorizationCodeGrant(cfg, currentUrl, {
        pkceCodeVerifier: pending.codeVerifier,
        expectedState: pending.state,
        expectedNonce: pending.nonce,
      });
      const claims = tokens.claims(); // ID token is signature- and audience-verified by openid-client
      const user = userFromClaims(claims);

      // New session id after login prevents session fixation.
      await regenerate(req);
      req.session.user = user;
      // We only need the ID token later, as `id_token_hint` for logout. Access/refresh tokens are not
      // stored: this app never calls a downstream API on the user's behalf.
      req.session.idToken = tokens.id_token;
      await save(req);

      log.info('user logged in', { userId: user.id, name: user.name });
      res.redirect('/');
    } catch (err) {
      log.warn('OIDC callback failed', { error: err.message });
      delete req.session.oidc;
      res.redirect('/?error=login_failed');
    }
  });

  // RP-initiated logout: clear our session AND the provider's SSO session (when supported).
  router.get('/logout', async (req, res) => {
    const idToken = req.session.idToken;
    const userId = req.session.user?.id;
    await destroy(req);
    res.clearCookie('llt.sid');
    if (userId) log.info('user logged out', { userId });

    try {
      const cfg = await getOidcConfig();
      if (cfg.serverMetadata().end_session_endpoint) {
        const url = oidc.buildEndSessionUrl(cfg, {
          post_logout_redirect_uri: config.appBaseUrl,
          ...(idToken ? { id_token_hint: idToken } : {}),
        });
        return res.redirect(url.href);
      }
    } catch (err) {
      log.warn('could not build provider logout url', { error: err.message });
    }
    res.redirect('/');
  });

  return router;
}
