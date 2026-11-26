import { SessionUserSchema, type SessionUser } from '@llt/contracts';
import * as oidc from 'openid-client';

export interface LoginStart {
  url: string;
  state: string;
  codeVerifier: string;
  nonce: string;
}

export interface LoginResult {
  user: SessionUser;
  idToken: string | null;
  refreshToken: string | null;
}

/** What the HTTP routes need from an identity provider. Tests plug in a fake. */
export interface OidcProvider {
  startLogin(): Promise<LoginStart>;
  finishLogin(
    callbackUrl: URL,
    expected: { state: string; codeVerifier: string; nonce: string },
  ): Promise<LoginResult>;
  /** RP-initiated logout URL, or null when the provider has no end_session_endpoint. */
  logoutUrl(idToken: string | null): Promise<string | null>;
}

export interface OidcOptions {
  issuerUrl: string;
  clientId: string;
  clientSecret: string;
  scopes: string;
  redirectUri: string;
  postLogoutRedirectUri: string;
  allowInsecure: boolean;
}

const MAX_NAME = 80;

/** Maps verified ID-token claims to the session user. `sub` is the stable user id. */
export function userFromClaims(claims: {
  sub: string;
  name?: unknown;
  preferred_username?: unknown;
  email?: unknown;
}): SessionUser {
  const pick = (v: unknown): string | undefined =>
    typeof v === 'string' && v.trim() !== '' ? v.trim() : undefined;
  const displayName = (
    pick(claims.name) ??
    pick(claims.preferred_username) ??
    pick(claims.email) ??
    claims.sub
  ).slice(0, MAX_NAME);

  const email = pick(claims.email);
  const withEmail = SessionUserSchema.safeParse({ sub: claims.sub, displayName, email });
  if (withEmail.success) return withEmail.data;
  // An odd email claim must not block login: drop it and keep the rest.
  return SessionUserSchema.parse({ sub: claims.sub, displayName });
}

export class OpenIdClientProvider implements OidcProvider {
  private discovery: Promise<oidc.Configuration> | undefined;

  constructor(private readonly options: OidcOptions) {}

  // Fetched lazily and cached. If the IdP is not up yet (Keycloak boots slowly) the next
  // login simply tries again.
  private config(): Promise<oidc.Configuration> {
    if (!this.discovery) {
      const { issuerUrl, clientId, clientSecret, allowInsecure } = this.options;
      this.discovery = oidc
        .discovery(
          new URL(issuerUrl),
          clientId,
          clientSecret,
          undefined,
          allowInsecure ? { execute: [oidc.allowInsecureRequests] } : undefined,
        )
        .catch((err: unknown) => {
          this.discovery = undefined;
          throw err;
        });
    }
    return this.discovery;
  }

  async startLogin(): Promise<LoginStart> {
    const cfg = await this.config();
    const codeVerifier = oidc.randomPKCECodeVerifier();
    const codeChallenge = await oidc.calculatePKCECodeChallenge(codeVerifier);
    const state = oidc.randomState();
    const nonce = oidc.randomNonce();
    const url = oidc.buildAuthorizationUrl(cfg, {
      redirect_uri: this.options.redirectUri,
      scope: this.options.scopes,
      code_challenge: codeChallenge,
      code_challenge_method: 'S256',
      state,
      nonce,
    });
    return { url: url.href, state, codeVerifier, nonce };
  }

  async finishLogin(
    callbackUrl: URL,
    expected: { state: string; codeVerifier: string; nonce: string },
  ): Promise<LoginResult> {
    const cfg = await this.config();
    // Verifies state, PKCE, and the ID token's signature, issuer, audience, expiry and nonce.
    const tokens = await oidc.authorizationCodeGrant(cfg, callbackUrl, {
      pkceCodeVerifier: expected.codeVerifier,
      expectedState: expected.state,
      expectedNonce: expected.nonce,
    });
    const claims = tokens.claims();
    if (!claims) throw new Error('identity provider returned no ID token');
    return {
      user: userFromClaims(claims),
      idToken: tokens.id_token ?? null,
      refreshToken: tokens.refresh_token ?? null,
    };
  }

  async logoutUrl(idToken: string | null): Promise<string | null> {
    const cfg = await this.config();
    if (!cfg.serverMetadata().end_session_endpoint) return null;
    const url = oidc.buildEndSessionUrl(cfg, {
      post_logout_redirect_uri: this.options.postLogoutRedirectUri,
      ...(idToken ? { id_token_hint: idToken } : {}),
    });
    return url.href;
  }
}
