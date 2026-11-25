# 06 · Auth & Security

## OIDC flow (BFF pattern)

```mermaid
sequenceDiagram
  autonumber
  participant B as Browser
  participant G as Gateway (BFF)
  participant R as Redis (sessions)
  participant I as IdP (Keycloak/Auth0)

  B->>G: GET /auth/login
  G->>R: store {code_verifier, state, nonce, returnTo} (pre-auth session, 10 min TTL)
  G-->>B: 302 → IdP /authorize?response_type=code&code_challenge=S256…
  B->>I: login (+ MFA if configured)
  I-->>B: 302 → /auth/callback?code&state
  B->>G: GET /auth/callback
  G->>I: POST /token (code + code_verifier + client secret)
  I-->>G: id_token, access_token, refresh_token
  G->>G: verify id_token (sig, iss, aud, exp, nonce)
  G->>R: regenerate session id; store {user:{sub,name,email}, refresh_token (encrypted), idp_session_exp}
  G-->>B: Set-Cookie __Host-sid; HttpOnly; Secure; SameSite=Lax → 302 /
  B->>G: WebSocket handshake (cookie sent automatically)
  G->>R: load session → socket.data.user
```

### Session & token rules
| Item | Decision |
|---|---|
| Cookie | `__Host-sid`, HttpOnly, Secure, SameSite=Lax, Path=/, no Domain |
| Session store | Redis, idle timeout 30 min (sliding), absolute 8 h |
| Tokens | never sent to the browser. The refresh token is stored **encrypted (AES-GCM)** in the session, only to keep the IdP session valid and support back-channel logout |
| User id | OIDC `sub` (namespaced `iss|sub` if more than one IdP is ever supported) |
| Logout | RP-initiated logout (`end_session_endpoint`, `id_token_hint`) + session destroy + `auth:expired` to every socket of the session |
| Back-channel logout *(SHOULD)* | IdP POSTs a logout token → gateway destroys matching sessions and disconnects their sockets |

### Socket authorization
- The handshake is rejected unless a valid session exists. **Origin is checked** against an allow-list (prevents cross-site WebSocket hijacking, because cookies are sent cross-site on WS upgrades).
- `userId` is **always taken from the session**; payload fields like `userId` are stripped by the Zod schema (`.strict()` rejects unknown keys).
- Every event re-checks session expiry; logout or revocation pushes `auth:expired` through the Redis adapter.

## Threat model (STRIDE, short)

| Threat | Example | Mitigation |
|---|---|---|
| **Spoofing** | sending locations as another user | identity only from the server session; payload userId rejected |
| **Tampering** | teleporting coordinates, 0,0, NaN | Zod ranges; **plausibility check** (implied speed > 300 m/s → flag/drop); server-side `receivedAt` |
| **Repudiation** | "I never sent that" | every event has eventId, userId, receivedAt, gateway pod, Kafka offset; audit log for auth events |
| **Information disclosure** | stalking via history | history API only for the owner; live view only for authenticated users; *(COULD)* visibility groups; 30-day retention; no locations in logs at INFO |
| **Denial of service** | flooding events, huge payloads, connection floods | token bucket per user + per IP at the edge; `maxHttpBufferSize` 4 KB; connection limits per IP; payload schema `.strict()` |
| **Elevation of privilege** | reaching admin endpoints | no admin surface in the app; Kafka/Redis/DB on a private network only |

## Web security baseline
- **CSP:** `default-src 'self'; img-src 'self' data: https://*.tile.openstreetmap.org; connect-src 'self' wss://<host>; script-src 'self'` (bundle Leaflet, no inline scripts).
- **Headers** via `@fastify/helmet`: HSTS, X-Content-Type-Options, Referrer-Policy, Permissions-Policy `geolocation=(self)`.
- **CSRF:** SameSite=Lax + all state-changing HTTP is POST with an Origin check (logout is POST in v1).
- **Secrets:** `.env` locally; in production Doppler / 1Password / cloud secret manager. Never committed. `gitleaks` runs in CI.
- **Dependencies:** Renovate + `pnpm audit` + GitHub Dependabot alerts; container images scanned with Trivy.
- **Transport:** TLS at the edge (Caddy auto-HTTPS). Kafka/Redis/DB use SASL/TLS when outside a private network.

## Privacy (location data is sensitive personal data)
- Explicit opt-in: sharing is **off** until the user clicks "Start sharing". A clear indicator shows while active.
- Data minimisation: store downsampled history only; drop heading and speed from history if not needed.
- Retention: 30 days of raw history (Timescale retention policy), then aggregates only.
- Right to erasure: `DELETE /api/me/history` removes that user's rows (hypertable DELETE by user_id) and produces a tombstone to the compacted `user-last-location` topic.
