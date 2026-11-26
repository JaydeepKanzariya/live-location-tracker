# Live Location Tracker v1 — Status

## Current Phase
**Phase 0 (Foundations): DONE 2026-11-26.** Reviewed, review fixes applied, CI green on GitHub (run 45692142195, commit `6787cb6`): lint, typecheck, build, unit tests, prototype tests, and the infrastructure smoke test.
**Phase 1 (Auth + gateway): BUILT 2026-11-26, READY FOR REVIEW** on branch `phase-1/auth-gateway` (pull request #1 into `main`). CI runs on every push to the PR. Merge only after the Phase 1 review. Docker now runs locally, so Phase 1 integration tests can run here as well as in CI.

---

## Phase 1: Auth + gateway (built 2026-11-26)

### What was built (`apps/gateway`)
- **OIDC BFF login (Keycloak):** `GET /auth/login` (PKCE S256, state, nonce), `GET /auth/callback`, `GET /auth/me`, `POST /auth/logout` (Origin-checked, returns the IdP end-session URL). The pending login is stored in Redis for one use, and bound to the browser by a short-lived state cookie (stops login CSRF). `returnTo` only accepts same-site paths.
- **Sessions:** Redis, opaque 256-bit id in an HttpOnly, SameSite=Lax cookie (`__Host-sid` in production, `sid` on http dev). Redis keys use `HMAC(secret, id)`, so a Redis dump holds no usable cookies. 30 min sliding idle timeout, 8 h absolute limit. A new id is issued at login (no fixation). The refresh token is stored AES-256-GCM encrypted; tokens never reach the browser.
- **Socket.IO:** handshake needs a live session and an allowed `Origin` (a missing Origin is refused). `userId` always comes from the session. Every event re-checks the session; a periodic sweep (uses `peek`, so it does not keep idle sessions alive) sends `auth:expired` and closes expired sockets. `maxHttpBufferSize` is 4 KB.
- **`location:update` pipeline:** Zod strict parse, clock-skew check, Redis dedupe (`SET NX EX`), Redis Lua token bucket (1/s, burst 3, Redis server clock), enrich from session, idempotent produce, ack with partition and offset.
- **Broadcaster:** consumer group `broadcaster-<INSTANCE_ID>` (every pod sees every event), keeps the newest entry per user, sends one `location:batch` to the `authenticated` room every 150 ms. Poison records go to the DLQ topic.
- **Also:** `/healthz`, `/readyz` (Redis + Kafka, 503 while draining), helmet headers, `Permissions-Policy: geolocation=(self)`, graceful `stop()` (SIGTERM/SIGINT in `index.ts`).

### Phase 1 exit checks and the test that proves each
| Check from BUILD-PROMPT | Test |
|---|---|
| Unauthenticated socket rejected | `gateway.integration.test.ts` (no cookie, fake cookie, bad Origin, missing Origin) |
| Spoofed `userId` ignored | `pipeline.test.ts`, `gateway.integration.test.ts`, and on real Kafka in `stack.integration.test.ts` |
| Duplicate `eventId` gives one Kafka record | in-memory bus + 4-socket race with real Redis; on the **real Kafka topic** in `stack.integration.test.ts` |
| Rate limit | `pipeline.test.ts`, `gateway.integration.test.ts` (5 sent, 3 accepted), `session-store.integration.test.ts` (atomic under 50 concurrent calls) |
| Invalid payloads | table of 9 cases in `pipeline.test.ts`, 5 over a real socket, plus a 10 KB payload that closes the socket |
| Session expiry | on event, on sweep, on handshake (idle timeout), 8 h absolute limit, logout closing sockets |
| Real login through Keycloak | `stack.integration.test.ts`: headless login (PKCE), wrong password, logout also ends the Keycloak SSO session |

### How to run and verify this phase
```bash
npm install
npm run v1:build && npm run v1:typecheck && npm run v1:lint
npm run v1:test                                   # unit tests (no Docker): 62 in the gateway
npm run test:integration -w @llt/gateway          # Testcontainers: starts a throwaway Redis (needs Docker)
npm run v1:infra:up && npm run v1:infra:smoke     # start and check Kafka, Keycloak, Redis, Postgres
npm run test:stack -w @llt/gateway                # real Keycloak + real Kafka (needs the stack above)
node apps/gateway/dist/index.js                   # run the gateway on :3000 (loads .env if present)
```
Manual browser check: with the stack and the gateway running, open `http://localhost:3000/auth/login`, sign in as `alice` / `alice`, then open `http://localhost:3000/auth/me`. It should show alice's id and name as JSON (there is no web page until Phase 3, so `/` itself is a 404 after login). `POST /auth/logout` needs an `Origin` header, so test it with the integration test, not the address bar.

### Verified on 2026-11-26 (Windows 11, Node 24, Docker Desktop)
- `v1:build` 9/9, `v1:typecheck` 13/13, `v1:lint` clean, `v1:test` all packages pass (gateway 62, contracts 18, config 11, kafka 12, observability 4), Prettier clean on `apps/gateway`.
- `test:integration`: 42/42 (real Redis). `test:stack`: 6/6 (real Keycloak 26 and Kafka 4.1 with the **Confluent driver**; its logs show consumers joining and rebalancing).
- The built gateway process was started against the local stack: `/healthz` 200, `/readyz` 200 with `redisOk` and `kafkaOk` true, `/auth/login` 302 to Keycloak, `/auth/me` 401 without a session.
- CI: new job `gateway-integration` runs the integration and full-stack tests on every push. **It has not run on GitHub yet** (see below).

### Files that matter most
`src/location/pipeline.ts` (the update pipeline), `src/auth/routes.ts` and `src/auth/oidc.ts` (login), `src/session/session-store.ts`, `src/socket/socket-server.ts`, `src/broadcaster.ts`, `src/ratelimit/token-bucket.ts` (Lua), `src/app.ts` (wiring), `src/integration/*.test.ts`.

### Decisions and flags (read before reviewing)
1. **Dedupe key is released when an update is not produced.** The plan runs dedupe before the rate limit. Done literally, a rate-limited update stays "seen", so the client's retry with the same `eventId` would be acked as a duplicate and lost. The key is therefore released on `RATE_LIMITED` and on `UNAVAILABLE` (tests: `RETRY`, `UNAVAILABLE`). The plan doc does not say this yet.
2. **Missing `Origin` is refused on the socket handshake.** Strict, as the plan says. The simulator and Artillery must send an `Origin` header.
3. **Logout returns JSON `{ redirectTo }`**, not a 302, so a `fetch()` call from the web app can read it and navigate (a cross-origin redirect to Keycloak would fail CORS). Phase 3 must do the navigation.
4. **Logout in a multi-pod setup:** `endSession` closes only the sockets on the pod that handled the logout. Other pods close theirs on the next sweep (5 s) because the session is gone from Redis. The Socket.IO Redis adapter for instant cross-pod close is not added (plan: Phase 4).
5. **Not done, on purpose (scope or later phase):** `presence:snapshot` is an empty list until the presence-projector exists (Phase 2); `sharing:stop` and the 5 s disconnect grace timer with `presence.offline` (Phase 2); plausibility check on implied speed (plan 06; not in the Phase 1 brief); back-channel logout (SHOULD); H3 cell tagging (COULD).
6. **In-memory Kafka bus in the Redis-only integration tests.** Counting "records on the topic" there uses `InMemoryEventBus`. The real-Kafka proof is `stack.integration.test.ts`; both run in CI.
7. **Helmet defaults** add a CSP with `upgrade-insecure-requests` and HSTS. Fine for a JSON API; Phase 3 needs the plan's own CSP once the gateway serves or fronts the web app.
8. **Fastify 5.12 deprecation warning** for `disableRequestLogging` (kept on purpose: `/auth/callback` URLs carry a one-time code). Replace with `logController` before Fastify 6.
9. **Identity-provider test double:** the Redis-only tests use `FakeOidc` to exercise our own cookie/state/session code. All real-IdP behaviour is covered only by the stack test.
10. **The real browser login has not been done by a person.** The stack test simulates a browser (cookie jar, form post). Please do the manual browser check above once.
11. **`npm install -w @llt/gateway` crashes npm 11 internally** ("Cannot read properties of null (reading 'location')"); dependencies were added to `apps/gateway/package.json` by hand, then `npm install` at the root.

---

## Local Docker Setup (2026-11-26)
- Docker Desktop installed (Docker 29.9.3, Compose v5.7.1). Stack started with `docker compose -f infra/compose/docker-compose.yml up -d`; `npm run v1:infra:smoke` passed all five checks on this machine (Kafka 4 topics, Keycloak realm `tracker`, Redis PING, Postgres timescaledb + postgis, Kafka UI).
- **Fix to `infra/compose/smoke-test.sh`:** on Windows (Git Bash) the Kafka topics check failed for 300 s even though the topics existed, because Git Bash rewrote the container path `/opt/kafka/bin/kafka-topics.sh` into a Windows path. The Kafka `docker exec` call now sets `MSYS_NO_PATHCONV=1`. It does nothing on Linux, so CI is unaffected. It is set on that one command only, because turning it off for the whole script breaks `curl -o /dev/null` in the Kafka UI check.

---

## Phase 0 Review Fixes (applied 2026-11-26)
| # | Problem found in review | Fix | Test |
|---|---|---|---|
| 1 | Confluent `eachBatchAutoResolve` defaults to `true`, so a paused batch was marked processed and history rows could be lost | `CONFLUENT_BATCH_RUN_OPTIONS = { eachBatchAutoResolve: false }` passed to `consumer.run` | `kafka-config.test.ts` |
| 2 | KafkaJS `commitOffsetsIfNecessary()` with no arguments is a no-op when autoCommit=false, so offsets were never committed | `makeKafkaJsCommit` resolves the offset and commits `offset + 1` explicitly | `kafka-config.test.ts` (mocked payload) |
| 3 | SSL/SASL settings were only given to the producer; consumers and admin could not authenticate to managed Kafka | Security settings moved onto the shared Confluent client (`buildConfluentClientConfig`); the client copies them into producer, consumers and admin | `kafka-config.test.ts` |
| 4 | Production could start with the published dev secrets and an insecure OIDC issuer | `GatewayEnvSchema` refuses dev `SESSION_SECRET` / `SESSION_ENCRYPTION_KEY` / `OIDC_CLIENT_SECRET` and `OIDC_ALLOW_INSECURE=true` in production; the flag now defaults to true only outside production; encryption key must be 64 hex chars | `config.test.ts` (7 new) |
| 5 | `InMemoryEventBus` split a group's work per message (round-robin), unlike Kafka | Assignment by partition (`partition % members`), so one key always reaches one member, in order | `event-bus.test.ts` |
| 6 | `publish()` returned a made-up partition 0 / offset 0 when the send result was empty | `toPublishResult` throws instead | `kafka-config.test.ts` |
| 7 | `KAFKA_CLUSTER_ID` was the wrong variable name and an invalid 24-char id; Kafka UI used `latest`; topic init ignored failures | Line removed (with a comment), Kafka UI pinned to `v1.4.2`, `set -e` in `kafka-init` | `infra-smoke` CI job (not yet run) |
| 8 | Log redaction covered only one level deep | Coordinates redacted up to two levels deep and inside arrays; secrets one level deep; limit documented in `logger.ts` | `observability.test.ts` |

Also: Zod 4 `.merge()` replaced with `.extend(KafkaEnvSchema.shape)`; added `infra/compose/smoke-test.sh` and the `v1:infra:smoke` script.

**Not changed:** zstd compression (the plan's nit) is left off on purpose. KafkaJS cannot read zstd without an extra codec, so a mixed-driver setup would break.

**Verified after fixes:** `npm run v1:build` 9/9, `v1:typecheck` 13/13, `v1:lint` clean, `v1:test` 45/45 (contracts 18, config 11, kafka 12, observability 4), prototype `npm test` 14/14, `bash -n smoke-test.sh` OK, Prettier clean on changed files.

---

## Done (Phase 0, as first delivered; see the review fixes above for what changed)
- **Monorepo workspace:** npm workspaces (`apps/*`, `packages/*`) + Turborepo (`turbo.json`), TypeScript strict base configs (`tsconfig.base.json`, `packages/tsconfig`), ESLint 9 flat config (`eslint.config.mjs`), Prettier (`.prettierrc`), updated `.gitignore`, updated `.env.example`.
- **Untouched prototype:** Existing `src/`, `public/`, `test/`, `scripts/`, root `docker-compose.yml`, and prototype scripts/dependencies in root `package.json` were preserved intact.
- **Shared packages (`packages/`):**
  - `@llt/contracts`: strict Zod v4 schemas (`z.strictObject`) for Kafka events (`LocationUpdatedEvent`, `PresenceEvent`, `UserLastLocation`), Socket.IO events (`ClientLocationUpdate` rejecting unknown keys / spoofed `userId`), canonical topic names, header constants, and plan timing constants (15 s heartbeat, 25 s stale fade, 30 s server stale, 35 s client removal).
  - `@llt/config`: typed Zod environment loaders for `gateway`, `history-writer`, `presence-projector`, and `simulator`.
  - `@llt/kafka`: `EventBus` interface, `KafkaEventBus` supporting both `@confluentinc/kafka-javascript` (default, idempotent producer, `acks=-1`) and `kafkajs` fallback, DLQ helper with source metadata headers, and `InMemoryEventBus` modeling Kafka consumer-group fan-out vs. work-queue splitting.
  - `@llt/observability`: `pino` logger redacting raw coordinates (`lat`, `lng`, `position`) and auth secrets at INFO, `hashUserId()`, and OpenTelemetry W3C `traceparent` header injection/extraction helpers.
- **App skeletons (`apps/`):** minimal buildable/runnable TypeScript shells for `gateway`, `history-writer`, `presence-projector`, `simulator`, and `web` (React 19 + Vite with `/auth`, `/api`, `/socket.io` proxy to `:3000`).
- **Infrastructure & CI:**
  - `infra/compose/docker-compose.yml`: Apache Kafka 4.1.0 (KRaft), one-shot topic init (`location-updates`, `presence-events`, `user-last-location` compacted, `location-updates.dlq`), Kafbat Kafka UI (now pinned to `ghcr.io/kafbat/kafka-ui:v1.4.2`), Keycloak 26.0.7 with realm import, Redis 7, TimescaleDB + PostGIS (`timescale/timescaledb-ha:pg17`).
  - `infra/keycloak/tracker-realm.json`: updated backwards-compatibly to allow `http://localhost:5173` alongside `http://localhost:3000`.
  - `.github/workflows/ci.yml`: lint, typecheck, build, v1 unit tests, prototype safety-net tests, and (added in review) the `infra-smoke` Docker Compose job.

---

## Verified at first delivery (superseded by "Verified after fixes" above: 45/45 v1 tests now)
- `npm install`: exited 0 (added 256 packages, 384 total).
- `npm run v1:build`: exited 0 — **9/9** packages built (`@llt/contracts`, `@llt/config`, `@llt/kafka`, `@llt/observability`, `@llt/gateway`, `@llt/history-writer`, `@llt/presence-projector`, `@llt/simulator`, `@llt/web` Vite bundle `dist/assets/index-*.js` 96.69 kB gzip).
- `npm run v1:typecheck`: exited 0 — **13/13** Turborepo tasks passed.
- `npm run v1:lint`: exited 0 — 0 errors, 0 warnings across all v1 TypeScript files.
- `npm run v1:test`: exited 0 — **28/28** unit tests passed:
  - `@llt/contracts`: 18 passed (valid payloads, nullable telemetry, strict rejection of spoofed `userId` and invalid lat/lng/UUID/types, Kafka event schemas, plan timing constants).
  - `@llt/config`: 4 passed (defaults, CSV brokers, booleans, invalid env rejection).
  - `@llt/kafka`: 3 passed (fan-out across distinct consumer groups vs. split within shared `location-db-writer` group, DLQ headers, manual batch offset commits).
  - `@llt/observability`: 3 passed (INFO coordinate redaction, `hashUserId`, trace header preservation).
- `node apps/gateway/dist/index.js`: exited 0 — emitted structured `pino` JSON startup log.
- `node -e "import('@confluentinc/kafka-javascript').then(() => console.log('CONFLUENT_OK'))"`: printed `CONFLUENT_OK` on Windows 11 + Node `v24.22.1`.

---

## Not Done
- **Phase 0 live infra check:** ✅ passed in GitHub Actions (run 45692142195) and, since 2026-11-26, locally on Windows 11 with Docker Desktop (all five smoke checks OK).
  - First CI run failed: a missing space in `kafka-init` created a topic named `location-updates--partitions` instead of `location-updates` (typo introduced during the review fixes). Fixed in `6787cb6`; the smoke test caught it as intended.
- **Git repository:** done. Initialized on `main`, pushed to https://github.com/JaydeepKanzariya/live-location-tracker (commit `3516da0`). Repo-local git identity; hooks disabled for this repo only.
- **Phases 1–5:** Auth + gateway (Phase 1), Pipelines (Phase 2), Frontend (Phase 3), Production-grade observability/deploy/load test (Phase 4), Final README + demo script (Phase 5).

---

## Known Issues / Honest Flags
1. **Docker on this machine (resolved 2026-11-26):** the stack now runs locally and the smoke test passes, so Testcontainers tests and the real Keycloak login can run here as well as in CI. Windows notes: (a) Memurai (a Redis server for Windows) uses port 6379 and must stay stopped (`net stop memurai`; startup type is now Manual); (b) in Git Bash, set `MSYS_NO_PATHCONV=1` on any command that passes a container path (for example `/opt/kafka/bin/...`) to `docker exec`, or Git Bash turns it into a Windows path; (c) Docker keeps its images in a virtual disk under `AppData\Local\Docker\wsl\disk` on C: (about 7 GB so far).
2. **npm 11 `allowScripts` warning (resolved for Kafka, 2026-11-26):** npm still warns that install scripts for `@confluentinc/kafka-javascript`, `esbuild` and others are not covered by `allowScripts`. The Confluent driver nevertheless connects to a real Kafka 4.1 broker on Windows (producer, consumers, rebalance), proven by `test:stack`. No need to switch to `kafkajs`.
3. **ESLint 9 deprecation notice:** npm warned that `eslint@9.x` has been superseded by ESLint 10; ESLint 9 is working cleanly with `typescript-eslint@8` right now.
4. **Prototype tests:** all 14 pass locally, including `test/gateway.integration.test.js` (re-run during review on 2026-11-26).
5. **Vite env files:** Vite reads `.env` from `apps/web`, not the repo root, so `VITE_ENABLE_SIMULATOR` in the root `.env.example` is not seen yet. Set `envDir` in `apps/web/vite.config.ts` in Phase 3.
6. **CI actions:** bumped `actions/checkout` and `actions/setup-node` to `v7` to clear the Node 20 deprecation warning.

---

## Decisions That Deviate from the Plan
| Area | Plan | Implemented | Why |
|---|---|---|---|
| Package manager | `pnpm` workspaces (ADR-02) | `npm` workspaces (`npm@11.19.1`) | Owner requested `npm`; avoids lockfile conflict with prototype and gives smoother Windows native-addon resolution. **Plan docs updated 2026-11-26** (ADR-02, 00, 06, 10, 12, BUILD-PROMPT) |
| Kafka UI image | `provectuslabs/kafka-ui:v0.7.2` | `ghcr.io/kafbat/kafka-ui:v1.4.2` | Provectus repo is archived; Kafbat is the active fork with Kafka 4 KRaft support. Pinned in review. Plan doc 02 updated |
| Zod version | Zod 3 syntax in plan snippets | Zod 4 (`^4.0.0`, `z.strictObject`, `z.uuid`) | Current stable version. Plan doc 04 snippet updated |
| DB writer group ID | `history-writer` in 03/04 docs | `location-db-writer` | Matches `docs/BUILD-PROMPT.md` Non-Negotiable Rule #4. Plan docs 03, 04, 07, 12 updated; the app is still named `history-writer` |
| Timings and endpoints | heartbeat 10 s (05); history at `/api/me/history` (06, 07) | heartbeat 15 s; `/api/locations/history` | Frontend Part 2 values adopted everywhere; reconciliation table in 08 marked resolved |

---

## Later (Deferred / Out of Scope for Now)
- H3 viewport-scoped fan-out (COULD)
- Protobuf + Schema Registry (COULD)
- History playback timeline scrubber (COULD)
- Kubernetes + Strimzi manifests (COULD)
