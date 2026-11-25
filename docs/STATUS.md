# Live Location Tracker v1 — Status

## Current Phase
**Phase 0 (Foundations): Reviewed 2026-11-26, review fixes applied** — code and tests verified locally.
Live infrastructure is still unverified: no Docker on this machine. The new CI job `infra-smoke` proves it on GitHub once the repo is pushed (or run `npm run v1:infra:up` then `npm run v1:infra:smoke` with Docker installed).

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

## Done (Phase 0)
- **Monorepo workspace:** npm workspaces (`apps/*`, `packages/*`) + Turborepo (`turbo.json`), TypeScript strict base configs (`tsconfig.base.json`, `packages/tsconfig`), ESLint 9 flat config (`eslint.config.mjs`), Prettier (`.prettierrc`), updated `.gitignore`, updated `.env.example`.
- **Untouched prototype:** Existing `src/`, `public/`, `test/`, `scripts/`, root `docker-compose.yml`, and prototype scripts/dependencies in root `package.json` were preserved intact.
- **Shared packages (`packages/`):**
  - `@llt/contracts`: strict Zod v4 schemas (`z.strictObject`) for Kafka events (`LocationUpdatedEvent`, `PresenceEvent`, `UserLastLocation`), Socket.IO events (`ClientLocationUpdate` rejecting unknown keys / spoofed `userId`), canonical topic names, header constants, and plan timing constants (15 s heartbeat, 25 s stale fade, 30 s server stale, 35 s client removal).
  - `@llt/config`: typed Zod environment loaders for `gateway`, `history-writer`, `presence-projector`, and `simulator`.
  - `@llt/kafka`: `EventBus` interface, `KafkaEventBus` supporting both `@confluentinc/kafka-javascript` (default, idempotent producer, `acks=-1`) and `kafkajs` fallback, DLQ helper with source metadata headers, and `InMemoryEventBus` modeling Kafka consumer-group fan-out vs. work-queue splitting.
  - `@llt/observability`: `pino` logger redacting raw coordinates (`lat`, `lng`, `position`) and auth secrets at INFO, `hashUserId()`, and OpenTelemetry W3C `traceparent` header injection/extraction helpers.
- **App skeletons (`apps/`):** minimal buildable/runnable TypeScript shells for `gateway`, `history-writer`, `presence-projector`, `simulator`, and `web` (React 19 + Vite with `/auth`, `/api`, `/socket.io` proxy to `:3000`).
- **Infrastructure & CI:**
  - `infra/compose/docker-compose.yml`: Apache Kafka 4.1.0 (KRaft), one-shot topic init (`location-updates`, `presence-events`, `user-last-location` compacted, `location-updates.dlq`), Kafbat Kafka UI (`ghcr.io/kafbat/kafka-ui:latest`), Keycloak 26.0.7 with realm import, Redis 7, TimescaleDB + PostGIS (`timescale/timescaledb-ha:pg17`).
  - `infra/keycloak/tracker-realm.json`: updated backwards-compatibly to allow `http://localhost:5173` alongside `http://localhost:3000`.
  - `.github/workflows/ci.yml`: lint, typecheck, build, v1 unit tests, and prototype safety-net tests.

---

## Verified (with Command Output Summary)
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
- **Phase 0 live infra check:** `npm run v1:infra:up`, Kafka UI (`http://localhost:8081`), and Keycloak (`http://localhost:8080`) could **not** be run yet because Docker is not installed on this machine.
- **Git repository:** `.git` is not initialized yet (awaiting owner confirmation to run `git init`).
- **Phases 1–5:** Auth + gateway (Phase 1), Pipelines (Phase 2), Frontend (Phase 3), Production-grade observability/deploy/load test (Phase 4), Final README + demo script (Phase 5).

---

## Known Issues / Honest Flags
1. **Docker not installed:** `infra/compose/docker-compose.yml` has been written and checked carefully, but has not been executed against a real Docker daemon yet.
2. **npm 11 `allowScripts` warning:** During `npm install`, npm 11 warned that install scripts for `@confluentinc/kafka-javascript@1.11.0` and `esbuild` were not yet covered by `allowScripts`. While `import('@confluentinc/kafka-javascript')` succeeded in Node 24, we should verify connecting to a real Kafka broker once Docker is installed (and run `npm install-scripts approve @confluentinc/kafka-javascript` if native bindings complain, or switch `KAFKA_DRIVER=kafkajs`).
3. **ESLint 9 deprecation notice:** npm warned that `eslint@9.x` has been superseded by ESLint 10; ESLint 9 is working cleanly with `typescript-eslint@8` right now.
4. **Prototype integration test in sandbox:** All 12 prototype unit tests pass; `test/gateway.integration.test.js` requires local socket binding (run directly in your terminal with `npm test` to verify).

---

## Decisions That Deviate from the Plan
| Area | Plan | Implemented | Why |
|---|---|---|---|
| Package manager | `pnpm` workspaces (ADR-02) | `npm` workspaces (`npm@11.19.1`) | Owner requested `npm`; avoids lockfile conflict with prototype and gives smoother Windows native-addon resolution |
| Kafka UI image | `provectuslabs/kafka-ui:v0.7.2` | `ghcr.io/kafbat/kafka-ui:latest` | Provectus repo is archived; Kafbat is the active fork with Kafka 4 KRaft support |
| Zod version | Zod 3 syntax in plan snippets | Zod 4 (`^4.0.0`, `z.strictObject`, `z.uuid`) | Current stable version; faster runtime + TS compilation |
| DB writer group ID | `history-writer` in 03/04 docs | `location-db-writer` | Matches `docs/BUILD-PROMPT.md` Non-Negotiable Rule #4 |

---

## Later (Deferred / Out of Scope for Now)
- H3 viewport-scoped fan-out (COULD)
- Protobuf + Schema Registry (COULD)
- History playback timeline scrubber (COULD)
- Kubernetes + Strimzi manifests (COULD)
