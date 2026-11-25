# Live Location Tracker

Real-time location sharing. Signed-in users share their position from the browser and see every other signed-in user move on a map.

**Kafka is the main path, not an add-on.** Every location update goes browser → Socket.IO → **Kafka** → consumers → browsers. The gateway never sends a location straight back out to other sockets. A marker only moves after its event has been written to Kafka and read back by a consumer.

[![CI](https://github.com/JaydeepKanzariya/live-location-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/JaydeepKanzariya/live-location-tracker/actions/workflows/ci.yml)

| | |
|---|---|
| **Repository** | https://github.com/JaydeepKanzariya/live-location-tracker |
| **Demo video** | _TODO: paste YouTube unlisted link_ |
| **Live deployment** | _TODO: paste link (or "local only", see [Deployment](#deployment))_ |

---

## Project status

This repository holds two versions side by side:

| Version | Where | State | Run it |
|---|---|---|---|
| **v0 prototype** (JavaScript) | `src/`, `public/`, `scripts/`, `test/`, root `docker-compose.yml` | **Complete.** Covers every assignment requirement and is the fallback submission. Most of this README describes it. | [Setup](#setup) |
| **v1** (TypeScript monorepo) | `apps/`, `packages/`, `infra/` | **In progress.** Phase 0 (foundations) is done and reviewed; Phase 1 (auth + gateway) is next. | [v1 development](#v1-development-in-progress) |

- Plan: [`docs/plan/`](docs/plan/00-overview.md) (all of it in one file: [`docs/PLAN-COMPLETE.md`](docs/PLAN-COMPLETE.md))
- Visual design rules: [`DESIGN.md`](DESIGN.md)
- Current progress, verification results and known issues: [`docs/STATUS.md`](docs/STATUS.md)
- Build instructions for each phase: [`docs/BUILD-PROMPT.md`](docs/BUILD-PROMPT.md)

When v1 reaches the same features as the prototype, the prototype moves to `legacy/` and this README switches to v1.

---

## Contents
- [Project status](#project-status)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Setup](#setup)
- [Environment variables](#environment-variables)
- [OIDC auth setup](#oidc-auth-setup)
- [Socket event flow](#socket-event-flow)
- [Kafka event flow](#kafka-event-flow)
- [Database processor and write strategy](#database-processor-and-write-strategy)
- [Handling disconnects, invalid data and duplicates](#handling-disconnects-invalid-data-and-duplicates)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Assumptions and limitations](#assumptions-and-limitations)
- [v1 development (in progress)](#v1-development-in-progress)

---

## Architecture

```
 Browser (Leaflet + Socket.IO client)
   │  1. login  ───────────────►  OIDC provider (Keycloak / Google / Auth0)
   │  2. geolocation permission
   │  3. emit "location:update" every N s  (session cookie authenticates the socket)
   ▼
 ┌──────────────────────── Gateway  (npm run dev) ────────────────────────┐
 │ Socket.IO gateway: auth → validate → dedupe → rate-limit                │
 │        │ 4. produce (key = userId)                                       │
 │        ▼                                                                 │
 │   ┌─────────────────── Kafka topic: location-updates (3 partitions) ──┐ │
 │   └───────────┬──────────────────────────────────────────┬───────────┘ │
 │               │ 5. group broadcaster-<instance>          │             │
 │               ▼                                          │             │
 │   Broadcaster consumer → presence store                  │             │
 │        │ 6. io.emit("location:broadcast")                │             │
 └────────┼─────────────────────────────────────────────────┼─────────────┘
          ▼                                                  │ 7. group location-db-writer
   All signed-in browsers: markers move                      ▼
                                          DB processor  (npm run processor, separate process)
                                          batch + downsample → Postgres (or JSONL file)
```

The same topic is read by **two consumer groups**. Each group gets every message and keeps its own offsets:

| Consumer group | Process | Purpose | Scaling |
|---|---|---|---|
| `<KAFKA_BROADCASTER_GROUP>-<INSTANCE_ID>`, e.g. `broadcaster-gateway-1` | gateway | push live updates to sockets | **unique id per instance** → every gateway instance gets *every* event (fan-out), because each instance holds different sockets |
| `location-db-writer` | db-processor | persist history | **shared id** → Kafka *splits* the 3 partitions across processor instances (work queue) |

The two consumers don't depend on each other. If Postgres is slow or down, the map keeps updating; the DB processor falls behind and catches up later from its last committed offset.

## Tech stack

This table is the **v0 prototype**. v1 uses TypeScript, Fastify, Kafka 4.1 with the Confluent client, Redis, TimescaleDB + PostGIS and React; see [ADR summary](docs/plan/02-tech-stack.md).

| Layer | Choice |
|---|---|
| Backend | Node.js 22+ (ESM), Express 5 |
| Realtime | Socket.IO 4 |
| Event streaming | Apache Kafka 3.9 (KRaft, no ZooKeeper) via **KafkaJS** |
| Auth | OpenID Connect, Authorization Code + PKCE, via **openid-client v6**; **Keycloak 26** locally |
| Sessions | express-session (HttpOnly cookie), shared with the Socket.IO handshake |
| Persistence | PostgreSQL 16 (`pg`), with a "simulated" JSONL-file mode when there is no DB |
| Frontend | Plain HTML/CSS/JS modules, **Leaflet 1.9** + OpenStreetMap tiles |
| Infra | Docker Compose: Kafka, Kafka UI, Keycloak, Postgres |
| Tests | `node:test` unit tests + a Socket.IO integration test |

## Project structure

```
── v0 prototype ─────────────────────────────────────────────────────────────
├── docker-compose.yml           Kafka, Kafka UI, Keycloak, Postgres
├── infra/keycloak/              realm import: client + test users alice/bob (shared with v1)
├── public/                      frontend (served by the gateway)
│   ├── index.html               login view + app view
│   ├── app.js                   wiring: auth check, socket events, sidebar
│   ├── map-view.js              Leaflet markers, trails, smooth movement, stale fading
│   └── location-sharer.js       geolocation, send interval, ack + retry
├── scripts/simulate-riders.js   publishes fake riders straight to Kafka (demo / load)
├── src/
│   ├── server.js                gateway entry point (HTTP + Socket.IO + producer + broadcaster)
│   ├── config.js                every env var, with defaults
│   ├── auth/                    oidc.js (login/callback/logout), session.js
│   ├── events/                  location-event.js (event schema + validation), recent-ids.js (dedupe)
│   ├── kafka/                   client.js (topic bootstrap), producer.js
│   ├── realtime/                socket-gateway.js, broadcaster.js (consumer #1), presence.js
│   ├── processors/              db-processor.js (consumer #2), write-planner.js, sinks.js
│   └── db/                      schema.sql, pool.js, history-repo.js
├── test/                        unit + integration tests
│
── v1 (in progress) ─────────────────────────────────────────────────────────
├── apps/                        gateway, history-writer, presence-projector, simulator, web (Phase 0 shells)
├── packages/
│   ├── contracts/               Zod schemas for Kafka events + Socket.IO payloads (single source of truth)
│   ├── config/                  typed env loading with production guards
│   ├── kafka/                   EventBus over @confluentinc/kafka-javascript (KafkaJS fallback), in-memory bus for tests
│   ├── observability/           pino logger with coordinate redaction, OpenTelemetry trace headers
│   └── tsconfig/                shared TypeScript settings
├── infra/compose/               v1 Docker Compose (Kafka 4, Kafka UI, Keycloak, Redis, TimescaleDB + PostGIS) + smoke-test.sh
├── .github/workflows/ci.yml     lint, typecheck, build, tests, infrastructure smoke test
│
── docs ─────────────────────────────────────────────────────────────────────
├── DESIGN.md                    visual design rules
└── docs/                        plan/, PLAN-COMPLETE.md, STATUS.md, BUILD-PROMPT.md
```

## Setup

These steps run the **v0 prototype**. For v1, see [v1 development](#v1-development-in-progress).

**You need:** Node.js ≥ 22.9 and Docker Desktop.

```bash
# 1. Install dependencies
npm install

# 2. Configure (the defaults already match docker-compose)
cp .env.example .env

# 3. Start Kafka, Kafka UI, Keycloak and Postgres
docker compose up -d
#    Keycloak takes ~30-60 s on first boot. Check: http://localhost:8080/realms/tracker

# 4. Terminal A: gateway (HTTP + Socket.IO + Kafka producer + broadcaster consumer)
npm run dev

# 5. Terminal B: database processor (Kafka consumer #2)
npm run processor

# 6. Open http://localhost:3000
```

**Try it with two users.** Open `http://localhost:3000` in a normal window and log in as `alice` / `alice`. Open it again in an **incognito** window (or another browser) and log in as `bob` / `bob`. Click **Start sharing** in both windows. If you're sitting still, tick **Simulate walking** to see movement.

**Optional extras**

```bash
npm run simulate -- --riders 10 --interval 2000   # 10 fake riders published straight to Kafka
npm test                                          # unit + integration tests (no Docker needed)
```

- Kafka UI at **http://localhost:8081** shows the `location-updates` topic, its partitions and messages, and both consumer groups with their lag.
- Stop `npm run processor` for a minute while users keep sharing, then start it again. The lag in Kafka UI grows, then drops back to 0 as it catches up. Nothing is lost.

> Geolocation needs a *secure context*. `http://localhost` counts as secure. Any other host needs HTTPS.

## Environment variables

All variables are in [`.env.example`](.env.example). The prototype reads them in [`src/config.js`](src/config.js); v1 reads them in [`packages/config`](packages/config/src/index.ts). The table below lists the ones the prototype uses. `.env.example` also has v1-only settings (Redis, Kafka driver and extra topics, session encryption key, timing values, OpenTelemetry), each with a comment.

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `APP_BASE_URL` | `http://localhost:3000` | Public URL; redirect URI = `${APP_BASE_URL}/auth/callback` |
| `INSTANCE_ID` | hostname | Unique per gateway; suffix of the broadcaster consumer group |
| `SESSION_SECRET` | dev value | Signs the session cookie. **Required in production** |
| `SESSION_MAX_AGE_MS` | `28800000` (8 h) | Session lifetime; sockets are cut off once it expires |
| `OIDC_ISSUER_URL` | local Keycloak realm | OIDC issuer (discovery via `/.well-known/openid-configuration`) |
| `OIDC_CLIENT_ID` / `OIDC_CLIENT_SECRET` | `location-tracker` / `location-tracker-secret` | Confidential client credentials |
| `OIDC_SCOPES` | `openid profile email` | Requested scopes |
| `OIDC_ALLOW_INSECURE` | `true` outside production | Allow an `http://` issuer (local Keycloak only) |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated brokers |
| `KAFKA_TOPIC` | `location-updates` | Topic name |
| `KAFKA_TOPIC_PARTITIONS` | `3` | Partitions created on first start |
| `KAFKA_BROADCASTER_GROUP` | `broadcaster` in `.env.example` (`socket-broadcaster` if unset) | Broadcaster group prefix; the full id is `<prefix>-<INSTANCE_ID>` |
| `KAFKA_DB_WRITER_GROUP` | `location-db-writer` | DB processor group |
| `KAFKA_SSL`, `KAFKA_SASL_*` | off | For managed Kafka (Confluent, Aiven, Redpanda Cloud) |
| `LOCATION_MIN_INTERVAL_MS` | `1000` | Server-side rate limit per user |
| `LOCATION_STALE_AFTER_MS` | `30000` | A user with no update for this long is removed from the map |
| `LOCATION_STALE_SWEEP_MS` | `5000` | How often the stale check runs |
| `LOCATION_MAX_CLOCK_SKEW_MS` | `60000` | Reject client timestamps further than this from server time |
| `DATABASE_URL` | local Postgres | **Leave empty to simulate persistence** (writes to `HISTORY_FILE`) |
| `HISTORY_FILE` | `data/location-history.jsonl` | Output file for simulated persistence |
| `DB_BATCH_MAX_SIZE` | `500` | Max rows per DB write |
| `HISTORY_MIN_DISTANCE_M` / `HISTORY_MIN_INTERVAL_MS` | `10` / `30000` | History downsampling thresholds |

## OIDC auth setup

**Flow:** Authorization Code with PKCE, using a confidential client ([`src/auth/oidc.js`](src/auth/oidc.js)).

1. `GET /auth/login` creates a PKCE `code_verifier`, a `state` and a `nonce`, saves them in the session, and redirects to the provider's authorize endpoint.
2. The user signs in at the provider. The provider redirects to `GET /auth/callback?code=…&state=…`.
3. The server exchanges the code for tokens, sending the `code_verifier`. openid-client checks `state`, `nonce`, and the ID-token signature, issuer and audience.
4. The session is **regenerated** to prevent session fixation. It then stores `user = { id: sub, name, email }` plus the ID token, which is only used as `id_token_hint` at logout.
5. `GET /auth/logout` destroys the local session and redirects to the provider's `end_session_endpoint` (RP-initiated logout), so the SSO session ends too.

**Token and session handling.** The browser only ever holds an **HttpOnly, SameSite=Lax session cookie**. Tokens never reach JavaScript. Access and refresh tokens are not stored because the app doesn't call any API on the user's behalf; it only needs the user's verified identity. The user id used everywhere is the OIDC `sub` claim.

**Keycloak (default, no account needed).** `docker compose up` imports [`infra/keycloak/tracker-realm.json`](infra/keycloak/tracker-realm.json):
- realm `tracker`, confidential client `location-tracker` (PKCE S256 required), redirect URI `http://localhost:3000/auth/callback`
- users `alice/alice` and `bob/bob`; self-registration is enabled too
- admin console: http://localhost:8080 (`admin` / `admin`)

**Other providers.** Only env vars change:

| Provider | `OIDC_ISSUER_URL` | Register redirect URI |
|---|---|---|
| Google | `https://accounts.google.com` | Google Cloud Console → Credentials → OAuth client (Web) → `http://localhost:3000/auth/callback` |
| Auth0 | `https://<tenant>.auth0.com/` | Application → Allowed Callback URLs; add `http://localhost:3000` to Allowed Logout URLs |

Set `OIDC_CLIENT_ID` and `OIDC_CLIENT_SECRET` from the provider. Google has no `end_session_endpoint`, so logout there only clears the app session.

**How auth reaches the socket.** The **same** `express-session` middleware runs on the Socket.IO handshake (`io.engine.use(sessionMiddleware)`). An `io.use()` guard rejects the connection with `unauthorized` if the session has no user. Otherwise it puts `session.user` on `socket.data.user`. From then on:
- the server **never reads a user id from the client payload**; the event's `userId` always comes from the session (there's a test for this)
- each socket joins room `user:<sub>`, so presence is tracked **per user, not per socket**. Two tabs are one user.
- each event checks whether the session has expired; if it has, the server emits `auth:expired` and disconnects the socket

## Socket event flow

| Direction | Event | Payload | Notes |
|---|---|---|---|
| server → client | `session:ready` | `{ user, settings }` | sent after the handshake is authenticated |
| server → client | `presence:snapshot` | `[location…]` | everyone currently on the map, so a new tab isn't empty |
| client → server | `location:update` | `{ eventId, lat, lng, accuracy, heading, speed, clientTs, source }` + **ack** | ack is `{ ok, partition, offset }`, `{ ok, duplicate }` or `{ ok:false, error }` |
| client → server | `location:stop` | – | user stopped sharing; publishes a `user.offline` event |
| server → client | `location:broadcast` | `{ userId, displayName, lat, lng, …, kafka:{partition, offset} }` | sent **from the Kafka consumer**, not from the handler that received the update |
| server → client | `user:offline` | `{ userId, reason }` | `stopped-sharing`, `disconnected` or `stale` |
| server → client | `auth:expired` | – | client redirects to login |

**Send frequency.** `watchPosition` keeps the latest GPS fix. A timer (2, 3, 5 or 10 s; default 3 s) sends it only if the user moved at least 2 m, or if 10 s have passed since the last send (a heartbeat). So load is predictable (≈ users ÷ interval events per second) however chatty the device GPS is, and a stationary user still doesn't go stale (30 s). The server also enforces at most 1 update per second per user. 3 s is a common compromise for rider apps: smooth enough on a map, cheap on battery and bandwidth.

## Kafka event flow

**Topic** `location-updates`, 3 partitions, 24 h retention (docker-compose).

**Message key = `userId`.** Kafka hashes the key to choose a partition, so all of one user's events go to the same partition **in order**, and different users spread across partitions. Ordering per user is all we need; no ordering across users is required.

**Event schema** (`schemaVersion: 1`, defined once in [`src/events/location-event.js`](src/events/location-event.js)):

```json
{
  "schemaVersion": 1, "type": "location.updated",
  "eventId": "3f0c…", "userId": "<oidc sub>", "displayName": "Alice Rider",
  "lat": 19.07, "lng": 72.87, "accuracy": 12, "heading": 90, "speed": 1.4,
  "source": "gps", "clientTs": 1764050000000, "serverTs": 1764050000040,
  "gatewayInstance": "gateway-1"
}
```

A second type, `user.offline`, goes on the **same topic with the same key**, so it is ordered after that user's last location.

**Producer** ([`src/kafka/producer.js`](src/kafka/producer.js)): `acks: -1` (all in-sync replicas) and gzip. The socket ack returns `{partition, offset}` only **after** Kafka has stored the event, and the UI shows it (`Last Kafka write: partition 1, offset 42`).

**Consumer #1, broadcaster** ([`src/realtime/broadcaster.js`](src/realtime/broadcaster.js)): `eachMessage`, `maxWaitTimeInMs: 100` for low latency, `fromBeginning: false` because live view only needs new positions. Each event is parsed and validated, deduplicated, applied to the presence store (out-of-order events are dropped), and then emitted to the `authenticated` room.

**Consumer #2, DB processor**: see the next section.

**Proof that the map is driven by Kafka:** `npm run simulate` writes fake riders **straight to the topic**, with no socket involved, and they appear on every map.

## Database processor and write strategy

`npm run processor` → [`src/processors/db-processor.js`](src/processors/db-processor.js), a separate process in consumer group `location-db-writer`.

**Why not write to the DB on every socket event?** 1,000 riders each sending every 3 s is ~333 writes/s, or ~29 M rows/day. Each write is a round trip, a transaction, an index update and WAL. Doing that inline in the socket handler:
- couples live latency to DB latency: a slow DB makes the map lag
- has no buffer: a DB outage means updates are lost or handlers pile up
- stores lots of near-duplicate points (a parked phone sending the same spot every 3 s)

**What the processor does instead:**
1. **Micro-batches from Kafka.** `minBytes: 16 KB`, `maxWaitTimeInMs: 2000`: the broker holds a fetch until it has 16 KB or 2 s have passed. One batch becomes one multi-row `INSERT` (chunks of `DB_BATCH_MAX_SIZE`), not N inserts.
2. **Downsamples history** ([`write-planner.js`](src/processors/write-planner.js)). A point is kept only if the user moved ≥ 10 m **or** ≥ 30 s passed since the last kept point. A stationary user goes from ~20 rows/min to 2.
3. **Collapses the latest position.** `user_last_location` gets **one upsert per user per batch**, however many events arrived.
4. **Is idempotent.** `location_history.event_id` is `UNIQUE` and inserts use `ON CONFLICT DO NOTHING`. The upsert only moves forward in time (`WHERE recorded_at < EXCLUDED.recorded_at`).
5. **Commits offsets after the write** (`eachBatchAutoResolve: false`). Each batch is written in a transaction, then `resolveOffset` and commit. If it crashes before the commit, Kafka re-delivers the batch and step 4 makes the replay harmless. That's at-least-once delivery with effectively-once storage. The downsampling state only moves forward after a successful write, so a retried batch is planned exactly the same way.
6. **Catches up after an outage.** Kafka keeps 24 h, so if Postgres is down the processor builds lag and catches up later. Live tracking isn't affected.

**Tables** ([`src/db/schema.sql`](src/db/schema.sql), applied automatically):
- `location_history`: append-only trail, index `(user_id, recorded_at DESC)`, with the Kafka partition and offset stored for traceability
- `user_last_location`: one row per user

**Simulated mode.** With `DATABASE_URL` empty, the same pipeline appends rows to `data/location-history.jsonl`. The UI's **Show my stored history** button reads `GET /api/history`, which needs Postgres; it draws your persisted trail as a dashed yellow line.

**Scaling further** (designed, not built): partition `location_history` by day (native partitioning or TimescaleDB) and drop old partitions; replace multi-row INSERT with `COPY`; move cold history to object storage.

## Handling disconnects, invalid data and duplicates

| Case | Handling |
|---|---|
| Unauthenticated socket | Handshake rejected (`unauthorized`); the client redirects to login |
| Session expires while connected | `auth:expired`, socket disconnected |
| Malformed or out-of-range data | `validateClientLocation`: UUID `eventId`; lat/lng ranges; rejects `0,0`; timestamp within ±60 s; optional numeric fields range-checked; `source` allow-listed; Socket.IO `maxHttpBufferSize` 10 KB. Rejections are returned in the ack and logged |
| Bad message on the topic | Every consumer re-validates (`parseKafkaEvent`) and skips it rather than crashing |
| Too-frequent updates | Per-**user** rate limit (opening many tabs doesn't get around it) |
| Duplicate from a client retry | If no ack arrives in 5 s, the client resends **the same `eventId`**. The gateway remembers recent ids per user and acks `duplicate: true` without publishing |
| Duplicate from Kafka redelivery | Broadcaster: per-user recent-id memory plus "ignore if older than current". DB: `UNIQUE(event_id)` |
| Out-of-order events | Presence store ignores events older than the current one; the DB upsert only moves forward |
| Tab closed or network lost | After a **5 s grace period** (covers page reloads), if the user has no other sockets, a `user.offline` event is published and markers disappear everywhere |
| Gateway crashed (no offline event sent) | Stale sweep: no update in 30 s → removed and `user:offline {reason: stale}`. The client also fades quiet markers after 15 s and removes them after 35 s as a backstop |
| Kafka unavailable | Publish fails → ack `{ok:false, retryable:true}`, the event id is released so a retry works, `/healthz` returns 503 |

## Design decisions and trade-offs

- **Why Kafka here?** Location data is high-volume, append-only and read by several independent consumers (live map, history, and later ETA, geofencing, analytics). Kafka absorbs bursts, keeps events for replay, lets each consumer go at its own speed, and lets new consumers be added without touching the gateway. Rider and customer tracking apps work this way: driver apps stream GPS into a log, and separate services fan it out to the customer's screen, the dispatch system and storage.
- **Why not broadcast directly from the socket handler?** It would be lower latency (~5–50 ms saved), but then Kafka would only be a side-channel. Going through Kafka means every instance sees the same ordered stream, a multi-instance deployment needs no Redis adapter, and what you see on the map is exactly what was stored.
- **Why sessions instead of passing a JWT to the socket?** Same-origin app, HttpOnly cookie, no token in JS, logout and expiry handled in one place. To scale out, use a shared session store (Redis or Postgres).
- **Why a broadcaster group per instance?** It's pub/sub semantics: every instance must push to its own sockets. The cost is that every instance reads the whole topic. At very large scale you'd shard sockets by region or partition.
- **Why gzip and `acks=-1`?** Durability before we tell the client "accepted". Latency costs a few ms.
- **Trade-off:** a restarted gateway starts with an empty presence map (`fromBeginning:false`) and fills up within one send interval (≤10 s heartbeat). A compacted "latest position" topic could fix this; it's left out to keep things simple.

## Deployment

Kafka is the hard part to host. Options:
- **Gateway + processor**: Render, Railway or Fly.io (two services from this repo: `npm start` and `npm run processor`). Set `NODE_ENV=production`, `SESSION_SECRET`, `APP_BASE_URL=https://…`.
- **Kafka**: a managed cluster (Confluent Cloud, Aiven, Redpanda Cloud). Set `KAFKA_BROKERS`, `KAFKA_SSL=true` and `KAFKA_SASL_*`.
- **OIDC**: Auth0 or Google (add the production redirect URI). **Postgres**: Neon, Supabase or Render.
- Use a single gateway instance unless you add a shared session store (see limitations).

## Assumptions and limitations

- **Session store is in memory** (express-session MemoryStore). Fine for one gateway instance; restarting it logs everyone out. Multiple instances need `connect-redis` or `connect-pg-simple`.
- **Everyone who is signed in sees everyone else.** There are no groups, friends or rider↔customer pairing. In a real app the broadcaster would emit to per-trip rooms instead of `authenticated`.
- The `presence:snapshot` comes from each instance's in-memory view; after a gateway restart it fills up within ~10 s.
- Dedupe memories are bounded and in memory (last 200 ids per user). The DB's unique constraint is the final safeguard.
- Single-broker Kafka with replication factor 1 is for local development only. Production needs 3 brokers with `min.insync.replicas=2`.
- Browser geolocation accuracy depends on the device (desktop Wi-Fi location can be hundreds of metres off). Use **Simulate walking** to show movement in a demo.
- Background tabs throttle timers, so a backgrounded tab may send less often (the stale logic handles this).
- OSM public tiles are for low-volume demos only (see their tile usage policy).
- Tests cover validation, dedupe, presence, write planning and the authenticated socket path, using an in-memory producer. The Kafka, Keycloak and Postgres paths are not covered by automated tests; check them manually with docker-compose.
- The prototype's `docker-compose.yml` and v1's `infra/compose/docker-compose.yml` use the same ports (9092, 8080, 8081, 5432). Run only one stack at a time (`docker compose down` the other first).

## v1 development (in progress)

v1 rebuilds the same system as a TypeScript monorepo with production-grade pieces: Fastify, Redis-backed sessions and presence, TimescaleDB + PostGIS history, a React + Leaflet frontend, OpenTelemetry and full test levels. The plan is in [`docs/plan/`](docs/plan/00-overview.md) and progress in [`docs/STATUS.md`](docs/STATUS.md).

| Phase | Scope | State |
|---|---|---|
| 0 · Foundations | workspace, shared packages, Compose infra, CI | **Done** (reviewed; CI green, including the infrastructure smoke test) |
| 1 · Auth + gateway | OIDC BFF, Socket.IO auth, validation, dedupe, rate limit, producer, broadcaster | Next |
| 2 · Pipelines | history-writer, presence-projector, DLQ, history API | Planned |
| 3 · Frontend | React + Vite + Leaflet, the "Signal" design | Planned |
| 4 · Production-grade | tracing, dashboards, Docker images, HTTPS deploy, load test | Planned |
| 5 · Ship | final README, demo | Planned |

**Commands** (npm workspaces + Turborepo; Node ≥ 22.9):

```bash
npm install
npm run v1:build          # build all packages and apps
npm run v1:typecheck
npm run v1:lint
npm run v1:test           # unit tests (no Docker needed)

npm run v1:infra:up       # Kafka 4, Kafka UI, Keycloak, Redis, TimescaleDB + PostGIS (needs Docker)
npm run v1:infra:smoke    # checks topics, Keycloak realm, Redis, Postgres extensions, Kafka UI
npm run v1:infra:down
```

CI runs all of the above on every push to `main`, including the infrastructure smoke test on GitHub's Docker-enabled runners.

Key v1 rules (from [`docs/BUILD-PROMPT.md`](docs/BUILD-PROMPT.md)):
- Kafka is the only path to other users' screens.
- `userId` always comes from the authenticated session.
- Kafka offsets are committed only after the database transaction commits.
- The history writer is a separate process in consumer group `location-db-writer`; each gateway uses its own broadcaster group.
