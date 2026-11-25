# Live Location Tracker: Engineering Plan

> Status: **Proposed** · Date: 2026-11-25 · Submission deadline: 2026-12-31
> The current JavaScript prototype in this repo stays as-is. It becomes the reference implementation (`v0-prototype`) while v1 is built next to it (see [12-roadmap.md](12-roadmap.md)).

## 1. What we are building

A real-time location platform, the same shape as rider/customer tracking in Uber, Swiggy or Zomato:

- users sign in with **OIDC**, grant location permission, and stream their position
- every update goes **Socket.IO → Kafka → consumers**. One consumer group fans updates out to live maps; another persists history
- the map (**Leaflet**) shows everyone's live markers moving smoothly, removes stale users, and can replay history

## 2. Guiding principles

1. **Kafka is the backbone.** No location reaches another user or the database without passing through the log.
2. **Identity comes from the session, never from the payload.** `userId` = OIDC `sub`.
3. **Every write is idempotent.** Assume at-least-once delivery everywhere and make duplicates harmless.
4. **Separate the hot path from the cold path.** Live fan-out (milliseconds) never waits for the database.
5. **Build only what we can explain.** Every component has to be explainable in the demo video. If we can't explain it, we don't add it.
6. **Do the boring thing first.** Pick well-proven tools and add complexity only when a number justifies it.

## 3. The stack in one table

| Concern | Choice | Doc |
|---|---|---|
| Language | **TypeScript** end to end (Node.js 22 LTS) | [02](02-tech-stack.md) |
| Repo | **pnpm workspaces + Turborepo** monorepo | [10](10-infrastructure-devops.md) |
| Frontend | **React 19 + Vite**, **Leaflet** (react-leaflet), TanStack Query, Zustand, Tailwind + shadcn/ui | [08](08-frontend.md) |
| Realtime gateway | **Fastify + Socket.IO** | [05](05-realtime-gateway.md) |
| Event streaming | **Apache Kafka 4 (KRaft)** · client **@confluentinc/kafka-javascript** | [04](04-kafka-event-streaming.md) |
| Event contracts | **Zod** schemas in a shared package, versioned envelope (Protobuf + Schema Registry later) | [04](04-kafka-event-streaming.md) |
| Auth | **OIDC Auth Code + PKCE, BFF pattern** (HttpOnly session cookie) · **Keycloak** locally, Auth0/Google in prod | [06](06-auth-security.md) |
| Hot state | **Redis 7 / Valkey**: sessions, presence (GEO), rate limits, dedupe | [07](07-data-storage.md) |
| Durable store | **PostgreSQL 17 + PostGIS + TimescaleDB** (hypertables, compression, retention) · **Drizzle ORM** | [07](07-data-storage.md) |
| Observability | **OpenTelemetry** (traces through Kafka headers) · **Prometheus + Grafana + Loki** · **pino** logs | [09](09-observability-reliability.md) |
| Testing | **Vitest**, **Testcontainers** (Kafka/Postgres/Redis/Keycloak), **Playwright** (geolocation mocking), **Artillery** load tests | [11](11-testing-strategy.md) |
| Local/infra | **Docker Compose** · deploy on a VM with **Caddy** TLS · production target **Kubernetes + Strimzi** | [10](10-infrastructure-devops.md) |

## 4. Document map

| # | Document | Answers |
|---|---|---|
| 01 | [Requirements & capacity](01-requirements.md) | What must it do, how big, how fast? |
| 02 | [Tech stack & decisions](02-tech-stack.md) | Why each tool, what we rejected |
| 03 | [System architecture](03-system-architecture.md) | Components, data flows, diagrams |
| 04 | [Kafka & event streaming](04-kafka-event-streaming.md) | Topics, keys, partitions, groups, schemas, delivery semantics |
| 05 | [Realtime gateway](05-realtime-gateway.md) | Socket protocol, scaling, fan-out, backpressure |
| 06 | [Auth & security](06-auth-security.md) | OIDC flow, socket auth, threat model |
| 07 | [Data storage](07-data-storage.md) | Redis + Postgres/Timescale schema, write strategy, retention |
| 08 | [Frontend](08-frontend.md) | App structure, map rendering, UX states |
| 09 | [Observability & reliability](09-observability-reliability.md) | SLOs, metrics, tracing, failure modes |
| 10 | [Infrastructure & DevOps](10-infrastructure-devops.md) | Repo layout, CI/CD, environments, deployment |
| 11 | [Testing strategy](11-testing-strategy.md) | Test pyramid, what each level proves |
| 12 | [Roadmap](12-roadmap.md) | Week-by-week plan to Dec 31, scope tiers, demo script |

## 5. Scope tiers

"World-class" includes knowing what *not* to build before a deadline.

| Tier | Meaning | Examples |
|---|---|---|
| **MUST** | Graded; must be built and shown in the demo | OIDC, Socket.IO auth, Kafka producer + 2 consumer groups, Leaflet live map, history persistence, disconnect/invalid/duplicate handling, README, demo |
| **SHOULD** | Makes it industry-grade; build if on schedule | TypeScript monorepo, Redis presence, TimescaleDB, OpenTelemetry traces, Testcontainers, Playwright, CI, deployed URL |
| **COULD** | Designed and documented, built only if time remains | Viewport/H3-based subscriptions, DLQ replay tool, Protobuf + Schema Registry, Kubernetes manifests, history playback |
| **WON'T (now)** | Explicitly out of scope | Native mobile apps, ETA/routing, payments, multi-region active-active |

The evaluation rubric names **Leaflet**, **Socket.IO** and **Kafka** explicitly, so those three are fixed choices, not open for debate.
