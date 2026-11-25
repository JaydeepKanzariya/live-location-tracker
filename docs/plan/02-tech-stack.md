# 02 · Tech Stack & Architecture Decisions

Each decision below is written as a short ADR: **choice → why → rejected alternatives → when to revisit.**

---

## ADR-01 · TypeScript everywhere (Node.js 22 LTS)
- **Why:** one language and one set of shared types for client, gateway, consumers and event schemas. A field renamed in the event contract breaks compilation in all four places instead of failing at runtime in production. Node's event loop is a natural fit for I/O-bound socket gateways.
- **Rejected:** *Go* (better raw CPU/memory per connection, but you lose the shared types and Socket.IO's reference server is Node); *Elixir/Phoenix* (excellent for presence, but adds a second ecosystem and the rubric expects Socket.IO); plain JS (the current prototype; fine for a prototype, risky as it grows).
- **Revisit when:** a single gateway pod needs > 100k connections and CPU profiles show JSON/socket overhead dominating. Then consider a Go gateway that speaks the Socket.IO protocol or uses raw WebSockets.

## ADR-02 · Monorepo: pnpm workspaces + Turborepo
- **Why:** apps (`web`, `gateway`, `history-writer`, `simulator`) and packages (`contracts`, `config`, `observability`) version together. Turborepo caches builds and tests, and pnpm is fast and strict about phantom dependencies.
- **Rejected:** Nx (more powerful but heavier than this project needs); polyrepo (contract changes would need coordinated releases).

## ADR-03 · Frontend: React 19 + Vite SPA, Leaflet via react-leaflet
- **Why:** the app sits entirely behind login and is a live, stateful map, so there's nothing to gain from SSR or SEO. Vite gives instant HMR and a static build that any CDN can serve. **Leaflet is named in the rubric.** react-leaflet gives declarative layers, and for performance we drive markers imperatively through refs (see [08](08-frontend.md)).
- **Supporting libraries:** TanStack Query (REST data: me, history), Zustand (live presence store, which updates many times per second without re-rendering the whole tree), Tailwind CSS + shadcn/ui (accessible components we own), Leaflet canvas renderer + supercluster for many markers.
- **Rejected:** Next.js (SSR and server components add complexity a map SPA doesn't need, and websocket handling in serverless deployments is awkward); MapLibre GL (better at 10k+ markers with WebGL vector tiles, but the rubric asks for Leaflet; noted as an upgrade path); Redux (too much ceremony for this state).

## ADR-04 · Gateway: Fastify + Socket.IO
- **Why:** Fastify is the fastest mainstream Node HTTP framework, with schema-validated routes, a solid plugin system and first-class TypeScript. Socket.IO gives acks, automatic reconnection, rooms, a polling fallback for corporate proxies, and is required by the rubric.
- **Rejected:** Express (slower, older middleware model; fine but not best-in-class); NestJS (good DI, but its Kafka "microservices" layer hides the very consumer/producer mechanics we're graded on); raw `ws` (we'd have to rebuild acks, rooms and reconnection ourselves).

## ADR-05 · Event streaming: Apache Kafka 4.x (KRaft)
- **Why:** a durable, partitioned, replayable log with consumer groups, which is the industry standard for high-volume telemetry. Kafka 4 removed ZooKeeper completely (KRaft only), so local setup is a single container.
- **Rejected for this role:** RabbitMQ (a queue, not a replayable log; fan-out to independent consumers with separate offsets is not natural); Redis Streams (fine at small scale, but memory-bound and weaker in the ecosystem); NATS JetStream (excellent, but not what the rubric asks for); **Redpanda** (Kafka-API compatible and lighter, a valid drop-in for local dev; we keep Apache Kafka for authenticity).

## ADR-06 · Kafka client: @confluentinc/kafka-javascript
- **Why:** officially supported by Confluent, built on **librdkafka** (the C client behind most production Kafka), with a KafkaJS-compatible API so migrating from the prototype is mostly mechanical. It supports idempotent producers and transactions properly.
- **Rejected:** KafkaJS (the prototype uses it; pure JS and easy to start with, but no release since 2023 and effectively unmaintained); node-rdkafka (older API, superseded by the Confluent client).
- **Risk:** native binary. Prebuilt binaries exist for Linux, macOS and Windows x64. If one fails on a dev machine, KafkaJS stays as a fallback behind a small `EventBus` interface.

## ADR-07 · Event contracts: Zod schemas + versioned JSON envelope
- **Why:** human-readable in Kafka UI (good for the demo and for debugging), with one schema package shared by every producer and consumer, runtime validation at every boundary, and generated TS types. The envelope carries `schemaVersion`, `type`, `eventId`, `occurredAt` and `traceparent`.
- **Rejected for now:** Protobuf/Avro + Schema Registry (smaller payloads and enforced compatibility rules; the right call at ~33k events/s across many teams, but too much ceremony for one team and one deadline). The envelope is designed so we can switch encodings without changing topic semantics.

## ADR-08 · Auth: OIDC Authorization Code + PKCE using the BFF pattern
- **Why:** the IETF *OAuth 2.0 for Browser-Based Apps* BCP recommends a Backend-for-Frontend: tokens stay on the server and the browser holds only an HttpOnly, Secure, SameSite cookie. The same cookie authenticates the Socket.IO handshake, so no tokens exist in JavaScript to steal through XSS.
- **IdP:** **Keycloak** in Docker for local development (zero signup, test users, realm-as-code). **Auth0** or **Google** in production by changing env vars. Library: `openid-client` v6 (certified OIDC RP).
- **Rejected:** storing access tokens in localStorage (XSS exposure); sending a JWT in the socket `auth` payload (workable, but you have to handle refresh and revocation yourselves); Firebase/Supabase auth (vendor lock-in, and it hides the OIDC mechanics being graded).

## ADR-09 · Hot state: Redis 7 (or Valkey 8)
- **Used for:** session store (any gateway can read any session); **live presence** using `GEOADD` + a per-user TTL key (also enables "who is near me" queries); per-user rate limiting (token bucket in Lua); short-term dedupe (`SET eventId NX EX 300`); Socket.IO Redis *streams* adapter for cross-instance room emits (control messages only, not the location firehose).
- **Rejected:** in-process maps (the prototype; breaks with two or more gateway instances); Memcached (no GEO, no TTL-per-member, no pub/sub).

## ADR-10 · Durable store: PostgreSQL 17 + PostGIS + TimescaleDB, Drizzle ORM
- **Why:** location history is a **time series with geometry**. TimescaleDB hypertables partition by time automatically, compress old chunks ~10–20× and drop expired data with a retention policy (no DELETE storms). PostGIS gives `geography(Point)`, distance and geofence queries. It's one database engine with SQL, transactions and mature tooling.
- **ORM:** Drizzle: typed SQL, lightweight, migrations as code. Hot-path bulk inserts use raw multi-row `INSERT … ON CONFLICT` or `COPY`.
- **Rejected:** MongoDB (time-series collections exist, but weaker geo and transaction semantics for this access pattern); Cassandra/ScyllaDB (great write scale, but you design every query up front and run a cluster; justified only far beyond our target); InfluxDB (no relational data, weak geo); plain Postgres (works, but manual partitioning and retention).
- **Hosting:** Timescale Cloud or self-hosted on the VM. *Note:* many managed Postgres services (e.g. Neon) ship only the Apache-licensed TimescaleDB, which **lacks compression**; check before choosing.

## ADR-11 · Observability: OpenTelemetry + Prometheus + Grafana + Loki, pino logs
- **Why:** vendor-neutral. A single trace can follow one location event: `socket.receive → kafka.produce → kafka.consume(broadcaster) → socket.emit` and `kafka.consume(history) → db.insert`, with `traceparent` carried in Kafka headers. Prometheus scrapes service and Kafka consumer-lag metrics; Grafana shows the dashboards; Loki stores logs.
- **Rejected:** a vendor APM agent (lock-in); console logs only (the prototype).

## ADR-12 · Testing: Vitest · Testcontainers · Playwright · Artillery
- See [11-testing-strategy.md](11-testing-strategy.md). Playwright can **grant geolocation permission and set coordinates**, so the full permission-to-marker flow can be tested end to end in CI.

## ADR-13 · Runtime & deployment
- **Local:** Docker Compose (Kafka, Kafka UI, Keycloak, Redis, Timescale, Grafana stack).
- **Demo/production (pragmatic):** one VM (Hetzner, DigitalOcean or AWS Lightsail) running Compose behind **Caddy** with automatic HTTPS. This is the cheapest way to host Kafka plus everything else under a real URL.
- **Production (target design):** Kubernetes with **Strimzi** for Kafka (or Confluent Cloud/MSK), HPA on gateway connections, PodDisruptionBudgets. Documented and templated; not required for submission.
- **CI/CD:** GitHub Actions → GHCR images → deploy over SSH (VM) or Argo CD (Kubernetes).

---

## Summary: prototype → v1

| Area | Prototype (now) | v1 (planned) |
|---|---|---|
| Language | JS (ESM) | TypeScript, strict |
| HTTP | Express 5 | Fastify 5 |
| Kafka client | KafkaJS | @confluentinc/kafka-javascript |
| Contracts | hand-rolled validation | Zod in `packages/contracts` |
| Sessions | in-memory | Redis |
| Presence | in-process Map | Redis GEO + TTL |
| DB | Postgres | Postgres 17 + PostGIS + TimescaleDB, Drizzle |
| Frontend | vanilla JS + Leaflet | React 19 + Vite + react-leaflet + Zustand + Tailwind |
| Observability | JSON console logs | OTel traces + Prometheus + Grafana + Loki |
| Tests | node:test | Vitest + Testcontainers + Playwright + Artillery |
