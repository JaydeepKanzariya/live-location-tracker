# 10 · Infrastructure & DevOps

## Monorepo layout (v1)

> Updated 2026-11-26 to match the repository after Phase 0. Items marked *(Phase N)* don't exist yet.

```
live-location-tracker/
├── apps/
│   ├── web/                 React + Vite + Leaflet
│   ├── gateway/             Fastify + Socket.IO + producer + broadcaster consumer
│   ├── history-writer/      Kafka → Timescale (consumer group `location-db-writer`)
│   ├── presence-projector/  Kafka → Redis GEO, stale detection
│   └── simulator/           synthetic riders → Kafka
├── packages/
│   ├── contracts/           Zod 4 event + socket schemas, TS types (single source of truth)
│   ├── kafka/               EventBus (Confluent client, KafkaJS fallback, in-memory test bus), DLQ helper
│   ├── config/              typed env loading (Zod) with production guards, shared by all apps
│   ├── observability/       OTel trace headers + pino setup with redaction
│   └── tsconfig/            shared TypeScript settings (ESLint config lives in root eslint.config.mjs)
├── infra/
│   ├── compose/             docker-compose.yml + smoke-test.sh (observability overlay: Phase 4)
│   ├── keycloak/            realm export (shared with the prototype)
│   ├── grafana/             dashboards + datasources as code (Phase 4)
│   ├── caddy/               Caddyfile (Phase 4)
│   └── k8s/                 (COULD) Helm chart / Kustomize + Strimzi Kafka CR
├── docs/plan/               ← these documents
├── src/ public/ test/ scripts/ docker-compose.yml
│                            v0 JS prototype, kept at the root until v1 reaches parity,
│                            then moved to legacy/ (owner approval required)
├── turbo.json  package.json (npm workspaces)  tsconfig.base.json  eslint.config.mjs  .env.example
```

## Local development

```bash
npm install
npm run v1:infra:up      # kafka, kafka-ui, keycloak, redis, timescale+postgis
npm run v1:infra:smoke   # proves the stack: topics, Keycloak realm, Redis, Postgres extensions, Kafka UI
npm run v1:dev           # turbo runs the apps (web 5173, gateway 3000, history-writer, presence-projector)
npm run v1:test          # unit tests, no Docker needed
```

The prototype's root `docker-compose.yml` uses the same ports; run one stack at a time.

Vite proxies `/auth`, `/api` and `/socket.io` to the gateway, so the browser sees **one origin** (needed for cookie auth).

## Environments

| Env | Where | Kafka | DB | IdP |
|---|---|---|---|---|
| local | Docker Compose | apache/kafka:4 single node | timescale/timescaledb-ha (PostGIS incl.) | Keycloak container |
| ci | GitHub Actions + Testcontainers | container per test suite | container | Keycloak container |
| demo/prod | 1 VM (4 vCPU / 8 GB) with Compose + Caddy auto-TLS | same images, RF=1 (documented limitation) | same | Auth0 or Keycloak on the VM |
| prod (target) | Kubernetes | Strimzi 3 brokers or Confluent Cloud | Timescale Cloud | Auth0 / Entra ID |

## Containers
- Multi-stage Dockerfiles: `node:22-alpine` build → `gcr.io/distroless/nodejs22` runtime, non-root, read-only FS.
- `web` builds to static files served by Caddy (or a CDN), not by Node.
- Images tagged with the git SHA and pushed to GHCR.

## CI/CD (GitHub Actions)

**In place now** (`.github/workflows/ci.yml`, runs on push and PR to `main`):
- `verify`: `npm ci` → lint → typecheck → build → v1 unit tests → prototype tests
- `infra-smoke`: `docker compose up` on the GitHub runner → `smoke-test.sh` → logs on failure → `down -v`

**Target** (added phase by phase):

```
on PR:
  install (npm cache) → lint (eslint) → typecheck (tsc -b) → unit (vitest)
  → integration (testcontainers: kafka, redis, timescale, keycloak)
  → e2e (playwright, compose stack, 2 browser contexts, mocked geolocation)
  → build images → trivy scan → gitleaks
on main:
  all of the above → push images → deploy (ssh + docker compose pull/up on VM)
  → smoke test (/readyz, login via playwright) → automatic rollback to previous tag on failure
```

Turborepo remote cache keeps PR runs fast; only affected apps are rebuilt and tested.

## Kubernetes target (documented, COULD)
- Gateway `Deployment` + HPA on `socket_connections` (custom metric) and event-loop lag; `PodDisruptionBudget`; `preStop` drain hook (see [05](05-realtime-gateway.md#connection-lifecycle)).
- Ingress (NGINX) with WebSocket support and cookie affinity.
- Consumers as `Deployment`s; replicas ≤ partition count; KEDA can scale on Kafka lag.
- Strimzi `Kafka` + `KafkaTopic` CRs (topics as code), `min.insync.replicas=2`.
- Secrets via External Secrets Operator.

## Code quality gates
- TypeScript `strict`, `noUncheckedIndexedAccess`.
- ESLint (typescript-eslint, import order), Prettier, **conventional commits** + commitlint, Husky pre-commit (lint-staged).
- PR template: what/why, screenshots, test evidence. Branch protection: CI green + 1 review.
