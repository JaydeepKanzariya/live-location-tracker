# 10 · Infrastructure & DevOps

## Monorepo layout (v1)

```
live-location-tracker/
├── apps/
│   ├── web/                 React + Vite + Leaflet
│   ├── gateway/             Fastify + Socket.IO + producer + broadcaster consumer
│   ├── history-writer/      Kafka → Timescale
│   ├── presence-projector/  Kafka → Redis GEO, stale detection
│   └── simulator/           synthetic riders → Kafka
├── packages/
│   ├── contracts/           Zod event + socket schemas, TS types (single source of truth)
│   ├── kafka/               EventBus wrapper (producer/consumer, tracing headers, DLQ helper)
│   ├── config/              typed env loading (zod) shared by all apps
│   ├── observability/       OTel + pino setup
│   └── tsconfig/ eslint-config/
├── infra/
│   ├── compose/             docker-compose.yml (+ .observability.yml overlay)
│   ├── keycloak/            realm export
│   ├── grafana/             dashboards + datasources as code
│   ├── caddy/               Caddyfile
│   └── k8s/                 (COULD) Helm chart / Kustomize + Strimzi Kafka CR
├── docs/plan/               ← these documents
├── legacy/ (or tag v0-prototype)   current JS prototype, kept for reference
├── turbo.json  pnpm-workspace.yaml  .env.example  README.md
```

## Local development

```bash
pnpm i
pnpm infra:up      # kafka, kafka-ui, keycloak, redis, timescale (+ grafana stack with --profile obs)
pnpm dev           # turbo runs web (5173), gateway (3000), history-writer, presence-projector
pnpm simulate --riders 50
```

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

```
on PR:
  install (pnpm cache) → lint (eslint) → typecheck (tsc -b) → unit (vitest)
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
