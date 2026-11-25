# 12 · Roadmap (2026-11-25 → 2026-12-31)

## Strategy: strangler, not rewrite-in-place

The current JS prototype **stays untouched and working** the whole time. It is the safety net: if v1 slips, the prototype already meets the MUST tier. v1 is built in `apps/` and `packages/` next to it. When v1 reaches parity (end of week 3), the prototype is tagged `v0-prototype` and moved to `legacy/` (or deleted from `main`, but kept in git history).

## Timeline

| Week | Dates | Goal | Deliverables | Exit criteria |
|---|---|---|---|---|
| **0** | Nov 25–27 | Foundations | pnpm + Turborepo skeleton, `packages/contracts|config|kafka|observability`, Compose v2 (Kafka 4, Redis, Timescale+PostGIS, Keycloak), CI (lint/typecheck/unit) | `pnpm dev` boots empty apps; CI green |
| **1** | Nov 28–Dec 4 | Auth + gateway | Fastify BFF OIDC (PKCE, Redis sessions, `__Host-` cookie, logout), Socket.IO auth + Origin check, Zod validation, Redis dedupe + token bucket, idempotent producer, broadcaster consumer | Integration tests 1, 2, 5, 6, 9 pass; alice and bob see each other with the vanilla prototype UI pointed at the new gateway |
| **2** | Dec 5–11 | Pipelines | history-writer (batch, downsample, Timescale, pause/resume, DLQ), presence-projector (Redis GEO, stale events), `presence-events` topic, history API | Tests 3, 4, 7, 8, 10 pass; Kafka UI shows 3+ groups with lag ≈ 0 |
| **3** | Dec 12–18 | Frontend v1 | React + Vite + react-leaflet, Zustand presence, imperative marker layer, all UX states, history layer, diagnostics panel | Playwright two-user E2E green; **parity with the prototype → tag `v0-prototype`** |
| **4** | Dec 19–24 | Production-grade | OTel traces through Kafka, Grafana dashboards, batched `location:batch` emits, Dockerfiles, Caddy, deploy to VM with HTTPS, Artillery load test (1k sockets) | Live URL works with Auth0/Keycloak; dashboard screenshot; load-test numbers in README |
| **5** | Dec 25–31 | Ship | README (all required sections + architecture diagrams + trade-offs + load numbers), `.env.example`, **demo video**, buffer for fixes | Submitted by **Dec 30** (one day of buffer) |
| Stretch | if ahead | COULD tier | H3 viewport fan-out, history playback scrubber, Protobuf + Schema Registry, K8s manifests | – |

## Risks & mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| `@confluentinc/kafka-javascript` native build issues on Windows | medium | `packages/kafka` hides the client behind an interface; KafkaJS adapter as fallback |
| Hosting Kafka for the live link | medium | single VM with Compose (cheapest, full control); Confluent Cloud free trial as a backup |
| Scope creep ("world's best") | high | strict MUST/SHOULD/COULD tiers; COULD items only after week 4 exit criteria |
| Keycloak/HTTPS redirect mismatch in deploy | medium | realm-as-code with env-substituted redirect URIs; smoke test logs in on every deploy |
| Geolocation inaccurate on desktop for the demo | high | "Simulate walking" + simulator riders; record one segment on a phone |

## Demo video script (8–10 min)

1. **Architecture (1.5 min):** the diagram from [03](03-system-architecture.md); one sentence per component.
2. **Login (1 min):** alice in a normal window, bob in incognito, through the Keycloak screen; show the HttpOnly cookie and that no token is in JS.
3. **Permission (0.5 min):** prompt → allow; show the denied state + simulate fallback.
4. **Live movement (1.5 min):** both sharing; markers glide; diagnostics show partition/offset per update.
5. **Kafka (2 min):** Kafka UI → `location-updates` messages keyed by userId → consumer groups `broadcaster-*`, `history-writer`, `presence-projector`; explain split vs fan-out.
6. **Resilience (1.5 min):** stop history-writer → map still live, lag grows → restart → lag drains; send a duplicate → ignored; close bob's tab → marker removed.
7. **Persistence (1 min):** history layer from Timescale; explain downsampling + batching numbers.
8. **Trade-offs (1 min):** why Kafka, why not direct DB writes, how it scales (viewport fan-out).
