# Build prompt: Live Location Tracker v1

> Copy everything below the line into the other AI coding agent (Claude Code, Cursor, Codex, etc.), with the repository open.

---

You are a senior full-stack engineer building **Live Location Tracker v1** in this repository. The planning is finished. Your job is to implement it faithfully, in phases, and stop at the end of each phase so the owner can review.

## 1. Read first, in this order

1. `DESIGN.md`: the visual rules (short).
2. `docs/plan/00-overview.md`: stack summary and scope tiers (MUST / SHOULD / COULD / WON'T).
3. `docs/plan/01-requirements.md` to `docs/plan/12-roadmap.md`: the full plan. `docs/PLAN-COMPLETE.md` is the same content in one file.
4. `docs/plan/08-frontend.md` has three parts. **Part 3 (visual system) overrides Part 2 (screens and behaviour) on colours, panel styling and marker animation. Part 2 overrides Part 1.** Read the reconciliation tables at the end of Parts 2 and 3.

The existing JavaScript prototype (`src/`, `public/`, `test/`, `scripts/`, root `package.json`) works and passes its tests. **Do not delete or break it.** It is the fallback submission. Build v1 next to it in `apps/` and `packages/`, as the plan describes. When v1 reaches parity, move the prototype to `legacy/` in a separate commit, only after the owner approves.

## 2. What this is for (so you make the right trade-offs)

A student assignment graded on a rubric, and the owner must **explain every part in a demo video**. The graders check that Kafka is really in the flow, auth is tied to the socket, the DB write strategy is understood, and the map works. Therefore:

- **Fixed choices (not negotiable):** OIDC / OAuth 2.0, **Socket.IO**, **Apache Kafka** (topic `location-updates`), **Leaflet**.
- **Readable beats clever.** No abstractions that need a diagram to follow. Name things plainly. Add a short comment explaining *why* wherever a decision isn't obvious (consumer group choice, commit-after-write, user id from session, etc.).
- Build the **MUST** tier completely and correctly before touching SHOULD. Do **not** build COULD or WON'T items unless the owner asks.

## 3. The stack (decided; see `docs/plan/02-tech-stack.md`)

TypeScript (strict) on Node 22 · npm workspaces + Turborepo (changed from pnpm; see ADR-02) · Fastify 5 + Socket.IO 4 · Apache Kafka 4 (KRaft) via `@confluentinc/kafka-javascript` · Zod event contracts in `packages/contracts` · OIDC code + PKCE, BFF pattern, `openid-client`, Keycloak locally · Redis (sessions, presence, rate limits, dedupe) · PostgreSQL 17 + PostGIS + TimescaleDB with Drizzle · React 19 + Vite, react-leaflet, Zustand, TanStack Query, Tailwind + shadcn/ui, Motion · OpenTelemetry + Prometheus + Grafana + Loki · Vitest, Testcontainers, Playwright, Artillery.

**Verify before installing:** check the current stable version of every package and that its API still matches the plan. If something in the plan is outdated or won't install on Windows (especially the native Kafka client), **stop and tell the owner** and propose a fallback. The plan names KafkaJS behind an `EventBus` interface as the fallback.

## 4. Non-negotiable rules

1. **Kafka is the only path to other users' screens.** The gateway never emits a location straight to other sockets. A marker moves only after the event came back out of Kafka.
2. **`userId` always comes from the authenticated session** (OIDC `sub`), never from the client payload. Payload schemas are strict; unknown keys are rejected. Add a test for this.
3. **Every write is idempotent** (dedupe by `eventId`; `ON CONFLICT DO NOTHING`). Kafka offsets are committed **after** the DB transaction commits.
4. **The DB processor is a separate process** with its own consumer group (`location-db-writer`). Gateway instances use a **per-instance** group id. Explain both in code comments.
5. **Bad data never crashes a consumer.** Validate on every consumer; poison messages go to the DLQ topic.
6. **No secrets in git.** `.env` is ignored; keep `.env.example` complete and accurate. Never log raw coordinates at INFO.
7. **Tokens never reach browser JavaScript.** Session cookie only: `HttpOnly`, `Secure` in production, `SameSite=Lax`.
8. **Follow `DESIGN.md`.** One accent colour (orange for "you"), glass only on the three small floating pieces, CARTO tiles (not default OSM), markers moved with `requestAnimationFrame` (not a CSS transition), follow-me camera that never fights a manual pan or zoom.
9. **Keep numbers consistent with the plan** (heartbeat 15 s, stale fade 25 s, client removal 35 s, server stale 30 s). If the plan contradicts itself, use the reconciliation tables; if still unclear, ask.

## 5. Environment notes

- The owner uses **Windows 11**. The scripts must work in PowerShell and Git Bash.
- Docker may **not** be installed on the owner's machine. At the start of Phase 0, check `docker --version`. If it is missing, say so plainly and ask how to proceed (install Docker Desktop, or use a cloud Kafka and Postgres); do not pretend infrastructure runs.
- The prototype was **never run against a real Kafka, Keycloak or Postgres**. Treat its behaviour as unproven until you run it for real.

## 6. Phases (stop after each one)

At the end of each phase: run the checks listed, update `docs/STATUS.md`, and post a short report (what was built, what was verified and how, what is not done, any plan deviations). **Do not start the next phase until the owner says so.**

**Phase 0: Foundations.** Monorepo skeleton (`apps/*`, `packages/*`), `packages/contracts|config|kafka|observability`, Docker Compose v2 (Kafka, Kafka UI, Keycloak with realm import, Redis, Timescale+PostGIS), CI (lint, typecheck, unit).
*Check:* `npm install`, `npm run v1:build`, `npm run v1:test` pass from a clean clone; `npm run v1:infra:up` + `npm run v1:infra:smoke` pass (also runs in CI). **Status: done 2026-11-26 with review fixes; CI green including the infra smoke test.**

**Phase 1: Auth and gateway.** Fastify BFF OIDC (PKCE, Redis sessions, logout), Socket.IO handshake auth with Origin check, Zod validation, Redis dedupe and token bucket, idempotent producer, broadcaster consumer.
*Check:* integration tests (Testcontainers) for: unauthenticated socket rejected, spoofed `userId` ignored, duplicate `eventId` produces one Kafka record, rate limit, invalid payloads, session expiry. A real login through Keycloak works in a browser.

**Phase 2: Pipelines.** `history-writer` (batch, downsample, Timescale, pause/resume on DB failure, DLQ), `presence-projector` (Redis GEO, stale events), `presence-events` topic, history API.
*Check:* replay test (crash before offset commit, then restart, row count unchanged); stop Postgres for 60 s while publishing, then confirm all rows arrive after restart; Kafka UI shows all consumer groups with lag near 0.

**Phase 3: Frontend.** React + Vite app per `docs/plan/08-frontend.md` Parts 2 and 3, including all states (signed out, permission prompt/denied/unavailable, reconnecting, expired session, empty, history).
*Check:* Playwright two-user test (alice and bob logged in through Keycloak, geolocation mocked) proves alice's marker moves on bob's map and disappears after logout. Run through `DESIGN.md`'s "not made by a template" checklist. Take screenshots.

**Phase 4: Production-grade.** OpenTelemetry traces through Kafka headers, Grafana dashboards, batched `location:batch` emits, Dockerfiles, Caddy, deploy to a VM with HTTPS, Artillery load test (1k sockets).
*Check:* live URL logs in and shows two users moving; trace of one event across gateway, Kafka and consumers; load-test numbers recorded.

**Phase 5: Ship.** Final README (all sections the assignment requires, architecture diagram, trade-offs, load numbers), accurate `.env.example`, demo script.
*Check:* a fresh clone follows the README and works.

## 7. Working rules

- **Small commits**, conventional commit messages, one concern each. Don't commit generated files, `node_modules` or secrets.
- **Tests alongside code.** Every requirement ID in `docs/plan/01-requirements.md` maps to at least one automated test.
- **Run it for real.** Never claim something works because it compiles or because a mock passes. State exactly what you ran and what you saw. If a check fails or you could not run it, say so; do not hide it.
- **Ask, don't guess,** when the plan is ambiguous, a dependency won't install, or a decision would cost the owner marks. List options with a recommendation.
- **No scope creep.** Note ideas in `docs/STATUS.md` under "Later" instead of building them.
- **Security and privacy:** follow `docs/plan/06-auth-security.md`. Location data is sensitive.
- Keep `docs/STATUS.md` current: current phase, done, verified (with command output summary), not done, known issues, decisions that deviate from the plan.

## 8. Handoff for code review

The owner will bring the finished code back for review. So at every phase end, make review easy:

- `docs/STATUS.md` up to date as above.
- A short "how to run and verify this phase" section with exact commands.
- A list of the files that matter most for the phase (not every file).
- Any place you are unsure about, flagged honestly with the reason.

## 9. Definition of done (whole project)

- Every MUST item in `docs/plan/00-overview.md` works and is shown by an automated test or a recorded manual check.
- A fresh clone plus the README gets two users seeing each other move, with Kafka in the path.
- CI is green. No secrets in git. `.env.example` matches the code.
- The README contains: overview, tech stack, setup, environment variables, OIDC setup, socket event flow, Kafka event flow, demo video link, assumptions and limitations.

Start with Phase 0. First reply with: (1) the results of your environment checks, (2) any plan items you think are risky or outdated, and (3) the exact list of files you plan to create. Then wait for the owner's go-ahead.
