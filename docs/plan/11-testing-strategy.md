# 11 · Testing Strategy

| Level | Tool | Runs | What it proves |
|---|---|---|---|
| **Unit** | Vitest | every commit, < 10 s | contracts (Zod accept/reject tables), downsampling planner, token bucket, presence reducer, H3 cover logic |
| **Component (web)** | Vitest + Testing Library | every commit | sharing panel states (prompt/denied/granted), user list, store reducers |
| **Integration** | Vitest + **Testcontainers** (Kafka, Redis, Timescale, Keycloak) | every PR, ~2 min | produce→consume round trip, both consumer groups get the event, history row written once despite redelivery, DLQ on poison message, offsets committed only after DB COMMIT, socket handshake rejected without a session |
| **Contract** | shared `packages/contracts` + type tests | compile time | gateway, consumers and web agree on every payload |
| **E2E** | **Playwright** | every PR | two browser contexts log in via Keycloak (alice, bob), `context.grantPermissions(['geolocation'])` + `setGeolocation()` moves alice → bob's map marker moves; logout removes the marker |
| **Load** | **Artillery** (socket.io engine) + simulator | before release / nightly | 1k–10k sockets at 3 s intervals; e2e p99, gateway CPU, consumer lag |
| **Chaos** (COULD) | scripts | manual | kill Postgres → map keeps working, lag recovers; kill a gateway → clients reconnect without re-login |

## Key test cases (must exist)

1. **Spoofing:** payload containing `userId: "bob"` from alice's socket → event `userId === alice.sub`.
2. **Duplicate:** same `eventId` sent twice → one Kafka record, ack `duplicate:true`.
3. **Redelivery:** history-writer processes a batch, crashes before committing (simulated) → restart → row count unchanged.
4. **Out of order:** older `receivedAt` after newer → presence and latest-location unchanged.
5. **Invalid:** lat 91, NaN, string numbers, missing eventId, unknown keys, 10 KB payload → rejected with the right code; nothing produced.
6. **Rate limit:** 5 updates in 1 s → 1–3 accepted (bucket), rest `RATE_LIMITED` with `retryAfterMs`.
7. **Disconnect:** close the only tab → `user:offline` after the grace period; reload within the grace period → no offline.
8. **Stale:** stop heartbeats → removed after 30 s on every client.
9. **Auth expiry:** session TTL passes → `auth:expired`, socket closed.
10. **DB outage:** stop Postgres for 60 s while sending → no client errors; after restart all rows present (minus downsampled).

## Coverage policy
Line coverage is not the goal. Every **requirement ID** in [01](01-requirements.md) maps to at least one automated test, tracked in a table in the README.
