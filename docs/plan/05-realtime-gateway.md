# 05 · Realtime Gateway (Fastify + Socket.IO)

## Socket protocol (v1)

All events are typed in `packages/contracts/socket.ts` and shared by client and server (`Server<ClientToServer, ServerToClient>`).

| Dir | Event | Payload | Ack / notes |
|---|---|---|---|
| S→C | `session:ready` | `{ user, settings{minIntervalMs, staleAfterMs, heartbeatMs} }` | after an authenticated handshake |
| S→C | `presence:snapshot` | `PresenceEntry[]` | read from Redis (cluster-wide) |
| C→S | `location:update` | `{ eventId, lat, lng, accuracyM, headingDeg, speedMps, occurredAt, source }` | ack `{ok:true, partition, offset}` · `{ok:true, duplicate:true}` · `{ok:false, code, retryAfterMs?}` |
| C→S | `sharing:stop` | `{}` | produces `presence.offline` |
| C→S | `viewport:set` *(COULD)* | `{ bbox }` | server joins the socket to the H3 cell rooms covering the bbox |
| S→C | `location:batch` | `PresenceEntry[]` | **batched** every 100–250 ms per socket (see below) |
| S→C | `user:offline` | `{ userId, reason: 'stopped'│'disconnected'│'stale' }` | |
| S→C | `auth:expired` | `{}` | client redirects to login |

Error codes are a closed enum: `INVALID_PAYLOAD`, `RATE_LIMITED`, `SESSION_EXPIRED`, `UNAVAILABLE`.

## Request pipeline per `location:update`

```
auth check (socket.data.user, session expiry)
 → Zod parse (reject: INVALID_PAYLOAD)
 → Redis dedupe  SET loc:dedupe:{eventId} NX EX 300   (exists → ack duplicate)
 → Redis token bucket per userId (1/s, burst 3)        (empty → RATE_LIMITED + retryAfterMs)
 → enrich (userId, displayName, receivedAt, h3r7, traceparent)
 → produce (await ack)                                  (error → UNAVAILABLE, release dedupe key)
 → ack client {partition, offset}
```

## Fan-out design

### Phase 1 (MUST): broadcast to all authenticated users
Each gateway pod has its own consumer group and receives every event. It emits to the `authenticated` room on **its own** sockets. No Redis adapter is needed for the firehose.

### Batching outbound messages (SHOULD)
Instead of one `emit` per Kafka record, each pod collects records for 100–250 ms and sends **one `location:batch`** per socket, keeping only the newest record per user (later updates overwrite earlier ones). At 1,000 users this cuts socket frames ~10× and keeps the browser's main thread calm. Markers still look continuous because the client animates between positions.

### Viewport-scoped fan-out (COULD, the scale answer)
Broadcasting to everyone is O(N²). The fix:
1. The gateway tags every event with an **H3 cell** (resolution 7, about 5 km²).
2. The client sends `viewport:set` whenever the map moves; the server computes the covering cells (`polygonToCells`) and joins the socket to `cell:<h3>` rooms (capped at ~50 cells; if zoomed out, it falls back to a coarser resolution or shows clustered counts).
3. The broadcaster emits each event only to `cell:<event.h3r7>`.

Then each viewer gets only nearby users: about 1.7 M msgs/s at 100k users instead of 3.3 B (see [01](01-requirements.md#capacity-math-design-target)).

## Connection lifecycle

| Event | Handling |
|---|---|
| connect | session from cookie (Redis store) → reject `unauthorized` if absent · join `user:<sub>` + `authenticated` · send `session:ready` + `presence:snapshot` |
| disconnect | if the socket was sharing: start a **5 s grace timer**; when it fires, check in Redis whether the user has any socket on any pod (`presence:sockets:<sub>` counter); if 0 → produce `presence.offline` |
| server shutdown (SIGTERM) | stop accepting → `/readyz` 503 → emit `server:draining` → close sockets gradually over 10 s (avoids a reconnect stampede) → flush producer → leave consumer group |
| session expiry | checked on each event + by a periodic sweep → `auth:expired` + disconnect |

## Scaling & load balancing

- **Sticky sessions** are required only if HTTP long-polling is enabled. Plan: `transports: ['websocket', 'polling']` with Caddy/NGINX cookie-based stickiness, or WebSocket-only if every client supports it.
- **Capacity per pod (Node, 1 vCPU):** ~10–20k idle sockets, ~50k outbound msgs/s with batching. Autoscale on **active connections** and **event-loop lag**, not CPU.
- **Socket.IO Redis Streams adapter** is used only for *targeted* cross-pod emits (e.g. `auth:expired` to every socket of a user). The location firehose never goes through it.
- **Backpressure:** if a socket's buffered bytes exceed a threshold (slow mobile client), drop intermediate batches for it. The newest position is all that matters.

## Client send strategy

- `watchPosition` keeps only the latest fix; a timer sends at **3 s** (user-adjustable 2/5/10 s).
- Send only if moved ≥ 2 m **or** ≥ 10 s passed (heartbeat). Stale threshold is 30 s, giving ~3 missed heartbeats of tolerance.
- **Adaptive (SHOULD):** use `speed` to choose the interval: 2 s when moving fast, 10 s when stationary. Saves battery and roughly halves traffic.
- Retry on ack timeout (5 s) **with the same `eventId`** at most once; then drop the fix (a newer one will come).
- While disconnected, don't queue old positions. On reconnect, send the newest one only.
