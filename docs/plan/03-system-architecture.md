# 03 · System Architecture

## Context (C4 level 1)

```mermaid
flowchart LR
  U[User's browser] -- HTTPS / WSS --> P[Live Location Tracker]
  P -- OIDC --> IDP[(Identity provider<br/>Keycloak / Auth0 / Google)]
  U -- login redirect --> IDP
  P -- tiles --> OSM[(Map tiles<br/>OSM / MapTiler)]
  U -- tiles --> OSM
```

## Containers (C4 level 2)

```mermaid
flowchart TB
  subgraph Browser
    WEB[web<br/>React + Leaflet + socket.io-client]
  end

  subgraph Edge
    CADDY[Caddy / Ingress<br/>TLS, sticky sessions]
  end

  subgraph Services
    GW[gateway ×N<br/>Fastify + Socket.IO<br/>BFF auth · validate · produce · fan-out]
    HW[history-writer ×M<br/>Kafka consumer → Timescale]
    PR[presence-projector ×1..P<br/>Kafka consumer → Redis GEO]
    SIM[simulator<br/>load / demo producer]
  end

  subgraph Data
    K[(Kafka<br/>location-updates · presence-events · *.dlq)]
    R[(Redis<br/>sessions · presence · rate limits · dedupe)]
    DB[(Postgres 17<br/>TimescaleDB + PostGIS)]
  end

  WEB <--> CADDY <--> GW
  GW -- produce key=userId --> K
  K -- group: broadcaster-&lt;pod&gt; --> GW
  K -- group: history-writer --> HW --> DB
  K -- group: presence-projector --> PR --> R
  GW <--> R
  GW -- history API --> DB
  SIM -- produce --> K
```

### Responsibilities

| Container | Owns | Does NOT do |
|---|---|---|
| **web** | UI, geolocation, send loop, map rendering | trust anything it receives without validating it |
| **gateway** | OIDC BFF endpoints, socket auth, validation, dedupe, rate limit, **produce**; consumes the topic to **fan out** to its own sockets | write to the DB; broadcast before Kafka acknowledges |
| **history-writer** | batched, downsampled, idempotent history writes; latest-position table | anything real-time |
| **presence-projector** | maintains the Redis presence view (GEO set + last-seen), emits `user.stale` events | socket I/O |
| **simulator** | synthetic riders straight into Kafka (demo + load) | bypass the contracts package |

> Why a separate **presence-projector** instead of each gateway keeping its own map (as the prototype does)? A newly started gateway can serve a correct `presence:snapshot` immediately from Redis, and stale detection runs **once** for the cluster instead of once per pod. Scaling is cheap: it's partitioned by userId like everything else.

## Hot path: one location update

```mermaid
sequenceDiagram
  autonumber
  participant B as Browser (Alice)
  participant G as Gateway pod A
  participant R as Redis
  participant K as Kafka
  participant G2 as Gateway pod B
  participant O as Browser (Bob)

  B->>G: location:update {eventId, lat, lng, ts} (ack cb)
  G->>G: session → userId · Zod validate
  G->>R: SET dedupe:{eventId} NX EX 300 · token-bucket check
  G->>K: produce(topic=location-updates, key=userId, headers{traceparent})
  K-->>G: ack (partition, offset)   [acks=all, idempotent producer]
  G-->>B: ack {ok, partition, offset}
  par every gateway pod consumes (own group)
    K-->>G: record
    K-->>G2: record
  end
  G2->>O: location:broadcast (only to rooms that should see Alice)
```

**Latency budget (p99 500 ms):** validate + Redis 5 ms · produce with `acks=all` 10–30 ms · consumer poll ≤ 100 ms (`fetch.wait.max.ms=50–100`) · emit 5 ms · network both ways 100–200 ms.

## Cold path: history

```mermaid
sequenceDiagram
  participant K as Kafka
  participant H as history-writer
  participant DB as Timescale
  K-->>H: batch (fetch.min.bytes=16KB or 2s)
  H->>H: validate · downsample (10 m / 30 s) · collapse latest-per-user
  H->>DB: BEGIN · INSERT … ON CONFLICT(event_id) DO NOTHING · UPSERT latest · COMMIT
  H->>K: commit offsets (only after COMMIT)
  Note over H,DB: crash before offset commit → batch redelivered → idempotent no-op
```

## Failure modes (summary; detail in [09](09-observability-reliability.md))

| Failure | User impact | Behaviour |
|---|---|---|
| Postgres down | none on live map | history-writer pauses and lag grows; catches up on recovery (Kafka keeps 24–72 h) |
| Kafka broker lost (RF=3) | none | leader election; producer retries (idempotent, no duplicates) |
| Kafka cluster unavailable | sharing paused | gateway acks `{ok:false, retryable}`; client backs off; `/readyz` fails so new connections go elsewhere |
| Gateway pod dies | its sockets reconnect | Socket.IO auto-reconnect to another pod; session in Redis means no re-login |
| Redis down | rate limit/dedupe fail open; new logins fail | live updates continue (Kafka path); alerts fire |
| IdP down | new logins fail | existing sessions continue until they expire |
