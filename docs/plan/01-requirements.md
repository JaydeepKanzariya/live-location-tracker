# 01 · Requirements & Capacity

## Functional requirements

| ID | Requirement | Tier |
|---|---|---|
| F1 | Users sign in via OIDC / OAuth 2.0; signing out ends the IdP session too | MUST |
| F2 | Only authenticated users can open a socket or send locations | MUST |
| F3 | The browser asks for location permission and handles granted / denied / unavailable | MUST |
| F4 | The client sends location at a controlled interval (default 3 s, plus a heartbeat when stationary) | MUST |
| F5 | The gateway validates, deduplicates and rate-limits, then publishes to `location-updates` | MUST |
| F6 | A broadcaster consumer group pushes updates to connected clients | MUST |
| F7 | A separate DB-processor consumer group persists history using batched writes | MUST |
| F8 | The map shows markers per user; the current user is visually distinct; markers glide between positions | MUST |
| F9 | Stale or disconnected users are removed from every map | MUST |
| F10 | A user can view their own stored history trail | SHOULD |
| F11 | Viewport-scoped updates: a client receives only users inside its map bounds | COULD |
| F12 | History playback (scrub a timeline) | COULD |
| F13 | Sharing visibility controls (everyone / group / nobody) | COULD |

## Non-functional requirements

| Category | Target |
|---|---|
| **Latency** | p50 < 150 ms, p99 < 500 ms **end to end** (sender emit → other clients' marker update) |
| **Throughput (design)** | 100k concurrent sharers × 1 msg / 3 s ≈ **33k events/s** sustained, 3× burst |
| **Throughput (demo)** | 10–1,000 simulated users on one VM |
| **Availability** | 99.9% for live tracking; history persistence may lag but must not lose data |
| **Durability** | No acknowledged event lost (`acks=all`, `min.insync.replicas=2` in production) |
| **Freshness** | A user is removed from maps ≤ 35 s after their last update |
| **Privacy** | Locations are visible only to authenticated users; history only to its owner; raw history retained for 30 days |
| **Security** | OWASP ASVS L2 controls for session, input validation, transport |
| **Operability** | Every component exposes health, metrics and traces; consumer lag is visible |

## Capacity math (design target)

```
Sharers:                 100,000 concurrent
Send interval:           3 s           → 33,333 events/s
Event size (JSON, gzip): ~250 B raw, ~120 B compressed
Kafka ingress:           33k × 250 B ≈ 8.3 MB/s raw (~4 MB/s compressed)
Kafka retention 24 h:    ~350 GB raw per replica (~170 GB compressed) → fine for 3 brokers
Partitions:              24  (≈1.4k events/s per partition; leaves room for 24 consumer instances)

History after downsampling (keep a point if moved ≥10 m or ≥30 s):
  ~40% of events kept    → ~13k rows/s → ~1.1 B rows/day
  TimescaleDB compression (~10–20×) → ~60–90 GB/day on disk → 30-day retention ≈ 2–3 TB

Fan-out (the real bottleneck):
  Naive "everyone sees everyone": 33k events/s × 100k viewers = 3.3 B socket msgs/s → impossible
  Viewport-scoped (each viewer sees ~50 users): 100k × 50 / 3 s ≈ 1.7 M msgs/s
     → ~35 gateway pods at ~50k msgs/s each
```

**Conclusion:** at scale, the fan-out pattern matters more than Kafka throughput. Phase 1 broadcasts to everyone, which is correct for the assignment and demo size. The architecture is designed so viewport-scoped rooms (H3 cells, see [05](05-realtime-gateway.md#viewport-scoped-fan-out)) can be added without changing the event contracts.

## Constraints & assumptions

- Browser-only clients (secure context required: HTTPS or `localhost`).
- Single region. Clock skew between client and server is tolerated up to ±60 s; ordering uses **server** time.
- The assignment rubric fixes the choice of **Leaflet**, **Socket.IO** and **Kafka**.
- One developer, about 5 weeks until submission.
