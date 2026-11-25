# 07 · Data Storage

Two stores, two jobs:

| Store | Data | Access pattern | Lifetime |
|---|---|---|---|
| **Redis** | sessions, live presence, rate limits, dedupe keys | sub-ms key/GEO lookups, very high write rate | seconds → hours (TTL everywhere) |
| **Postgres 17 + TimescaleDB + PostGIS** | users, history, latest location, audit | time-range + geo queries, batched writes | days → forever |

## Redis key design

| Key | Type | TTL | Written by | Purpose |
|---|---|---|---|---|
| `sess:<sid>` | string (JSON) | 30 min sliding / 8 h abs | gateway | BFF session |
| `presence:geo` | GEO sorted set | – (members removed by projector) | presence-projector | `GEOSEARCH` "users near X", snapshot |
| `presence:user:<sub>` | hash `{lat,lng,name,receivedAt,partition,offset}` | 35 s (refreshed per event) | presence-projector | snapshot payload, freshness |
| `presence:sockets:<sub>` | counter | 1 h | gateway | "does this user still have a socket anywhere?" |
| `loc:dedupe:<eventId>` | string | 5 min | gateway | client-retry dedupe |
| `rl:<sub>` | hash (token bucket, Lua) | 10 s | gateway | per-user rate limit |

**Stale detection:** presence-projector runs a sweep every 5 s with `ZRANGEBYSCORE presence:lastseen -inf (now-30s)`, removes those users from the GEO set and produces `presence.stale` to `presence-events`. Gateways consume it and send `user:offline`. Because it happens once for the whole cluster, every viewer sees the same state.

## Postgres schema (Drizzle-managed migrations)

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- identity mirror (upserted at login)
CREATE TABLE users (
  id            TEXT PRIMARY KEY,               -- OIDC sub
  issuer        TEXT NOT NULL,
  display_name  TEXT NOT NULL,
  email         TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- time-series history (hypertable)
CREATE TABLE location_history (
  recorded_at     TIMESTAMPTZ      NOT NULL,    -- receivedAt
  user_id         TEXT             NOT NULL,
  event_id        UUID             NOT NULL,
  geom            GEOGRAPHY(POINT, 4326) NOT NULL,
  accuracy_m      REAL,
  speed_mps       REAL,
  heading_deg     REAL,
  source          TEXT             NOT NULL,
  kafka_partition SMALLINT         NOT NULL,
  kafka_offset    BIGINT           NOT NULL,
  PRIMARY KEY (user_id, recorded_at, event_id)  -- unique constraints on hypertables must include the time column
);
SELECT create_hypertable('location_history', by_range('recorded_at', INTERVAL '1 day'));
CREATE INDEX ON location_history (user_id, recorded_at DESC);

-- compress chunks older than 2 days, segment by user for fast per-user scans
ALTER TABLE location_history SET (timescaledb.compress, timescaledb.compress_segmentby = 'user_id',
                                  timescaledb.compress_orderby = 'recorded_at DESC');
SELECT add_compression_policy('location_history', INTERVAL '2 days');
SELECT add_retention_policy('location_history', INTERVAL '30 days');

-- one row per user
CREATE TABLE user_last_location (
  user_id     TEXT PRIMARY KEY REFERENCES users(id),
  geom        GEOGRAPHY(POINT, 4326) NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON user_last_location USING GIST (geom);

-- daily distance per user (continuous aggregate, COULD)
-- CREATE MATERIALIZED VIEW daily_user_stats WITH (timescaledb.continuous) AS …
```

**Idempotency on a hypertable:** unique indexes must include the partition column, so the key is `(user_id, recorded_at, event_id)`. Because `recorded_at` comes from the event itself (gateway `receivedAt`), a replayed event has the identical key, so `ON CONFLICT DO NOTHING` works.

## Write strategy (history-writer)

Why we never write to the DB on each socket event: 33k events/s inline would tie map latency to DB latency, give no buffer during outages, and store mostly redundant points.

1. **Kafka-side batching:** `fetch.min.bytes=16KB`, `fetch.wait.max.ms=2000`, so we get bigger batches.
2. **Downsample:** keep a point if moved ≥ 10 m **or** ≥ 30 s since the last kept point (per-user state lives in memory and moves forward only after COMMIT).
3. **Collapse:** one `user_last_location` upsert per user per batch.
4. **Bulk write:** multi-row `INSERT … ON CONFLICT DO NOTHING` (≤ 1,000 rows/stmt), or `COPY` into a temp table + `INSERT … SELECT` when the batch is > 5k rows.
5. **Transaction, then commit offsets.**
6. **Backpressure:** on DB errors, `pause()` the partition, retry with exponential backoff and jitter, `resume()` when healthy. Lag is the visible signal.

Expected cost: about 13k rows/s at the design target, which is well within one tuned Timescale node.

## Read APIs

| Endpoint | Query | Notes |
|---|---|---|
| `GET /api/me/history?from&to&limit` | `WHERE user_id=$1 AND recorded_at BETWEEN …` | owner only; returns GeoJSON LineString |
| `GET /api/presence/near?lat&lng&radius` *(COULD)* | Redis `GEOSEARCH` | live, not from Postgres |
| `DELETE /api/me/history` | delete by user_id | privacy / erasure |

## Backups & migrations
- Migrations: Drizzle Kit generates SQL, which is reviewed in the PR and applied by a one-shot `migrate` job before deploy (never on app boot in production).
- Backups: daily `pg_dump` or WAL-G to object storage, 7-day PITR. Redis is a cache/projection and can be rebuilt from Kafka (no backup needed).
