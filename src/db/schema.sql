-- Applied automatically by the DB processor on startup (idempotent).

-- Append-only, downsampled trail of where each user has been.
CREATE TABLE IF NOT EXISTS location_history (
  id              BIGSERIAL PRIMARY KEY,
  event_id        UUID             NOT NULL UNIQUE,   -- idempotency key: Kafka replays become no-ops
  user_id         TEXT             NOT NULL,          -- OIDC `sub`
  lat             DOUBLE PRECISION NOT NULL,
  lng             DOUBLE PRECISION NOT NULL,
  accuracy        REAL,
  heading         REAL,
  speed           REAL,
  source          TEXT             NOT NULL,
  recorded_at     TIMESTAMPTZ      NOT NULL,          -- server receive time (event.serverTs)
  kafka_partition INTEGER          NOT NULL,
  kafka_offset    BIGINT           NOT NULL,
  inserted_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS location_history_user_time_idx
  ON location_history (user_id, recorded_at DESC);

-- Exactly one row per user: "where was this user last seen?" without scanning history.
CREATE TABLE IF NOT EXISTS user_last_location (
  user_id      TEXT PRIMARY KEY,
  display_name TEXT,
  lat          DOUBLE PRECISION NOT NULL,
  lng          DOUBLE PRECISION NOT NULL,
  recorded_at  TIMESTAMPTZ      NOT NULL,
  updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
);
