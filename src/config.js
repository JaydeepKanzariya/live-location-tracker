// Central place for every environment variable the app reads.
// Each value has a safe local-dev default so `docker compose up` + `npm run dev` just works.
import os from 'node:os';

function str(name, fallback) {
  const value = process.env[name];
  return value === undefined || value === '' ? fallback : value;
}

function int(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed)) throw new Error(`Env var ${name} must be an integer, got "${raw}"`);
  return parsed;
}

function bool(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

const isProduction = str('NODE_ENV', 'development') === 'production';
const instanceId = str('INSTANCE_ID', os.hostname());

export const config = {
  isProduction,
  port: int('PORT', 3000),
  appBaseUrl: str('APP_BASE_URL', 'http://localhost:3000'),
  instanceId,

  session: {
    secret: str('SESSION_SECRET', 'dev-only-change-me'),
    maxAgeMs: int('SESSION_MAX_AGE_MS', 8 * 60 * 60 * 1000), // 8 hours
  },

  oidc: {
    issuerUrl: str('OIDC_ISSUER_URL', 'http://localhost:8080/realms/tracker'),
    clientId: str('OIDC_CLIENT_ID', 'location-tracker'),
    clientSecret: str('OIDC_CLIENT_SECRET', 'location-tracker-secret'),
    scopes: str('OIDC_SCOPES', 'openid profile email'),
    // Keycloak in docker-compose runs on plain http; never enable this in production.
    allowInsecureIssuer: bool('OIDC_ALLOW_INSECURE', !isProduction),
  },

  kafka: {
    clientId: str('KAFKA_CLIENT_ID', 'live-location-tracker'),
    brokers: str('KAFKA_BROKERS', 'localhost:9092').split(',').map((b) => b.trim()),
    topic: str('KAFKA_TOPIC', 'location-updates'),
    topicPartitions: int('KAFKA_TOPIC_PARTITIONS', 3),
    ssl: bool('KAFKA_SSL', false),
    sasl: process.env.KAFKA_SASL_USERNAME
      ? {
          mechanism: str('KAFKA_SASL_MECHANISM', 'plain'),
          username: process.env.KAFKA_SASL_USERNAME,
          password: str('KAFKA_SASL_PASSWORD', ''),
        }
      : undefined,
    // Each gateway instance must receive EVERY event to push it to its own sockets,
    // so the broadcaster group id is unique per instance.
    broadcasterGroupId: `${str('KAFKA_BROADCASTER_GROUP', 'socket-broadcaster')}-${instanceId}`,
    // DB writers share one group id so partitions are SPLIT between them (scale-out).
    dbWriterGroupId: str('KAFKA_DB_WRITER_GROUP', 'location-db-writer'),
  },

  location: {
    // Server refuses updates faster than this per user (protects Kafka + DB from a buggy/malicious client).
    minIntervalMs: int('LOCATION_MIN_INTERVAL_MS', 1000),
    // A user with no update for this long is removed from everyone's map.
    staleAfterMs: int('LOCATION_STALE_AFTER_MS', 30_000),
    staleSweepIntervalMs: int('LOCATION_STALE_SWEEP_MS', 5_000),
    // Reject client timestamps too far from server time.
    maxClockSkewMs: int('LOCATION_MAX_CLOCK_SKEW_MS', 60_000),
  },

  db: {
    url: str('DATABASE_URL', ''), // empty => processor "simulates" persistence by appending to a JSONL file
    simulatedFile: str('HISTORY_FILE', 'data/location-history.jsonl'),
    batchMaxSize: int('DB_BATCH_MAX_SIZE', 500),
    // Downsampling: only keep a history point if the user moved this far OR this much time passed.
    historyMinDistanceM: int('HISTORY_MIN_DISTANCE_M', 10),
    historyMinIntervalMs: int('HISTORY_MIN_INTERVAL_MS', 30_000),
  },
};

if (config.isProduction && config.session.secret === 'dev-only-change-me') {
  throw new Error('SESSION_SECRET must be set in production');
}
