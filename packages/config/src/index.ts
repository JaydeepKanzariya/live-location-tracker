import { DEFAULT_TIMINGS, TOPIC_NAMES } from '@llt/contracts';
import { z } from 'zod';

const booleanFromEnv = z
  .union([z.boolean(), z.string()])
  .transform((val) => {
    if (typeof val === 'boolean') return val;
    const normalized = val.trim().toLowerCase();
    return normalized === 'true' || normalized === '1' || normalized === 'yes';
  });

const csvList = z
  .string()
  .transform((val) =>
    val
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean),
  );

export const BaseEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  OTEL_SERVICE_NAME: z.string().min(1).default('live-location-tracker'),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().optional().default(''),
});

export const KafkaEnvSchema = z.object({
  KAFKA_BROKERS: csvList.default(['localhost:9092']),
  KAFKA_CLIENT_ID: z.string().min(1).default('live-location-tracker'),
  KAFKA_DRIVER: z.enum(['confluent', 'kafkajs']).default('confluent'),
  KAFKA_TOPIC: z.string().min(1).default(TOPIC_NAMES.LOCATION_UPDATES),
  KAFKA_PRESENCE_TOPIC: z.string().min(1).default(TOPIC_NAMES.PRESENCE_EVENTS),
  KAFKA_LAST_LOCATION_TOPIC: z.string().min(1).default(TOPIC_NAMES.USER_LAST_LOCATION),
  KAFKA_DLQ_TOPIC: z.string().min(1).default(TOPIC_NAMES.LOCATION_UPDATES_DLQ),
  KAFKA_TOPIC_PARTITIONS: z.coerce.number().int().positive().default(3),
  KAFKA_BROADCASTER_GROUP: z.string().min(1).default('broadcaster'),
  KAFKA_DB_WRITER_GROUP: z.string().min(1).default('location-db-writer'),
  KAFKA_PRESENCE_GROUP: z.string().min(1).default('presence-projector'),
  KAFKA_SSL: booleanFromEnv.default(false),
  KAFKA_SASL_MECHANISM: z.enum(['plain', 'scram-sha-256', 'scram-sha-512']).default('plain'),
  KAFKA_SASL_USERNAME: z.string().optional().default(''),
  KAFKA_SASL_PASSWORD: z.string().optional().default(''),
});

/**
 * Local-development defaults. They are public (they're in this repo), so the gateway
 * refuses to start in production while any of them is still in use.
 */
export const DEV_DEFAULTS = {
  SESSION_SECRET: 'dev-only-change-me',
  SESSION_ENCRYPTION_KEY: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
  OIDC_CLIENT_SECRET: 'location-tracker-secret',
} as const;

const GatewayEnvObjectSchema = BaseEnvSchema.extend(KafkaEnvSchema.shape).extend({
  PORT: z.coerce.number().int().min(1).max(65_535).default(3000),
  APP_BASE_URL: z.url().default('http://localhost:3000'),
  CORS_ALLOWED_ORIGINS: csvList.default(['http://localhost:5173', 'http://localhost:3000']),
  INSTANCE_ID: z.string().min(1).default('gateway-1'),
  SESSION_SECRET: z.string().min(8).default(DEV_DEFAULTS.SESSION_SECRET),
  // 32 random bytes as hex (AES-256-GCM key for refresh tokens stored in Redis)
  SESSION_ENCRYPTION_KEY: z
    .string()
    .regex(/^[0-9a-fA-F]{64}$/, 'must be 64 hex characters (32 bytes)')
    .default(DEV_DEFAULTS.SESSION_ENCRYPTION_KEY),
  SESSION_IDLE_TTL_MS: z.coerce.number().int().positive().default(1_800_000),
  SESSION_MAX_AGE_MS: z.coerce.number().int().positive().default(28_800_000),
  OIDC_ISSUER_URL: z.url().default('http://localhost:8080/realms/tracker'),
  OIDC_CLIENT_ID: z.string().min(1).default('location-tracker'),
  OIDC_CLIENT_SECRET: z.string().min(1).default(DEV_DEFAULTS.OIDC_CLIENT_SECRET),
  OIDC_SCOPES: z.string().min(1).default('openid profile email'),
  // Left unset => true in development (local Keycloak is plain http), false in production.
  OIDC_ALLOW_INSECURE: booleanFromEnv.optional(),
  REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
  DATABASE_URL: z.string().min(1).default('postgres://tracker:tracker@localhost:5432/tracker'),
  LOCATION_MIN_INTERVAL_MS: z.coerce.number().int().positive().default(DEFAULT_TIMINGS.minIntervalMs),
  LOCATION_HEARTBEAT_MS: z.coerce.number().int().positive().default(DEFAULT_TIMINGS.heartbeatMs),
  LOCATION_STALE_FADE_MS: z.coerce.number().int().positive().default(DEFAULT_TIMINGS.staleFadeMs),
  LOCATION_STALE_AFTER_MS: z.coerce.number().int().positive().default(DEFAULT_TIMINGS.serverStaleMs),
  LOCATION_CLIENT_REMOVE_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(DEFAULT_TIMINGS.clientRemoveMs),
  LOCATION_MAX_CLOCK_SKEW_MS: z.coerce.number().int().positive().default(60_000),
  LOCATION_DEDUPE_TTL_SEC: z.coerce.number().int().positive().default(300),
});

export const GatewayEnvSchema = GatewayEnvObjectSchema.superRefine((cfg, ctx) => {
  if (cfg.NODE_ENV !== 'production') return;
  // Production must never run with secrets that are published in this repository.
  for (const key of ['SESSION_SECRET', 'SESSION_ENCRYPTION_KEY', 'OIDC_CLIENT_SECRET'] as const) {
    if (cfg[key] === DEV_DEFAULTS[key]) {
      ctx.addIssue({
        code: 'custom',
        path: [key],
        message: `${key} is still the development default; set a real secret in production`,
      });
    }
  }
  if (cfg.OIDC_ALLOW_INSECURE === true) {
    ctx.addIssue({
      code: 'custom',
      path: ['OIDC_ALLOW_INSECURE'],
      message: 'OIDC_ALLOW_INSECURE must not be true in production (the issuer must use https)',
    });
  }
}).transform((cfg) => ({
  ...cfg,
  OIDC_ALLOW_INSECURE: cfg.OIDC_ALLOW_INSECURE ?? cfg.NODE_ENV !== 'production',
}));

export const HistoryWriterEnvSchema = BaseEnvSchema.extend(KafkaEnvSchema.shape).extend({
  DATABASE_URL: z.string().min(1).default('postgres://tracker:tracker@localhost:5432/tracker'),
  DB_BATCH_MAX_SIZE: z.coerce.number().int().positive().max(5_000).default(500),
  HISTORY_MIN_DISTANCE_M: z.coerce.number().nonnegative().default(10),
  HISTORY_MIN_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
});

export const PresenceProjectorEnvSchema = BaseEnvSchema.extend(KafkaEnvSchema.shape).extend({
  REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
  LOCATION_STALE_AFTER_MS: z.coerce.number().int().positive().default(DEFAULT_TIMINGS.serverStaleMs),
  LOCATION_STALE_SWEEP_MS: z.coerce.number().int().positive().default(5_000),
});

export const SimulatorEnvSchema = BaseEnvSchema.extend(KafkaEnvSchema.shape).extend({
  SIM_CENTER_LAT: z.coerce.number().min(-90).max(90).default(23.0225),
  SIM_CENTER_LNG: z.coerce.number().min(-180).max(180).default(72.5714),
});

export type GatewayEnv = z.infer<typeof GatewayEnvSchema>;
export type HistoryWriterEnv = z.infer<typeof HistoryWriterEnvSchema>;
export type PresenceProjectorEnv = z.infer<typeof PresenceProjectorEnvSchema>;
export type SimulatorEnv = z.infer<typeof SimulatorEnvSchema>;

export function loadConfig<TSchema extends z.ZodType>(
  schema: TSchema,
  env: Record<string, string | undefined> = process.env,
): z.infer<TSchema> {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    throw new Error(`Invalid configuration:\n${z.prettifyError(parsed.error)}`);
  }
  return parsed.data;
}
