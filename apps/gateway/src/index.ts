import { GatewayEnvSchema, loadConfig } from '@llt/config';
import { KafkaEventBus } from '@llt/kafka';
import { createLogger } from '@llt/observability';
import { Redis } from 'ioredis';
import { buildGateway } from './app.js';
import { OpenIdClientProvider } from './auth/oidc.js';

const config = loadConfig(GatewayEnvSchema);
const logger = createLogger({ service: `gateway:${config.INSTANCE_ID}`, level: config.LOG_LEVEL });

const redis = new Redis(config.REDIS_URL, { maxRetriesPerRequest: 3 });
redis.on('error', (err) => logger.error({ err: err.message }, 'redis error'));

const bus = new KafkaEventBus({
  brokers: config.KAFKA_BROKERS,
  clientId: `${config.KAFKA_CLIENT_ID}-${config.INSTANCE_ID}`,
  driver: config.KAFKA_DRIVER,
  ssl: config.KAFKA_SSL,
  ...(config.KAFKA_SASL_USERNAME
    ? {
        sasl: {
          mechanism: config.KAFKA_SASL_MECHANISM,
          username: config.KAFKA_SASL_USERNAME,
          password: config.KAFKA_SASL_PASSWORD,
        },
      }
    : {}),
});

const oidc = new OpenIdClientProvider({
  issuerUrl: config.OIDC_ISSUER_URL,
  clientId: config.OIDC_CLIENT_ID,
  clientSecret: config.OIDC_CLIENT_SECRET,
  scopes: config.OIDC_SCOPES,
  redirectUri: new URL('/auth/callback', config.APP_BASE_URL).href,
  postLogoutRedirectUri: config.APP_BASE_URL,
  allowInsecure: config.OIDC_ALLOW_INSECURE,
});

async function main(): Promise<void> {
  await bus.connect();
  const gateway = await buildGateway({ config, redis, bus, oidc, logger });
  const port = await gateway.start();
  logger.info({ port, instanceId: config.INSTANCE_ID }, 'gateway listening');

  let stopping = false;
  const shutdown = async (signal: string) => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, 'shutting down');
    try {
      await gateway.stop();
      await bus.disconnect();
      await redis.quit();
    } finally {
      process.exit(0);
    }
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err: unknown) => {
  logger.fatal({ err: err instanceof Error ? err.message : String(err) }, 'gateway failed to start');
  process.exit(1);
});
