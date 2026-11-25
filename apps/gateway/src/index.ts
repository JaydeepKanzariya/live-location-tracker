import { GatewayEnvSchema, loadConfig } from '@llt/config';
import { TOPIC_NAMES } from '@llt/contracts';
import { createLogger } from '@llt/observability';

/**
 * Phase 0 skeleton for the Fastify + Socket.IO realtime gateway.
 * Full OIDC BFF, Socket.IO auth, Redis dedupe/rate-limit, Kafka producer, and
 * per-instance broadcaster consumer (broadcaster-<INSTANCE_ID>) are built in Phase 1.
 */
const config = loadConfig(GatewayEnvSchema);
const logger = createLogger({ service: `gateway:${config.INSTANCE_ID}`, level: config.LOG_LEVEL });

logger.info(
  {
    port: config.PORT,
    instanceId: config.INSTANCE_ID,
    broadcasterGroupId: `${config.KAFKA_BROADCASTER_GROUP}-${config.INSTANCE_ID}`,
    locationTopic: TOPIC_NAMES.LOCATION_UPDATES,
  },
  'gateway skeleton initialized (Phase 0)',
);
