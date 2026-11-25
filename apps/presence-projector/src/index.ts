import { loadConfig, PresenceProjectorEnvSchema } from '@llt/config';
import { TOPIC_NAMES } from '@llt/contracts';
import { createLogger } from '@llt/observability';

/**
 * Phase 0 skeleton for presence-projector (Redis GEO + cluster-wide stale detection).
 * Full implementation is built in Phase 2.
 */
const config = loadConfig(PresenceProjectorEnvSchema);
const logger = createLogger({ service: 'presence-projector', level: config.LOG_LEVEL });

logger.info(
  {
    consumerGroup: config.KAFKA_PRESENCE_GROUP,
    locationTopic: TOPIC_NAMES.LOCATION_UPDATES,
    presenceTopic: TOPIC_NAMES.PRESENCE_EVENTS,
    staleAfterMs: config.LOCATION_STALE_AFTER_MS,
  },
  'presence-projector skeleton initialized (Phase 0)',
);
