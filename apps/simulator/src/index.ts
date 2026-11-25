import { loadConfig, SimulatorEnvSchema } from '@llt/config';
import { TOPIC_NAMES } from '@llt/contracts';
import { createLogger } from '@llt/observability';

/**
 * Phase 0 skeleton for the synthetic rider load/demo producer.
 */
const config = loadConfig(SimulatorEnvSchema);
const logger = createLogger({ service: 'simulator', level: config.LOG_LEVEL });

logger.info(
  {
    topic: TOPIC_NAMES.LOCATION_UPDATES,
    brokers: config.KAFKA_BROKERS,
  },
  'simulator skeleton initialized (Phase 0)',
);
