import { HistoryWriterEnvSchema, loadConfig } from '@llt/config';
import { TOPIC_NAMES } from '@llt/contracts';
import { createLogger } from '@llt/observability';

/**
 * Phase 0 skeleton for the standalone DB history-writer process.
 * WHY a separate process & consumer group (`location-db-writer`):
 * Database stalls or slow bulk writes must NEVER slow down the live map fan-out.
 * Partitions split across history-writer replicas, and offsets commit only AFTER DB COMMIT.
 */
const config = loadConfig(HistoryWriterEnvSchema);
const logger = createLogger({ service: 'history-writer', level: config.LOG_LEVEL });

logger.info(
  {
    consumerGroup: config.KAFKA_DB_WRITER_GROUP,
    topic: TOPIC_NAMES.LOCATION_UPDATES,
    batchMaxSize: config.DB_BATCH_MAX_SIZE,
    minDistanceM: config.HISTORY_MIN_DISTANCE_M,
  },
  'history-writer skeleton initialized (Phase 0)',
);
