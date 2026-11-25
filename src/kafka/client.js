// Shared Kafka client + topic bootstrap.
import { Kafka, logLevel } from 'kafkajs';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('kafka');

// Route kafkajs' own logs through our JSON logger (and keep them quiet unless they matter).
const kafkaLogCreator = () => ({ level, log: entry }) => {
  const { message, ...extra } = entry;
  if (level === logLevel.ERROR) log.error(message, extra);
  else if (level === logLevel.WARN) log.warn(message, extra);
};

export const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  ssl: config.kafka.ssl,
  sasl: config.kafka.sasl,
  logLevel: logLevel.WARN,
  logCreator: kafkaLogCreator,
  retry: { initialRetryTime: 300, retries: 10 },
});

/**
 * Make sure `location-updates` exists with enough partitions.
 * Partitions = unit of parallelism: with 3 partitions, up to 3 DB-writer instances can share the work.
 */
export async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const created = await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: config.kafka.topic, numPartitions: config.kafka.topicPartitions }],
    });
    log.info(created ? 'topic created' : 'topic already exists', {
      topic: config.kafka.topic,
      partitions: config.kafka.topicPartitions,
    });
  } finally {
    await admin.disconnect();
  }
}
