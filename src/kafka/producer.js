// Publishes location events to Kafka.
// The message KEY is the userId: Kafka hashes the key to pick a partition, so every event of one
// user lands on the same partition and is consumed IN ORDER. Different users spread across partitions.
import { CompressionTypes } from 'kafkajs';
import { kafka } from './client.js';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('producer');

export function createLocationProducer() {
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  let connected = false;

  producer.on(producer.events.DISCONNECT, () => {
    connected = false;
  });

  return {
    async connect() {
      await producer.connect();
      connected = true;
      log.info('producer connected', { topic: config.kafka.topic });
    },

    isConnected: () => connected,

    /** @returns {Promise<{partition:number, offset:string}>} where Kafka stored the event */
    async publish(event) {
      const [meta] = await producer.send({
        topic: config.kafka.topic,
        // acks=-1 (all in-sync replicas): don't tell the client "accepted" until Kafka has it durably.
        acks: -1,
        compression: CompressionTypes.GZIP,
        messages: [
          {
            key: event.userId,
            value: JSON.stringify(event),
            headers: { 'event-type': event.type, 'schema-version': String(event.schemaVersion) },
          },
        ],
      });
      return { partition: meta.partition, offset: meta.baseOffset };
    },

    async disconnect() {
      await producer.disconnect();
    },
  };
}
