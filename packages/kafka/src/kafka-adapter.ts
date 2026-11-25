import { KafkaJS as ConfluentKafkaJS } from '@confluentinc/kafka-javascript';
import { injectTraceHeaders } from '@llt/observability';
import { Kafka as PureKafkaJS, logLevel } from 'kafkajs';
import {
  buildDlqPublishRecord,
  type BatchHandler,
  type ConsumedMessage,
  type ConsumerHandle,
  type DlqRecord,
  type EventBus,
  type MessageHandler,
  type PublishRecord,
  type PublishResult,
  type SubscribeOptions,
} from './event-bus.js';
import {
  CONFLUENT_BATCH_RUN_OPTIONS,
  buildConfluentClientConfig,
  makeConfluentCommit,
  makeKafkaJsCommit,
  toPublishResult,
} from './kafka-config.js';

export interface KafkaEventBusConfig {
  brokers: string[];
  clientId: string;
  /**
   * 'confluent' uses @confluentinc/kafka-javascript (librdkafka, recommended).
   * 'kafkajs' is the pure-JS fallback if native binaries cannot load on a dev machine.
   */
  driver?: 'confluent' | 'kafkajs';
  ssl?: boolean;
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
  };
}

function decodeHeaders(raw?: Record<string, unknown>): Record<string, string> {
  const out: Record<string, string> = {};
  if (!raw) return out;
  for (const [k, v] of Object.entries(raw)) {
    if (typeof v === 'string') {
      out[k] = v;
    } else if (Buffer.isBuffer(v)) {
      out[k] = v.toString('utf8');
    } else if (Array.isArray(v) && v.length > 0) {
      const first = v[0];
      out[k] = Buffer.isBuffer(first) ? first.toString('utf8') : String(first);
    }
  }
  return out;
}

/**
 * Production Kafka implementation behind EventBus.
 * Uses idempotent producer + acks=all so retries never create duplicates and
 * no acknowledged location update is lost.
 */
export class KafkaEventBus implements EventBus {
  private readonly config: KafkaEventBusConfig;
  private readonly driver: 'confluent' | 'kafkajs';
  private confluentClient: ConfluentKafkaJS.Kafka | null = null;
  private confluentProducer: ConfluentKafkaJS.Producer | null = null;
  private kafkaJsClient: PureKafkaJS | null = null;
  private kafkaJsProducer: ReturnType<PureKafkaJS['producer']> | null = null;
  private readonly activeConsumers: ConsumerHandle[] = [];

  constructor(config: KafkaEventBusConfig) {
    this.config = config;
    this.driver = config.driver ?? 'confluent';
  }

  async connect(): Promise<void> {
    if (this.driver === 'confluent') {
      // Brokers, client id AND security settings live on the shared client, so the producer,
      // every consumer and the admin client all inherit them (see buildConfluentClientConfig).
      this.confluentClient = new ConfluentKafkaJS.Kafka(buildConfluentClientConfig(this.config));
      this.confluentProducer = this.confluentClient.producer({
        'enable.idempotence': true,
        acks: -1, // -1 == acks=all in librdkafka / KafkaJS API
        'linger.ms': 5,
      });
      await this.confluentProducer.connect();
      return;
    }

    const sasl = this.config.sasl
      ? this.config.sasl.mechanism === 'plain'
        ? {
            mechanism: 'plain' as const,
            username: this.config.sasl.username,
            password: this.config.sasl.password,
          }
        : this.config.sasl.mechanism === 'scram-sha-256'
          ? {
              mechanism: 'scram-sha-256' as const,
              username: this.config.sasl.username,
              password: this.config.sasl.password,
            }
          : {
              mechanism: 'scram-sha-512' as const,
              username: this.config.sasl.username,
              password: this.config.sasl.password,
            }
      : undefined;

    this.kafkaJsClient = new PureKafkaJS({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
      ssl: this.config.ssl,
      sasl,
      logLevel: logLevel.WARN,
    });
    this.kafkaJsProducer = this.kafkaJsClient.producer({
      idempotent: true,
      maxInFlightRequests: 5,
    });
    await this.kafkaJsProducer.connect();
  }

  async publish(record: PublishRecord): Promise<PublishResult> {
    const headers = injectTraceHeaders(record.headers ?? {});

    if (this.driver === 'confluent') {
      if (!this.confluentProducer) {
        throw new Error('KafkaEventBus not connected. Call connect() before publish().');
      }
      const res = await this.confluentProducer.send({
        topic: record.topic,
        messages: [{ key: record.key, value: record.value, headers }],
      });
      return toPublishResult(res[0], record.topic);
    }

    if (!this.kafkaJsProducer) {
      throw new Error('KafkaEventBus not connected. Call connect() before publish().');
    }
    const res = await this.kafkaJsProducer.send({
      topic: record.topic,
      acks: -1, // acks=all
      messages: [{ key: record.key, value: record.value, headers }],
    });
    return toPublishResult(res[0], record.topic);
  }

  async publishDlq(record: DlqRecord): Promise<PublishResult> {
    return this.publish(buildDlqPublishRecord(record));
  }

  async subscribe(options: SubscribeOptions, handler: MessageHandler): Promise<ConsumerHandle> {
    if (this.driver === 'confluent') {
      if (!this.confluentClient) {
        throw new Error('KafkaEventBus not connected. Call connect() before subscribe().');
      }
      const consumer = this.confluentClient.consumer({
        'group.id': options.groupId,
        'auto.offset.reset': options.fromBeginning ? 'earliest' : 'latest',
        'enable.auto.commit': options.autoCommit ?? true,
        'partition.assignment.strategy': 'cooperative-sticky',
      });
      await consumer.connect();
      await consumer.subscribe({ topics: options.topics });
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const consumed: ConsumedMessage = {
            topic,
            partition,
            offset: message.offset,
            key: message.key ? message.key.toString('utf8') : null,
            value: message.value ? message.value.toString('utf8') : null,
            headers: decodeHeaders(message.headers as Record<string, unknown> | undefined),
            timestamp: message.timestamp ?? String(Date.now()),
          };
          await handler(consumed);
        },
      });
      const handle: ConsumerHandle = {
        stop: async () => {
          await consumer.disconnect();
        },
      };
      this.activeConsumers.push(handle);
      return handle;
    }

    if (!this.kafkaJsClient) {
      throw new Error('KafkaEventBus not connected. Call connect() before subscribe().');
    }
    const consumer = this.kafkaJsClient.consumer({ groupId: options.groupId });
    await consumer.connect();
    await consumer.subscribe({
      topics: options.topics,
      fromBeginning: options.fromBeginning ?? false,
    });
    await consumer.run({
      autoCommit: options.autoCommit ?? true,
      eachMessage: async ({ topic, partition, message }) => {
        const consumed: ConsumedMessage = {
          topic,
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString('utf8') : null,
          value: message.value ? message.value.toString('utf8') : null,
          headers: decodeHeaders(message.headers as Record<string, unknown> | undefined),
          timestamp: message.timestamp,
        };
        await handler(consumed);
      },
    });
    const handle: ConsumerHandle = {
      stop: async () => {
        await consumer.disconnect();
      },
    };
    this.activeConsumers.push(handle);
    return handle;
  }

  async subscribeBatch(options: SubscribeOptions, handler: BatchHandler): Promise<ConsumerHandle> {
    if (this.driver === 'confluent') {
      if (!this.confluentClient) {
        throw new Error('KafkaEventBus not connected. Call connect() before subscribeBatch().');
      }
      const consumer = this.confluentClient.consumer({
        'group.id': options.groupId,
        'auto.offset.reset': options.fromBeginning ? 'earliest' : 'latest',
        'enable.auto.commit': options.autoCommit ?? false,
        'partition.assignment.strategy': 'cooperative-sticky',
        'fetch.min.bytes': 16_384,
        'fetch.wait.max.ms': 2_000,
      });
      await consumer.connect();
      await consumer.subscribe({ topics: options.topics });
      await consumer.run({
        ...CONFLUENT_BATCH_RUN_OPTIONS, // eachBatchAutoResolve: false - see kafka-config.ts
        eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, pause }) => {
          const messages: ConsumedMessage[] = batch.messages.map((message) => ({
            topic: batch.topic,
            partition: batch.partition,
            offset: message.offset,
            key: message.key ? message.key.toString('utf8') : null,
            value: message.value ? message.value.toString('utf8') : null,
            headers: decodeHeaders(message.headers as Record<string, unknown> | undefined),
            timestamp: message.timestamp ?? String(Date.now()),
          }));

          let resumeFn: (() => void) | null = null;
          await handler({
            topic: batch.topic,
            partition: batch.partition,
            messages,
            commitOffset: makeConfluentCommit({ resolveOffset, commitOffsetsIfNecessary }),
            pause: () => {
              resumeFn = pause();
            },
            resume: () => {
              resumeFn?.();
              resumeFn = null;
            },
          });
        },
      });
      const handle: ConsumerHandle = {
        stop: async () => {
          await consumer.disconnect();
        },
      };
      this.activeConsumers.push(handle);
      return handle;
    }

    if (!this.kafkaJsClient) {
      throw new Error('KafkaEventBus not connected. Call connect() before subscribeBatch().');
    }
    const consumer = this.kafkaJsClient.consumer({
      groupId: options.groupId,
      minBytes: 16_384,
      maxWaitTimeInMs: 2_000,
    });
    await consumer.connect();
    await consumer.subscribe({
      topics: options.topics,
      fromBeginning: options.fromBeginning ?? false,
    });
    await consumer.run({
      autoCommit: options.autoCommit ?? false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, pause }) => {
        const messages: ConsumedMessage[] = batch.messages.map((message) => ({
          topic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
          key: message.key ? message.key.toString('utf8') : null,
          value: message.value ? message.value.toString('utf8') : null,
          headers: decodeHeaders(message.headers as Record<string, unknown> | undefined),
          timestamp: message.timestamp,
        }));

        let resumeFn: (() => void) | null = null;
        await handler({
          topic: batch.topic,
          partition: batch.partition,
          messages,
          // Explicit offsets: the argument-less form is a no-op with autoCommit=false (see kafka-config.ts)
          commitOffset: makeKafkaJsCommit(
            { resolveOffset, commitOffsetsIfNecessary },
            batch.topic,
            batch.partition,
          ),
          pause: () => {
            resumeFn = pause();
          },
          resume: () => {
            resumeFn?.();
            resumeFn = null;
          },
        });
      },
    });
    const handle: ConsumerHandle = {
      stop: async () => {
        await consumer.disconnect();
      },
    };
    this.activeConsumers.push(handle);
    return handle;
  }

  async healthCheck(): Promise<boolean> {
    try {
      if (this.driver === 'confluent' && this.confluentClient) {
        const admin = this.confluentClient.admin();
        await admin.connect();
        await admin.listTopics();
        await admin.disconnect();
        return true;
      }
      if (this.kafkaJsClient) {
        const admin = this.kafkaJsClient.admin();
        await admin.connect();
        await admin.listTopics();
        await admin.disconnect();
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }

  async disconnect(): Promise<void> {
    await Promise.allSettled(this.activeConsumers.map((c) => c.stop()));
    this.activeConsumers.length = 0;
    if (this.confluentProducer) {
      await this.confluentProducer.disconnect();
      this.confluentProducer = null;
    }
    if (this.kafkaJsProducer) {
      await this.kafkaJsProducer.disconnect();
      this.kafkaJsProducer = null;
    }
  }
}
