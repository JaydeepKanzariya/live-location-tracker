import { KAFKA_HEADERS, TOPIC_NAMES } from '@llt/contracts';

export interface PublishRecord {
  topic: string;
  key: string;
  value: string | null; // null represents a Kafka tombstone (e.g. right-to-erasure on compacted topics)
  headers?: Record<string, string>;
}

export interface PublishResult {
  partition: number;
  offset: string;
}

export interface ConsumedMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
  timestamp: string;
}

export interface SubscribeOptions {
  /**
   * Consumer group ID determines Kafka delivery topology:
   * - Shared group ID (e.g. `location-db-writer`, `presence-projector`): partitions are SPLIT
   *   across instances as a work queue so each message is processed once per group.
   * - Per-instance group ID (e.g. `broadcaster-gateway-1`): every gateway pod receives ALL
   *   messages so it can fan out to its own connected sockets.
   */
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
  /**
   * Set autoCommit=false for durable consumers (like history-writer) that MUST commit
   * Kafka offsets only AFTER the downstream DB transaction commits.
   */
  autoCommit?: boolean;
}

export interface BatchContext {
  topic: string;
  partition: number;
  messages: ConsumedMessage[];
  /** Commit offset for the last processed message in this batch. */
  commitOffset: (offset: string) => Promise<void>;
  /** Pause/resume this partition on transient sink failures (e.g. Postgres down). */
  pause: () => void;
  resume: () => void;
}

export type MessageHandler = (message: ConsumedMessage) => Promise<void>;
export type BatchHandler = (batch: BatchContext) => Promise<void>;

export interface ConsumerHandle {
  stop: () => Promise<void>;
}

export interface DlqRecord {
  dlqTopic?: string;
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: string;
  key: string | null;
  rawValue: string | null;
  errorMessage: string;
  headers?: Record<string, string>;
}

/**
 * Transport-agnostic EventBus interface hiding the underlying Kafka driver
 * (@confluentinc/kafka-javascript or KafkaJS fallback) and enabling deterministic
 * in-memory unit tests.
 */
export interface EventBus {
  connect(): Promise<void>;
  publish(record: PublishRecord): Promise<PublishResult>;
  publishDlq(record: DlqRecord): Promise<PublishResult>;
  subscribe(options: SubscribeOptions, handler: MessageHandler): Promise<ConsumerHandle>;
  subscribeBatch(options: SubscribeOptions, handler: BatchHandler): Promise<ConsumerHandle>;
  healthCheck(): Promise<boolean>;
  disconnect(): Promise<void>;
}

export function buildDlqPublishRecord(record: DlqRecord): PublishRecord {
  return {
    topic: record.dlqTopic ?? TOPIC_NAMES.LOCATION_UPDATES_DLQ,
    key: record.key ?? 'unknown',
    value: record.rawValue ?? '',
    headers: {
      ...record.headers,
      [KAFKA_HEADERS.DLQ_ERROR]: record.errorMessage.slice(0, 500),
      [KAFKA_HEADERS.DLQ_SOURCE_TOPIC]: record.sourceTopic,
      [KAFKA_HEADERS.DLQ_SOURCE_PARTITION]: String(record.sourcePartition),
      [KAFKA_HEADERS.DLQ_SOURCE_OFFSET]: record.sourceOffset,
    },
  };
}
