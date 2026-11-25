/**
 * Pure helpers for the Kafka adapter. They hold the rules that are easy to get wrong
 * (security settings, offset commits) and have no dependency on the native Kafka client,
 * so they can be unit tested on any machine.
 */

export interface KafkaSecurityConfig {
  ssl?: boolean;
  sasl?: {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
  };
}

/**
 * librdkafka security properties for @confluentinc/kafka-javascript.
 *
 * WHY these go on the shared client (see buildConfluentClientConfig), not on the producer:
 * the Confluent client copies client-level settings into every producer, consumer AND admin
 * it creates. If they were only on the producer, consumers would fail to authenticate
 * against a managed Kafka (Confluent Cloud, Aiven, Redpanda Cloud).
 */
export function buildConfluentSecurityConfig(
  security: KafkaSecurityConfig,
): Record<string, string> {
  const out: Record<string, string> = {};
  if (security.ssl) {
    out['security.protocol'] = security.sasl ? 'sasl_ssl' : 'ssl';
  } else if (security.sasl) {
    out['security.protocol'] = 'sasl_plaintext';
  }
  if (security.sasl) {
    out['sasl.mechanism'] = security.sasl.mechanism.toUpperCase();
    out['sasl.username'] = security.sasl.username;
    out['sasl.password'] = security.sasl.password;
  }
  return out;
}

export function buildConfluentClientConfig(
  config: { brokers: string[]; clientId: string } & KafkaSecurityConfig,
): Record<string, unknown> {
  return {
    kafkaJS: { brokers: config.brokers, clientId: config.clientId },
    ...buildConfluentSecurityConfig(config),
  };
}

/**
 * consumer.run() options for durable batch consumers on the Confluent driver.
 *
 * WHY eachBatchAutoResolve: false: the Confluent client defaults it to TRUE, which marks the
 * whole batch as processed whenever the handler returns, even if the handler paused because
 * Postgres was down and wrote nothing. Those rows would never be written. With false, only
 * offsets we explicitly commit (after the DB transaction commits) count as processed.
 */
export const CONFLUENT_BATCH_RUN_OPTIONS = { eachBatchAutoResolve: false } as const;

/** Kafka commits the NEXT offset to read, so "I finished offset 41" is committed as 42. */
export function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
}

/** The subset of a KafkaJS / Confluent eachBatch payload the commit helpers need. */
export interface BatchCommitPayload {
  resolveOffset: (offset: string) => void;
  commitOffsetsIfNecessary: (offsets?: {
    topics: { topic: string; partitions: { partition: number; offset: string }[] }[];
  }) => Promise<void>;
}

/**
 * Commit for the KafkaJS fallback driver.
 *
 * WHY explicit offsets: with autoCommit=false and no autoCommitInterval/Threshold,
 * KafkaJS's argument-less commitOffsetsIfNecessary() is a no-op, so nothing would ever be
 * committed and every restart would replay the whole topic. Passing the offsets forces a commit.
 * resolveOffset() also moves the in-memory position forward so the batch isn't re-fetched.
 */
export function makeKafkaJsCommit(
  payload: BatchCommitPayload,
  topic: string,
  partition: number,
): (offset: string) => Promise<void> {
  return async (offset: string) => {
    payload.resolveOffset(offset);
    await payload.commitOffsetsIfNecessary({
      topics: [{ topic, partitions: [{ partition, offset: nextOffset(offset) }] }],
    });
  };
}

/**
 * Commit for the Confluent driver. With enable.auto.commit=false, the Confluent client's
 * commitOffsetsIfNecessary() commits every offset stored by resolveOffset() (it already adds +1).
 */
export function makeConfluentCommit(payload: BatchCommitPayload): (offset: string) => Promise<void> {
  return async (offset: string) => {
    payload.resolveOffset(offset);
    await payload.commitOffsetsIfNecessary();
  };
}

/**
 * Validate a producer send() result. Returning a made-up "partition 0, offset 0" would tell the
 * browser its update was stored somewhere it wasn't, so a missing result is an error instead.
 */
export function toPublishResult(
  first: { partition?: number | null; baseOffset?: string | null; offset?: string | null } | undefined,
  topic: string,
): { partition: number; offset: string } {
  const offset = first?.baseOffset ?? first?.offset ?? null;
  if (!first || typeof first.partition !== 'number' || first.partition < 0 || offset === null) {
    throw new Error(`Kafka send to "${topic}" returned no partition/offset; treating as not stored.`);
  }
  return { partition: first.partition, offset };
}
