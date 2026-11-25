import { injectTraceHeaders } from '@llt/observability';
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

interface GroupSubscription {
  id: number;
  groupId: string;
  topics: Set<string>;
  handler?: MessageHandler;
  batchHandler?: BatchHandler;
}

/**
 * Deterministic in-memory EventBus for unit tests. It models the consumer-group rules the
 * app relies on (it is NOT a full Kafka: no rebalancing, retries or real pause):
 * - Distinct groupIds each receive every message (pub/sub fan-out).
 * - Members of the SAME groupId split the work BY PARTITION, like Kafka's assignor:
 *   member = partition % memberCount. Because the partition comes from the key, every event
 *   for one userId always reaches the same member, in order.
 */
export class InMemoryEventBus implements EventBus {
  private readonly partitions: number;
  private readonly offsets = new Map<string, number>();
  private readonly subscriptions: GroupSubscription[] = [];
  private readonly committedOffsets = new Map<string, string>();
  private readonly records: ConsumedMessage[] = [];
  private nextSubId = 1;
  private connected = false;

  constructor(partitions = 3) {
    this.partitions = partitions;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async publish(record: PublishRecord): Promise<PublishResult> {
    const partition = this.partitionForKey(record.key);
    const offsetKey = `${record.topic}:${partition}`;
    const offsetNum = this.offsets.get(offsetKey) ?? 0;
    this.offsets.set(offsetKey, offsetNum + 1);

    const msg: ConsumedMessage = {
      topic: record.topic,
      partition,
      offset: String(offsetNum),
      key: record.key,
      value: record.value,
      headers: injectTraceHeaders(record.headers ?? {}),
      timestamp: String(Date.now()),
    };
    this.records.push(msg);

    // Group active subscriptions by groupId so each group gets the record once
    const byGroup = new Map<string, GroupSubscription[]>();
    for (const sub of this.subscriptions) {
      if (!sub.topics.has(record.topic)) continue;
      const list = byGroup.get(sub.groupId) ?? [];
      list.push(sub);
      byGroup.set(sub.groupId, list);
    }

    for (const [groupId, members] of byGroup.entries()) {
      // Partition-based assignment (members keep subscription order), never per-message round-robin.
      const chosen = members[partition % members.length];
      if (!chosen) continue;

      if (chosen.handler) {
        await chosen.handler(msg);
      } else if (chosen.batchHandler) {
        await chosen.batchHandler({
          topic: msg.topic,
          partition: msg.partition,
          messages: [msg],
          commitOffset: async (offset: string) => {
            this.committedOffsets.set(`${groupId}:${msg.topic}:${msg.partition}`, offset);
          },
          pause: () => {},
          resume: () => {},
        });
      }
    }

    return { partition, offset: msg.offset };
  }

  async publishDlq(record: DlqRecord): Promise<PublishResult> {
    return this.publish(buildDlqPublishRecord(record));
  }

  async subscribe(options: SubscribeOptions, handler: MessageHandler): Promise<ConsumerHandle> {
    const id = this.nextSubId++;
    this.subscriptions.push({
      id,
      groupId: options.groupId,
      topics: new Set(options.topics),
      handler,
    });
    return {
      stop: async () => {
        const idx = this.subscriptions.findIndex((s) => s.id === id);
        if (idx >= 0) this.subscriptions.splice(idx, 1);
      },
    };
  }

  async subscribeBatch(options: SubscribeOptions, handler: BatchHandler): Promise<ConsumerHandle> {
    const id = this.nextSubId++;
    this.subscriptions.push({
      id,
      groupId: options.groupId,
      topics: new Set(options.topics),
      batchHandler: handler,
    });
    return {
      stop: async () => {
        const idx = this.subscriptions.findIndex((s) => s.id === id);
        if (idx >= 0) this.subscriptions.splice(idx, 1);
      },
    };
  }

  async healthCheck(): Promise<boolean> {
    return this.connected;
  }

  async disconnect(): Promise<void> {
    this.subscriptions.length = 0;
    this.connected = false;
  }

  getPublishedRecords(topic?: string): ConsumedMessage[] {
    return topic ? this.records.filter((r) => r.topic === topic) : [...this.records];
  }

  getCommittedOffset(groupId: string, topic: string, partition: number): string | undefined {
    return this.committedOffsets.get(`${groupId}:${topic}:${partition}`);
  }

  private partitionForKey(key: string): number {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      hash = (hash * 31 + key.charCodeAt(i)) | 0;
    }
    return Math.abs(hash) % this.partitions;
  }
}
