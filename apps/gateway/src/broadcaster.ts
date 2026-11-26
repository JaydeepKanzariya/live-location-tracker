import {
  LocationUpdatedEventSchema,
  type LocationUpdatedEvent,
  type PresenceEntry,
} from '@llt/contracts';
import type { ConsumedMessage, ConsumerHandle, EventBus } from '@llt/kafka';
import type { Logger } from 'pino';
import { AUTHENTICATED_ROOM, type GatewayServer } from './socket/socket-server.js';

export interface BroadcasterOptions {
  bus: EventBus;
  io: Pick<GatewayServer, 'to'>;
  topic: string;
  /**
   * One group per gateway pod (e.g. `broadcaster-gateway-1`) so EVERY pod sees EVERY event
   * and can push it to its own sockets. A shared group would split the events between pods.
   */
  groupId: string;
  /** Updates are collected for this long and sent as one `location:batch` (plan: 100-250 ms). */
  flushIntervalMs: number;
  logger: Logger;
}

export function toPresenceEntry(
  event: LocationUpdatedEvent,
  msg: Pick<ConsumedMessage, 'partition' | 'offset'>,
): PresenceEntry {
  return {
    userId: event.userId,
    displayName: event.displayName,
    lat: event.position.lat,
    lng: event.position.lng,
    accuracyM: event.position.accuracyM,
    headingDeg: event.position.headingDeg,
    speedMps: event.position.speedMps,
    occurredAt: event.occurredAt,
    receivedAt: event.receivedAt,
    eventId: event.eventId,
    source: event.source,
    partition: msg.partition,
    offset: msg.offset,
  };
}

/**
 * Consumes location-updates and fans out to this pod's authenticated sockets.
 * Per flush it keeps only the NEWEST entry per user: the map only needs the latest position,
 * and it cuts the number of socket frames a lot.
 */
export class Broadcaster {
  private pending = new Map<string, PresenceEntry>();
  private timer: NodeJS.Timeout | undefined;
  private consumer: ConsumerHandle | undefined;

  constructor(private readonly options: BroadcasterOptions) {}

  async start(): Promise<void> {
    const { bus, topic, groupId, logger } = this.options;
    this.consumer = await bus.subscribe(
      // Live view only needs what happens from now on; history is the history-writer's job.
      { groupId, topics: [topic], fromBeginning: false },
      async (msg) => this.onMessage(msg),
    );
    this.timer = setInterval(() => this.flush(), this.options.flushIntervalMs);
    this.timer.unref();
    logger.info({ groupId, topic }, 'broadcaster started');
  }

  private async onMessage(msg: ConsumedMessage): Promise<void> {
    const { bus, logger } = this.options;
    try {
      if (msg.value === null) return; // tombstone: nothing to show
      const event = LocationUpdatedEventSchema.parse(JSON.parse(msg.value));
      const entry = toPresenceEntry(event, msg);
      const current = this.pending.get(entry.userId);
      // Partitions are per user, so order is normally kept; this guards against the odd retry.
      if (!current || entry.receivedAt >= current.receivedAt) this.pending.set(entry.userId, entry);
    } catch (err) {
      // A bad record must not stop the consumer. Park it in the DLQ and carry on.
      logger.warn(
        { topic: msg.topic, partition: msg.partition, offset: msg.offset },
        'unparseable location record sent to DLQ',
      );
      await bus
        .publishDlq({
          sourceTopic: msg.topic,
          sourcePartition: msg.partition,
          sourceOffset: msg.offset,
          key: msg.key,
          rawValue: msg.value,
          errorMessage: err instanceof Error ? err.message : String(err),
        })
        .catch((dlqErr: unknown) =>
          logger.error({ err: String(dlqErr) }, 'could not publish to DLQ'),
        );
    }
  }

  /** Sends everything collected so far. Exposed for tests; normally called by the timer. */
  flush(): void {
    if (this.pending.size === 0) return;
    const batch = [...this.pending.values()];
    this.pending = new Map();
    this.options.io.to(AUTHENTICATED_ROOM).emit('location:batch', batch);
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.flush();
    await this.consumer?.stop();
  }
}
