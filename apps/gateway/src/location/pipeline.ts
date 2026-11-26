import {
  ClientLocationUpdateSchema,
  KAFKA_HEADERS,
  LocationUpdatedEventSchema,
  type LocationUpdateAck,
  type LocationUpdatedEvent,
  type SessionUser,
} from '@llt/contracts';
import type { EventBus } from '@llt/kafka';
import { hashUserId } from '@llt/observability';
import type { Logger } from 'pino';
import type { RateLimiter } from '../ratelimit/token-bucket.js';
import type { DedupeStore } from './dedupe.js';

export interface LocationPipelineDeps {
  bus: Pick<EventBus, 'publish'>;
  dedupe: DedupeStore;
  limiter: RateLimiter;
  topic: string;
  maxClockSkewMs: number;
  logger: Logger;
  now?: () => number;
  newId?: () => string;
}

/**
 * location:update pipeline (docs/plan/05-realtime-gateway.md):
 *   Zod parse -> dedupe -> rate limit -> enrich from session -> produce -> ack
 *
 * The caller has already checked the session. `user` comes from that session, never from
 * the payload: ClientLocationUpdateSchema is strict, so a payload carrying `userId` fails
 * validation and nothing is produced.
 */
export function createLocationPipeline(deps: LocationPipelineDeps) {
  const now = deps.now ?? Date.now;

  return async function handleLocationUpdate(
    user: SessionUser,
    rawPayload: unknown,
  ): Promise<LocationUpdateAck> {
    const parsed = ClientLocationUpdateSchema.safeParse(rawPayload);
    if (!parsed.success) {
      deps.logger.debug({ user: hashUserId(user.sub) }, 'rejected invalid location payload');
      return { ok: false, code: 'INVALID_PAYLOAD' };
    }
    const update = parsed.data;
    const receivedAt = now();

    // A client clock far in the future would corrupt ordering and staleness checks.
    if (update.occurredAt > receivedAt + deps.maxClockSkewMs) {
      return { ok: false, code: 'INVALID_PAYLOAD' };
    }

    // Atomic "first one wins". The loser is a retry of something already accepted.
    if (!(await deps.dedupe.claim(update.eventId))) {
      return { ok: true, duplicate: true };
    }

    // From here on, any early return must release the id: the update was NOT produced, so
    // the client's retry (same eventId) has to be allowed through rather than "deduplicated".
    try {
      const limit = await deps.limiter.take(user.sub);
      if (!limit.allowed) {
        await deps.dedupe.release(update.eventId);
        return {
          ok: false,
          code: 'RATE_LIMITED',
          ...(limit.retryAfterMs !== undefined ? { retryAfterMs: limit.retryAfterMs } : {}),
        };
      }

      const event: LocationUpdatedEvent = LocationUpdatedEventSchema.parse({
        schemaVersion: 1,
        type: 'location.updated',
        eventId: update.eventId,
        occurredAt: update.occurredAt,
        receivedAt,
        userId: user.sub,
        displayName: user.displayName,
        position: {
          lat: update.lat,
          lng: update.lng,
          accuracyM: update.accuracyM,
          headingDeg: update.headingDeg,
          speedMps: update.speedMps,
        },
        source: update.source,
      });

      const result = await deps.bus.publish({
        topic: deps.topic,
        key: user.sub, // same user -> same partition -> ordered
        value: JSON.stringify(event),
        headers: {
          [KAFKA_HEADERS.CONTENT_TYPE]: 'application/json',
          [KAFKA_HEADERS.SCHEMA_VERSION]: '1',
          [KAFKA_HEADERS.EVENT_TYPE]: event.type,
        },
      });
      return { ok: true, partition: result.partition, offset: result.offset };
    } catch (err) {
      await deps.dedupe.release(update.eventId).catch(() => undefined);
      deps.logger.error(
        { user: hashUserId(user.sub), err: err instanceof Error ? err.message : String(err) },
        'location update failed before it reached Kafka',
      );
      return { ok: false, code: 'UNAVAILABLE' };
    }
  };
}
