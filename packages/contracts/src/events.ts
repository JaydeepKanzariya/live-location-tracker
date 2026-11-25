import { z } from 'zod';

/**
 * Canonical Kafka topic names shared across gateway, consumers, and simulator.
 * Splitting presence-events from location-updates keeps history-writer focused
 * purely on coordinates while presence-projector can emit synthetic stale events.
 */
export const TOPIC_NAMES = {
  LOCATION_UPDATES: 'location-updates',
  PRESENCE_EVENTS: 'presence-events',
  USER_LAST_LOCATION: 'user-last-location',
  LOCATION_UPDATES_DLQ: 'location-updates.dlq',
} as const;

/**
 * Plan-mandated timing constants (docs/plan/08-frontend.md Parts 2 & 3).
 * Keeping them in contracts ensures server and browser never drift apart.
 */
export const DEFAULT_TIMINGS = {
  sendIntervalMs: 3_000,
  minIntervalMs: 1_000,
  heartbeatMs: 15_000,
  staleFadeMs: 25_000,
  serverStaleMs: 30_000,
  clientRemoveMs: 35_000,
} as const;

export const KAFKA_HEADERS = {
  TRACEPARENT: 'traceparent',
  CONTENT_TYPE: 'content-type',
  SCHEMA_VERSION: 'schema-version',
  EVENT_TYPE: 'event-type',
  DLQ_ERROR: 'x-error',
  DLQ_SOURCE_TOPIC: 'x-source-topic',
  DLQ_SOURCE_PARTITION: 'x-source-partition',
  DLQ_SOURCE_OFFSET: 'x-source-offset',
} as const;

export const LocationSourceSchema = z.enum(['gps', 'simulated']);
export type LocationSource = z.infer<typeof LocationSourceSchema>;

export const LocationPositionSchema = z.strictObject({
  lat: z.number().finite().min(-90).max(90),
  lng: z.number().finite().min(-180).max(180),
  accuracyM: z.number().finite().min(0).max(100_000).nullable(),
  headingDeg: z.number().finite().min(0).max(360).nullable(),
  speedMps: z.number().finite().min(0).max(1_000).nullable(),
});
export type LocationPosition = z.infer<typeof LocationPositionSchema>;

/**
 * Versioned Kafka event envelope for location-updates.
 * Key = userId so all events for one user stay ordered in a single partition.
 */
export const LocationUpdatedEventSchema = z.strictObject({
  schemaVersion: z.literal(1),
  type: z.literal('location.updated'),
  eventId: z.uuid(), // Client-generated UUID used as end-to-end idempotency key
  occurredAt: z.number().int().nonnegative(), // Client clock (informational)
  receivedAt: z.number().int().nonnegative(), // Gateway server clock (ordering + staleness)
  userId: z.string().min(1).max(256), // OIDC sub, set exclusively by gateway from session
  displayName: z.string().min(1).max(80),
  position: LocationPositionSchema,
  source: LocationSourceSchema,
  h3r7: z.string().min(1).max(32).optional(),
});
export type LocationUpdatedEvent = z.infer<typeof LocationUpdatedEventSchema>;

export const PresenceOfflineReasonSchema = z.enum(['stopped', 'disconnected', 'stale']);
export type PresenceOfflineReason = z.infer<typeof PresenceOfflineReasonSchema>;

export const PresenceOnlineEventSchema = z.strictObject({
  schemaVersion: z.literal(1),
  type: z.literal('presence.online'),
  eventId: z.uuid(),
  occurredAt: z.number().int().nonnegative(),
  userId: z.string().min(1).max(256),
  displayName: z.string().min(1).max(80),
});
export type PresenceOnlineEvent = z.infer<typeof PresenceOnlineEventSchema>;

export const PresenceOfflineEventSchema = z.strictObject({
  schemaVersion: z.literal(1),
  type: z.literal('presence.offline'),
  eventId: z.uuid(),
  occurredAt: z.number().int().nonnegative(),
  userId: z.string().min(1).max(256),
  reason: PresenceOfflineReasonSchema,
});
export type PresenceOfflineEvent = z.infer<typeof PresenceOfflineEventSchema>;

export const PresenceEventSchema = z.discriminatedUnion('type', [
  PresenceOnlineEventSchema,
  PresenceOfflineEventSchema,
]);
export type PresenceEvent = z.infer<typeof PresenceEventSchema>;

/**
 * Value stored in the compacted user-last-location topic.
 * A null payload acts as a Kafka tombstone for right-to-erasure.
 */
export const UserLastLocationSchema = z.strictObject({
  schemaVersion: z.literal(1),
  userId: z.string().min(1).max(256),
  displayName: z.string().min(1).max(80),
  position: LocationPositionSchema,
  occurredAt: z.number().int().nonnegative(),
  receivedAt: z.number().int().nonnegative(),
  eventId: z.uuid(),
  source: LocationSourceSchema,
});
export type UserLastLocation = z.infer<typeof UserLastLocationSchema>;
