import { z } from 'zod';
import {
  LocationSourceSchema,
  PresenceOfflineReasonSchema,
} from './events.js';

export const SocketErrorCodeSchema = z.enum([
  'INVALID_PAYLOAD',
  'RATE_LIMITED',
  'SESSION_EXPIRED',
  'UNAVAILABLE',
]);
export type SocketErrorCode = z.infer<typeof SocketErrorCodeSchema>;

/**
 * Strict client-to-server location:update payload.
 * WHY strictObject: userId MUST come from the authenticated server session (OIDC sub),
 * never from the client. Rejecting unknown keys guarantees spoofed userId fields fail fast.
 */
export const ClientLocationUpdateSchema = z.strictObject({
  eventId: z.uuid(),
  lat: z.number().finite().min(-90).max(90),
  lng: z.number().finite().min(-180).max(180),
  accuracyM: z.number().finite().min(0).max(100_000).nullable(),
  headingDeg: z.number().finite().min(0).max(360).nullable(),
  speedMps: z.number().finite().min(0).max(1_000).nullable(),
  occurredAt: z.number().int().nonnegative(),
  source: LocationSourceSchema.default('gps'),
});
export type ClientLocationUpdate = z.infer<typeof ClientLocationUpdateSchema>;

export const LocationUpdateAckSchema = z.union([
  z.strictObject({
    ok: z.literal(true),
    partition: z.number().int().nonnegative(),
    offset: z.string().min(1),
  }),
  z.strictObject({
    ok: z.literal(true),
    duplicate: z.literal(true),
  }),
  z.strictObject({
    ok: z.literal(false),
    code: SocketErrorCodeSchema,
    retryAfterMs: z.number().int().positive().optional(),
  }),
]);
export type LocationUpdateAck = z.infer<typeof LocationUpdateAckSchema>;

export const SessionUserSchema = z.strictObject({
  sub: z.string().min(1).max(256),
  displayName: z.string().min(1).max(80),
  email: z.email().optional(),
});
export type SessionUser = z.infer<typeof SessionUserSchema>;

export const SessionSettingsSchema = z.strictObject({
  minIntervalMs: z.number().int().positive(),
  heartbeatMs: z.number().int().positive(),
  staleFadeMs: z.number().int().positive(),
  staleAfterMs: z.number().int().positive(),
  clientRemoveMs: z.number().int().positive(),
});
export type SessionSettings = z.infer<typeof SessionSettingsSchema>;

export const SessionReadyPayloadSchema = z.strictObject({
  user: SessionUserSchema,
  settings: SessionSettingsSchema,
});
export type SessionReadyPayload = z.infer<typeof SessionReadyPayloadSchema>;

export const PresenceEntrySchema = z.strictObject({
  userId: z.string().min(1).max(256),
  displayName: z.string().min(1).max(80),
  lat: z.number().finite().min(-90).max(90),
  lng: z.number().finite().min(-180).max(180),
  accuracyM: z.number().finite().min(0).max(100_000).nullable(),
  headingDeg: z.number().finite().min(0).max(360).nullable(),
  speedMps: z.number().finite().min(0).max(1_000).nullable(),
  occurredAt: z.number().int().nonnegative(),
  receivedAt: z.number().int().nonnegative(),
  eventId: z.uuid(),
  source: LocationSourceSchema,
  partition: z.number().int().nonnegative().optional(),
  offset: z.string().min(1).optional(),
});
export type PresenceEntry = z.infer<typeof PresenceEntrySchema>;

export const UserOfflinePayloadSchema = z.strictObject({
  userId: z.string().min(1).max(256),
  reason: PresenceOfflineReasonSchema,
});
export type UserOfflinePayload = z.infer<typeof UserOfflinePayloadSchema>;

export interface ClientToServerEvents {
  'location:update': (
    payload: ClientLocationUpdate,
    ack?: (response: LocationUpdateAck) => void,
  ) => void;
  'sharing:stop': (payload?: Record<string, never>) => void;
}

export interface ServerToClientEvents {
  'session:ready': (payload: SessionReadyPayload) => void;
  'presence:snapshot': (entries: PresenceEntry[]) => void;
  'location:batch': (entries: PresenceEntry[]) => void;
  'user:offline': (payload: UserOfflinePayload) => void;
  'auth:expired': (payload: Record<string, never>) => void;
  'server:draining': (payload: Record<string, never>) => void;
}
