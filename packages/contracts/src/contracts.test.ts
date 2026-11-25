import { describe, expect, it } from 'vitest';
import {
  ClientLocationUpdateSchema,
  DEFAULT_TIMINGS,
  LocationUpdatedEventSchema,
  PresenceEventSchema,
} from './index.js';

const VALID_UUID = '550e8400-e29b-41d4-a716-446655440000';

describe('ClientLocationUpdateSchema', () => {
  const basePayload = {
    eventId: VALID_UUID,
    lat: 23.0225,
    lng: 72.5714,
    accuracyM: 8.5,
    headingDeg: 180,
    speedMps: 4.2,
    occurredAt: 1_764_000_000_000,
    source: 'gps' as const,
  };

  it('accepts a valid location update payload', () => {
    const result = ClientLocationUpdateSchema.safeParse(basePayload);
    expect(result.success).toBe(true);
  });

  it('accepts nullable telemetry fields (desktop browsers often omit heading and speed)', () => {
    const result = ClientLocationUpdateSchema.safeParse({
      ...basePayload,
      accuracyM: null,
      headingDeg: null,
      speedMps: null,
    });
    expect(result.success).toBe(true);
  });

  it('rejects spoofed userId and any unknown keys (strict schema)', () => {
    const result = ClientLocationUpdateSchema.safeParse({
      ...basePayload,
      userId: 'bob',
    });
    expect(result.success).toBe(false);
  });

  it.each([
    ['latitude > 90', { lat: 91 }],
    ['latitude < -90', { lat: -90.1 }],
    ['longitude > 180', { lng: 181 }],
    ['NaN latitude', { lat: Number.NaN }],
    ['Infinity longitude', { lng: Number.POSITIVE_INFINITY }],
    ['string latitude', { lat: '23.0225' }],
    ['invalid eventId', { eventId: 'not-a-uuid' }],
    ['missing eventId', { eventId: undefined }],
    ['negative accuracy', { accuracyM: -1 }],
    ['heading > 360', { headingDeg: 361 }],
    ['speed > 1000 m/s', { speedMps: 1_001 }],
    ['unknown source', { source: 'spoofed' }],
  ])('rejects invalid payload: %s', (_label, override) => {
    const result = ClientLocationUpdateSchema.safeParse({
      ...basePayload,
      ...override,
    });
    expect(result.success).toBe(false);
  });
});

describe('Kafka event schemas', () => {
  it('validates a canonical LocationUpdatedEvent', () => {
    const event = {
      schemaVersion: 1,
      type: 'location.updated',
      eventId: VALID_UUID,
      occurredAt: 1_764_000_000_000,
      receivedAt: 1_764_000_000_120,
      userId: 'sub-alice-123',
      displayName: 'Alice Rider',
      position: {
        lat: 23.0225,
        lng: 72.5714,
        accuracyM: 12,
        headingDeg: 90,
        speedMps: 3.1,
      },
      source: 'gps',
    };
    const parsed = LocationUpdatedEventSchema.safeParse(event);
    expect(parsed.success).toBe(true);
  });

  it('validates presence.online and presence.offline events', () => {
    const online = PresenceEventSchema.safeParse({
      schemaVersion: 1,
      type: 'presence.online',
      eventId: VALID_UUID,
      occurredAt: 1_764_000_000_000,
      userId: 'sub-alice-123',
      displayName: 'Alice Rider',
    });
    expect(online.success).toBe(true);

    const offline = PresenceEventSchema.safeParse({
      schemaVersion: 1,
      type: 'presence.offline',
      eventId: VALID_UUID,
      occurredAt: 1_764_000_030_000,
      userId: 'sub-alice-123',
      reason: 'stale',
    });
    expect(offline.success).toBe(true);
  });

  it('keeps plan timing constants consistent', () => {
    expect(DEFAULT_TIMINGS.heartbeatMs).toBe(15_000);
    expect(DEFAULT_TIMINGS.staleFadeMs).toBe(25_000);
    expect(DEFAULT_TIMINGS.serverStaleMs).toBe(30_000);
    expect(DEFAULT_TIMINGS.clientRemoveMs).toBe(35_000);
  });
});
