// The contract for everything that travels through the `location-updates` topic.
// Both producers (gateway, simulator) and consumers (broadcaster, DB processor) use this file,
// so the shape of an event is defined in exactly one place.
import { randomUUID } from 'node:crypto';

export const SCHEMA_VERSION = 1;
export const EVENT_TYPES = Object.freeze({
  LOCATION_UPDATED: 'location.updated',
  USER_OFFLINE: 'user.offline',
});

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ALLOWED_SOURCES = new Set(['gps', 'simulated']);

const isFiniteNumber = (v) => typeof v === 'number' && Number.isFinite(v);
const optionalNumber = (v, min, max) =>
  v === undefined || v === null ? null : isFiniteNumber(v) && v >= min && v <= max ? v : undefined;

/**
 * Validate a raw `location:update` payload coming from a browser.
 * NOTE: the client never sends its user id - identity comes from the authenticated session.
 * @returns {{ok: true, value: object} | {ok: false, error: string}}
 */
export function validateClientLocation(payload, { now = Date.now(), maxClockSkewMs = 60_000 } = {}) {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return { ok: false, error: 'payload must be an object' };
  }
  const { eventId, lat, lng, accuracy, heading, speed, clientTs, source = 'gps' } = payload;

  if (typeof eventId !== 'string' || !UUID_RE.test(eventId)) return { ok: false, error: 'eventId must be a UUID' };
  if (!isFiniteNumber(lat) || lat < -90 || lat > 90) return { ok: false, error: 'lat must be a number in [-90, 90]' };
  if (!isFiniteNumber(lng) || lng < -180 || lng > 180) return { ok: false, error: 'lng must be a number in [-180, 180]' };
  if (lat === 0 && lng === 0) return { ok: false, error: 'lat/lng 0,0 looks like an uninitialised position' };
  if (!isFiniteNumber(clientTs)) return { ok: false, error: 'clientTs must be a unix epoch in ms' };
  if (Math.abs(now - clientTs) > maxClockSkewMs) return { ok: false, error: 'clientTs is too far from server time' };
  if (!ALLOWED_SOURCES.has(source)) return { ok: false, error: `source must be one of ${[...ALLOWED_SOURCES].join(', ')}` };

  const acc = optionalNumber(accuracy, 0, 100_000);
  const hdg = optionalNumber(heading, 0, 360);
  const spd = optionalNumber(speed, 0, 1_000); // m/s
  if (acc === undefined) return { ok: false, error: 'accuracy must be a number in [0, 100000]' };
  if (hdg === undefined) return { ok: false, error: 'heading must be a number in [0, 360]' };
  if (spd === undefined) return { ok: false, error: 'speed must be a number in [0, 1000]' };

  return {
    ok: true,
    value: { eventId, lat, lng, accuracy: acc, heading: hdg, speed: spd, clientTs, source },
  };
}

/** Build the Kafka event for a validated location. `user` comes from the OIDC session. */
export function buildLocationEvent(location, user, { gatewayInstance, now = Date.now() } = {}) {
  return {
    schemaVersion: SCHEMA_VERSION,
    type: EVENT_TYPES.LOCATION_UPDATED,
    eventId: location.eventId,
    userId: user.id,
    displayName: user.name,
    lat: location.lat,
    lng: location.lng,
    accuracy: location.accuracy,
    heading: location.heading,
    speed: location.speed,
    source: location.source,
    clientTs: location.clientTs,
    serverTs: now,
    gatewayInstance: gatewayInstance ?? null,
  };
}

export function buildOfflineEvent(user, reason, { now = Date.now() } = {}) {
  return {
    schemaVersion: SCHEMA_VERSION,
    type: EVENT_TYPES.USER_OFFLINE,
    eventId: randomUUID(),
    userId: user.id,
    displayName: user.name,
    reason,
    serverTs: now,
  };
}

/**
 * Parse + validate a message read from Kafka. Consumers never trust the topic blindly:
 * a bad producer or a schema change must not crash the consumer loop.
 * @returns {object|null} the event, or null if it should be skipped
 */
export function parseKafkaEvent(buffer) {
  let event;
  try {
    event = JSON.parse(buffer.toString('utf8'));
  } catch {
    return null;
  }
  if (!event || event.schemaVersion !== SCHEMA_VERSION) return null;
  if (typeof event.userId !== 'string' || event.userId.length === 0) return null;
  if (typeof event.eventId !== 'string' || !isFiniteNumber(event.serverTs)) return null;

  if (event.type === EVENT_TYPES.LOCATION_UPDATED) {
    if (!isFiniteNumber(event.lat) || event.lat < -90 || event.lat > 90) return null;
    if (!isFiniteNumber(event.lng) || event.lng < -180 || event.lng > 180) return null;
    return event;
  }
  if (event.type === EVENT_TYPES.USER_OFFLINE) return event;
  return null;
}

/** Great-circle distance in metres (haversine). Used for history downsampling. */
export function distanceMeters(a, b) {
  const R = 6_371_000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}
