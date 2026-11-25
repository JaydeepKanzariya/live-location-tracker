import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import {
  validateClientLocation, buildLocationEvent, buildOfflineEvent, parseKafkaEvent, distanceMeters, EVENT_TYPES,
} from '../src/events/location-event.js';

const now = 1_750_000_000_000;
const valid = () => ({ eventId: randomUUID(), lat: 19.07, lng: 72.87, accuracy: 12, clientTs: now, source: 'gps' });

test('accepts a well-formed location', () => {
  const r = validateClientLocation(valid(), { now });
  assert.equal(r.ok, true);
  assert.equal(r.value.heading, null);
});

test('rejects invalid payloads', () => {
  const cases = [
    [null, /object/],
    ['hello', /object/],
    [{ ...valid(), eventId: 'abc' }, /UUID/],
    [{ ...valid(), lat: 91 }, /lat/],
    [{ ...valid(), lng: -181 }, /lng/],
    [{ ...valid(), lat: '19.0' }, /lat/],
    [{ ...valid(), lat: NaN }, /lat/],
    [{ ...valid(), lat: 0, lng: 0 }, /0,0/],
    [{ ...valid(), clientTs: now - 5 * 60_000 }, /clientTs/],
    [{ ...valid(), accuracy: -1 }, /accuracy/],
    [{ ...valid(), heading: 400 }, /heading/],
    [{ ...valid(), source: 'hacker' }, /source/],
  ];
  for (const [payload, msg] of cases) {
    const r = validateClientLocation(payload, { now });
    assert.equal(r.ok, false, `expected rejection for ${JSON.stringify(payload)}`);
    assert.match(r.error, msg);
  }
});

test('client cannot spoof user id: event takes identity from the session user', () => {
  const r = validateClientLocation({ ...valid(), userId: 'someone-else' }, { now });
  const event = buildLocationEvent(r.value, { id: 'real-sub', name: 'Alice' }, { now });
  assert.equal(event.userId, 'real-sub');
  assert.equal(event.type, EVENT_TYPES.LOCATION_UPDATED);
});

test('parseKafkaEvent round-trips and rejects garbage', () => {
  const r = validateClientLocation(valid(), { now });
  const event = buildLocationEvent(r.value, { id: 'u1', name: 'A' }, { now });
  assert.deepEqual(parseKafkaEvent(Buffer.from(JSON.stringify(event))), event);

  const offline = buildOfflineEvent({ id: 'u1', name: 'A' }, 'disconnected', { now });
  assert.equal(parseKafkaEvent(Buffer.from(JSON.stringify(offline))).type, EVENT_TYPES.USER_OFFLINE);

  assert.equal(parseKafkaEvent(Buffer.from('not json')), null);
  assert.equal(parseKafkaEvent(Buffer.from(JSON.stringify({ ...event, schemaVersion: 99 }))), null);
  assert.equal(parseKafkaEvent(Buffer.from(JSON.stringify({ ...event, lat: 200 }))), null);
  assert.equal(parseKafkaEvent(Buffer.from(JSON.stringify({ ...event, type: 'unknown' }))), null);
});

test('distanceMeters is roughly right', () => {
  // ~111 m per 0.001 deg latitude
  const d = distanceMeters({ lat: 19, lng: 72 }, { lat: 19.001, lng: 72 });
  assert.ok(d > 110 && d < 112, `got ${d}`);
});
