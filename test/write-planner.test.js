import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planWrites } from '../src/processors/write-planner.js';
import { EVENT_TYPES } from '../src/events/location-event.js';

const opts = { minDistanceM: 10, minIntervalMs: 30_000 };
let offset = 0;
const item = (userId, serverTs, lat, lng = 72) => ({
  event: { type: EVENT_TYPES.LOCATION_UPDATED, eventId: `e${offset}`, userId, displayName: userId, lat, lng, serverTs },
  partition: 0,
  offset: String(offset++),
});

test('one latest-row per user per batch, history downsampled', () => {
  const items = [
    item('u1', 0, 19.0),
    item('u1', 3_000, 19.00001), // ~1 m, 3 s -> skipped
    item('u1', 6_000, 19.0002), // ~22 m -> kept
    item('u2', 1_000, 18.0),
    { event: { type: EVENT_TYPES.USER_OFFLINE, userId: 'u2', serverTs: 2_000 }, partition: 0, offset: '99' },
  ];
  const plan = planWrites(items, new Map(), opts);
  assert.equal(plan.historyRows.length, 3);
  assert.equal(plan.skippedDownsampled, 1);
  assert.equal(plan.latestRows.length, 2);
  assert.equal(plan.latestRows.find((r) => r.userId === 'u1').serverTs, 6_000, 'latest row is the newest even if not kept in history');
});

test('time threshold keeps a point for a stationary user', () => {
  const plan = planWrites([item('u1', 0, 19), item('u1', 10_000, 19), item('u1', 31_000, 19)], new Map(), opts);
  assert.deepEqual(plan.historyRows.map((r) => r.serverTs), [0, 31_000]);
});

test('does not mutate lastKept, so a failed batch is re-planned identically', () => {
  const lastKept = new Map();
  const items = [item('u1', 0, 19)];
  const first = planWrites(items, lastKept, opts);
  assert.equal(lastKept.size, 0);
  const retry = planWrites(items, lastKept, opts);
  assert.equal(retry.historyRows.length, first.historyRows.length);
  assert.equal(first.keptUpdates.get('u1').serverTs, 0);
});
