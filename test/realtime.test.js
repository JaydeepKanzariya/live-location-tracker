import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RecentIds } from '../src/events/recent-ids.js';
import { PresenceStore } from '../src/realtime/presence.js';

test('RecentIds detects duplicates per user and stays bounded', () => {
  const ids = new RecentIds(3);
  assert.equal(ids.seen('u1', 'a'), false);
  assert.equal(ids.seen('u1', 'a'), true);
  assert.equal(ids.seen('u2', 'a'), false, 'same id for a different user is not a duplicate');
  ids.seen('u1', 'b');
  ids.seen('u1', 'c');
  ids.seen('u1', 'd'); // evicts 'a'
  assert.equal(ids.seen('u1', 'a'), false);
  ids.release('u1', 'd');
  assert.equal(ids.seen('u1', 'd'), false, 'released id can be retried');
});

const loc = (userId, serverTs, extra = {}) => ({ userId, displayName: userId, lat: 1, lng: 1, source: 'gps', serverTs, ...extra });

test('PresenceStore ignores out-of-order location events', () => {
  const p = new PresenceStore();
  assert.equal(p.applyLocation(loc('u1', 200, { lat: 2 }), {}), true);
  assert.equal(p.applyLocation(loc('u1', 100, { lat: 9 }), {}), false);
  assert.equal(p.users.get('u1').lat, 2);
});

test('PresenceStore offline only removes if no newer location arrived', () => {
  const p = new PresenceStore();
  p.applyLocation(loc('u1', 200), {});
  assert.equal(p.applyOffline({ userId: 'u1', serverTs: 150 }), false);
  assert.equal(p.users.has('u1'), true);
  assert.equal(p.applyOffline({ userId: 'u1', serverTs: 250 }), true);
  assert.equal(p.users.has('u1'), false);
});

test('PresenceStore sweeps stale users', () => {
  const p = new PresenceStore();
  p.applyLocation(loc('old', 1_000), {});
  p.applyLocation(loc('fresh', 40_000), {});
  assert.deepEqual(p.sweepStale(45_000, 30_000), ['old']);
  assert.deepEqual(p.snapshot().map((u) => u.userId), ['fresh']);
});
