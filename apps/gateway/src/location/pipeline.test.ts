import { LocationUpdatedEventSchema, type SessionUser } from '@llt/contracts';
import { InMemoryEventBus } from '@llt/kafka';
import { createLogger } from '@llt/observability';
import { describe, expect, it, vi } from 'vitest';
import { InMemoryRateLimiter } from '../ratelimit/token-bucket.js';
import { InMemoryDedupeStore } from './dedupe.js';
import { createLocationPipeline } from './pipeline.js';

const alice: SessionUser = { sub: 'alice-sub', displayName: 'Alice' };
const NOW = 1_800_000_000_000;

function validPayload(overrides: Record<string, unknown> = {}) {
  return {
    eventId: crypto.randomUUID(),
    lat: 23.0225,
    lng: 72.5714,
    accuracyM: 12,
    headingDeg: null,
    speedMps: null,
    occurredAt: NOW - 100,
    source: 'gps',
    ...overrides,
  };
}

async function setup(limiter = new InMemoryRateLimiter({ refillPerSec: 1, burst: 3 }, () => NOW)) {
  const bus = new InMemoryEventBus(3);
  await bus.connect();
  const received: { key: string | null; value: string | null; headers: Record<string, string> }[] =
    [];
  await bus.subscribe({ groupId: 'test', topics: ['location-updates'] }, async (m) => {
    received.push({ key: m.key, value: m.value, headers: m.headers });
  });
  const handle = createLocationPipeline({
    bus,
    dedupe: new InMemoryDedupeStore(),
    limiter,
    topic: 'location-updates',
    maxClockSkewMs: 60_000,
    logger: createLogger({ service: 'test', level: 'silent' }),
    now: () => NOW,
  });
  return { handle, received, bus };
}

describe('location pipeline', () => {
  it('accepts a valid update, enriches it from the session and produces one record', async () => {
    const { handle, received } = await setup();
    const payload = validPayload();
    const ack = await handle(alice, payload);

    expect(ack).toMatchObject({ ok: true, partition: expect.any(Number), offset: expect.any(String) });
    expect(received).toHaveLength(1);
    expect(received[0]?.key).toBe('alice-sub');
    const event = LocationUpdatedEventSchema.parse(JSON.parse(received[0]?.value ?? ''));
    expect(event.userId).toBe('alice-sub');
    expect(event.displayName).toBe('Alice');
    expect(event.receivedAt).toBe(NOW);
    expect(event.eventId).toBe(payload.eventId);
    expect(received[0]?.headers['event-type']).toBe('location.updated');
  });

  it('SPOOFING: a payload that carries someone else\'s userId is rejected and nothing is produced', async () => {
    const { handle, received } = await setup();
    const ack = await handle(alice, validPayload({ userId: 'bob-sub' }));
    expect(ack).toEqual({ ok: false, code: 'INVALID_PAYLOAD' });
    expect(received).toHaveLength(0);
  });

  it('DUPLICATE: the same eventId twice gives one record and a duplicate ack', async () => {
    const { handle, received } = await setup();
    const payload = validPayload();
    const first = await handle(alice, payload);
    const second = await handle(alice, payload);
    expect(first.ok && 'partition' in first).toBe(true);
    expect(second).toEqual({ ok: true, duplicate: true });
    expect(received).toHaveLength(1);
  });

  it.each([
    ['latitude 91', { lat: 91 }],
    ['longitude -181', { lng: -181 }],
    ['NaN latitude', { lat: Number.NaN }],
    ['numbers sent as strings', { lat: '23.0', lng: '72.5' }],
    ['missing eventId', { eventId: undefined }],
    ['eventId not a UUID', { eventId: 'abc' }],
    ['unknown key', { extra: 1 }],
    ['negative accuracy', { accuracyM: -5 }],
    ['clock far in the future', { occurredAt: NOW + 10 * 60_000 }],
  ])('INVALID: %s is rejected and nothing is produced', async (_name, override) => {
    const { handle, received } = await setup();
    const ack = await handle(alice, validPayload(override));
    expect(ack).toEqual({ ok: false, code: 'INVALID_PAYLOAD' });
    expect(received).toHaveLength(0);
  });

  it.each([null, undefined, 'text', 42, []])('INVALID: non-object payload %j is rejected', async (p) => {
    const { handle, received } = await setup();
    expect(await handle(alice, p)).toEqual({ ok: false, code: 'INVALID_PAYLOAD' });
    expect(received).toHaveLength(0);
  });

  it('RATE LIMIT: 5 updates at once -> 3 accepted (burst), 2 RATE_LIMITED with retryAfterMs', async () => {
    const { handle, received } = await setup();
    const acks = [];
    for (let i = 0; i < 5; i++) acks.push(await handle(alice, validPayload()));
    expect(acks.filter((a) => a.ok)).toHaveLength(3);
    const limited = acks.filter((a) => !a.ok);
    expect(limited).toHaveLength(2);
    for (const a of limited) {
      expect(a).toMatchObject({ ok: false, code: 'RATE_LIMITED' });
      expect((a as { retryAfterMs: number }).retryAfterMs).toBeGreaterThan(0);
    }
    expect(received).toHaveLength(3);
  });

  it('RATE LIMIT: limits are per user, so bob is not slowed down by alice', async () => {
    const { handle } = await setup();
    for (let i = 0; i < 5; i++) await handle(alice, validPayload());
    const bobAck = await handle({ sub: 'bob-sub', displayName: 'Bob' }, validPayload());
    expect(bobAck.ok).toBe(true);
  });

  it('RETRY: a rate-limited update is not remembered as a duplicate, so the retry works later', async () => {
    let t = NOW;
    const limiter = new InMemoryRateLimiter({ refillPerSec: 1, burst: 1 }, () => t);
    const { handle, received } = await setup(limiter);
    await handle(alice, validPayload()); // uses the only token
    const retried = validPayload();
    expect(await handle(alice, retried)).toMatchObject({ ok: false, code: 'RATE_LIMITED' });
    t += 1_500; // a token has refilled
    const again = await handle(alice, retried); // same eventId
    expect(again).toMatchObject({ ok: true, partition: expect.any(Number) });
    expect(received).toHaveLength(2);
  });

  it('UNAVAILABLE: when Kafka fails the client gets UNAVAILABLE and the retry is not "duplicate"', async () => {
    const { handle, bus } = await setup();
    const publish = vi.spyOn(bus, 'publish').mockRejectedValueOnce(new Error('broker down'));
    const payload = validPayload();
    expect(await handle(alice, payload)).toEqual({ ok: false, code: 'UNAVAILABLE' });
    publish.mockRestore();
    const retry = await handle(alice, payload);
    expect(retry).toMatchObject({ ok: true, partition: expect.any(Number) });
  });
});
