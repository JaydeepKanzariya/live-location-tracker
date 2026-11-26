import type { LocationUpdatedEvent } from '@llt/contracts';
import { InMemoryEventBus } from '@llt/kafka';
import { createLogger } from '@llt/observability';
import { describe, expect, it, vi } from 'vitest';
import { Broadcaster } from './broadcaster.js';

const logger = createLogger({ service: 'test', level: 'silent' });

function event(userId: string, lat: number, receivedAt: number): LocationUpdatedEvent {
  return {
    schemaVersion: 1,
    type: 'location.updated',
    eventId: crypto.randomUUID(),
    occurredAt: receivedAt - 5,
    receivedAt,
    userId,
    displayName: userId.toUpperCase(),
    position: { lat, lng: 72.5, accuracyM: null, headingDeg: null, speedMps: null },
    source: 'gps',
  };
}

async function setup(groupId = 'broadcaster-gateway-1', bus = new InMemoryEventBus(3)) {
  await bus.connect();
  const emitted: { room: string; event: string; payload: unknown }[] = [];
  const io = {
    to: (room: string) => ({
      emit: (ev: string, payload: unknown) => {
        emitted.push({ room, event: ev, payload });
        return true;
      },
    }),
  };
  const broadcaster = new Broadcaster({
    bus,
    io: io as never,
    topic: 'location-updates',
    groupId,
    flushIntervalMs: 60_000, // flushed by hand so the tests are deterministic
    logger,
  });
  await broadcaster.start();
  const publish = (e: LocationUpdatedEvent) =>
    bus.publish({ topic: 'location-updates', key: e.userId, value: JSON.stringify(e) });
  return { bus, broadcaster, emitted, publish };
}

describe('Broadcaster', () => {
  it('sends one batch to the authenticated room with the newest entry per user', async () => {
    const { broadcaster, emitted, publish } = await setup();
    await publish(event('alice', 1, 1_000));
    await publish(event('alice', 2, 2_000));
    await publish(event('alice', 3, 3_000));
    await publish(event('bob', 9, 1_500));
    broadcaster.flush();

    expect(emitted).toHaveLength(1);
    expect(emitted[0]).toMatchObject({ room: 'authenticated', event: 'location:batch' });
    const batch = emitted[0]!.payload as { userId: string; lat: number }[];
    expect(batch).toHaveLength(2);
    expect(batch.find((e) => e.userId === 'alice')?.lat).toBe(3);
    expect(batch.find((e) => e.userId === 'bob')?.lat).toBe(9);
  });

  it('does not let an older record overwrite a newer one', async () => {
    const { broadcaster, emitted, publish } = await setup();
    await publish(event('alice', 5, 5_000));
    await publish(event('alice', 1, 1_000)); // late arrival
    broadcaster.flush();
    expect((emitted[0]!.payload as { lat: number }[])[0]?.lat).toBe(5);
  });

  it('emits nothing when nothing arrived, and clears the buffer after each flush', async () => {
    const { broadcaster, emitted, publish } = await setup();
    broadcaster.flush();
    expect(emitted).toHaveLength(0);
    await publish(event('alice', 1, 1_000));
    broadcaster.flush();
    broadcaster.flush();
    expect(emitted).toHaveLength(1);
  });

  it('copies partition and offset into the entry', async () => {
    const { broadcaster, emitted, publish } = await setup();
    const ack = await publish(event('alice', 1, 1_000));
    broadcaster.flush();
    expect(emitted[0]!.payload).toEqual([
      expect.objectContaining({ partition: ack.partition, offset: ack.offset }),
    ]);
  });

  it('sends a poison record to the DLQ and keeps consuming', async () => {
    const { bus, broadcaster, emitted, publish } = await setup();
    const dlq = vi.spyOn(bus, 'publishDlq');
    await bus.publish({ topic: 'location-updates', key: 'x', value: '{not json' });
    await bus.publish({
      topic: 'location-updates',
      key: 'y',
      value: JSON.stringify({ ...event('mallory', 1, 1), extra: 'field' }), // strict schema rejects it
    });
    await publish(event('alice', 1, 1_000));
    broadcaster.flush();

    expect(dlq).toHaveBeenCalledTimes(2);
    expect(emitted).toHaveLength(1);
    expect((emitted[0]!.payload as { userId: string }[]).map((e) => e.userId)).toEqual(['alice']);
  });

  it('ignores tombstones', async () => {
    const { bus, broadcaster, emitted } = await setup();
    await bus.publish({ topic: 'location-updates', key: 'alice', value: null });
    broadcaster.flush();
    expect(emitted).toHaveLength(0);
  });

  it('FAN-OUT: broadcasters in different groups each get every event (one group per pod)', async () => {
    const bus = new InMemoryEventBus(3);
    const pod1 = await setup('broadcaster-gateway-1', bus);
    const pod2 = await setup('broadcaster-gateway-2', bus);
    await pod1.publish(event('alice', 1, 1_000));
    pod1.broadcaster.flush();
    pod2.broadcaster.flush();
    expect(pod1.emitted).toHaveLength(1);
    expect(pod2.emitted).toHaveLength(1);
  });

  it('stop() flushes what is waiting', async () => {
    const { broadcaster, emitted, publish } = await setup();
    await publish(event('alice', 1, 1_000));
    await broadcaster.stop();
    expect(emitted).toHaveLength(1);
  });
});
