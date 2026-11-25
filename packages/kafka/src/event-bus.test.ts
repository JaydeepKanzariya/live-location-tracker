import { KAFKA_HEADERS, TOPIC_NAMES } from '@llt/contracts';
import { describe, expect, it } from 'vitest';
import { InMemoryEventBus } from './index.js';

describe('@llt/kafka InMemoryEventBus', () => {
  it('fans out to distinct consumer groups (broadcasters) while splitting within a shared group (db writer)', async () => {
    const bus = new InMemoryEventBus(3);
    await bus.connect();

    const gw1Received: string[] = [];
    const gw2Received: string[] = [];
    const dbWorker1: string[] = [];
    const dbWorker2: string[] = [];

    await bus.subscribe(
      { groupId: 'broadcaster-gw-1', topics: [TOPIC_NAMES.LOCATION_UPDATES] },
      async (m) => {
        gw1Received.push(m.value ?? '');
      },
    );
    await bus.subscribe(
      { groupId: 'broadcaster-gw-2', topics: [TOPIC_NAMES.LOCATION_UPDATES] },
      async (m) => {
        gw2Received.push(m.value ?? '');
      },
    );
    await bus.subscribe(
      { groupId: 'location-db-writer', topics: [TOPIC_NAMES.LOCATION_UPDATES] },
      async (m) => {
        dbWorker1.push(m.value ?? '');
      },
    );
    await bus.subscribe(
      { groupId: 'location-db-writer', topics: [TOPIC_NAMES.LOCATION_UPDATES] },
      async (m) => {
        dbWorker2.push(m.value ?? '');
      },
    );

    const users = ['alice', 'bob', 'carol', 'dave', 'erin', 'frank'];
    const sent: string[] = [];
    for (const round of [1, 2]) {
      for (const user of users) {
        const value = `${user}-${round}`;
        sent.push(value);
        await bus.publish({ topic: TOPIC_NAMES.LOCATION_UPDATES, key: user, value });
      }
    }

    // Both gateways get every message, in publish order (fan-out)
    expect(gw1Received).toEqual(sent);
    expect(gw2Received).toEqual(sent);
    // Shared DB writer group splits the work so each message is written exactly once
    expect([...dbWorker1, ...dbWorker2].sort()).toEqual([...sent].sort());
    expect(dbWorker1.length).toBeGreaterThan(0);
    expect(dbWorker2.length).toBeGreaterThan(0);
  });

  it('sends every event for one key to the same group member, in order (partition assignment)', async () => {
    const bus = new InMemoryEventBus(3);
    await bus.connect();
    const byMember: string[][] = [[], []];
    for (const member of [0, 1]) {
      await bus.subscribe(
        { groupId: 'location-db-writer', topics: [TOPIC_NAMES.LOCATION_UPDATES] },
        async (m) => {
          byMember[member]?.push(m.value ?? '');
        },
      );
    }

    for (let i = 1; i <= 5; i++) {
      for (const user of ['alice', 'bob', 'carol']) {
        await bus.publish({ topic: TOPIC_NAMES.LOCATION_UPDATES, key: user, value: `${user}-${i}` });
      }
    }

    for (const user of ['alice', 'bob', 'carol']) {
      const owners = byMember.filter((values) => values.some((v) => v.startsWith(`${user}-`)));
      expect(owners).toHaveLength(1); // never split across members
      const ownSeq = owners[0]!.filter((v) => v.startsWith(`${user}-`));
      expect(ownSeq).toEqual([1, 2, 3, 4, 5].map((i) => `${user}-${i}`)); // ordered
    }
  });

  it('routes poison messages to DLQ with source metadata headers', async () => {
    const bus = new InMemoryEventBus(3);
    await bus.connect();

    await bus.publishDlq({
      sourceTopic: TOPIC_NAMES.LOCATION_UPDATES,
      sourcePartition: 2,
      sourceOffset: '42',
      key: 'alice',
      rawValue: '{invalid-json',
      errorMessage: 'Unexpected token',
    });

    const dlqRecords = bus.getPublishedRecords(TOPIC_NAMES.LOCATION_UPDATES_DLQ);
    expect(dlqRecords).toHaveLength(1);
    expect(dlqRecords[0]?.headers[KAFKA_HEADERS.DLQ_ERROR]).toBe('Unexpected token');
    expect(dlqRecords[0]?.headers[KAFKA_HEADERS.DLQ_SOURCE_PARTITION]).toBe('2');
    expect(dlqRecords[0]?.headers[KAFKA_HEADERS.DLQ_SOURCE_OFFSET]).toBe('42');
  });

  it('tracks manual offset commits in batch consumers', async () => {
    const bus = new InMemoryEventBus(3);
    await bus.connect();

    await bus.subscribeBatch(
      { groupId: 'location-db-writer', topics: [TOPIC_NAMES.LOCATION_UPDATES], autoCommit: false },
      async (batch) => {
        const last = batch.messages[batch.messages.length - 1];
        if (last) {
          await batch.commitOffset(last.offset);
        }
      },
    );

    const res = await bus.publish({
      topic: TOPIC_NAMES.LOCATION_UPDATES,
      key: 'alice',
      value: 'ok',
    });

    expect(
      bus.getCommittedOffset('location-db-writer', TOPIC_NAMES.LOCATION_UPDATES, res.partition),
    ).toBe(res.offset);
  });
});
