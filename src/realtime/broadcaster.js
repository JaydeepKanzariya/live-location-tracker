// Kafka consumer #1: reads `location-updates` and pushes each event to connected browsers.
//
// Consumer group: `socket-broadcaster-<INSTANCE_ID>`. The id is UNIQUE PER GATEWAY INSTANCE on purpose:
// sockets are spread across instances, so every instance must see every event (fan-out / pub-sub).
// Compare with the DB processor, whose instances SHARE one group id so they split the work (queue).
import { kafka } from '../kafka/client.js';
import { config } from '../config.js';
import { createLogger } from '../logger.js';
import { parseKafkaEvent, EVENT_TYPES } from '../events/location-event.js';
import { RecentIds } from '../events/recent-ids.js';
import { AUTHENTICATED_ROOM } from './socket-gateway.js';

const log = createLogger('broadcaster');

export function createBroadcaster({ io, presence }) {
  const consumer = kafka.consumer({
    groupId: config.kafka.broadcasterGroupId,
    // Low latency matters more than batching here: return a fetch as soon as any data exists.
    maxWaitTimeInMs: 100,
  });
  const seen = new RecentIds(200);
  let sweepTimer;

  async function handleMessage({ partition, message }) {
    const event = parseKafkaEvent(message.value);
    if (!event) {
      log.warn('skipping invalid event', { partition, offset: message.offset });
      return;
    }
    // Kafka is at-least-once: after a crash/rebalance the same message can be delivered again.
    if (seen.seen(event.userId, event.eventId)) return;

    if (event.type === EVENT_TYPES.LOCATION_UPDATED) {
      const kafkaMeta = { partition, offset: message.offset };
      if (!presence.applyLocation(event, kafkaMeta)) return; // older than what we have
      io.to(AUTHENTICATED_ROOM).emit('location:broadcast', presence.users.get(event.userId));
    } else if (event.type === EVENT_TYPES.USER_OFFLINE) {
      if (!presence.applyOffline(event)) return;
      seen.forget(event.userId);
      io.to(AUTHENTICATED_ROOM).emit('user:offline', { userId: event.userId, reason: event.reason });
    }
  }

  function sweepStaleUsers() {
    const removed = presence.sweepStale(Date.now(), config.location.staleAfterMs);
    for (const userId of removed) {
      seen.forget(userId);
      io.to(AUTHENTICATED_ROOM).emit('user:offline', { userId, reason: 'stale' });
    }
    if (removed.length) log.info('removed stale users', { userIds: removed });
  }

  return {
    async start() {
      await consumer.connect();
      // fromBeginning:false -> a (re)started gateway only cares about live positions, not old history.
      await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: false });
      await consumer.run({ eachMessage: handleMessage });
      sweepTimer = setInterval(sweepStaleUsers, config.location.staleSweepIntervalMs);
      log.info('broadcaster consuming', { groupId: config.kafka.broadcasterGroupId, topic: config.kafka.topic });
    },
    async stop() {
      clearInterval(sweepTimer);
      await consumer.disconnect();
    },
  };
}
