// Kafka consumer #2 (separate process): persists location history.
//
//   npm run processor
//
// Consumer group `location-db-writer` is SHARED by all processor instances, so Kafka splits the
// topic's partitions between them. Run 3 copies -> each owns 1 of the 3 partitions.
// It is completely independent of the socket broadcaster: if Postgres is slow or down, live map
// updates keep flowing; this consumer just builds up lag and catches up later from its committed offset.
import { kafka, ensureTopic } from '../kafka/client.js';
import { config } from '../config.js';
import { createLogger } from '../logger.js';
import { parseKafkaEvent } from '../events/location-event.js';
import { planWrites } from './write-planner.js';
import { PostgresSink, JsonlFileSink } from './sinks.js';

const log = createLogger('db-processor');

const sink = config.db.url ? new PostgresSink() : new JsonlFileSink(config.db.simulatedFile);
const lastKept = new Map(); // downsampling state per user (resets on restart -> at most one extra point)

const consumer = kafka.consumer({
  groupId: config.kafka.dbWriterGroupId,
  // Natural micro-batching: the broker holds the fetch until 16 KB is available or 2 s pass.
  // Fewer, bigger batches => fewer DB round-trips.
  minBytes: 16 * 1024,
  maxWaitTimeInMs: 2_000,
});

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function handleBatch({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) {
  for (const messages of chunk(batch.messages, config.db.batchMaxSize)) {
    if (!isRunning() || isStale()) return;

    const items = [];
    let invalid = 0;
    for (const message of messages) {
      const event = parseKafkaEvent(message.value);
      if (event) items.push({ event, partition: batch.partition, offset: message.offset });
      else invalid += 1;
    }

    const plan = planWrites(items, lastKept, {
      minDistanceM: config.db.historyMinDistanceM,
      minIntervalMs: config.db.historyMinIntervalMs,
    });

    // If this throws, offsets are NOT committed and kafkajs re-delivers the batch (at-least-once).
    // Re-delivery is safe because history inserts are idempotent (ON CONFLICT (event_id) DO NOTHING).
    const { inserted } = await sink.write(plan);
    for (const [userId, point] of plan.keptUpdates) lastKept.set(userId, point);

    const lastOffset = messages[messages.length - 1].offset;
    resolveOffset(lastOffset);
    await commitOffsetsIfNecessary();
    await heartbeat();

    log.info('batch persisted', {
      partition: batch.partition,
      offsets: `${messages[0].offset}-${lastOffset}`,
      received: messages.length,
      invalid,
      historyInserted: inserted,
      downsampled: plan.skippedDownsampled,
      latestUpserts: plan.latestRows.length,
      lag: Number(batch.highWatermark) - Number(lastOffset) - 1,
    });
  }
}

async function main() {
  await sink.init();
  log.info('sink ready', { sink: sink.name });

  await ensureTopic();
  await consumer.connect();
  // fromBeginning:true -> a brand-new processor group replays retained events so no history is lost.
  await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) =>
    log.info('joined consumer group', { groupId: payload.groupId, assigned: payload.memberAssignment }),
  );

  await consumer.run({
    eachBatchAutoResolve: false, // we resolve offsets ourselves, only AFTER the DB write succeeds
    eachBatch: handleBatch,
  });
  log.info('db processor consuming', { groupId: config.kafka.dbWriterGroupId, topic: config.kafka.topic });
}

async function shutdown(signal) {
  log.info('shutting down', { signal });
  await consumer.disconnect().catch(() => {});
  await sink.close().catch(() => {});
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  log.error('db processor failed to start', { error: err.message });
  process.exit(1);
});
