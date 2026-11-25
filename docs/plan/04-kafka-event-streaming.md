# 04 · Kafka & Event Streaming

## Topics

| Topic | Key | Partitions (dev / prod) | Retention | Cleanup | Producers | Consumers |
|---|---|---|---|---|---|---|
| `location-updates` | `userId` | 3 / 24 | 24 h (prod 72 h) | delete | gateway, simulator | broadcaster-*, history-writer, presence-projector |
| `presence-events` | `userId` | 3 / 12 | 24 h | delete | gateway (online/offline), presence-projector (stale) | broadcaster-* |
| `user-last-location` | `userId` | 3 / 24 | ∞ | **compact** | presence-projector | gateway (warm start), future services |
| `location-updates.dlq` | `userId` | 1 / 3 | 14 d | delete | any consumer | humans / replay tool |

**Why split `presence-events` from `location-updates`?** They're different streams with different consumers: history doesn't care about online/offline, and presence needs to emit `stale` events that no client produced. Keeping both keyed by `userId` preserves per-user ordering within each topic. (The prototype put both on one topic, which was acceptable for its size.)

**Why a compacted `user-last-location` topic?** Kafka keeps only the latest record per key, which turns the topic into a table. A new consumer can rebuild "where is everyone right now" by reading it, without touching Postgres.

## Partitioning

- **Key = `userId`** → all of one user's events land on one partition **in order**. We need ordering per user only, never across users.
- **Partition count = maximum consumer parallelism per group.** 24 partitions allows 24 history-writer pods. Choose it up front: adding partitions later **re-maps keys**, which breaks per-user ordering during the transition.
- **Hot keys:** one user can't exceed the gateway rate limit (1 msg/s), so no single key can overload a partition.

## Consumer groups: the key concept

```
                      ┌──────────── location-updates (P0 P1 P2) ────────────┐
group broadcaster-gw-a ──► gw-a reads P0,P1,P2   (gets EVERYTHING)
group broadcaster-gw-b ──► gw-b reads P0,P1,P2   (gets EVERYTHING)        ← pub/sub
group history-writer   ──► hw-1 reads P0,P1 ; hw-2 reads P2  (SPLIT)      ← work queue
group presence-proj    ──► pp-1 reads P0,P1,P2
```

- **Same group id → partitions are split** (load balancing). Used for history-writer and presence-projector.
- **Different group ids → each group gets every message** (fan-out). Used for gateways, because each pod holds different sockets.
- Each group keeps **its own committed offsets**, so a slow history-writer never slows the broadcaster.
- Rebalancing uses the **cooperative-sticky** assignor (`partition.assignment.strategy=cooperative-sticky`): only the moved partitions pause, not the whole group.
- Gateway broadcaster group ids use the pod name with `auto.offset.reset=latest`. They're ephemeral, so we set a short `offsets.retention.minutes` and they get cleaned up.

## Delivery semantics

| Stage | Setting | Guarantee |
|---|---|---|
| Producer | `enable.idempotence=true`, `acks=all`, `max.in.flight=5`, `compression=zstd`, `linger.ms=5` | no duplicates from producer retries; durable before ack |
| Broker (prod) | RF=3, `min.insync.replicas=2`, `unclean.leader.election=false` | survives the loss of one broker with no data loss |
| Broadcaster | auto-commit, `latest` | at-most-once is fine (live view; the next update corrects it) |
| History-writer | manual commit **after** DB COMMIT | at-least-once + idempotent sink = **effectively once** |
| Presence-projector | manual commit after Redis write | at-least-once; Redis ops are idempotent (GEOADD / SET) |

**Why not Kafka transactions (EOS)?** EOS covers *Kafka → Kafka* pipelines. Our sinks are Postgres and Redis, outside the transaction. Idempotent sinks keyed by `eventId` are simpler and just as correct.

## Event envelope (packages/contracts)

```ts
// Zod schema, shared by every producer and consumer
export const LocationUpdated = z.object({
  schemaVersion: z.literal(1),
  type: z.literal('location.updated'),
  eventId: z.string().uuid(),          // client-generated → idempotency key end-to-end
  occurredAt: z.number().int(),        // client clock (informational)
  receivedAt: z.number().int(),        // gateway clock (ordering, staleness)
  userId: z.string().min(1),           // OIDC sub, set by gateway from session
  displayName: z.string().max(80),
  position: z.object({
    lat: z.number().min(-90).max(90),
    lng: z.number().min(-180).max(180),
    accuracyM: z.number().min(0).max(100_000).nullable(),
    headingDeg: z.number().min(0).max(360).nullable(),
    speedMps: z.number().min(0).max(1_000).nullable(),
  }),
  source: z.enum(['gps', 'simulated']),
  h3r7: z.string().optional(),         // H3 cell, computed by gateway (for viewport fan-out)
});
```

Kafka **headers**: `traceparent`, `content-type: application/json`, `schema-version`, `event-type`.

**Schema evolution rules:** only additive optional fields within a version. A breaking change means `schemaVersion: 2`; consumers accept v1 and v2 during migration; producers switch last. With Schema Registry (COULD tier) these rules are enforced as `BACKWARD` compatibility.

## Poison messages & DLQ

A consumer that can't parse or validate a record:
1. logs it with partition/offset, increments `events_invalid_total{reason}`
2. produces it to `location-updates.dlq` with headers `x-error`, `x-source-partition` and `x-source-offset`
3. **commits and moves on**, so one bad record never blocks a partition

Transient sink errors (DB timeout) are **not** sent to the DLQ: retry with exponential backoff, pause the partition, and let lag build.

## Why Kafka (for the README and demo)

1. **Decoupling:** the gateway doesn't know who consumes. Adding geofencing or ETA means adding a new consumer group, with zero changes to the gateway.
2. **Buffering:** bursts and slow consumers are absorbed by the log, not by request threads.
3. **Replay:** a new or fixed consumer can reprocess the last N hours.
4. **Ordered per user, parallel across users,** thanks to key-based partitioning.
5. This is how rider/customer tracking works in industry: driver GPS goes into a log, which fans out to the customer app, dispatch, analytics and storage.
