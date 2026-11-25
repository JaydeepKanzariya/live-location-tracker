# 09 · Observability & Reliability

## SLOs

| SLI | SLO | Measured by |
|---|---|---|
| End-to-end update latency (emit → other client receives) | p99 < 500 ms, 99.5% of 5-min windows | client-reported `receivedAt - occurredAt` histogram + server spans |
| Location acceptance success | ≥ 99.9% of valid updates acked `ok` | `location_updates_total{result}` |
| History freshness | history-writer consumer lag < 30 s, 99% of the time | consumer-lag exporter |
| Login success | ≥ 99.5% of callbacks succeed | `auth_callbacks_total{result}` |

## Metrics (Prometheus, via OTel SDK)

**Gateway:** `socket_connections` (gauge), `location_updates_total{result=ok|duplicate|invalid|rate_limited|unavailable}`, `kafka_produce_duration_seconds` (histogram), `socket_emit_batch_size`, `nodejs_eventloop_lag_seconds`.
**Consumers:** `events_consumed_total{group}`, `events_invalid_total{reason}`, `db_batch_duration_seconds`, `db_rows_inserted_total`, `history_downsampled_total`.
**Kafka:** consumer lag per group/partition (kafka-exporter or Kafka UI), under-replicated partitions, request latency.
**Business:** active sharers, users on map.

## Distributed tracing

`traceparent` is injected into Kafka headers by the producer and extracted by every consumer, so one trace shows:

```
socket location:update ─┬─ redis dedupe/ratelimit
                        ├─ kafka.produce location-updates
                        │      ├─ kafka.consume broadcaster-gw-b ─ socket.emit location:batch
                        │      ├─ kafka.consume history-writer ─ db.insert (batch link)
                        │      └─ kafka.consume presence-projector ─ redis.geoadd
```

Batch consumers use **span links** (one batch span links to N producer spans). Sample 1–5% in production, 100% in dev. Backend: Grafana Tempo (or Jaeger locally).

## Logs
`pino` JSON logs with `traceId`, `userId` (hashed at INFO), `eventId`, `partition`, `offset`. **No raw coordinates at INFO level** (privacy). Shipped to Loki and queryable together with traces in Grafana.

## Dashboards (Grafana, provisioned as code)
1. **Live overview:** connections, updates/s by result, e2e latency p50/p99, active sharers.
2. **Kafka:** produce rate, consumer lag per group, rebalances.
3. **History pipeline:** batch size, rows/s, downsample ratio, DB latency, DLQ count.

## Alerts
| Alert | Condition | Severity |
|---|---|---|
| E2E latency SLO burn | 2 h burn rate > 14× | page |
| History lag | lag > 5 min for 10 min | ticket |
| DLQ receiving | > 0 records in 15 min | ticket |
| Produce failures | `unavailable` > 1% for 5 min | page |
| Event loop lag | > 200 ms p99 for 5 min | ticket |

## Reliability patterns checklist
- [x] Idempotent producer, idempotent sinks, dedupe keys
- [x] Commit after side effect; pause/resume partitions on sink failure
- [x] DLQ for poison messages; never block a partition
- [x] Graceful shutdown: drain sockets, flush producer, leave group cleanly
- [x] Health endpoints: `/livez` (process up), `/readyz` (Kafka + Redis reachable)
- [x] Timeouts on every network call; exponential backoff with jitter
- [x] Bounded memory: TTLs and caps on every in-memory structure
- [ ] Chaos drills (COULD): kill a broker, kill Postgres, kill a gateway pod mid-demo and show recovery
