// Pure logic (no I/O) deciding WHAT the DB processor writes for a batch of events.
// Kept separate so it is unit-testable and easy to explain.
//
// Strategy:
//  1. Latest position  -> 1 upsert per user per batch (not per event). 20 events from one user = 1 write.
//  2. History trail    -> downsampled: keep a point only if the user moved >= minDistanceM
//                         OR >= minIntervalMs passed since the last kept point. A parked phone sending
//                         every 3 s produces ~2 rows/min instead of 20.
//  3. Offline events   -> nothing to persist (presence is a live concern, not history).
import { EVENT_TYPES, distanceMeters } from '../events/location-event.js';

/**
 * Does NOT mutate `lastKept`. The caller applies `keptUpdates` only after the write succeeded,
 * so a failed + retried batch is planned exactly the same way the second time.
 *
 * @param {Array<{event:object, partition:number, offset:string}>} items  events in Kafka order
 * @param {Map<string,{lat:number,lng:number,serverTs:number}>} lastKept  last stored point per user
 * @param {{minDistanceM:number, minIntervalMs:number}} opts
 */
export function planWrites(items, lastKept, { minDistanceM, minIntervalMs }) {
  const historyRows = [];
  const latestByUser = new Map();
  const keptUpdates = new Map();
  let skippedDownsampled = 0;

  for (const { event, partition, offset } of items) {
    if (event.type !== EVENT_TYPES.LOCATION_UPDATED) continue;

    const prev = latestByUser.get(event.userId);
    if (!prev || prev.serverTs <= event.serverTs) latestByUser.set(event.userId, event);

    const kept = keptUpdates.get(event.userId) ?? lastKept.get(event.userId);
    const keep =
      !kept ||
      event.serverTs - kept.serverTs >= minIntervalMs ||
      distanceMeters(kept, event) >= minDistanceM;

    if (keep) {
      historyRows.push({ ...event, partition, offset });
      keptUpdates.set(event.userId, { lat: event.lat, lng: event.lng, serverTs: event.serverTs });
    } else {
      skippedDownsampled += 1;
    }
  }

  return { historyRows, latestRows: [...latestByUser.values()], keptUpdates, skippedDownsampled };
}
