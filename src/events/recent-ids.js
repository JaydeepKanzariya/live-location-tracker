// Bounded "have I seen this event id recently?" memory, kept per user.
// Used by the gateway (client retries) and by the broadcaster (Kafka at-least-once redelivery).
// Memory is capped: each user keeps at most `maxPerKey` ids, oldest evicted first.
export class RecentIds {
  constructor(maxPerKey = 100) {
    this.maxPerKey = maxPerKey;
    this.byKey = new Map(); // key -> Set (Set preserves insertion order, so first item = oldest)
  }

  /** Returns true if `id` was already recorded for `key`; otherwise records it and returns false. */
  seen(key, id) {
    let ids = this.byKey.get(key);
    if (!ids) {
      ids = new Set();
      this.byKey.set(key, ids);
    }
    if (ids.has(id)) return true;
    ids.add(id);
    if (ids.size > this.maxPerKey) ids.delete(ids.values().next().value);
    return false;
  }

  /** Un-record an id, e.g. when publishing failed and the client should be allowed to retry it. */
  release(key, id) {
    this.byKey.get(key)?.delete(id);
  }

  forget(key) {
    this.byKey.delete(key);
  }
}
