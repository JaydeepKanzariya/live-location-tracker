// In-memory view of "who is on the map right now", built ONLY from Kafka events.
// Because every gateway instance consumes the full topic, every instance converges on the same view.
export class PresenceStore {
  constructor() {
    this.users = new Map(); // userId -> last known location event (+ kafka metadata)
  }

  /**
   * Apply a location event. Returns false if it is older than what we already have
   * (late/out-of-order delivery or a replay) so the caller doesn't broadcast it.
   */
  applyLocation(event, kafkaMeta) {
    const current = this.users.get(event.userId);
    if (current && current.serverTs > event.serverTs) return false;
    this.users.set(event.userId, {
      userId: event.userId,
      displayName: event.displayName,
      lat: event.lat,
      lng: event.lng,
      accuracy: event.accuracy ?? null,
      heading: event.heading ?? null,
      speed: event.speed ?? null,
      source: event.source,
      serverTs: event.serverTs,
      kafka: kafkaMeta,
    });
    return true;
  }

  /** Remove a user because of an offline event. Ignored if the user sent a newer location since. */
  applyOffline(event) {
    const current = this.users.get(event.userId);
    if (!current || current.serverTs > event.serverTs) return false;
    this.users.delete(event.userId);
    return true;
  }

  /** Remove users with no update for `staleAfterMs`. Returns the removed user ids. */
  sweepStale(now, staleAfterMs) {
    const removed = [];
    for (const [userId, entry] of this.users) {
      if (now - entry.serverTs > staleAfterMs) {
        this.users.delete(userId);
        removed.push(userId);
      }
    }
    return removed;
  }

  snapshot() {
    return [...this.users.values()];
  }
}
