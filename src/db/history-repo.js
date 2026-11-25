// SQL for location history. One multi-row INSERT per Kafka batch instead of one INSERT per event.
import { getPool } from './pool.js';

const HISTORY_COLUMNS = [
  'event_id', 'user_id', 'lat', 'lng', 'accuracy', 'heading', 'speed',
  'source', 'recorded_at', 'kafka_partition', 'kafka_offset',
];

/** Build "($1,$2,...),($12,...)" placeholders + flat params for a multi-row insert. */
function multiRowValues(rows, toValues) {
  const params = [];
  const tuples = rows.map((row) => {
    const values = toValues(row);
    const start = params.length;
    params.push(...values);
    return `(${values.map((_, i) => `$${start + i + 1}`).join(',')})`;
  });
  return { sql: tuples.join(','), params };
}

/**
 * Write one batch atomically: history rows (deduplicated by event_id) + latest-position upserts.
 * Runs in a transaction so a crash never leaves half a batch committed while Kafka offsets are not.
 * @returns {Promise<{inserted:number}>}
 */
export async function writeBatch({ historyRows, latestRows }) {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    let inserted = 0;

    if (historyRows.length) {
      const { sql, params } = multiRowValues(historyRows, (r) => [
        r.eventId, r.userId, r.lat, r.lng, r.accuracy, r.heading, r.speed,
        r.source, new Date(r.serverTs), r.partition, r.offset,
      ]);
      const res = await client.query(
        `INSERT INTO location_history (${HISTORY_COLUMNS.join(',')}) VALUES ${sql}
         ON CONFLICT (event_id) DO NOTHING`,
        params,
      );
      inserted = res.rowCount;
    }

    if (latestRows.length) {
      const { sql, params } = multiRowValues(latestRows, (r) => [
        r.userId, r.displayName, r.lat, r.lng, new Date(r.serverTs),
      ]);
      await client.query(
        `INSERT INTO user_last_location (user_id, display_name, lat, lng, recorded_at) VALUES ${sql}
         ON CONFLICT (user_id) DO UPDATE SET
           display_name = EXCLUDED.display_name, lat = EXCLUDED.lat, lng = EXCLUDED.lng,
           recorded_at = EXCLUDED.recorded_at, updated_at = now()
         WHERE user_last_location.recorded_at < EXCLUDED.recorded_at`,
        params,
      );
    }

    await client.query('COMMIT');
    return { inserted };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export async function getUserHistory(userId, limit) {
  const { rows } = await getPool().query(
    `SELECT lat, lng, accuracy, source, recorded_at AS "recordedAt"
       FROM location_history
      WHERE user_id = $1
      ORDER BY recorded_at DESC
      LIMIT $2`,
    [userId, limit],
  );
  return rows.reverse(); // oldest -> newest, ready to draw as a polyline
}
