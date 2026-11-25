import pg from 'pg';
import { readFile } from 'node:fs/promises';
import { config } from '../config.js';

let pool;

/** Lazily create the pool. Returns null when DATABASE_URL is not configured. */
export function getPool() {
  if (!config.db.url) return null;
  if (!pool) {
    pool = new pg.Pool({ connectionString: config.db.url, max: 5 });
    pool.on('error', (err) => console.error('[pg] idle client error', err.message));
  }
  return pool;
}

export async function applySchema() {
  const sql = await readFile(new URL('./schema.sql', import.meta.url), 'utf8');
  await getPool().query(sql);
}

export async function closePool() {
  if (pool) await pool.end();
  pool = undefined;
}
