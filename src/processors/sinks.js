// Where the DB processor writes. Two interchangeable implementations with the same interface:
//  - PostgresSink: real persistence (default with docker-compose)
//  - JsonlFileSink: "simulated" persistence when DATABASE_URL is empty (one JSON line per row)
import { appendFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { applySchema, closePool } from '../db/pool.js';
import { writeBatch } from '../db/history-repo.js';

export class PostgresSink {
  name = 'postgres';
  async init() {
    await applySchema();
  }
  async write(plan) {
    return writeBatch(plan);
  }
  async close() {
    await closePool();
  }
}

export class JsonlFileSink {
  name = 'jsonl-file (simulated DB)';
  constructor(filePath) {
    this.filePath = filePath;
  }
  async init() {
    await mkdir(path.dirname(this.filePath), { recursive: true });
  }
  async write({ historyRows }) {
    if (historyRows.length === 0) return { inserted: 0 };
    const lines = historyRows.map((row) => JSON.stringify({ ...row, writtenAt: new Date().toISOString() }));
    await appendFile(this.filePath, lines.join('\n') + '\n');
    return { inserted: historyRows.length };
  }
  async close() {}
}
