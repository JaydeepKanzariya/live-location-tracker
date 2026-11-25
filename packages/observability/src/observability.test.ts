import { Writable } from 'node:stream';
import { describe, expect, it } from 'vitest';
import { createLogger, hashUserId, injectTraceHeaders } from './index.js';

describe('@llt/observability', () => {
  it('redacts raw coordinates and auth headers from INFO logs', () => {
    const lines: string[] = [];
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    });

    const logger = createLogger({ service: 'test-service', level: 'info', destination });
    logger.info({ lat: 23.0225, lng: 72.5714, userHash: hashUserId('alice-sub') }, 'location received');

    expect(lines).toHaveLength(1);
    const parsed = JSON.parse(lines[0] ?? '{}') as Record<string, unknown>;
    expect(parsed['lat']).toBe('[REDACTED]');
    expect(parsed['lng']).toBe('[REDACTED]');
    expect(parsed['userHash']).toBe(hashUserId('alice-sub'));
    expect(lines[0]).not.toContain('23.0225');
    expect(lines[0]).not.toContain('72.5714');
  });

  it('redacts nested coordinates and secrets, including inside arrays of events', () => {
    const lines: string[] = [];
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    });
    const logger = createLogger({ service: 'test-service', level: 'info', destination });
    logger.info(
      {
        event: { position: { lat: 11.111, lng: 22.222 } },
        batch: { events: [{ lat: 33.333, lng: 44.444 }] },
        session: { refreshToken: 'rt-secret-value' },
      },
      'nested',
    );
    for (const leaked of ['11.111', '22.222', '33.333', '44.444', 'rt-secret-value']) {
      expect(lines[0]).not.toContain(leaked);
    }
  });

  it('produces a stable 12-char hex hash for userId', () => {
    const h1 = hashUserId('oidc-sub-alice');
    const h2 = hashUserId('oidc-sub-alice');
    const h3 = hashUserId('oidc-sub-bob');
    expect(h1).toHaveLength(12);
    expect(h1).toBe(h2);
    expect(h1).not.toBe(h3);
  });

  it('preserves existing headers when injecting trace context', () => {
    const out = injectTraceHeaders({ 'content-type': 'application/json' });
    expect(out['content-type']).toBe('application/json');
  });
});
