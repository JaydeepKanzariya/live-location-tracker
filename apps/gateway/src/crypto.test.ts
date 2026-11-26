import { describe, expect, it } from 'vitest';
import { decryptSecret, encryptSecret, newSessionId, sessionKeyId } from './crypto.js';

const KEY = 'ab'.repeat(32);

describe('crypto helpers', () => {
  it('round-trips a secret', () => {
    const enc = encryptSecret('refresh-token-value', KEY);
    expect(enc).not.toContain('refresh-token-value');
    expect(decryptSecret(enc, KEY)).toBe('refresh-token-value');
  });

  it('uses a fresh IV, so the same input encrypts differently each time', () => {
    expect(encryptSecret('x', KEY)).not.toBe(encryptSecret('x', KEY));
  });

  it('rejects a wrong key and tampered data', () => {
    const enc = encryptSecret('secret', KEY);
    expect(() => decryptSecret(enc, 'cd'.repeat(32))).toThrow();
    const [iv, tag, data] = enc.split('.') as [string, string, string];
    const flipped = (data[0] === 'A' ? 'B' : 'A') + data.slice(1);
    expect(() => decryptSecret([iv, tag, flipped].join('.'), KEY)).toThrow();
    expect(() => decryptSecret('not-valid', KEY)).toThrow(/malformed/);
  });

  it('creates unique, long session ids and a stable, different Redis key id', () => {
    const a = newSessionId();
    expect(a.length).toBeGreaterThanOrEqual(43);
    expect(newSessionId()).not.toBe(a);
    expect(sessionKeyId(a, 'secret')).toBe(sessionKeyId(a, 'secret'));
    expect(sessionKeyId(a, 'secret')).not.toBe(a);
    expect(sessionKeyId(a, 'secret')).not.toBe(sessionKeyId(a, 'other-secret'));
  });
});
