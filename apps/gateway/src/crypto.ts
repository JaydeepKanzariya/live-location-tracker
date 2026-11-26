import { createCipheriv, createDecipheriv, createHmac, randomBytes } from 'node:crypto';

/**
 * AES-256-GCM for secrets we must keep in Redis (the IdP refresh token).
 * Output format: base64url(iv).base64url(authTag).base64url(ciphertext)
 */
export function encryptSecret(plaintext: string, keyHex: string): string {
  const key = Buffer.from(keyHex, 'hex');
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [iv, tag, ciphertext].map((b) => b.toString('base64url')).join('.');
}

export function decryptSecret(payload: string, keyHex: string): string {
  const parts = payload.split('.');
  if (parts.length !== 3) throw new Error('malformed encrypted payload');
  const [iv, tag, ciphertext] = parts.map((p) => Buffer.from(p, 'base64url')) as [
    Buffer,
    Buffer,
    Buffer,
  ];
  const decipher = createDecipheriv('aes-256-gcm', Buffer.from(keyHex, 'hex'), iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8');
}

/** 256 bits of randomness, safe to use as a cookie value. */
export function newSessionId(): string {
  return randomBytes(32).toString('base64url');
}

/**
 * Redis keys use HMAC(secret, sessionId) instead of the raw id, so a Redis dump or a
 * `KEYS *` does not reveal values that are valid session cookies.
 */
export function sessionKeyId(sessionId: string, secret: string): string {
  return createHmac('sha256', secret).update(sessionId).digest('base64url');
}
