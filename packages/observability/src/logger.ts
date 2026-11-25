import { createHash } from 'node:crypto';
import pino, { type DestinationStream, type Logger, type LoggerOptions } from 'pino';

const COORDINATE_KEYS = ['lat', 'lng', 'position'];
const SECRET_KEYS = ['password', 'secret', 'accessToken', 'refreshToken', 'idToken'];

/**
 * Redact raw coordinates and auth secrets so they never leak into logs.
 * Location data is sensitive personal data (docs/plan/06-auth-security.md).
 *
 * LIMIT: pino redaction is path-based. These paths cover a key at the top level, one or two
 * objects deep, and inside arrays at those levels (e.g. `{ batch: { events: [{ lat }] } }`).
 * Anything deeper is NOT redacted. Rule: log ids, counts and hashed user ids, never whole
 * events or arrays of events.
 */
export const DEFAULT_REDACT_PATHS = [
  ...COORDINATE_KEYS.flatMap((k) => [k, `*.${k}`, `*.*.${k}`, `*[*].${k}`, `*.*[*].${k}`]),
  ...SECRET_KEYS.flatMap((k) => [k, `*.${k}`]),
  'req.headers.cookie',
  'req.headers.authorization',
];

export interface CreateLoggerOptions {
  service: string;
  level?: string;
  destination?: DestinationStream;
}

export function createLogger(options: CreateLoggerOptions): Logger {
  const pinoOptions: LoggerOptions = {
    level: options.level ?? process.env['LOG_LEVEL'] ?? 'info',
    base: { service: options.service },
    redact: {
      paths: DEFAULT_REDACT_PATHS,
      censor: '[REDACTED]',
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  };

  return options.destination ? pino(pinoOptions, options.destination) : pino(pinoOptions);
}

/**
 * One-way short hash of OIDC sub for correlating logs without logging raw user identifiers.
 */
export function hashUserId(userId: string): string {
  return createHash('sha256').update(userId).digest('hex').slice(0, 12);
}
