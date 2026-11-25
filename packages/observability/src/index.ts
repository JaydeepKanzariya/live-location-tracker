export { createLogger, DEFAULT_REDACT_PATHS, hashUserId } from './logger.js';
export type { CreateLoggerOptions } from './logger.js';
export {
  extractTraceContext,
  getActiveTraceId,
  getTracer,
  injectTraceHeaders,
} from './otel.js';
