import {
  context,
  propagation,
  trace,
  type Context,
  type TextMapGetter,
  type TextMapSetter,
  type Tracer,
} from '@opentelemetry/api';

const headerSetter: TextMapSetter<Record<string, string>> = {
  set(carrier, key, value) {
    carrier[key] = value;
  },
};

const headerGetter: TextMapGetter<Record<string, string | undefined>> = {
  keys(carrier) {
    return Object.keys(carrier);
  },
  get(carrier, key) {
    return carrier[key];
  },
};

export function getTracer(name: string, version = '1.0.0'): Tracer {
  return trace.getTracer(name, version);
}

/**
 * Inject active OpenTelemetry W3C trace context (traceparent/tracestate) into
 * Kafka message headers so one trace spans gateway -> Kafka -> consumers.
 */
export function injectTraceHeaders(
  headers: Record<string, string> = {},
  activeContext: Context = context.active(),
): Record<string, string> {
  const carrier: Record<string, string> = { ...headers };
  propagation.inject(activeContext, carrier, headerSetter);
  return carrier;
}

/**
 * Extract W3C trace context from incoming Kafka message headers.
 */
export function extractTraceContext(
  headers: Record<string, string | undefined>,
  parentContext: Context = context.active(),
): Context {
  return propagation.extract(parentContext, headers, headerGetter);
}

export function getActiveTraceId(): string | undefined {
  return trace.getSpan(context.active())?.spanContext().traceId;
}
