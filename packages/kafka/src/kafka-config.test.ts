import { describe, expect, it, vi } from 'vitest';
import {
  CONFLUENT_BATCH_RUN_OPTIONS,
  buildConfluentClientConfig,
  buildConfluentSecurityConfig,
  makeConfluentCommit,
  makeKafkaJsCommit,
  nextOffset,
  toPublishResult,
  type BatchCommitPayload,
} from './kafka-config.js';

function mockPayload(): BatchCommitPayload & {
  resolveOffset: ReturnType<typeof vi.fn>;
  commitOffsetsIfNecessary: ReturnType<typeof vi.fn>;
} {
  return {
    resolveOffset: vi.fn(),
    commitOffsetsIfNecessary: vi.fn().mockResolvedValue(undefined),
  };
}

describe('Confluent batch consumer options', () => {
  it('turns OFF eachBatchAutoResolve so a paused batch is not marked processed', () => {
    expect(CONFLUENT_BATCH_RUN_OPTIONS.eachBatchAutoResolve).toBe(false);
  });

  it('Confluent commit stores the offset then commits it', async () => {
    const payload = mockPayload();
    await makeConfluentCommit(payload)('41');
    expect(payload.resolveOffset).toHaveBeenCalledWith('41');
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
  });
});

describe('KafkaJS batch commit', () => {
  it('commits EXPLICIT next offsets (the argument-less call is a no-op with autoCommit=false)', async () => {
    const payload = mockPayload();
    await makeKafkaJsCommit(payload, 'location-updates', 2)('41');

    expect(payload.resolveOffset).toHaveBeenCalledWith('41');
    expect(payload.commitOffsetsIfNecessary).toHaveBeenCalledWith({
      topics: [{ topic: 'location-updates', partitions: [{ partition: 2, offset: '42' }] }],
    });
  });

  it('nextOffset handles offsets beyond Number.MAX_SAFE_INTEGER', () => {
    expect(nextOffset('0')).toBe('1');
    expect(nextOffset('9007199254740993')).toBe('9007199254740994');
  });
});

describe('Confluent security config', () => {
  const sasl = { mechanism: 'plain' as const, username: 'u', password: 'p' };

  it('maps ssl + sasl to librdkafka properties', () => {
    expect(buildConfluentSecurityConfig({})).toEqual({});
    expect(buildConfluentSecurityConfig({ ssl: true })).toEqual({ 'security.protocol': 'ssl' });
    expect(buildConfluentSecurityConfig({ ssl: true, sasl })).toEqual({
      'security.protocol': 'sasl_ssl',
      'sasl.mechanism': 'PLAIN',
      'sasl.username': 'u',
      'sasl.password': 'p',
    });
    expect(buildConfluentSecurityConfig({ sasl })['security.protocol']).toBe('sasl_plaintext');
  });

  it('puts security on the shared client so producer, consumers and admin all inherit it', () => {
    const cfg = buildConfluentClientConfig({ brokers: ['b:9092'], clientId: 'c', ssl: true, sasl });
    expect(cfg['kafkaJS']).toEqual({ brokers: ['b:9092'], clientId: 'c' });
    expect(cfg['security.protocol']).toBe('sasl_ssl');
    expect(cfg['sasl.username']).toBe('u');
  });
});

describe('toPublishResult', () => {
  it('returns partition and offset from a real send result', () => {
    expect(toPublishResult({ partition: 1, baseOffset: '7' }, 't')).toEqual({ partition: 1, offset: '7' });
    expect(toPublishResult({ partition: 0, offset: '3' }, 't')).toEqual({ partition: 0, offset: '3' });
  });

  it('throws instead of inventing "partition 0, offset 0"', () => {
    expect(() => toPublishResult(undefined, 'location-updates')).toThrow(/no partition\/offset/);
    expect(() => toPublishResult({ partition: 1 }, 't')).toThrow();
    expect(() => toPublishResult({ baseOffset: '1' }, 't')).toThrow();
  });
});
