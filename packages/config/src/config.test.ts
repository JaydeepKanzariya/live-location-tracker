import { describe, expect, it } from 'vitest';
import {
  GatewayEnvSchema,
  HistoryWriterEnvSchema,
  PresenceProjectorEnvSchema,
  loadConfig,
} from './index.js';

describe('@llt/config', () => {
  it('loads gateway defaults cleanly from an empty env', () => {
    const cfg = loadConfig(GatewayEnvSchema, {});
    expect(cfg.PORT).toBe(3000);
    expect(cfg.KAFKA_BROKERS).toEqual(['localhost:9092']);
    expect(cfg.KAFKA_DB_WRITER_GROUP).toBe('location-db-writer');
    expect(cfg.LOCATION_HEARTBEAT_MS).toBe(15_000);
    expect(cfg.LOCATION_STALE_AFTER_MS).toBe(30_000);
    expect(cfg.LOCATION_CLIENT_REMOVE_MS).toBe(35_000);
  });

  it('parses comma-separated brokers and boolean flags', () => {
    const cfg = loadConfig(GatewayEnvSchema, {
      KAFKA_BROKERS: 'broker1:9092, broker2:9092',
      KAFKA_SSL: 'true',
      OIDC_ALLOW_INSECURE: 'false',
    });
    expect(cfg.KAFKA_BROKERS).toEqual(['broker1:9092', 'broker2:9092']);
    expect(cfg.KAFKA_SSL).toBe(true);
    expect(cfg.OIDC_ALLOW_INSECURE).toBe(false);
  });

  it('rejects invalid port or malformed OIDC issuer URL', () => {
    expect(() =>
      loadConfig(GatewayEnvSchema, { PORT: '99999', OIDC_ISSUER_URL: 'not-a-url' }),
    ).toThrow(/Invalid configuration/);
  });

  describe('production guards', () => {
    const realSecrets = {
      NODE_ENV: 'production',
      SESSION_SECRET: 'a-long-random-production-secret',
      SESSION_ENCRYPTION_KEY: 'ab'.repeat(32),
      OIDC_CLIENT_SECRET: 'real-client-secret',
      OIDC_ISSUER_URL: 'https://auth.example.com/realms/tracker',
    };

    it('accepts production when real secrets are set, and defaults OIDC_ALLOW_INSECURE to false', () => {
      const cfg = loadConfig(GatewayEnvSchema, realSecrets);
      expect(cfg.OIDC_ALLOW_INSECURE).toBe(false);
    });

    it('defaults OIDC_ALLOW_INSECURE to true in development (local Keycloak is http)', () => {
      expect(loadConfig(GatewayEnvSchema, {}).OIDC_ALLOW_INSECURE).toBe(true);
    });

    it.each(['SESSION_SECRET', 'SESSION_ENCRYPTION_KEY', 'OIDC_CLIENT_SECRET'] as const)(
      'rejects the published dev default for %s in production',
      (key) => {
        const env: Record<string, string> = { ...realSecrets };
        delete env[key];
        expect(() => loadConfig(GatewayEnvSchema, env)).toThrow(new RegExp(key));
      },
    );

    it('rejects OIDC_ALLOW_INSECURE=true in production', () => {
      expect(() =>
        loadConfig(GatewayEnvSchema, { ...realSecrets, OIDC_ALLOW_INSECURE: 'true' }),
      ).toThrow(/OIDC_ALLOW_INSECURE/);
    });

    it('rejects a SESSION_ENCRYPTION_KEY that is not 64 hex characters', () => {
      expect(() =>
        loadConfig(GatewayEnvSchema, { SESSION_ENCRYPTION_KEY: 'z'.repeat(64) }),
      ).toThrow(/64 hex/);
    });
  });

  it('loads history-writer and presence-projector configs with expected defaults', () => {
    const hw = loadConfig(HistoryWriterEnvSchema, {});
    expect(hw.KAFKA_DB_WRITER_GROUP).toBe('location-db-writer');
    expect(hw.HISTORY_MIN_DISTANCE_M).toBe(10);

    const pp = loadConfig(PresenceProjectorEnvSchema, {});
    expect(pp.KAFKA_PRESENCE_GROUP).toBe('presence-projector');
    expect(pp.LOCATION_STALE_SWEEP_MS).toBe(5_000);
  });
});
