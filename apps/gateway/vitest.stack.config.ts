import { defineConfig } from 'vitest/config';

// Needs the compose stack running (npm run v1:infra:up): real Kafka and real Keycloak.
export default defineConfig({
  test: {
    include: ['src/**/stack.integration.test.ts'],
    testTimeout: 90_000,
    hookTimeout: 180_000,
    fileParallelism: false,
  },
});
