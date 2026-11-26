import { defineConfig } from 'vitest/config';

// Integration tests start real containers (Redis) through Testcontainers, so they need Docker.
export default defineConfig({
  test: {
    include: ['src/**/*.integration.test.ts'],
    exclude: ['src/**/stack.integration.test.ts', '**/node_modules/**'],
    testTimeout: 60_000,
    hookTimeout: 180_000,
    fileParallelism: false,
  },
});
