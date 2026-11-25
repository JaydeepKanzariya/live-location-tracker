import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // Proxy auth, REST API, and Socket.IO to the Fastify gateway so the browser
    // sees one origin (required for HttpOnly SameSite=Lax session cookies).
    proxy: {
      '/auth': 'http://localhost:3000',
      '/api': 'http://localhost:3000',
      '/socket.io': {
        target: 'http://localhost:3000',
        ws: true,
      },
    },
  },
});
