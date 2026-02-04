import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      'leaflet-draw/dist/leaflet.draw.js': '/src/lib/leaflet-draw-wrapper.ts',
    },
  },
  server: {
    host: true,
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        secure: false,
        ws: true,
        // Avoid dev-proxy 504s on heavier polygon searches.
        timeout: 300_000,
        proxyTimeout: 300_000,
      },
      '/api/': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        secure: false,
        ws: true,
        timeout: 300_000,
        proxyTimeout: 300_000,
      },
    },
  },
});
