import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const apiBaseUrl = process.env.VITE_API_URL || 'http://localhost:5002';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: apiBaseUrl,
        changeOrigin: true,
      },
      '/hubs': {
        target: apiBaseUrl,
        changeOrigin: true,
        ws: true,
      },
    },
  },
});