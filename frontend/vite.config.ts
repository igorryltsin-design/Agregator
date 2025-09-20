import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': { target: 'http://localhost:5050', changeOrigin: true },
      '/download': { target: 'http://localhost:5050', changeOrigin: true },
      '/media': { target: 'http://localhost:5050', changeOrigin: true },
      '/preview': { target: 'http://localhost:5050', changeOrigin: true },
      '/scan': { target: 'http://localhost:5050', changeOrigin: true },
      '/graph': { target: 'http://localhost:5050', changeOrigin: true },
      '/assets': { target: 'http://localhost:5050', changeOrigin: true }
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: false
  }
})
