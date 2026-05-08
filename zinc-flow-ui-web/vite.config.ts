import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

// Dev server proxies /api/* and /metrics to the running worker so
// the UI calls same-origin paths in both dev and prod.
//
// Build output lives at ../zinc-flow-csharp/ZincFlow/wwwroot
// so a fresh `npm run build` drops assets where the C# worker (and
// later the Java worker) serve them from.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    outDir: '../zinc-flow-csharp/ZincFlow/wwwroot',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:9092',
      '/metrics': 'http://localhost:9092',
    },
  },
})
