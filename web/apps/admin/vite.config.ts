import { fileURLToPath, URL } from 'node:url'
import vue from '@vitejs/plugin-vue'
import { defineConfig, loadEnv } from 'vite'
import { blockRemoteLogout, createDashboardDevProxy, dashboardRoot } from '../../dev-proxy.mts'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, dashboardRoot(), '')
  return {
    base: '/admin/',
    envDir: dashboardRoot(),
    plugins: [vue(), blockRemoteLogout(env)],
    resolve: { alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) } },
    build: {
      outDir: '../../../resource/web/admin',
      emptyOutDir: true,
    },
    server: { proxy: createDashboardDevProxy(env, 'admin') },
  }
})
