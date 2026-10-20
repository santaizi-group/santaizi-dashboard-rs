import { defineConfig } from 'vitepress'

const primaryUpstream = process.env.SANTAIZI_DOCS_UPSTREAM || 'http://127.0.0.1:8000'

export default defineConfig({
  lang: 'zh-CN',
  title: 'Santaizi API',
  description: 'Santaizi HTTP API v2',
  base: '/docs/api/',
  cleanUrls: true,
  appearance: true,
  outDir: '../../../../resource/web/api-docs',
  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'Santaizi API',
    nav: [
      { text: '指南', link: '/' },
      { text: '接口参考', link: '/reference' },
    ],
    sidebar: [
      {
        text: '开始',
        items: [
          { text: '概览', link: '/' },
          { text: '认证与 CSRF', link: '/guides/authentication' },
          { text: 'WebSocket', link: '/guides/websocket' },
        ],
      },
      {
        text: '参考',
        items: [{ text: '接口参考', link: '/reference' }],
      },
    ],
    socialLinks: [],
    outline: { label: '本页目录' },
    darkModeSwitchLabel: '外观',
    lightModeSwitchTitle: '切换到浅色',
    darkModeSwitchTitle: '切换到深色',
    sidebarMenuLabel: '菜单',
    returnToTopLabel: '回到顶部',
  },
  vite: {
    build: { emptyOutDir: true },
    server: {
      proxy: {
        '/openapi': { target: primaryUpstream, changeOrigin: true },
        '/api': { target: primaryUpstream, changeOrigin: true },
        '/oauth2': { target: primaryUpstream, changeOrigin: true },
        '/static': { target: primaryUpstream, changeOrigin: true },
      },
    },
  },
})
