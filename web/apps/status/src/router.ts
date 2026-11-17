import { defineComponent, h } from 'vue'
import { createRouter, createWebHistory } from 'vue-router'
import { useStatusStore } from './stores/status'
import {
  getPublicThemeDefinition,
  normalizePublicTheme,
  readStoredPublicTheme,
  resolvePublicTheme,
  setActivePublicTheme,
  writeStoredPublicTheme,
  type PublicThemeId,
} from './publicThemes'

export {
  normalizePublicTheme,
  readStoredPublicTheme,
  resolvePublicTheme,
  setActivePublicTheme,
  writeStoredPublicTheme,
  type PublicThemeId,
} from './publicThemes'

export function safeRedirectTarget(raw: unknown): string {
  const value = Array.isArray(raw) ? raw[0] : raw
  const path = typeof value === 'string' ? value.trim() : ''
  if (!path.startsWith('/') || path.startsWith('//') || path.startsWith('/view-password')) {
    return '/'
  }
  return path
}

function themedPage(page: 'Home' | 'Detail' | 'Services' | 'Network') {
  return defineComponent({
    name: `PublicTheme${page}`,
    inheritAttrs: false,
    setup(_, { attrs }) {
      return () => {
        const definition = getPublicThemeDefinition()
        const component = definition[page]
        return component ? h(component, attrs) : h(definition.Home)
      }
    },
  })
}

const HomePage = themedPage('Home')
const DetailPage = themedPage('Detail')
const ServicesPage = themedPage('Services')
const NetworkPage = themedPage('Network')

export const router = createRouter({
  history: createWebHistory('/'),
  scrollBehavior(to, from, savedPosition) {
    if (savedPosition) return savedPosition
    if (from.matched.length > 0 && to.path === from.path) return false
    return { top: 0 }
  },
  routes: [
    { path: '/', name: 'home', component: HomePage },
    {
      path: '/server/:serverId(\\d+)',
      name: 'public-detail',
      component: DetailPage,
      props: true,
      beforeEnter: (to) => (
        getPublicThemeDefinition().Detail
          ? true
          : { name: 'home', query: { server: String(to.params.serverId) } }
      ),
    },
    { path: '/service', name: 'shell-service', component: ServicesPage },
    { path: '/network', name: 'shell-network', component: NetworkPage },
    {
      path: '/view-password',
      name: 'view-password',
      component: () => import('./pages/ViewPasswordPage.vue'),
    },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
})

router.beforeEach(async (to, from) => {
  const store = useStatusStore()
  if (!store.bootstrapped) {
    await store.ensureLoaded()
  }
  if (store.bootstrap) {
    const resolved = resolvePublicTheme({
      siteTheme: store.bootstrap.theme,
      allowSwitch: store.bootstrap.allow_frontend_theme_switch !== false,
      stored: readStoredPublicTheme(),
    })
    setActivePublicTheme(resolved)
  }
  const locked = Boolean(store.bootstrap?.requires_view_password && !store.bootstrap.view_password_verified)
  if (locked) {
    if (to.name === 'view-password') return true
    const preserved = from.name === 'view-password' && typeof from.query.redirect === 'string'
      ? from.query.redirect
      : ''
    const target = to.fullPath && to.fullPath !== '/' && !to.path.startsWith('/view-password')
      ? to.fullPath
      : preserved
    return {
      name: 'view-password',
      query: target ? { redirect: target } : undefined,
      replace: true,
    }
  }
  if (to.name === 'view-password' && store.bootstrap) {
    return { path: safeRedirectTarget(to.query.redirect), replace: true }
  }
  return true
})
