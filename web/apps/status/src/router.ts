import { defineComponent, h } from 'vue'
import { createRouter, createWebHistory } from 'vue-router'
import {
  getActivePublicTheme,
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
  scrollBehavior(_to, _from, savedPosition) {
    if (savedPosition) return savedPosition
    return { top: 0 }
  },
  routes: [
    { path: '/', name: 'home', component: HomePage },
    {
      path: '/server/:serverId(\\d+)',
      name: 'public-detail',
      component: DetailPage,
      props: true,
      beforeEnter: () => getPublicThemeDefinition().Detail ? true : { name: 'home' },
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
