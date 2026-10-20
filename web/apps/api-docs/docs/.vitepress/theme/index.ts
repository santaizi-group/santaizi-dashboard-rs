import DefaultTheme from 'vitepress/theme'
import { h } from 'vue'
import type { Theme } from 'vitepress'
import 'remixicon/fonts/remixicon.css'
import '@santaizi/design/tokens.css'
import './style.css'
import RedocReference from './RedocReference.vue'

const theme: Theme = {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      'nav-bar-content-after': () =>
        h(
          'a',
          {
            class: 'sz-docs-admin-link',
            href: '/admin/',
          },
          [h('i', { class: 'ri-settings-3-line', 'aria-hidden': 'true' }), '管理后台'],
        ),
    }),
  enhanceApp({ app }) {
    app.component('RedocReference', RedocReference)
  },
}

export default theme
