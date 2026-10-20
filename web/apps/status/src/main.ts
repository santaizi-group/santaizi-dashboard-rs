import { createApp } from 'vue'
import ElementPlus from 'element-plus'
import { createSantaiziI18n, type Locale } from '@santaizi/i18n'
import 'element-plus/dist/index.css'
import 'element-plus/theme-chalk/dark/css-vars.css'
import 'remixicon/fonts/remixicon.css'
import 'flag-icons/css/flag-icons.min.css'
import 'font-logos/assets/font-logos.css'
import '@santaizi/design/tokens.css'
import '@santaizi/design/base.css'
import '@santaizi/theme-server-status/theme.css'
import '@santaizi/theme-server-grid/theme.css'
import '@santaizi/theme-nazhua/theme.css'
import App from './App.vue'
import { router } from './router'

const supported: Locale[] = ['zh-CN', 'zh-TW', 'en-US', 'es-ES']
const stored = localStorage.getItem('santaizi-locale') || document.documentElement.lang
const locale = supported.includes(stored as Locale) ? stored as Locale : 'zh-CN'
const app = createApp(App)
app.use(router)
app.use(createSantaiziI18n(locale))
app.use(ElementPlus)
app.mount('#app')
