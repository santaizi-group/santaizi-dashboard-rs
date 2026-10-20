import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import 'element-plus/theme-chalk/dark/css-vars.css'
import 'remixicon/fonts/remixicon.css'
import '@santaizi/design/tokens.css'
import '@santaizi/design/base.css'
import './styles/admin.css'
import App from './App.vue'
import { router } from './router'
import { createSantaiziI18n, type Locale } from '@santaizi/i18n'
import { installRouteDirtyGuard } from '@/composables/routeDirtyGuard'

const supported: Locale[] = ['zh-CN', 'zh-TW', 'en-US', 'es-ES']
const stored = localStorage.getItem('santaizi-locale') || document.documentElement.lang
const locale = supported.includes(stored as Locale) ? stored as Locale : 'zh-CN'
const i18n = createSantaiziI18n(locale)
const app = createApp(App)
app.use(createPinia())
app.use(router)
app.use(i18n)
app.use(ElementPlus)
installRouteDirtyGuard(router, i18n.global as never)
app.mount('#app')
