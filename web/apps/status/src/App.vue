<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, provide, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import zhTw from 'element-plus/es/locale/lang/zh-tw'
import en from 'element-plus/es/locale/lang/en'
import es from 'element-plus/es/locale/lang/es'
import { STATUS_STORE_KEY } from '@santaizi/status-core'
import { AppSkeleton } from '@santaizi/ui'
import type { Locale, ThemeMode } from '@santaizi/api'
import { useStatusStore } from './stores/status'
import {
  getPublicThemeDefinition,
  activePublicTheme,
  normalizePublicTheme,
  readStoredPublicTheme,
  resolvePublicTheme,
  setActivePublicTheme,
  writeStoredPublicTheme,
  type PublicThemeId,
} from './publicThemes'
import { applyFaviconHref, resolveFaviconHref } from './domain/favicon'

const SUPPORTED_LOCALES: Locale[] = ['zh-CN', 'zh-TW', 'en-US', 'es-ES']

function normalizeColorMode(raw: string | null): ThemeMode {
  if (raw === 'light' || raw === 'dark' || raw === 'system') return raw
  return 'system'
}

const { locale } = useI18n()
const elementLocale = computed(() => ({ 'zh-CN': zhCn, 'zh-TW': zhTw, 'en-US': en, 'es-ES': es }[locale.value] || zhCn))
const route = useRoute()
const router = useRouter()
const store = useStatusStore()
provide(STATUS_STORE_KEY, store)

const publicTheme = activePublicTheme
const activeDefinition = computed(() => getPublicThemeDefinition(publicTheme.value))
const colorMode = ref<ThemeMode>(normalizeColorMode(localStorage.getItem('santaizi-status-theme')))
const mediaQuery = typeof window !== 'undefined' && typeof window.matchMedia === 'function'
  ? window.matchMedia('(prefers-color-scheme: dark)')
  : null
const systemDark = ref(Boolean(mediaQuery?.matches))
const actualColorMode = computed(() => (
  colorMode.value === 'system'
    ? (systemDark.value ? 'dark' : 'light')
    : colorMode.value
))
const allowThemeSwitch = computed(() => store.bootstrap?.allow_frontend_theme_switch !== false)

function onSystemColorChange(event: MediaQueryListEvent) {
  systemDark.value = event.matches
}

function toggleColorMode() {
  const next: ThemeMode = colorMode.value === 'light'
    ? 'dark'
    : colorMode.value === 'dark'
      ? 'system'
      : 'light'
  colorMode.value = next
  localStorage.setItem('santaizi-status-theme', next)
}

function setLocale(value: string) {
  if (!SUPPORTED_LOCALES.includes(value as Locale)) return
  locale.value = value
  document.documentElement.lang = value
  localStorage.setItem('santaizi-locale', value)
}

async function setPublicTheme(theme: PublicThemeId) {
  const next = normalizePublicTheme(theme)
  if (!allowThemeSwitch.value && next !== normalizePublicTheme(store.bootstrap?.theme)) return
  writeStoredPublicTheme(next)
  setActivePublicTheme(next)
  if (!getPublicThemeDefinition(next).Detail && route.name === 'public-detail') {
    await router.replace({ name: 'home', query: { server: String(route.params.serverId || '') } })
  }
}

watch(locale, (value) => {
  document.documentElement.lang = value
}, { immediate: true })

watch(actualColorMode, (value) => {
  document.documentElement.dataset.theme = value
  document.documentElement.classList.toggle('dark', value === 'dark')
  document.documentElement.style.colorScheme = value === 'dark' ? 'dark' : 'light'
}, { immediate: true })

watch(() => store.bootstrap, (value) => {
  if (!value) return
  if (value.primary_color) document.documentElement.style.setProperty('--ss-accent', value.primary_color)
  if (value.brand) document.title = value.brand
  applyFaviconHref(resolveFaviconHref(value.logo_url))
  if (!localStorage.getItem('santaizi-locale') && SUPPORTED_LOCALES.includes(value.locale)) {
    locale.value = value.locale
    document.documentElement.lang = value.locale
  }
  let style = document.querySelector<HTMLStyleElement>('#santaizi-site-style')
  if (!style) {
    style = document.createElement('style')
    style.id = 'santaizi-site-style'
    document.head.append(style)
  }
  style.textContent = value.custom_css || ''
  const resolved = resolvePublicTheme({
    siteTheme: value.theme,
    allowSwitch: value.allow_frontend_theme_switch !== false,
    stored: readStoredPublicTheme(),
  })
  setActivePublicTheme(resolved)
}, { immediate: true, deep: true })

onMounted(async () => {
  mediaQuery?.addEventListener('change', onSystemColorChange)
  await store.ensureLoaded()
  if (store.bootstrap?.requires_view_password && !store.bootstrap.view_password_verified) {
    if (route.name !== 'view-password') {
      await router.replace({
        name: 'view-password',
        query: route.fullPath !== '/' ? { redirect: route.fullPath } : undefined,
      })
    }
    return
  }
  store.connect()
})

onBeforeUnmount(() => {
  mediaQuery?.removeEventListener('change', onSystemColorChange)
  store.stop()
})
</script>

<template>
  <el-config-provider :locale="elementLocale">
    <component
      v-if="store.bootstrapped"
      :is="activeDefinition.Shell"
      :key="publicTheme"
      :public-theme="publicTheme"
      :allow-theme-switch="allowThemeSwitch"
      :actual-color-mode="actualColorMode"
      :color-mode="colorMode"
      @select-theme="setPublicTheme"
      @select-locale="setLocale"
      @toggle-color="toggleColorMode"
    >
      <RouterView :key="`${publicTheme}:${route.path}`" />
    </component>
    <AppSkeleton v-else variant="boot" />
  </el-config-provider>
</template>
