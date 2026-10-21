<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { AppDrawer } from '@santaizi/ui'
import { useInjectedStatusStore } from '@santaizi/status-core'
import { provideStatusPageActions, type StatusPageAction } from './composables/statusPageActions'
import { formatProductVersion, PRODUCT_REPO_URL } from './domain/productVersion'

defineProps<{
  publicTheme: 'server-status' | 'server-grid' | 'nazhua'
  allowThemeSwitch: boolean
  actualColorMode: string
}>()

const emit = defineEmits<{
  selectTheme: [value: 'server-status' | 'server-grid' | 'nazhua']
  selectLocale: [value: string]
  toggleColor: []
}>()

const { t } = useI18n()
const route = useRoute()
const store = useInjectedStatusStore()
const pageActions = provideStatusPageActions()
const mobileOpen = ref(false)
const footerText = computed(() => store.bootstrap?.footer_text?.trim() || '')
const versionLine = computed(() => formatProductVersion(store.bootstrap?.version))
const brandName = computed(() => store.bootstrap?.brand || t('appName'))

watch(() => route.fullPath, () => {
  mobileOpen.value = false
})

function closeMenu() {
  mobileOpen.value = false
}

async function runAction(action: StatusPageAction) {
  mobileOpen.value = false
  await nextTick()
  action.run()
}
</script>

<template>
  <div class="server-status-shell" :class="'shell--' + publicTheme">
    <a href="#status-main" class="skip-link">{{ t('skipContent') }}</a>
    <header class="status-nav">
      <RouterLink to="/" class="status-brand">
        <img :src="store.bootstrap?.logo_url || '/static/logo.svg'" alt="">
        <span>{{ brandName }}</span>
      </RouterLink>
      <nav class="status-nav__links" :aria-label="t('statusNavigation')">
        <RouterLink to="/"><i class="ri-server-line"></i><span>{{ t('statusHome') }}</span></RouterLink>
        <RouterLink to="/service"><i class="ri-heart-pulse-line"></i><span>{{ t('statusServices') }}</span></RouterLink>
        <RouterLink to="/network"><i class="ri-line-chart-line"></i><span>{{ t('statusNetwork') }}</span></RouterLink>
      </nav>
      <div class="status-actions">
        <el-dropdown v-if="allowThemeSwitch" trigger="click" @command="emit('selectTheme', $event)">
          <button type="button" class="status-icon-btn" :aria-label="t('publicTheme')"><i class="ri-layout-masonry-line"></i></button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="server-status" :disabled="publicTheme === 'server-status'">{{ t('themeServerStatus') }}</el-dropdown-item>
              <el-dropdown-item command="server-grid" :disabled="publicTheme === 'server-grid'">{{ t('themeServerGrid') }}</el-dropdown-item>
              <el-dropdown-item command="nazhua" :disabled="publicTheme === 'nazhua'">{{ t('themeNazhua') }}</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-dropdown trigger="click" @command="emit('selectLocale', $event)">
          <button type="button" class="status-icon-btn" :aria-label="t('language')"><i class="ri-translate-2"></i></button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="zh-CN">简体中文</el-dropdown-item>
              <el-dropdown-item command="zh-TW">繁體中文</el-dropdown-item>
              <el-dropdown-item command="en-US">English</el-dropdown-item>
              <el-dropdown-item command="es-ES">Español</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <button type="button" class="status-icon-btn" :aria-label="t(actualColorMode === 'dark' ? 'light' : 'dark')" @click="emit('toggleColor')">
          <i :class="actualColorMode === 'dark' ? 'ri-sun-line' : 'ri-moon-line'"></i>
        </button>
        <a v-if="store.bootstrap?.authenticated" class="status-icon-btn" href="/admin/" :aria-label="t('adminPanel')"><i class="ri-settings-3-line"></i></a>
        <button type="button" class="status-icon-btn status-menu-btn" :aria-label="t('openNavigation')" @click="mobileOpen = true">
          <i class="ri-menu-line"></i>
        </button>
      </div>
    </header>
    <main id="status-main"><slot /></main>
    <AppDrawer
      v-if="mobileOpen"
      :model-value="true"
      :title="brandName"
      mode="view"
      direction="ltr"
      size="min(300px, 88vw)"
      @update:model-value="mobileOpen = $event"
    >
      <nav class="status-mobile-nav" :aria-label="t('statusNavigation')">
        <RouterLink to="/" @click="closeMenu"><i class="ri-server-line"></i><span>{{ t('statusHome') }}</span></RouterLink>
        <RouterLink to="/service" @click="closeMenu"><i class="ri-heart-pulse-line"></i><span>{{ t('statusServices') }}</span></RouterLink>
        <RouterLink to="/network" @click="closeMenu"><i class="ri-line-chart-line"></i><span>{{ t('statusNetwork') }}</span></RouterLink>
        <div v-if="pageActions.length" class="status-mobile-nav__actions">
          <button v-for="action in pageActions" :key="action.id" type="button" @click="runAction(action)">
            <i :class="action.icon"></i><span>{{ action.label }}</span>
          </button>
        </div>
      </nav>
    </AppDrawer>
    <footer>
      <span v-if="footerText">{{ footerText }}</span>
      <span>
        <a :href="PRODUCT_REPO_URL" target="_blank" rel="noopener noreferrer">{{ t('appName') }}</a>
        <template v-if="versionLine"> · {{ versionLine }}</template>
      </span>
    </footer>
  </div>
</template>
