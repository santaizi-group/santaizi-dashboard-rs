<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { useInjectedStatusStore } from '@santaizi/status-core'
import { isHostOnline } from '@santaizi/api'
import bgImage from './assets/bg.webp?url'
import NazhuaHeader from './components/layout/NazhuaHeader.vue'
import { useNavbarStats } from './composables/useServerListFilters'
import { formatProductVersion, PRODUCT_REPO_URL } from '@santaizi/theme-server-status'

const props = defineProps<{
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
const router = useRouter()
const store = useInjectedStatusStore()
const footerText = computed(() => store.bootstrap?.footer_text?.trim() || '')
const versionLine = computed(() => formatProductVersion(store.bootstrap?.version))
const navbarStats = useNavbarStats(() => store.servers)
const counts = computed(() => ({
  total: store.servers.length,
  online: store.servers.filter(isHostOnline).length,
  offline: store.servers.filter(server => !isHostOnline(server)).length,
}))

function menuCommand(command: string) {
  if (command.startsWith('route:')) {
    router.push(command.slice(6))
  } else if (command.startsWith('theme:')) {
    emit('selectTheme', command.slice(6) as 'server-status' | 'server-grid' | 'nazhua')
  } else if (command.startsWith('locale:')) {
    emit('selectLocale', command.slice(7))
  } else if (command === 'admin') {
    window.location.assign('/admin/')
  }
}
</script>

<template>
  <div class="nazhua-shell" :style="{ '--nazhua-bg-image': `url(${bgImage})` }">
    <a href="#status-main" class="skip-link">{{ t('skipContent') }}</a>
    <NazhuaHeader
      :brand="store.bootstrap?.brand"
      :total="counts.total"
      :online="counts.online"
      :offline="counts.offline"
      :transfer-in="navbarStats.transferIn"
      :transfer-out="navbarStats.transferOut"
      :speed-in="navbarStats.speedIn"
      :speed-out="navbarStats.speedOut"
    >
      <template #actions>
        <button
          type="button"
          class="nazhua-theme-button"
          :aria-label="t(actualColorMode === 'dark' ? 'light' : 'dark')"
          @click="emit('toggleColor')"
        >
          <i :class="actualColorMode === 'dark' ? 'ri-sun-line' : 'ri-moon-line'"></i>
        </button>
        <el-dropdown trigger="click" popper-class="nazhua-function-menu" @command="menuCommand">
          <button type="button" class="nazhua-menu-button" :aria-label="t('actions')">
            <i class="ri-menu-4-line"></i>
          </button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="route:/"><i class="ri-server-line"></i>{{ t('statusHome') }}</el-dropdown-item>
              <el-dropdown-item command="route:/service"><i class="ri-heart-pulse-line"></i>{{ t('statusServices') }}</el-dropdown-item>
              <el-dropdown-item command="route:/network"><i class="ri-line-chart-line"></i>{{ t('statusNetwork') }}</el-dropdown-item>
              <template v-if="allowThemeSwitch">
                <el-dropdown-item divided command="theme:server-status" :disabled="publicTheme === 'server-status'"><i class="ri-table-view"></i>{{ t('themeServerStatus') }}</el-dropdown-item>
                <el-dropdown-item command="theme:server-grid" :disabled="publicTheme === 'server-grid'"><i class="ri-layout-grid-line"></i>{{ t('themeServerGrid') }}</el-dropdown-item>
                <el-dropdown-item command="theme:nazhua" :disabled="publicTheme === 'nazhua'"><i class="ri-gallery-view-2"></i>{{ t('themeNazhua') }}</el-dropdown-item>
              </template>
              <el-dropdown-item divided command="locale:zh-CN"><i class="ri-translate-2"></i>简体中文</el-dropdown-item>
              <el-dropdown-item command="locale:zh-TW"><i class="ri-translate-2"></i>繁體中文</el-dropdown-item>
              <el-dropdown-item command="locale:en-US"><i class="ri-translate-2"></i>English</el-dropdown-item>
              <el-dropdown-item command="locale:es-ES"><i class="ri-translate-2"></i>Español</el-dropdown-item>
              <el-dropdown-item v-if="store.bootstrap?.authenticated" divided command="admin"><i class="ri-settings-3-line"></i>{{ t('adminPanel') }}</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </template>
    </NazhuaHeader>
    <main id="status-main"><slot /></main>
    <footer class="nazhua-footer">
      <span v-if="footerText">{{ footerText }}</span>
      <span>
        <a :href="PRODUCT_REPO_URL" target="_blank" rel="noopener noreferrer">{{ t('appName') }}</a>
        <template v-if="versionLine"> · {{ versionLine }}</template>
      </span>
      <span>Theme by <a href="https://github.com/hi2shark/nazhua" target="_blank" rel="noopener noreferrer">Nazhua</a></span>
    </footer>
  </div>
</template>
