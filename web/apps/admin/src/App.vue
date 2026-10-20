<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { RouterView, useRoute } from 'vue-router'
import AdminShell from '@/components/AdminShell.vue'
import { useSessionStore } from '@/stores/session'
import { useTheme } from '@/composables/theme'
import { useI18n } from 'vue-i18n'
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import zhTw from 'element-plus/es/locale/lang/zh-tw'
import en from 'element-plus/es/locale/lang/en'
import es from 'element-plus/es/locale/lang/es'

const route = useRoute()
const session = useSessionStore()
const { t, locale } = useI18n()
const elementLocale = computed(() => ({ 'zh-CN': zhCn, 'zh-TW': zhTw, 'en-US': en, 'es-ES': es }[locale.value] || zhCn))
useTheme()
const bare = computed(() => Boolean(route.meta.bare))
onMounted(() => session.load())
</script>

<template><el-config-provider :locale="elementLocale">
  <a href="#main-content" class="skip-link">{{ t('skipContent') }}</a>
  <RouterView v-if="bare" />
  <AdminShell v-else><RouterView /></AdminShell>
</el-config-provider></template>
