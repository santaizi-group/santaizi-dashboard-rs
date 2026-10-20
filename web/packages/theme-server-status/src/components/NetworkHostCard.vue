<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import type { MonitorHistory } from '@santaizi/api'
import { useInView } from '../composables/useInView'
import { loadNetworkHistory, peekNetworkHistory } from '../domain/networkHistoryCache'
import { seriesFromMonitorHistory, type NetworkHostTile } from '../domain/networkSparkline'
import DelaySparkline from './DelaySparkline.vue'
import OsLogo from './OsLogo.vue'

const props = defineProps<{
  server: NetworkHostTile
  selected?: boolean
}>()

const emit = defineEmits<{ select: [] }>()
const { t } = useI18n()
const root = ref<HTMLElement>()
const { load, keep } = useInView(root)
const rows = ref<MonitorHistory[] | null>(peekNetworkHistory(props.server.id) ?? null)
const failed = ref(false)
let requestSeq = 0

const series = computed(() => seriesFromMonitorHistory(rows.value || []))
const sparkPoints = computed(() => series.value.map((item) => item.points))
const showSpark = computed(() => keep.value && sparkPoints.value.length > 0)
const placeholder = computed(() => {
  if (failed.value) return t('loadFailed')
  if (!rows.value) return t('loading')
  return t('noData')
})

watch(load, async (shouldLoad) => {
  if (!shouldLoad || rows.value) return
  const seq = ++requestSeq
  try {
    const data = await loadNetworkHistory(props.server.id)
    if (seq !== requestSeq) return
    rows.value = data
    failed.value = false
  } catch {
    if (seq !== requestSeq) return
    failed.value = true
  }
}, { immediate: true })
</script>

<template>
  <button
    ref="root"
    type="button"
    class="network-tile"
    :class="{ 'is-selected': selected, 'is-offline': !server.online }"
    :aria-pressed="selected"
    :aria-label="server.name"
    @click="emit('select')"
  >
    <span class="network-tile__head">
      <i class="live-dot" :class="server.online ? 'online' : 'offline'"></i>
      <span
        v-if="server.flagCode"
        class="server-flag"
        :class="`fi fi-${server.flagCode}`"
        aria-hidden="true"
      />
      <span v-else class="server-flag server-flag--empty" aria-hidden="true"><i class="ri-global-line"></i></span>
      <OsLogo :platform="server.platform" />
      <strong>{{ server.name }}</strong>
    </span>
    <DelaySparkline v-if="showSpark" :series="sparkPoints" :height="40" />
    <span v-else class="network-tile__placeholder">{{ placeholder }}</span>
  </button>
</template>
