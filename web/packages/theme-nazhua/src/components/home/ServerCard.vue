<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { NazhuaServerView } from '../../domain/nazhuaServerView'
import { formatNazhuaBilling, resolveNazhuaTrafficLabel } from '../../domain/nazhuaServerView'
import { formatLiveSpeed, splitTransfer } from '../../utils/host'
import DonutChart from '../charts/DonutChart.vue'
import OsLogo from '../common/OsLogo.vue'

const props = defineProps<{ server: NazhuaServerView }>()
const { t, te, locale } = useI18n()

const trafficLabel = computed(() => resolveNazhuaTrafficLabel(props.server, t))

const billingText = computed(() => formatNazhuaBilling(props.server.publicNote, t, te))
const cpuValue = computed(() => (
  props.server.cpuPercent === null ? '—' : `${Number(props.server.cpuPercent.toFixed(1))}%`
))
const dayUnit = computed(() => (locale.value.toLowerCase().startsWith('zh') ? t('day') : 'd'))

const uptime = computed(() => {
  if (!props.server.online) return { value: '—', unit: '' }
  const seconds = Math.max(0, Math.floor(props.server.uptimeSeconds))
  if (seconds >= 86_400) return { value: String(Math.floor(seconds / 86_400)), unit: dayUnit.value }
  if (seconds >= 3_600) return { value: String(Math.floor(seconds / 3_600)), unit: 'h' }
  return { value: String(Math.floor(seconds / 60)), unit: 'm' }
})

const traffic = computed(() => splitTransfer(props.server.trafficBytes))
const inSpeed = computed(() => (props.server.online ? formatLiveSpeed(props.server.speedIn) : { value: '—', unit: '' }))
const outSpeed = computed(() => (props.server.online ? formatLiveSpeed(props.server.speedOut) : { value: '—', unit: '' }))
const showFoot = computed(() => props.server.publicNote.planTags.length > 0 || Boolean(props.server.orderLink || billingText.value))

function planTag(tag: string) {
  if (tag === '__dual_stack__') return t('dualStack')
  if (tag === '__ipv4_only__') return t('ipv4Only')
  if (tag === '__ipv6_only__') return t('ipv6Only')
  return tag
}
</script>

<template>
  <article class="nazhua-card" :class="{ offline: !server.online }">
    <RouterLink :to="{ name: 'public-detail', params: { serverId: String(server.id) } }" class="nazhua-card__head" :aria-label="server.name">
      <span v-if="server.flagClass" :class="server.flagClass" class="nazhua-flag" aria-hidden="true" />
      <span v-else class="nazhua-flag-fallback" aria-hidden="true"><i class="ri-global-line"></i></span>
      <strong>{{ server.name }}</strong>
      <span v-if="!server.online" class="nazhua-offline-label"><i class="ri-wifi-off-line"></i>{{ t('nazhua.offline') }}</span>
      <span v-else class="nazhua-card__spec">
        <OsLogo :platform="server.platform" />
        <span>{{ server.spec || '—' }}</span>
      </span>
    </RouterLink>
    <RouterLink :to="{ name: 'public-detail', params: { serverId: String(server.id) } }" class="nazhua-card__main" :aria-label="server.name">
      <div class="nazhua-card__metrics">
        <DonutChart label="CPU" :percent="server.cpuPercent" :value="cpuValue" :caption="server.cpuCaption" color="blue" />
        <DonutChart :label="t('nazhua.memory')" :percent="server.memoryPercent" :value="server.memoryValue" :caption="server.memoryTotalLabel" :caption-title="server.memoryCaption" color="green" />
        <DonutChart :label="t('nazhua.disk')" :percent="server.diskPercent" :value="server.diskValue" :caption="server.diskTotalLabel" :caption-title="server.diskCaption" color="cyan" />
      </div>
      <div class="nazhua-card__stats">
        <span><strong>{{ uptime.value }}<em>{{ uptime.unit }}</em></strong><small>{{ t('nazhua.uptime') }}</small></span>
        <span class="traffic"><strong>{{ traffic.value }}<em>{{ traffic.unit }}</em></strong><small>{{ trafficLabel }}</small></span>
        <span class="in"><strong>{{ inSpeed.value }}<em>{{ inSpeed.unit }}</em></strong><small>{{ t('nazhua.download') }}</small></span>
        <span class="out"><strong>{{ outSpeed.value }}<em>{{ outSpeed.unit }}</em></strong><small>{{ t('nazhua.upload') }}</small></span>
      </div>
    </RouterLink>
    <footer v-if="showFoot" class="nazhua-card__foot">
      <div class="nazhua-card__tags">
        <span v-for="tag in server.publicNote.planTags" :key="tag">{{ planTag(tag) }}</span>
      </div>
      <a v-if="server.orderLink" :href="server.orderLink" target="_blank" rel="noopener noreferrer" class="nazhua-card__buy">
        <i class="ri-shopping-bag-3-line"></i>{{ server.publicNote.presentation.buyBtnText || t('purchase') }}
      </a>
      <strong v-else-if="billingText" class="nazhua-card__billing">{{ billingText }}</strong>
    </footer>
  </article>
</template>
