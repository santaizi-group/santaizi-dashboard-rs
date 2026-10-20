<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import {
  OsLogo,
  billingText,
  remainingText,
  trafficUsageText,
  usageTone,
  type ServerStatusView,
  type StatusTableColumns,
} from '@santaizi/theme-server-status'

const props = defineProps<{
  server: ServerStatusView
  columns: StatusTableColumns
  showGroup?: boolean
}>()

const emit = defineEmits<{ select: [] }>()
const { t, te } = useI18n()

function translate(key: string, values?: Record<string, unknown>) {
  return values ? String(t(key, values)) : String(t(key))
}

const price = computed(() => billingText(props.server.publicNote, translate, te))
const remaining = computed(() => remainingText(props.server.publicNote, translate))
const traffic = computed(() => trafficUsageText(props.server.trafficUsage.kind, props.server.trafficUsage.valueLabel, translate))
const speed = computed(() => `${props.server.speedInLabel.replace(/\/s$/, '')} | ${props.server.speedOutLabel.replace(/\/s$/, '')}`)
const osText = computed(() => [props.server.platformLabel || props.server.platform, props.server.platformVersion].filter(Boolean).join(' '))
const availabilityLabel = computed(() => {
  if (props.server.available === true) return t('available')
  if (props.server.available === false) return t('unavailable')
  return t('unknown')
})
const bars = computed(() => [
  { id: 'cpu', label: t('cores'), percent: props.server.cpu.percent },
  { id: 'mem', label: t('memory'), percent: props.server.memory.percent },
  { id: 'disk', label: t('disk'), percent: props.server.disk.percent },
])
const metaTags = computed(() => {
  const rows: Array<{ id: string; text: string; tone: string }> = []
  if (props.showGroup !== false && props.server.group) rows.push({ id: 'group', text: props.server.group, tone: 'plan' })
  if (props.columns.price && price.value) rows.push({ id: 'price', text: price.value, tone: 'plan' })
  if (props.columns.remaining && remaining.value) {
    const tone = props.server.publicNote.bill.remainingTone
    rows.push({ id: 'remaining', text: remaining.value, tone: tone === 'warning' || tone === 'danger' ? tone : 'plan' })
  }
  return rows
})
const tileLabel = computed(() => `${props.server.name} · ${t('serverDetail')} · ${props.server.online ? t('online') : t('offline')}`)
</script>

<template>
  <div class="sg-tile" :class="{ 'is-offline': !server.online }">
    <button
      type="button"
      class="sg-tile__main"
      aria-haspopup="dialog"
      :aria-label="tileLabel"
      @click="emit('select')"
    >
      <strong class="sg-tile__name">{{ server.name }}</strong>
      <span class="sg-tile__sub">
        <span
          v-if="columns.location && server.flagCode"
          class="server-flag"
          :class="`fi fi-${server.flagCode}`"
          aria-hidden="true"
        />
        <span v-else-if="columns.location" class="server-flag server-flag--empty" aria-hidden="true"><i class="ri-global-line"></i></span>
        <span class="sg-tile__os-part">
          <OsLogo :platform="server.platform" />
          <span v-if="osText" class="sg-tile__os">{{ osText }}</span>
        </span>
      </span>
      <span class="sg-metrics">
        <span v-for="bar in bars" :key="bar.id" class="sg-metric">
          <span class="sg-metric__label">{{ bar.label }}</span>
          <span class="sg-metric__value" :class="`sg-metric__value--${usageTone(bar.percent)}`">{{ bar.percent.toFixed(1) }}%</span>
        </span>
      </span>
      <span class="sg-tile__foot">
        <span class="sg-tile__speed">{{ speed }}</span>
        <span
          class="sg-tile__traffic"
          :class="server.trafficUsage.statusLevel ? `ss-traffic--${server.trafficUsage.statusLevel}` : ''"
        >{{ traffic }}</span>
      </span>
      <span v-if="metaTags.length || columns.availability" class="sg-tile__meta">
        <span v-for="tag in metaTags" :key="tag.id" class="meta-tag" :class="`meta-tag--${tag.tone}`">{{ tag.text }}</span>
        <span v-if="columns.availability" class="ss-chip ss-chip--muted">
          {{ t('availability') }} · {{ availabilityLabel }}
        </span>
      </span>
    </button>
  </div>
</template>
