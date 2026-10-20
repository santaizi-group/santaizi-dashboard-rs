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
const osText = computed(() => [props.server.platformLabel || props.server.platform, props.server.platformVersion].filter(Boolean).join(' ') || '—')
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
  if (props.columns.price && price.value) rows.push({ id: 'price', text: price.value, tone: 'billing' })
  if (props.columns.remaining && remaining.value) {
    rows.push({
      id: 'remaining',
      text: remaining.value,
      tone: props.server.publicNote.bill.remainingTone || 'plan',
    })
  }
  return rows
})
</script>

<template>
  <div class="sg-tile" :class="{ 'is-offline': !server.online }">
    <button
      type="button"
      class="sg-tile__main"
      aria-haspopup="dialog"
      :aria-label="t('serverDetail')"
      @click="emit('select')"
    >
      <span class="sg-tile__head">
        <span class="status-dot" :class="server.online ? 'online' : 'offline'"></span>
        <strong class="sg-tile__name">{{ server.name }}</strong>
        <span v-if="showGroup !== false && server.group" class="ss-chip">{{ server.group }}</span>
      </span>
      <span class="sg-tile__sub">
        <OsLogo :platform="server.platform" />
        <span class="sg-tile__os">{{ osText }}</span>
      </span>
      <span v-if="columns.location" class="sg-tile__loc">
        <span
          v-if="server.flagCode"
          class="server-flag"
          :class="`fi fi-${server.flagCode}`"
          aria-hidden="true"
        />
        <span v-else class="server-flag server-flag--empty" aria-hidden="true"><i class="ri-global-line"></i></span>
        <span class="ss-chip ss-chip--muted"><span class="sg-tile__loc-text">{{ server.location || '—' }}</span></span>
      </span>
      <span class="sg-bars">
        <span v-for="bar in bars" :key="bar.id" class="sg-bar">
          <span class="sg-bar__label">{{ bar.label }}</span>
          <span class="sg-bar__track">
            <span
              class="sg-bar__fill"
              :class="`sg-bar__fill--${usageTone(bar.percent)}`"
              :style="{ width: `${Math.max(0, Math.min(100, bar.percent))}%` }"
            ></span>
          </span>
          <span class="sg-bar__value">{{ bar.percent.toFixed(1) }}%</span>
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
