<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import {
  OsLogo,
  billingText,
  trafficUsageParts,
  type ServerStatusView,
  type StatusTableColumns,
} from '@santaizi/theme-server-status'

// 四行纯文字布局（旗+名称 / 系统+价格 / 指标·分隔 / 网速+流量），无进度条、环图与 chip。
// 数值染色阈值：>50% 琥珀提示，≥90% 红色告警（后者保原有告警信号）。
const HOT_PERCENT = 50

const props = defineProps<{
  server: ServerStatusView
  columns: StatusTableColumns
}>()

const emit = defineEmits<{ select: [] }>()
const { t, te } = useI18n()

function translate(key: string, values?: Record<string, unknown>) {
  return values ? String(t(key, values)) : String(t(key))
}

const price = computed(() => billingText(props.server.publicNote, translate, te))
const osText = computed(() => [props.server.platformLabel || props.server.platform, props.server.platformVersion].filter(Boolean).join(' '))
const metrics = computed(() => [
  { id: 'cpu', label: t('cores'), percent: props.server.cpu.percent },
  { id: 'mem', label: t('memory'), percent: props.server.memory.percent },
  { id: 'disk', label: t('disk'), percent: props.server.disk.percent },
])
const trafficParts = computed(() => trafficUsageParts(props.server.trafficUsage.kind, props.server.trafficUsage.valueLabel, translate))
const trafficLevel = computed(() => props.server.trafficUsage.statusLevel)
const tileLabel = computed(() => `${props.server.name} · ${t('serverDetail')} · ${props.server.online ? t('online') : t('offline')}`)

function tone(percent: number) {
  if (percent >= 90) return 'danger'
  if (percent > HOT_PERCENT) return 'hot'
  return ''
}
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
      <span class="sg-tile__head">
        <span
          v-if="columns.location && server.flagCode"
          class="server-flag"
          :class="`fi fi-${server.flagCode}`"
          aria-hidden="true"
        />
        <span v-else-if="columns.location" class="server-flag server-flag--empty" aria-hidden="true"><i class="ri-global-line"></i></span>
        <strong class="sg-tile__name">{{ server.name }}</strong>
      </span>
      <span class="sg-tile__sub">
        <span class="sg-tile__os-part">
          <OsLogo :platform="server.platform" />
          <span v-if="osText" class="sg-tile__os">{{ osText }}</span>
        </span>
        <span v-if="columns.price && price" class="sg-tile__price">{{ price }}</span>
      </span>
      <span class="sg-metrics">
        <template v-for="(item, index) in metrics" :key="item.id">
          <i v-if="index" class="sg-metric__sep" aria-hidden="true">·</i>
          <span class="sg-metric">
            <span class="sg-metric__label">{{ item.label }}</span>
            <span class="sg-metric__value" :class="tone(item.percent)">{{ item.percent.toFixed(1) }}%</span>
          </span>
        </template>
      </span>
      <i class="sg-tile__divider" aria-hidden="true"></i>
      <span class="sg-tile__foot">
        <span class="sg-tile__speed">
          <span class="sg-tile__arrow">↑</span><span class="sg-tile__num">{{ server.speedInLabel.replace(/\/s$/, '') }}</span>
          <span class="sg-tile__arrow sg-tile__arrow--gap">↓</span><span class="sg-tile__num">{{ server.speedOutLabel.replace(/\/s$/, '') }}</span>
        </span>
        <span class="sg-tile__traffic" :class="trafficLevel ? `ss-traffic--${trafficLevel}` : ''">
          <span class="sg-tile__traffic-label">{{ trafficParts.label }}</span>
          <span v-if="trafficParts.value" class="sg-tile__num">{{ trafficParts.value }}</span>
        </span>
      </span>
    </button>
  </div>
</template>
