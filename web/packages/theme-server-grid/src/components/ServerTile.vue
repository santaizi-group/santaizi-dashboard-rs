<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import {
  OsLogo,
  billingText,
  formatTransfer,
  percentText,
  trafficUsageParts,
  useTweenedNumber,
  type ServerStatusView,
  type StatusTableColumns,
} from '@santaizi/theme-server-status'

// 四行纯文字布局（旗+名称 / 系统+价格 / 指标 / 网速+流量），无进度条、环图与 chip。
// 数值染色阈值：≥80% 琥珀提示，≥90% 红色强调，≥100% 红色发光闪烁。
const HOT_PERCENT = 80
const ALERT_PERCENT = 90

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
// 行 2：发行版 logo + 规格串（1C2G10G，用户定版替换系统名）；无规格数据回落系统名。
const osText = computed(() => props.server.specText || [props.server.platformLabel || props.server.platform, props.server.platformVersion].filter(Boolean).join(' '))
// 推送每 2s 一帧，瞬跳太硬：数值在帧间滚过去。颜色跟着补间后的显示值走，
// 避免「数字 89.7 却已亮红灯」的错位。
const cpuPercent = useTweenedNumber(() => props.server.cpu.percent)
const memoryPercent = useTweenedNumber(() => props.server.memory.percent)
const diskPercent = useTweenedNumber(() => props.server.disk.percent)
const speedIn = useTweenedNumber(() => props.server.speedIn)
const speedOut = useTweenedNumber(() => props.server.speedOut)
const trafficBytes = useTweenedNumber(() => props.server.trafficUsage.valueBytes)
const metrics = computed(() => [
  { id: 'cpu', label: t('cores'), percent: cpuPercent.value },
  { id: 'mem', label: t('memory'), percent: memoryPercent.value },
  { id: 'disk', label: t('disk'), percent: diskPercent.value },
])
const trafficParts = computed(() => trafficUsageParts(props.server.trafficUsage.kind, props.server.trafficUsage.valueLabel, translate))
const trafficLevel = computed(() => props.server.trafficUsage.statusLevel)
// 网速标签原本是 formatTransfer(x) + '/s' 再去掉后缀，补间后直接按同一格式化器渲染。
const speedInText = computed(() => formatTransfer(speedIn.value ?? 0))
const speedOutText = computed(() => formatTransfer(speedOut.value ?? 0))
// 标签为空（无限流量 / 配额未知）不显示数值；补间值缺失时回落标签文本。
const trafficText = computed(() => {
  if (!props.server.trafficUsage.valueLabel) return ''
  return trafficBytes.value === null ? trafficParts.value.value : formatTransfer(trafficBytes.value)
})
const tileLabel = computed(() => `${props.server.name} · ${t('serverDetail')} · ${props.server.online ? t('online') : t('offline')}`)

function tone(percent: number | null) {
  // 未知（null）不上色：不知道用了几成，谈不上告警。
  if (percent === null) return ''
  if (percent >= 100) return 'critical'
  if (percent >= ALERT_PERCENT) return 'danger'
  if (percent >= HOT_PERCENT) return 'hot'
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
        <span v-for="item in metrics" :key="item.id" class="sg-metric">
          <span class="sg-metric__label">{{ item.label }}</span>
          <span class="sg-metric__value" :class="tone(item.percent)">{{ percentText(item.percent) }}</span>
        </span>
      </span>
      <i class="sg-tile__divider" aria-hidden="true"></i>
      <span class="sg-tile__foot">
        <span class="sg-tile__speed">
          <span class="sg-tile__arrow">↑</span><span class="sg-tile__num">{{ speedInText }}</span>
          <span class="sg-tile__arrow sg-tile__arrow--gap">↓</span><span class="sg-tile__num">{{ speedOutText }}</span>
        </span>
        <span class="sg-tile__traffic" :class="trafficLevel ? `ss-traffic--${trafficLevel}` : ''">
          <span class="sg-tile__traffic-label">{{ trafficParts.label }}</span>
          <span v-if="trafficText" class="sg-tile__num">{{ trafficText }}</span>
        </span>
      </span>
    </button>
  </div>
</template>
