<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{
  label: string
  /// 总量未知时传 null：环不画填充，value 由调用方给「—」。
  percent: number | null
  value?: string
  caption?: string
  captionTitle?: string
  color?: 'blue' | 'green' | 'cyan' | 'orange'
}>(), {
  value: '',
  caption: '',
  captionTitle: '',
  color: 'blue',
})

const safePercent = computed(() => {
  const n = Number(props.percent)
  if (props.percent === null || !Number.isFinite(n)) return 0
  return Math.max(0, Math.min(100, n))
})
const dashOffset = computed(() => 100 - safePercent.value)
const displayValue = computed(() => {
  if (props.value) return props.value
  const pct = safePercent.value
  return `${pct.toFixed(pct < 10 && pct % 1 ? 1 : pct % 1 ? 1 : 0)}%`
})
</script>

<template>
  <div class="nazhua-donut" :class="`nazhua-donut--${color}`">
    <div class="nazhua-donut__ring">
      <svg viewBox="0 0 44 44" aria-hidden="true">
        <circle class="nazhua-donut__track" cx="22" cy="22" r="18" pathLength="100" />
        <circle class="nazhua-donut__value" cx="22" cy="22" r="18" pathLength="100" :stroke-dashoffset="dashOffset" />
      </svg>
      <div class="nazhua-donut__text">
        <strong>{{ displayValue }}</strong>
        <span>{{ label }}</span>
      </div>
    </div>
    <small v-if="caption" class="nazhua-donut__caption" :title="captionTitle || caption">{{ caption }}</small>
  </div>
</template>
