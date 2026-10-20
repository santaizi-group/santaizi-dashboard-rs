<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { TrafficHistoryPoint } from '@santaizi/api'
import { formatBytes, formatDateTime } from '@/composables/format'

const props = defineProps<{
  points: TrafficHistoryPoint[]
  grain: 'hour' | 'day'
}>()

const { locale } = useI18n()
const maxBytes = computed(() => Math.max(0, ...props.points.map(point => Number(point.bytes) || 0)))

function fillHeight(point: TrafficHistoryPoint) {
  const bytes = Number(point.bytes) || 0
  if (!maxBytes.value || bytes <= 0) return 0
  return Math.max(2, (bytes / maxBytes.value) * 100)
}

function tip(point: TrafficHistoryPoint) {
  return `${formatDateTime(point.window_start, locale.value)} · ${formatBytes(point.bytes, locale.value)}`
}
</script>

<template>
  <div class="traffic-bars">
    <el-tooltip v-for="(point, index) in points" :key="`${grain}-${point.window_start}-${index}`" :content="tip(point)" placement="top" :show-after="200">
      <button type="button" class="traffic-bar-slot" :aria-label="tip(point)">
        <span class="traffic-bar-fill" :style="{ height: `${fillHeight(point)}%` }"></span>
      </button>
    </el-tooltip>
  </div>
</template>

<style scoped>
.traffic-bars {
  display: flex;
  align-items: stretch;
  gap: 3px;
  height: 140px;
}
.traffic-bar-slot {
  flex: 1 1 0;
  min-width: 0;
  display: flex;
  align-items: flex-end;
  margin: 0;
  padding: 0;
  border: 0;
  border-radius: 4px;
  background: color-mix(in srgb, var(--sz-primary) 12%, var(--sz-surface));
  cursor: default;
}
.traffic-bar-fill {
  display: block;
  width: 100%;
  border-radius: 4px;
  background: var(--sz-primary);
}
</style>
