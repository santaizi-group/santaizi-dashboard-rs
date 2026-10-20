<script setup lang="ts">
import { computed } from 'vue'
import { sparklineGeometry } from '../domain/networkSparkline'

const props = withDefaults(defineProps<{
  points?: number[]
  series?: number[][]
  width?: number
  height?: number
}>(), {
  width: 240,
  height: 36,
})

const paths = computed(() => {
  const rows = props.series?.length ? props.series : props.points?.length ? [props.points] : []
  return rows.slice(0, 3).map((values, index) => ({
    ...sparklineGeometry(values, props.width, props.height),
    fill: index === 0,
    tone: index + 1,
  })).filter((item) => item.line)
})
</script>

<template>
  <svg
    v-if="paths.length"
    class="svc-spark"
    :viewBox="`0 0 ${width} ${height}`"
    preserveAspectRatio="none"
    aria-hidden="true"
  >
    <path
      v-for="item in paths"
      :key="`${item.tone}-line`"
      :d="item.fill ? item.area : item.line"
      :class="item.fill ? `svc-spark__fill svc-spark__s${item.tone}` : `svc-spark__line svc-spark__s${item.tone}`"
    />
    <path
      v-for="item in paths"
      :key="`${item.tone}-stroke`"
      :d="item.line"
      :class="`svc-spark__line svc-spark__s${item.tone}`"
    />
  </svg>
</template>
