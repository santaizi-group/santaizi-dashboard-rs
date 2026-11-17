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
    <template v-for="item in paths" :key="item.tone">
      <path
        v-if="item.fill && item.area"
        :d="item.area"
        :class="`svc-spark__fill svc-spark__s${item.tone}`"
      />
      <path
        :d="item.line"
        :class="`svc-spark__line svc-spark__s${item.tone}`"
      />
    </template>
  </svg>
</template>
