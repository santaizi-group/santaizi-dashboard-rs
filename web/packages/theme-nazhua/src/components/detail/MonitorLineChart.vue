<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { DataZoomComponent, GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import type { NetworkSeries } from '../../composables/useNetworkMonitorChart'
import {
  chartAxisStyle,
  chartLegendStyle,
  chartPalette,
  chartTooltipStyle,
  useChartThemeWatcher,
} from '../../composables/useChartTheme'

echarts.use([LineChart, DataZoomComponent, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

const props = defineProps<{
  series: NetworkSeries[]
  compact?: boolean
}>()

const node = ref<HTMLElement>()
let chart: echarts.ECharts | undefined
let resizeObserver: ResizeObserver | undefined

function draw() {
  if (!node.value || !props.series.length) return
  if (!node.value.clientWidth || !node.value.clientHeight) return
  const palette = chartPalette()
  const axis = chartAxisStyle(palette)
  if (!chart) chart = echarts.init(node.value)
  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      valueFormatter: (value: unknown) => `${Number(value || 0).toFixed(2)} ms`,
      ...chartTooltipStyle(palette),
    },
    legend: props.compact ? { show: false } : { top: 4, ...chartLegendStyle(palette) },
    grid: props.compact
      ? { left: 44, right: 12, top: 16, bottom: 40 }
      : { left: 48, right: 16, top: 40, bottom: 48 },
    dataZoom: [
      { type: 'inside' },
      { type: 'slider', height: 18, bottom: 8, borderColor: palette.border, textStyle: { color: palette.muted } },
    ],
    xAxis: { type: 'time', ...axis },
    yAxis: {
      type: 'value',
      name: 'ms',
      nameTextStyle: { color: palette.muted },
      ...axis,
    },
    series: props.series.map(item => ({
      name: item.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      data: item.points,
    })),
  }, true)
}

function onResize() {
  chart?.resize()
}

watch(() => props.series, async () => {
  await nextTick()
  if (!props.series.length) {
    chart?.clear()
    return
  }
  draw()
}, { deep: true })

useChartThemeWatcher(draw)

onMounted(async () => {
  window.addEventListener('resize', onResize)
  await nextTick()
  draw()
  if (node.value && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => {
      if (!chart) draw()
      else chart.resize()
    })
    resizeObserver.observe(node.value)
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', onResize)
  resizeObserver?.disconnect()
  chart?.dispose()
  chart = undefined
})
</script>

<template>
  <div ref="node" class="nazhua-monitor__chart" :class="{ 'is-compact': compact }" />
</template>
