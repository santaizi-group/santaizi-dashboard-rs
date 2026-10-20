<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { formatSpeed, formatTransfer, bytesAxisFormatter } from '@santaizi/theme-server-status'
import {
  chartAxisStyle,
  chartPalette,
  chartTooltipStyle,
  useChartThemeWatcher,
} from '../../composables/useChartTheme'

echarts.use([LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

export type HistoryUnit = 'percent' | 'bytes' | 'speed' | 'count'

export type HistorySeries = {
  name: string
  color: string
  data: Array<[string, number]>
}

const props = defineProps<{
  series: HistorySeries[]
  unit: HistoryUnit
  max?: number
}>()

const node = ref<HTMLElement>()
let chart: echarts.ECharts | undefined
let resizeObserver: ResizeObserver | undefined

function formatValue(value: number) {
  if (props.unit === 'percent') return `${Number(value).toFixed(1)}%`
  if (props.unit === 'speed') return formatSpeed(value)
  if (props.unit === 'count') return `${Math.round(Number(value || 0))}`
  return formatTransfer(value)
}

function axisBytesCap() {
  if (props.max && props.max > 0) return props.max
  return Math.max(0, ...props.series.flatMap((item) => item.data.map((point) => Number(point[1]) || 0)))
}

function formatAxis(value: number) {
  if (props.unit === 'bytes') return bytesAxisFormatter(axisBytesCap())(value)
  return formatValue(value)
}

function yName() {
  if (props.unit === 'percent') return '%'
  if (props.unit === 'speed') return 'B/s'
  return ''
}

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
      valueFormatter: (value: unknown) => formatValue(Number(value || 0)),
      ...chartTooltipStyle(palette),
    },
    legend: { show: false },
    grid: { left: 44, right: 12, top: 16, bottom: 28 },
    xAxis: { type: 'time', ...axis },
    yAxis: {
      type: 'value',
      min: 0,
      name: yName(),
      nameTextStyle: { color: palette.muted },
      ...axis,
      axisLabel: { ...axis.axisLabel, formatter: (value: number) => formatAxis(value) },
      ...(props.unit === 'percent' ? { max: 100 } : {}),
      ...(props.unit === 'bytes' && props.max && props.max > 0 ? { max: props.max } : {}),
    },
    series: props.series.map(item => ({
      name: item.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1.6, color: item.color },
      itemStyle: { color: item.color },
      areaStyle: { color: item.color, opacity: 0.08 },
      data: item.data,
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

watch(() => props.unit, draw)
watch(() => props.max, draw)

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
  <div ref="node" class="nazhua-history__chart" />
</template>
