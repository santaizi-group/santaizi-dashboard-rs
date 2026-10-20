import { onBeforeUnmount, onMounted } from 'vue'

export type ChartPalette = {
  text: string
  muted: string
  border: string
  surface: string
}

function isDarkTheme() {
  const root = document.documentElement
  return root.classList.contains('dark') || root.getAttribute('data-theme') === 'dark'
}

export function chartPalette(): ChartPalette {
  const shell = document.querySelector('.nazhua-shell')
  const style = getComputedStyle(shell instanceof HTMLElement ? shell : document.documentElement)
  const dark = isDarkTheme()
  const read = (token: string, fallback: string) => style.getPropertyValue(token).trim() || fallback
  return {
    text: read('--nazhua-text', dark ? '#f1f6f7' : '#111827'),
    muted: read('--nazhua-muted', dark ? '#aab7c4' : '#526072'),
    border: read('--nazhua-border', dark ? 'rgba(154, 173, 194, .2)' : 'rgba(15, 23, 42, .1)'),
    surface: read('--nazhua-surface', dark ? '#05090d' : '#ffffff'),
  }
}

// ECharts 画在 canvas 上，拿不到 CSS 继承，坐标轴/图例/提示框必须显式喂主题令牌。
export function chartAxisStyle(palette: ChartPalette) {
  return {
    axisLabel: { color: palette.muted, fontSize: 12 },
    axisLine: { lineStyle: { color: palette.border } },
    splitLine: { lineStyle: { color: palette.border, type: 'dashed' as const } },
  }
}

export function chartLegendStyle(palette: ChartPalette) {
  return { textStyle: { color: palette.muted, fontSize: 12 } }
}

export function chartTooltipStyle(palette: ChartPalette) {
  return {
    backgroundColor: palette.surface,
    borderColor: palette.border,
    textStyle: { color: palette.text, fontSize: 12 },
  }
}

export function useChartThemeWatcher(redraw: () => void) {
  let observer: MutationObserver | undefined
  onMounted(() => {
    if (typeof MutationObserver === 'undefined') return
    observer = new MutationObserver(() => requestAnimationFrame(redraw))
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class', 'data-theme'] })
  })
  onBeforeUnmount(() => observer?.disconnect())
}
