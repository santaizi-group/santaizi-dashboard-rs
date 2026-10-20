import type { ResourceRecord } from '@santaizi/api'

export type AvailabilityTone = 'good' | 'warn' | 'down'

export interface AvailabilityDay {
  percent: number
  tone: AvailabilityTone
}

export interface ServiceStatusView {
  id: string
  name: string
  live: boolean
  uptimePercent: number
  uptimeLabel: string
  days: AvailabilityDay[]
  delayPoints: number[]
  latencyMs: number | null
  latencyLabel: string
}

export function availabilityPercent(up: unknown, down: unknown) {
  const a = Number(up || 0)
  const b = Number(down || 0)
  if (!Number.isFinite(a) || !Number.isFinite(b) || a + b <= 0) return 0
  return 100 * a / (a + b)
}

export function availabilityTone(percent: number): AvailabilityTone {
  if (percent > 95) return 'good'
  if (percent > 80) return 'warn'
  return 'down'
}

export function readNumberList(value: unknown): number[] {
  if (Array.isArray(value)) {
    return value.map((item) => {
      const n = typeof item === 'number' ? item : Number(item)
      return Number.isFinite(n) ? n : 0
    })
  }
  if (typeof value === 'number' && Number.isFinite(value)) return [value]
  if (typeof value === 'string' && value.trim()) {
    const n = Number(value)
    return Number.isFinite(n) ? [n] : []
  }
  return []
}

export function lastFinite(values: number[]) {
  for (let index = values.length - 1; index >= 0; index -= 1) {
    const value = values[index]
    if (value !== undefined && Number.isFinite(value)) return value
  }
  return null
}

export function averagePositive(values: number[]) {
  let total = 0
  let count = 0
  for (const value of values) {
    if (Number.isFinite(value) && value > 0) {
      total += value
      count += 1
    }
  }
  return count ? total / count : null
}

export function formatLatencyMs(value: number | null) {
  if (value === null || !Number.isFinite(value)) return ''
  return value.toFixed(2)
}

export function toServiceStatusView(row: ResourceRecord): ServiceStatusView {
  const upList = readNumberList(row.up)
  const downList = readNumberList(row.down)
  const delayFromField = readNumberList(row.delay)
  const delayPoints = delayFromField.length ? delayFromField : readNumberList(row.avg_delay)
  const days = upList.map((up, index) => {
    const percent = availabilityPercent(up, downList[index])
    return { percent, tone: availabilityTone(percent) }
  })
  const uptimePercent = availabilityPercent(row.current_up, row.current_down)
  const latencyMs = averagePositive(delayPoints) ?? lastFinite(delayPoints)
  return {
    id: String(row.id || row.name || row.monitor_name || ''),
    name: String(row.name || row.monitor_name || ''),
    live: uptimePercent > 95,
    uptimePercent,
    uptimeLabel: `${uptimePercent.toFixed(2)}%`,
    days,
    delayPoints,
    latencyMs,
    latencyLabel: formatLatencyMs(latencyMs),
  }
}

export function toServiceStatusViews(rows: ResourceRecord[]) {
  return rows.map(toServiceStatusView)
}
