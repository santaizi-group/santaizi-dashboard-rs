import type { ResourceRecord } from '@santaizi/api'

export type AvailabilityTone = 'good' | 'warn' | 'down' | 'empty'

export interface AvailabilityDay {
  percent: number | null
  tone: AvailabilityTone
}

export interface ServiceStatusView {
  id: string
  name: string
  live: boolean
  tone: 'online' | 'offline' | 'empty'
  uptimePercent: number | null
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

export function availabilityPercentOrNull(up: unknown, down: unknown): number | null {
  const a = Number(up || 0)
  const b = Number(down || 0)
  if (!Number.isFinite(a) || !Number.isFinite(b) || a + b <= 0) return null
  return 100 * a / (a + b)
}

export function availabilityTone(percent: number | null): AvailabilityTone {
  if (percent === null || !Number.isFinite(percent)) return 'empty'
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
  const dayCount = Math.max(upList.length, downList.length)
  const days: AvailabilityDay[] = []
  for (let index = 0; index < dayCount; index += 1) {
    const percent = availabilityPercentOrNull(upList[index], downList[index])
    days.push({ percent, tone: availabilityTone(percent) })
  }
  let uptimePercent = availabilityPercentOrNull(row.current_up, row.current_down)
  if (uptimePercent === null) {
    for (let index = days.length - 1; index >= 0; index -= 1) {
      const sample = days[index]?.percent ?? null
      if (sample !== null) {
        uptimePercent = sample
        break
      }
    }
  }
  const latencyMs = averagePositive(delayPoints) ?? lastFinite(delayPoints)
  const live = uptimePercent !== null && uptimePercent > 95
  const tone: ServiceStatusView['tone'] = uptimePercent === null
    ? 'empty'
    : live
      ? 'online'
      : 'offline'
  return {
    id: String(row.id || row.name || row.monitor_name || ''),
    name: String(row.name || row.monitor_name || ''),
    live,
    tone,
    uptimePercent,
    uptimeLabel: uptimePercent === null ? '—' : `${uptimePercent.toFixed(2)}%`,
    days,
    delayPoints,
    latencyMs,
    latencyLabel: formatLatencyMs(latencyMs),
  }
}

export function toServiceStatusViews(rows: ResourceRecord[]) {
  return rows.map(toServiceStatusView)
}
