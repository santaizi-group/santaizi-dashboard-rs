import type { MonitorHistory, ServerRecord } from '@santaizi/api'
import { isHostOnline } from '@santaizi/api'
import { flagCode } from './publicNoteView'

export interface SparklineGeometry {
  line: string
  area: string
}

export interface NetworkSparklineSeries {
  name: string
  points: number[]
}

export interface NetworkHostTile {
  id: number
  name: string
  online: boolean
  platform: string
  flagCode: string
}

export function downsample(values: number[], maxPoints: number) {
  if (maxPoints <= 0 || values.length <= maxPoints) return values.slice()
  const result: number[] = []
  const bucket = values.length / maxPoints
  for (let index = 0; index < maxPoints; index += 1) {
    const start = Math.floor(index * bucket)
    const end = Math.max(start + 1, Math.floor((index + 1) * bucket))
    let total = 0
    let count = 0
    for (let cursor = start; cursor < end && cursor < values.length; cursor += 1) {
      const value = values[cursor]
      if (value === undefined || !Number.isFinite(value)) continue
      total += value
      count += 1
    }
    result.push(count ? total / count : 0)
  }
  return result
}

export function sparklineGeometry(values: number[], width: number, height: number, pad = 2): SparklineGeometry {
  if (!values.length || width <= 0 || height <= 0) return { line: '', area: '' }
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = max - min || 1
  const innerW = Math.max(1, width - pad * 2)
  const innerH = Math.max(1, height - pad * 2)
  const coords = values.map((value, index) => {
    const x = pad + (values.length === 1 ? innerW / 2 : (index / (values.length - 1)) * innerW)
    const y = pad + innerH - ((value - min) / span) * innerH
    return [x, y] as const
  })
  const line = coords.map((point, index) => `${index ? 'L' : 'M'}${point[0].toFixed(2)} ${point[1].toFixed(2)}`).join(' ')
  const last = coords[coords.length - 1]
  const first = coords[0]
  if (!last || !first) return { line: '', area: '' }
  const baseline = (height - pad).toFixed(2)
  const area = `${line} L${last[0].toFixed(2)} ${baseline} L${first[0].toFixed(2)} ${baseline} Z`
  return { line, area }
}

export function seriesFromMonitorHistory(rows: MonitorHistory[], maxSeries = 3, maxPoints = 48): NetworkSparklineSeries[] {
  return rows.slice(0, maxSeries).map((row, index) => {
    const delays = ((row.avg_delay as unknown[]) || []).map((value) => {
      const n = Number(value)
      return Number.isFinite(n) ? n : 0
    })
    return {
      name: String(row.monitor_name || `monitor-${index + 1}`),
      points: downsample(delays, maxPoints),
    }
  }).filter((item) => item.points.length > 0)
}

export function toNetworkHostTile(server: ServerRecord): NetworkHostTile {
  const host = server.host || {}
  return {
    id: server.id,
    name: server.name || `#${server.id}`,
    online: isHostOnline(server),
    platform: String(host.Platform || ''),
    flagCode: flagCode(server.public_note, host.CountryCode),
  }
}

export function toNetworkHostTiles(servers: ServerRecord[]) {
  return [...servers]
    .filter((server) => Number.isFinite(server.id) && server.id > 0)
    .sort((left, right) => right.display_index - left.display_index)
    .map(toNetworkHostTile)
}

export type NetworkGridDensity = 'few' | 'mid' | 'many'

/** 主机少用大卡，多用密铺。 */
export function networkGridDensity(count: number): NetworkGridDensity {
  if (count <= 4) return 'few'
  if (count <= 9) return 'mid'
  return 'many'
}

/** 0 表示交给 CSS auto-fit。 */
export function networkGridColumns(count: number) {
  if (count <= 0) return 1
  if (count <= 3) return count
  if (count === 4) return 2
  if (count <= 9) return 3
  return 0
}
