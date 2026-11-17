import type { AlertMetric } from '@/types/admin'

export type NetSpeedUnit = 'MB/s' | 'KB/s' | 'Mbps' | 'B/s'

export const NET_SPEED_UNITS: readonly NetSpeedUnit[] = ['MB/s', 'KB/s', 'Mbps', 'B/s']

export const NET_SPEED_FACTOR: Record<NetSpeedUnit, number> = {
  'B/s': 1,
  'KB/s': 1024,
  'MB/s': 1048576,
  'Mbps': 125000,
}

export function isNetSpeedMetric(type: AlertMetric): boolean {
  return type === 'net_in_speed' || type === 'net_out_speed' || type === 'net_all_speed'
}

export function defaultMaxForMetric(type: AlertMetric): number | null {
  if (['cpu', 'gpu', 'memory', 'swap', 'disk'].includes(type)) return 90
  if (type === 'temperature_max') return 85
  if (isNetSpeedMetric(type)) return 100 * NET_SPEED_FACTOR['MB/s']
  if (type.startsWith('load')) return 4
  if (type.endsWith('_count')) return 1000
  return null
}

export function inferNetSpeedUnit(bytesPerSec: number | null | undefined): NetSpeedUnit {
  const n = Number(bytesPerSec)
  if (!Number.isFinite(n) || n <= 0) return 'MB/s'
  if (n % NET_SPEED_FACTOR['MB/s'] === 0) return 'MB/s'
  if (n % NET_SPEED_FACTOR['Mbps'] === 0) return 'Mbps'
  if (n % NET_SPEED_FACTOR['KB/s'] === 0) return 'KB/s'
  if (n >= NET_SPEED_FACTOR['MB/s']) return 'MB/s'
  if (n >= NET_SPEED_FACTOR['KB/s']) return 'KB/s'
  return 'B/s'
}

export function formatNetSpeedAmount(bytesPerSec: number, unit: NetSpeedUnit): string {
  const amount = bytesPerSec / NET_SPEED_FACTOR[unit]
  if (!Number.isFinite(amount) || amount <= 0) return ''
  if (Number.isInteger(amount)) return String(amount)
  return String(Number(amount.toFixed(4)))
}

export function resolveConditionThreshold(
  rawInput: unknown,
  type: AlertMetric,
  speedUnit: NetSpeedUnit = 'MB/s',
): number | null {
  if (type === 'offline') return null
  const trimmed = String(rawInput ?? '').trim()
  if (!trimmed) return null
  const num = Number(trimmed)
  if (!Number.isFinite(num) || num <= 0) return null
  if (isNetSpeedMetric(type)) {
    return Math.max(1, Math.round(num * NET_SPEED_FACTOR[speedUnit]))
  }
  return num
}
