export type QuotaUnit = 'MB' | 'GB' | 'TB'

export const QUOTA_UNIT_FACTOR: Record<QuotaUnit, number> = {
  MB: 1048576,
  GB: 1073741824,
  TB: 1099511627776,
}

export const DEFAULT_QUOTA_BYTES = 1073741824
export const MIN_QUOTA_BYTES = 1048576
export const QUOTA_UNITS: readonly QuotaUnit[] = ['MB', 'GB', 'TB']

/**
 * 打开已有流量策略时选择最整洁的展示单位：
 * 能整除 TB 选 TB；否则能整除 GB 或 >= 1 GB 选 GB；< 1 GB 选 MB。
 */
export function inferQuotaUnit(bytes: number): QuotaUnit {
  const n = Number(bytes)
  if (!Number.isFinite(n) || n <= 0) return 'GB'
  if (n % QUOTA_UNIT_FACTOR.TB === 0) return 'TB'
  if (n % QUOTA_UNIT_FACTOR.GB === 0 || n >= QUOTA_UNIT_FACTOR.GB) return 'GB'
  return 'MB'
}

export function formatQuotaAmount(bytes: number, unit: QuotaUnit): string {
  const amount = bytes / QUOTA_UNIT_FACTOR[unit]
  if (!Number.isFinite(amount) || amount <= 0) return '1'
  if (Number.isInteger(amount)) return String(amount)
  return String(Number(amount.toFixed(6)))
}

export function parsePositiveQuotaBytes(value: unknown, unit: QuotaUnit): number | null {
  const trimmed = String(value ?? '').trim()
  if (!trimmed) return null
  const amount = Number(trimmed)
  if (!Number.isFinite(amount) || amount <= 0) return null
  return Math.max(MIN_QUOTA_BYTES, Math.round(amount * QUOTA_UNIT_FACTOR[unit]))
}

/**
 * 切换单位时保持输入框里的数字字面量，按新单位重新计算字节数（而非把旧字节数换算为新单位）。
 */
export function recalcQuotaBytesForNewUnit(
  currentBytes: number,
  prevUnit: QuotaUnit,
  nextUnit: QuotaUnit,
  draft?: string,
): number {
  const literal = draft !== undefined ? draft : formatQuotaAmount(currentBytes, prevUnit)
  const parsed = parsePositiveQuotaBytes(literal, nextUnit)
  return parsed ?? Math.max(MIN_QUOTA_BYTES, QUOTA_UNIT_FACTOR[nextUnit])
}
