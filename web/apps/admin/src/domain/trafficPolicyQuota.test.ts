import { describe, expect, it } from 'vitest'
import {
  QUOTA_UNIT_FACTOR,
  formatQuotaAmount,
  inferQuotaUnit,
  parsePositiveQuotaBytes,
  recalcQuotaBytesForNewUnit,
} from './trafficPolicyQuota'

describe('trafficPolicyQuota', () => {
  it('infers the cleanest initial unit from quota_bytes', () => {
    expect(inferQuotaUnit(2 * QUOTA_UNIT_FACTOR.TB)).toBe('TB')
    expect(inferQuotaUnit(500 * QUOTA_UNIT_FACTOR.GB)).toBe('GB')
    expect(inferQuotaUnit(1.5 * QUOTA_UNIT_FACTOR.GB)).toBe('GB')
    expect(inferQuotaUnit(500 * QUOTA_UNIT_FACTOR.MB)).toBe('MB')
    expect(inferQuotaUnit(0)).toBe('GB')
  })

  it('keeps the numeric literal when switching unit even after blur committed bytes', () => {
    // 用户输入 500（默认 GB）失焦后 quota_bytes = 500 GB，随后切换下拉为 MB：
    // 结果应为 500 MB，绝不能变成 512000 MB。
    const afterBlurBytes = parsePositiveQuotaBytes('500', 'GB')!
    expect(afterBlurBytes).toBe(500 * QUOTA_UNIT_FACTOR.GB)

    const switchedBytes = recalcQuotaBytesForNewUnit(afterBlurBytes, 'GB', 'MB')
    expect(switchedBytes).toBe(500 * QUOTA_UNIT_FACTOR.MB)
    expect(formatQuotaAmount(switchedBytes, 'MB')).toBe('500')
  })

  it('syncs positive numeric input while returning null for intermediate partial input', () => {
    expect(parsePositiveQuotaBytes('0.', 'GB')).toBeNull()
    expect(parsePositiveQuotaBytes('', 'GB')).toBeNull()
    expect(parsePositiveQuotaBytes('0.5', 'TB')).toBe(Math.round(0.5 * QUOTA_UNIT_FACTOR.TB))
  })
})
