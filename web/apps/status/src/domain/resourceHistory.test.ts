import { describe, expect, it } from 'vitest'
import { resolutionFor } from '@santaizi/theme-server-status'

const hour = 3_600_000

describe('resolutionFor', () => {
  it('≤24h 用 1m', () => {
    expect(resolutionFor(0, hour)).toBe('1m')
    expect(resolutionFor(0, 24 * hour)).toBe('1m')
  })

  it('>24h 用 1h', () => {
    expect(resolutionFor(0, 24 * hour + 1)).toBe('1h')
    expect(resolutionFor(0, 30 * 24 * hour)).toBe('1h')
  })

  it('非法区间回落 1m', () => {
    expect(resolutionFor(10, 5)).toBe('1m')
  })
})
