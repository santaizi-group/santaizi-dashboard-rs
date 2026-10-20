import { describe, expect, it } from 'vitest'
import {
  availabilityPercent,
  availabilityTone,
  formatLatencyMs,
  readNumberList,
  toServiceStatusView,
} from '@santaizi/theme-server-status'

describe('serviceStatusView', () => {
  it('reads delay arrays and scalar avg_delay without joining them into a label', () => {
    const fromArray = toServiceStatusView({
      id: 1,
      name: 'Cloudflare.V4',
      current_up: 99,
      current_down: 1,
      up: [99, 100, 85, 10],
      down: [1, 0, 15, 90],
      delay: [0, 0, 0, 1.5860779],
    })
    expect(fromArray.uptimeLabel).toBe('99.00%')
    expect(fromArray.live).toBe(true)
    expect(fromArray.days.map((day) => day.tone)).toEqual(['good', 'good', 'warn', 'down'])
    expect(fromArray.latencyLabel).toBe('1.59')
    expect(fromArray.latencyLabel).not.toContain('[')
    expect(fromArray.delayPoints).toEqual([0, 0, 0, 1.5860779])

    const fromScalar = toServiceStatusView({
      id: 2,
      name: 'Public API',
      current_up: 42,
      current_down: 8,
      avg_delay: 42,
    })
    expect(fromScalar.latencyLabel).toBe('42.00')
    expect(fromScalar.delayPoints).toEqual([42])
  })

  it('formats availability and latency helpers', () => {
    expect(availabilityPercent(99, 1)).toBe(99)
    expect(availabilityTone(96)).toBe('good')
    expect(availabilityTone(81)).toBe('warn')
    expect(availabilityTone(10)).toBe('down')
    expect(readNumberList([1, '2', null])).toEqual([1, 2, 0])
    expect(formatLatencyMs(null)).toBe('')
    expect(formatLatencyMs(1.586)).toBe('1.59')
  })
})
