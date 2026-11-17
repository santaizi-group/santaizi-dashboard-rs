import { describe, expect, it } from 'vitest'
import {
  defaultMaxForMetric,
  formatNetSpeedAmount,
  inferNetSpeedUnit,
  isNetSpeedMetric,
  resolveConditionThreshold,
} from './alertThreshold'

describe('alertThreshold', () => {
  it('identifies net speed metrics and provides appropriate defaults', () => {
    expect(isNetSpeedMetric('net_in_speed')).toBe(true)
    expect(isNetSpeedMetric('cpu')).toBe(false)
    expect(defaultMaxForMetric('cpu')).toBe(90)
    expect(defaultMaxForMetric('net_in_speed')).toBe(104857600)
    expect(defaultMaxForMetric('offline')).toBeNull()
  })

  it('infers and echoes human-readable net speed units from B/s', () => {
    expect(inferNetSpeedUnit(104857600)).toBe('MB/s')
    expect(formatNetSpeedAmount(104857600, 'MB/s')).toBe('100')

    expect(inferNetSpeedUnit(12500000)).toBe('Mbps')
    expect(formatNetSpeedAmount(12500000, 'Mbps')).toBe('100')

    expect(inferNetSpeedUnit(512000)).toBe('KB/s')
    expect(formatNetSpeedAmount(512000, 'KB/s')).toBe('500')

    expect(inferNetSpeedUnit(500)).toBe('B/s')
    expect(formatNetSpeedAmount(500, 'B/s')).toBe('500')
  })

  it('converts threshold inputs accurately back to B/s on submit', () => {
    expect(resolveConditionThreshold('100', 'net_out_speed', 'MB/s')).toBe(104857600)
    expect(resolveConditionThreshold('50', 'net_in_speed', 'Mbps')).toBe(6250000)
    expect(resolveConditionThreshold('256', 'net_all_speed', 'KB/s')).toBe(262144)
    expect(resolveConditionThreshold('80', 'cpu', 'MB/s')).toBe(80)
    expect(resolveConditionThreshold('', 'cpu')).toBeNull()
  })
})
