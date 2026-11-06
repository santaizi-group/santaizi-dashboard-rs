import { describe, expect, it } from 'vitest'
import { hostCoverageIcon, hostCoverageTone, hostListTone } from './serverPresence'

const seen = '2026-01-01T00:00:00Z'

describe('host list presence', () => {
  it('follows the report-based online flag', () => {
    expect(hostListTone({ online: true, last_active: seen, telemetry: { connectivity: 'full', available: true } })).toBe('online')
    expect(hostListTone({ online: true, last_active: seen, telemetry: { connectivity: 'partial', available: true } })).toBe('degraded')
    expect(hostListTone({ online: false, last_active: seen, telemetry: { connectivity: 'unavailable', available: false } })).toBe('offline')
  })

  it('keeps the dot green even when observers cannot see the host', () => {
    // 主端仍在收报；观测点覆盖走可用性列，不再把主机判成离线。
    expect(hostListTone({
      online: true,
      last_active: seen,
      telemetry: { host: 'offline', connectivity: 'unavailable', available: false },
    })).toBe('online')
  })

  it('treats a host that never reported as unknown, not offline', () => {
    expect(hostListTone({ online: false, telemetry: { host: 'unknown', connectivity: 'unknown', available: null } })).toBe('')
  })

  it('falls back to telemetry when the online flag is absent', () => {
    expect(hostListTone({ telemetry: { host: 'online', connectivity: 'unknown', available: null } })).toBe('online')
    expect(hostListTone({ telemetry: { host: 'offline', connectivity: 'unknown', available: null } })).toBe('offline')
    expect(hostListTone({ telemetry: { connectivity: 'full', available: true } })).toBe('online')
    expect(hostListTone({ telemetry: { connectivity: 'partial', available: true } })).toBe('degraded')
  })

  it('marks partial coverage as warning, not ok', () => {
    expect(hostCoverageTone({ online: true, telemetry: { connectivity: 'partial', available: true } })).toBe('is-warn')
    expect(hostCoverageIcon('is-warn')).toBe('ri-error-warning-fill')
    expect(hostCoverageTone({ online: true, telemetry: { connectivity: 'full', available: true } })).toBe('is-ok')
    expect(hostCoverageTone({ online: false, telemetry: { connectivity: 'unavailable', available: false } })).toBe('is-bad')
    expect(hostCoverageTone({ online: false, telemetry: { connectivity: 'unknown', available: null } })).toBe('is-unknown')
  })
})
