import { describe, expect, it } from 'vitest'
import { hostCoverageIcon, hostCoverageTone, hostListTone } from './serverPresence'

describe('host list presence', () => {
  it('uses connectivity for four-state dots', () => {
    expect(hostListTone({ online: true, telemetry: { connectivity: 'full', available: true } })).toBe('online')
    expect(hostListTone({ online: true, telemetry: { connectivity: 'partial', available: true } })).toBe('degraded')
    expect(hostListTone({ online: true, telemetry: { connectivity: 'unavailable', available: false } })).toBe('offline')
    expect(hostListTone({ online: false, telemetry: { connectivity: 'unknown', available: null } })).toBe('')
  })

  it('treats host online as present even when LastActive flag is false', () => {
    expect(hostListTone({
      online: false,
      telemetry: { host: 'online', connectivity: 'unknown', available: null },
    })).toBe('online')
  })

  it('treats unavailable observers as offline even when LastActive flag is true', () => {
    expect(hostListTone({
      online: true,
      telemetry: { host: 'offline', connectivity: 'unavailable', available: false },
    })).toBe('offline')
  })

  it('falls back to online flag when telemetry connectivity is missing', () => {
    expect(hostListTone({ online: true })).toBe('online')
    expect(hostListTone({ online: false })).toBe('offline')
  })

  it('marks partial coverage as warning, not ok', () => {
    expect(hostCoverageTone({ online: true, telemetry: { connectivity: 'partial', available: true } })).toBe('is-warn')
    expect(hostCoverageIcon('is-warn')).toBe('ri-error-warning-fill')
    expect(hostCoverageTone({ online: true, telemetry: { connectivity: 'full', available: true } })).toBe('is-ok')
    expect(hostCoverageTone({ online: false, telemetry: { connectivity: 'unavailable', available: false } })).toBe('is-bad')
    expect(hostCoverageTone({ online: false, telemetry: { connectivity: 'unknown', available: null } })).toBe('is-unknown')
  })
})
