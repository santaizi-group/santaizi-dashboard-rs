import { describe, expect, it } from 'vitest'
import { mergeServerSnapshot, normalizeServer } from '@santaizi/status-core'

describe('ServerStatus runtime normalization', () => {
  it('accepts canonical API v2 fields', () => {
    expect(normalizeServer({
      id: 7,
      name: 'edge-a',
      tag: 'edge',
      online: true,
      telemetry: { host: 'online', connectivity: 'partial', available: true, coverage: 'partial' },
    })).toMatchObject({ id: 7, name: 'edge-a', tag: 'edge', online: true })
  })

  it('normalizes a canonical realtime update', () => {
    expect(normalizeServer({ id: 8, name: 'edge-b', tag: 'edge', online: true })).toMatchObject({
      id: 8,
      name: 'edge-b',
      tag: 'edge',
      online: true,
    })
  })

  it('keeps previous public_note when realtime payload omits it', () => {
    const prev = normalizeServer({
      id: 1,
      name: 'Demo1',
      public_note: { customData: { location: 'LAX' }, billingDataMod: { amount: '1999CNY' } },
    })
    const next = normalizeServer({
      ID: 1,
      Name: 'Demo1',
      State: { CPU: 0 },
    })
    expect(mergeServerSnapshot(prev, next).public_note).toEqual(prev.public_note)
    expect(mergeServerSnapshot(prev, next).state).toEqual({ CPU: 0 })
  })

  it('keeps previous public_note when WS sends empty object', () => {
    const prev = normalizeServer({
      id: 2,
      name: 'Demo2',
      public_note: { customData: { slogan: 'edge' } },
    })
    const next = normalizeServer({ id: 2, name: 'Demo2', public_note: {}, state: { CPU: 12 } })
    expect(mergeServerSnapshot(prev, next).public_note).toEqual(prev.public_note)
  })

  it('merges public_note sections when WS sends partial note', () => {
    const prev = normalizeServer({
      id: 3,
      name: 'Demo3',
      public_note: {
        customData: { location: 'HKG', slogan: 'old' },
        billingDataMod: { amount: '$1' },
      },
    })
    const next = normalizeServer({
      id: 3,
      name: 'Demo3',
      public_note: { customData: { slogan: 'new' } },
    })
    expect(mergeServerSnapshot(prev, next).public_note).toEqual({
      customData: { location: 'HKG', slogan: 'new' },
      billingDataMod: { amount: '$1' },
    })
  })

  it('does not blank name or host with empty WS fields', () => {
    const prev = normalizeServer({
      id: 4,
      name: 'KeepMe',
      tag: 'edge',
      host: { Platform: 'linux' },
      state: { CPU: 1 },
    })
    const next = normalizeServer({
      id: 4,
      name: '',
      tag: '',
      host: {},
      state: { CPU: 9 },
    })
    const merged = mergeServerSnapshot(prev, next)
    expect(merged.name).toBe('KeepMe')
    expect(merged.tag).toBe('edge')
    expect(merged.host).toEqual({ Platform: 'linux' })
    expect(merged.state).toEqual({ CPU: 9 })
  })

  it('does not infer online from a fresh last_active timestamp', () => {
    expect(normalizeServer({
      id: 5,
      name: 'fresh',
      last_active: new Date().toISOString(),
    }).online).toBeUndefined()
  })

  it('prefers observer telemetry over the LastActive online flag', () => {
    expect(normalizeServer({
      id: 6,
      name: 'seen',
      online: false,
      telemetry: { host: 'online', connectivity: 'partial', available: true, coverage: '1/2' },
    }).online).toBe(true)
    expect(normalizeServer({
      id: 7,
      name: 'gone',
      online: true,
      telemetry: { host: 'offline', connectivity: 'unavailable', available: false, coverage: '0/2' },
    }).online).toBe(false)
  })

  it('keeps previous online state when a WS frame omits online and telemetry', () => {
    const prev = normalizeServer({
      id: 8,
      name: 'edge',
      online: true,
      telemetry: { host: 'online', connectivity: 'full', available: true, coverage: '2/2' },
    })
    const next = normalizeServer({
      ID: 8,
      Name: 'edge',
      State: { CPU: 4 },
    })
    expect(mergeServerSnapshot(prev, next).online).toBe(true)
    expect(mergeServerSnapshot(prev, next).telemetry).toEqual(prev.telemetry)
  })
})
