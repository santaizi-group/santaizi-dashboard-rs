import { describe, expect, it } from 'vitest'
import type { ServerRecord } from '@santaizi/api'
import { sortServers } from '@santaizi/status-core'
import { filterAndSortServers, matchesServerSearch } from '../../../../packages/theme-nazhua/src/composables/useServerListFilters'
import { resolveServerLocation } from '../../../../packages/theme-nazhua/src/utils/worldMap'

function row(
  id: number,
  name: string,
  tag: string,
  displayIndex: number,
  online: boolean,
  country: string,
  platform: string,
  extras: Partial<ServerRecord> = {},
): ServerRecord {
  return {
    id, name, tag, display_index: displayIndex, online,
    hide_for_guest: false, enable_ddns: false,
    host: { CountryCode: country, Platform: platform, CPU: ['2 Physical Core'], MemTotal: id * 1024 },
    ...extras,
  }
}

const rows = [
  row(1, 'Tokyo', 'APAC', 2, true, 'JP', 'linux'),
  row(2, 'Hong Kong', 'APAC', 8, false, 'HK', 'freebsd'),
  row(3, 'Frankfurt', 'EU', 4, true, 'DE', 'linux'),
]

describe('Nazhua filtering and location', () => {
  it('filters by group, search and online state before sorting', () => {
    expect(filterAndSortServers(rows, {
      tag: 'APAC', online: 'all', search: '', sort: 'display_index', order: 'desc',
    }).map(row => row.id)).toEqual([2, 1])
    expect(filterAndSortServers(rows, {
      tag: '', online: 'online', search: 'linux', sort: 'name', order: 'asc',
    }).map(row => row.name)).toEqual(['Frankfurt', 'Tokyo'])
    expect(filterAndSortServers(rows, {
      tag: '', online: 'all', search: '', sort: 'mem_total', order: 'desc',
    }).map(row => row.id)).toEqual([3, 2, 1])
    expect(filterAndSortServers(rows, {
      tag: '', online: 'all', search: '', sort: 'platform', order: 'asc',
    }).map(row => row.name)).toEqual(['Hong Kong', 'Tokyo', 'Frankfurt'])
  })

  it('resolves known public-note and host locations and rejects unknown codes', () => {
    expect(resolveServerLocation(rows[0]!)?.countryCode).toBe('jp')
    expect(resolveServerLocation({ host: {}, public_note: { customData: { location: 'HKG' } } })?.code).toBeTruthy()
    expect(resolveServerLocation({ host: { CountryCode: 'ZZZ' }, public_note: {} })).toBeNull()
    // CAN 对应广州而非重庆；非 31 国 ISO2 与公开备注经纬度均可落点
    const gz = resolveServerLocation({ host: {}, public_note: { customData: { location: 'CAN' } } })
    expect(gz?.name).toBe('广州')
    expect(gz?.countryCode).toBe('cn')
    const ch = resolveServerLocation({ host: { CountryCode: 'CH' }, public_note: {} })
    expect(ch?.countryCode).toBe('ch')
    expect(ch?.x).toBeTypeOf('number')
    const latlng = resolveServerLocation({ host: {}, public_note: { customData: { latlng: '47.38, 8.54', location: 'CH' } } })
    expect(latlng?.lat).toBeCloseTo(47.38)
    expect(latlng?.lon).toBeCloseTo(8.54)
  })

  it('matches region names in zh/en, note location and OS label in search', () => {
    const target = row(10, 'Edge-01', 'prod', 1, true, 'JP', 'debian', {
      public_note: { customData: { location: 'OSA' } },
    })
    expect(matchesServerSearch(target, '大阪')).toBe(true)
    expect(matchesServerSearch(target, 'Osaka')).toBe(true)
    expect(matchesServerSearch(target, '日本')).toBe(true)
    expect(matchesServerSearch(target, 'Japan')).toBe(true)
    expect(matchesServerSearch(target, 'Debian')).toBe(true)
  })

  it('sorts memory/disk by usage percent and puts offline/unknown hosts last in asc & desc', () => {
    const sample: ServerRecord[] = [
      row(1, 'Big-LowPct', 'g', 1, true, 'US', 'linux', {
        host: { MemTotal: 64_000, DiskTotal: 100_000 },
        state: { CPU: 20, MemUsed: 6_400, DiskUsed: 10_000 }, // 10%
      }),
      row(2, 'Small-HighPct', 'g', 2, true, 'US', 'linux', {
        host: { MemTotal: 1_000, DiskTotal: 10_000 },
        state: { CPU: 50, MemUsed: 900, DiskUsed: 9_000 }, // 90%
      }),
      row(3, 'Offline-DeadHigh', 'g', 3, false, 'US', 'linux', {
        host: { MemTotal: 1_000, DiskTotal: 10_000 },
        state: { CPU: 99, MemUsed: 990, DiskUsed: 9_900 },
      }),
      row(4, 'Unknown-Total', 'g', 4, true, 'US', 'linux', {
        host: { MemTotal: 0, DiskTotal: 0 },
        state: { CPU: 5, MemUsed: 500, DiskUsed: 500 },
      }),
    ]
    expect(sortServers(sample, 'mem_used', 'desc').map(s => s.id)).toEqual([2, 1, 4, 3])
    expect(sortServers(sample, 'mem_used', 'asc').map(s => s.id)).toEqual([1, 2, 4, 3])
    expect(sortServers(sample, 'cpu', 'asc').map(s => s.id)).toEqual([4, 1, 2, 3])
    expect(sortServers(sample, 'cpu', 'desc').map(s => s.id)).toEqual([2, 1, 4, 3])
  })
})
