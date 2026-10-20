import { describe, expect, it } from 'vitest'
import type { ServerRecord } from '@santaizi/api'
import { filterAndSortServers } from '../../../../packages/theme-nazhua/src/composables/useServerListFilters'
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
  })
})
