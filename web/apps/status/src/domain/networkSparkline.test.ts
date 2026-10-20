import { describe, expect, it } from 'vitest'
import type { ServerRecord } from '@santaizi/api'
import {
  downsample,
  networkGridColumns,
  networkGridDensity,
  seriesFromMonitorHistory,
  sparklineGeometry,
  toNetworkHostTiles,
} from '@santaizi/theme-server-status'

describe('networkSparkline', () => {
  it('downsamples and builds an svg path', () => {
    const points = downsample([1, 2, 3, 4, 5, 6, 7, 8], 4)
    expect(points).toHaveLength(4)
    const geometry = sparklineGeometry([0, 10, 5], 100, 20)
    expect(geometry.line.startsWith('M')).toBe(true)
    expect(geometry.area.endsWith('Z')).toBe(true)
    expect(sparklineGeometry([], 100, 20)).toEqual({ line: '', area: '' })
  })

  it('keeps at most three monitor series and host tiles sorted by display index', () => {
    const series = seriesFromMonitorHistory([
      { monitor_name: 'A', avg_delay: [1, 2, 3] },
      { monitor_name: 'B', avg_delay: [4, 5] },
      { monitor_name: 'C', avg_delay: [6] },
      { monitor_name: 'D', avg_delay: [7] },
    ], 3, 48)
    expect(series.map((item) => item.name)).toEqual(['A', 'B', 'C'])

    const tiles = toNetworkHostTiles([
      { id: 2, name: 'SGP', tag: '', display_index: 10, hide_for_guest: false, enable_ddns: false, online: true, host: { Platform: 'debian', CountryCode: 'SG' } },
      { id: 1, name: 'HKG', tag: '', display_index: 30, hide_for_guest: false, enable_ddns: false, online: false, host: { Platform: 'ubuntu', CountryCode: 'HK' } },
    ] as ServerRecord[])
    expect(tiles.map((tile) => tile.name)).toEqual(['HKG', 'SGP'])
    expect(tiles[0]?.online).toBe(false)
    expect(tiles[0]?.flagCode).toBe('hk')
  })

  it('uses fewer columns and a larger density when host count is small', () => {
    expect(networkGridDensity(1)).toBe('few')
    expect(networkGridDensity(4)).toBe('few')
    expect(networkGridDensity(8)).toBe('mid')
    expect(networkGridDensity(12)).toBe('many')
    expect(networkGridColumns(1)).toBe(1)
    expect(networkGridColumns(3)).toBe(3)
    expect(networkGridColumns(4)).toBe(2)
    expect(networkGridColumns(7)).toBe(3)
    expect(networkGridColumns(12)).toBe(0)
  })
})
