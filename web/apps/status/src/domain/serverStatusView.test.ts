import { describe, expect, it } from 'vitest'
import type { ResourceRecord, ServerRecord } from '@santaizi/api'
import {
  clampPercent,
  formatBytes,
  formatConnPair,
  mapCycleTransfers,
  percentOf,
  toServerStatusView,
} from '@santaizi/theme-server-status'

function server(overrides: Partial<ServerRecord> = {}): ServerRecord {
  return {
    id: 7,
    name: 'HKG-EDGE',
    tag: 'HKG',
    display_index: 10,
    hide_for_guest: false,
    enable_ddns: false,
    online: true,
    host: {
      Platform: 'linux',
      PlatformVersion: '6.8',
      Arch: 'amd64',
      Virtualization: 'kvm',
      CountryCode: 'HK',
      CPU: ['AMD EPYC 7763'],
      GPU: ['Tesla T4'],
      MemTotal: 2_147_483_648,
      DiskTotal: 21_474_836_480,
      SwapTotal: 1_073_741_824,
      Version: '1.0.0',
      BootTime: 1_700_000_000,
    },
    state: {
      CPU: 12.5,
      MemUsed: 1_073_741_824,
      DiskUsed: 10_737_418_240,
      SwapUsed: 268_435_456,
      GPU: 18,
      Uptime: 172_900,
      NetInSpeed: 4096,
      NetOutSpeed: 2048,
      NetInTransfer: 1024,
      NetOutTransfer: 2048,
      Load1: 0.2,
      Load5: 0.4,
      Load15: 0.5,
      TcpConnCount: 12,
      UdpConnCount: 3,
      ProcessCount: 88,
      Temperatures: [{ Name: 'cpu', Temperature: 47 }],
    },
    public_note: {
      customData: { location: 'HKG', slogan: '香港边缘', flag: 'hk', orderLink: 'https%3A%2F%2Fexample.com' },
      planDataMod: { networkRoute: 'CN2,GIA', IPv4: '1', IPv6: '1', bandwidth: '1 Gbps', trafficVol: '2 TB', trafficType: '2' },
      billingDataMod: { amount: '9.99CNY', cycle: '月' },
    },
    telemetry: { host: 'online', connectivity: 'healthy', available: true, coverage: '2/2' },
    ...overrides,
  }
}

describe('ServerStatus view adapter', () => {
  it('normalizes metrics, host specs and public notes without dual-key lookups in callers', () => {
    const view = toServerStatusView(server())
    expect(view.online).toBe(true)
    expect(view.cpu.percent).toBe(12.5)
    expect(view.memory.percent).toBe(50)
    expect(view.disk.percent).toBe(50)
    expect(view.memory.usedLabel).toBe('1024M')
    expect(view.memory.totalLabel).toBe('2G')
    expect(view.cpuCoreCount).toBe(0)
    expect(view.trafficUsage.kind).toBe('both')
    expect(view.transferInLabel).toBe('1K')
    expect(view.lastActiveLabel).toBe('')
    expect(view.gpu?.percent).toBe(18)
    expect(view.cpuModels).toEqual(['AMD EPYC 7763'])
    expect(view.gpuNames).toEqual(['Tesla T4'])
    expect(view.swap?.percent).toBe(25)
    expect(view.load1).toBe(0.2)
    expect(view.hasLoad).toBe(true)
    expect(view.tcp).toBe(12)
    expect(view.temperatures).toEqual([{ name: 'cpu', value: 47 }])
    expect(view.slogan).toBe('香港边缘')
    expect(view.flagCode).toBe('hk')
    expect(view.location).toBe('香港')
    expect(view.orderLink).toBe('https://example.com')
    expect(view.publicNote.planTags).toEqual(['CN2', 'GIA', '__dual_stack__'])
    expect(view.available).toBe(true)
    expect(view.hasSpecs).toBe(true)
  })

  it('reads PascalCase host and state snapshots', () => {
    const view = toServerStatusView(server({
      host: { Platform: 'freebsd', MemTotal: 1024, CountryCode: 'SG' },
      state: { CPU: 8, MemUsed: 512, NetInSpeed: 100 },
      public_note: {},
    }))
    expect(view.platform).toBe('freebsd')
    expect(view.cpu.percent).toBe(8)
    expect(view.memory.percent).toBe(50)
    expect(view.speedIn).toBe(100)
    expect(view.flagCode).toBe('sg')
    expect(view.location).toBe('新加坡')
  })

  it('omits empty GPU/swap and keeps unknown telemetry offline', () => {
    const view = toServerStatusView(server({
      online: false,
      host: { Platform: 'linux', MemTotal: 1024 },
      state: { CPU: 1, MemUsed: 10 },
      telemetry: { host: 'unknown', connectivity: 'unknown', available: null, coverage: '' },
    }))
    expect(view.online).toBe(false)
    expect(view.gpu).toBeNull()
    expect(view.swap).toBeNull()
    expect(view.tcp).toBeNull()
    expect(view.hasLoad).toBe(false)
    expect(view.available).toBeNull()
  })

  it('treats observer-seen hosts as online even when the LastActive flag is false', () => {
    const view = toServerStatusView(server({
      online: false,
      telemetry: { host: 'online', connectivity: 'partial', available: true, coverage: '1/2' },
    }))
    expect(view.online).toBe(true)
  })

  it('shows GPU names in specs without a usage bar when percent is absent', () => {
    const view = toServerStatusView(server({
      host: { Platform: 'linux', GPU: ['Tesla T4'] },
      state: { CPU: 1, MemUsed: 10 },
    }))
    expect(view.gpu).toBeNull()
    expect(view.gpuNames).toEqual(['Tesla T4'])
    expect(view.hasSpecs).toBe(true)
  })

  it('keeps each cycle-transfer policy instead of aggregating', () => {
    const rows: ResourceRecord[] = [
      { server_id: 7, policy_id: 1, name: '月流量', used_bytes: 30, quota_bytes: 100, status: 'normal' },
      { server_id: 7, policy_id: 2, name: '附加', used_bytes: 20, quota_bytes: 50, status: 'warning' },
      { server_id: 8, policy_id: 3, used_bytes: 5, quota_bytes: 10 },
    ]
    const cycles = mapCycleTransfers(rows)
    expect(cycles.get(7)).toHaveLength(2)
    expect(cycles.get(7)?.[1]).toMatchObject({ name: '附加', usagePercent: 40, status: 'warning' })
    expect(toServerStatusView(server(), cycles).cycles).toHaveLength(2)
  })

  it('parses physical cores, prefers cycle remaining, and hides zero last_active', () => {
    const cycles = mapCycleTransfers([
      { server_id: 7, policy_id: 1, name: '月流量', used_bytes: 30, quota_bytes: 100, remaining_bytes: 70, usage_percent: 30, status: 'normal' },
    ])
    const view = toServerStatusView(server({
      host: { Platform: 'debian', CPU: ['AMD EPYC 2 Physical Core'], MemTotal: 1024 },
      state: { CPU: 1, MemUsed: 10, Uptime: 90000, NetInTransfer: 10, NetOutTransfer: 20 },
      last_active: '0001-01-01T00:00:00Z',
    }), cycles)
    expect(view.cpuCoreCount).toBe(2)
    expect(view.platformLabel).toBe('Debian')
    expect(view.trafficUsage.kind).toBe('cycle')
    expect(view.trafficUsage.statusLevel).toBe('fine')
    expect(view.lastActiveLabel).toBe('')
    expect(view.uptimeLabel).toContain('1')
  })

  it('keeps format helpers bounded', () => {
    expect(clampPercent(140)).toBe(100)
    expect(percentOf(1, 4)).toBe(25)
    expect(formatBytes(1024)).toBe('1.0 KB')
    expect(formatBytes(0)).toBe('0 B')
    expect(formatConnPair(16, 5)).toBe('16 / 5')
    expect(formatConnPair(0, 0)).toBe('0 / 0')
    expect(formatConnPair(12, null)).toBe('12 / 0')
    expect(formatConnPair(null, 3)).toBe('0 / 3')
    expect(formatConnPair(null, null)).toBe('')
  })
})
