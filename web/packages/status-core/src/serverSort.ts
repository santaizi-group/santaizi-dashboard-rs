import type { ServerRecord } from '@santaizi/api'
import { isHostOnline } from '@santaizi/api'

export type ServerSortProp =
  | 'display_index'
  | 'name'
  | 'online'
  | 'country_code'
  | 'platform'
  | 'boot_time'
  | 'net_in_speed'
  | 'net_out_speed'
  | 'net_in_transfer'
  | 'net_out_transfer'
  | 'total_transfer'
  | 'tcp_conn_count'
  | 'udp_conn_count'
  | 'total_conn_count'
  | 'load1'
  | 'cpu'
  | 'cpu_cores'
  | 'mem_used'
  | 'mem_total'
  | 'swap_used'
  | 'swap_total'
  | 'disk_used'
  | 'disk_total'
export type ServerSortOrder = 'asc' | 'desc'
export type ServerSortGroup = 'base' | 'host' | 'net' | 'conn' | 'load' | 'cap'

export interface ServerSortOption {
  prop: ServerSortProp
  labelKey: string
  group: ServerSortGroup
}

export const SERVER_SORT_OPTIONS: ServerSortOption[] = [
  { prop: 'display_index', labelKey: 'nazhua.sortWeight', group: 'base' },
  { prop: 'name', labelKey: 'nazhua.sortName', group: 'base' },
  { prop: 'online', labelKey: 'nazhua.sortOnline', group: 'base' },
  { prop: 'country_code', labelKey: 'nazhua.sortCountry', group: 'host' },
  { prop: 'platform', labelKey: 'nazhua.sortPlatform', group: 'host' },
  { prop: 'boot_time', labelKey: 'nazhua.sortUptime', group: 'host' },
  { prop: 'net_in_speed', labelKey: 'nazhua.sortNetInSpeed', group: 'net' },
  { prop: 'net_out_speed', labelKey: 'nazhua.sortNetOutSpeed', group: 'net' },
  { prop: 'net_in_transfer', labelKey: 'nazhua.sortNetInTransfer', group: 'net' },
  { prop: 'net_out_transfer', labelKey: 'nazhua.sortNetOutTransfer', group: 'net' },
  { prop: 'total_transfer', labelKey: 'nazhua.sortTotalTransfer', group: 'net' },
  { prop: 'tcp_conn_count', labelKey: 'nazhua.sortTcp', group: 'conn' },
  { prop: 'udp_conn_count', labelKey: 'nazhua.sortUdp', group: 'conn' },
  { prop: 'total_conn_count', labelKey: 'nazhua.sortTotalConn', group: 'conn' },
  { prop: 'load1', labelKey: 'nazhua.sortLoad', group: 'load' },
  { prop: 'cpu', labelKey: 'nazhua.sortCpu', group: 'load' },
  { prop: 'cpu_cores', labelKey: 'nazhua.sortCpuCores', group: 'load' },
  { prop: 'mem_used', labelKey: 'nazhua.sortMemUsed', group: 'cap' },
  { prop: 'mem_total', labelKey: 'nazhua.sortMemTotal', group: 'cap' },
  { prop: 'swap_used', labelKey: 'nazhua.sortSwapUsed', group: 'cap' },
  { prop: 'swap_total', labelKey: 'nazhua.sortSwapTotal', group: 'cap' },
  { prop: 'disk_used', labelKey: 'nazhua.sortDiskUsed', group: 'cap' },
  { prop: 'disk_total', labelKey: 'nazhua.sortDiskTotal', group: 'cap' },
]

const SORT_PROPS = new Set(SERVER_SORT_OPTIONS.map((option) => option.prop))

export interface ServerSortColumn {
  group: ServerSortGroup
  options: ServerSortOption[]
}

export const SERVER_SORT_COLUMNS: ServerSortColumn[] = SERVER_SORT_OPTIONS.reduce<ServerSortColumn[]>((columns, option) => {
  const current = columns[columns.length - 1]
  if (current && current.group === option.group) current.options.push(option)
  else columns.push({ group: option.group, options: [option] })
  return columns
}, [])

function textList(value: unknown) {
  if (Array.isArray(value)) return value.map((item) => String(item).trim()).filter(Boolean)
  if (typeof value === 'string' && value.trim()) return [value.trim()]
  return []
}

export function parseCpuCores(cpu: unknown) {
  const texts = textList(cpu)
  let cores = 0
  for (const text of texts) {
    const match = text.match(/(\d+)\s+(Virtual|Physics|Physical)\s+Core/i)
    if (match) {
      cores += Number(match[1])
      continue
    }
    if (/^\d+(\.\d+)?$/.test(text)) cores += Number(text)
  }
  if (cores > 0) return cores
  if (typeof cpu === 'number' && Number.isFinite(cpu) && cpu > 0) return cpu
  return texts.length
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function num(source: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const raw = source[key]
    if (raw !== undefined && raw !== null && raw !== '') {
      const n = typeof raw === 'number' ? raw : Number(raw)
      if (Number.isFinite(n)) return n
    }
  }
  return 0
}

function text(source: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const raw = source[key]
    if (raw !== undefined && raw !== null && String(raw).trim() !== '') return String(raw)
  }
  return ''
}

function optNum(source: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const raw = source[key]
    if (raw !== undefined && raw !== null && raw !== '') {
      const n = typeof raw === 'number' ? raw : Number(raw)
      if (Number.isFinite(n)) return n
    }
  }
  return null
}

function usageRatio(used: number, total: number): number | null {
  if (!(total > 0) || !Number.isFinite(used) || !Number.isFinite(total)) return null
  return Math.max(0, Math.min(100, (used / total) * 100))
}

/**
 * 提取排序值。返回 `null` 表示未知/离线/未上报：
 * 无论升序还是降序，缺失指标一律排在有效数据末尾，避免离线或总量未知的主机在升序时霸占榜首。
 */
export function sortValue(server: ServerRecord, prop: ServerSortProp): number | string | null {
  const host = asRecord(server.host)
  const state = asRecord(server.state)
  const online = isHostOnline(server)
  switch (prop) {
    case 'name':
      return server.name
    case 'online':
      return Number(online)
    case 'country_code': {
      const code = text(host, 'CountryCode', 'country_code').toLowerCase()
      return code || null
    }
    case 'platform': {
      const plat = text(host, 'Platform', 'platform').toLowerCase()
      return plat || null
    }
    case 'boot_time': {
      if (!online) return null
      const uptime = num(state, 'Uptime', 'uptime')
      if (uptime > 0) return uptime
      const boot = num(host, 'BootTime', 'boot_time')
      if (!(boot > 0)) return null
      const bootSec = boot > 1e12 ? boot / 1000 : boot
      return Math.max(0, Date.now() / 1000 - bootSec)
    }
    case 'net_in_speed':
      return online ? num(state, 'NetInSpeed', 'net_in_speed') : null
    case 'net_out_speed':
      return online ? num(state, 'NetOutSpeed', 'net_out_speed') : null
    case 'net_in_transfer':
      return online && server.state ? num(state, 'NetInTransfer', 'net_in_transfer') : null
    case 'net_out_transfer':
      return online && server.state ? num(state, 'NetOutTransfer', 'net_out_transfer') : null
    case 'total_transfer':
      return online && server.state
        ? num(state, 'NetInTransfer', 'net_in_transfer') + num(state, 'NetOutTransfer', 'net_out_transfer')
        : null
    case 'tcp_conn_count':
      return online ? optNum(state, 'TcpConnCount', 'tcp_conn_count') : null
    case 'udp_conn_count':
      return online ? optNum(state, 'UdpConnCount', 'udp_conn_count') : null
    case 'total_conn_count': {
      if (!online) return null
      const tcp = optNum(state, 'TcpConnCount', 'tcp_conn_count')
      const udp = optNum(state, 'UdpConnCount', 'udp_conn_count')
      return tcp === null && udp === null ? null : (tcp ?? 0) + (udp ?? 0)
    }
    case 'load1':
      return online ? optNum(state, 'Load1', 'load1') : null
    case 'cpu':
      return online ? optNum(state, 'CPU', 'Cpu', 'cpu') : null
    case 'cpu_cores': {
      const cores = parseCpuCores(host.CPU ?? host.cpu)
      return cores > 0 ? cores : null
    }
    case 'mem_used': {
      if (!online || !server.state) return null
      const total = num(host, 'MemTotal', 'mem_total') || num(state, 'MemTotal', 'mem_total')
      return usageRatio(num(state, 'MemUsed', 'mem_used'), total)
    }
    case 'mem_total': {
      const total = num(host, 'MemTotal', 'mem_total') || num(state, 'MemTotal', 'mem_total')
      return total > 0 ? total : null
    }
    case 'swap_used': {
      if (!online || !server.state) return null
      const total = num(host, 'SwapTotal', 'swap_total') || num(state, 'SwapTotal', 'swap_total')
      return usageRatio(num(state, 'SwapUsed', 'swap_used'), total)
    }
    case 'swap_total': {
      const total = num(host, 'SwapTotal', 'swap_total') || num(state, 'SwapTotal', 'swap_total')
      return total > 0 ? total : null
    }
    case 'disk_used': {
      if (!online || !server.state) return null
      const total = num(host, 'DiskTotal', 'disk_total') || num(state, 'DiskTotal', 'disk_total')
      return usageRatio(num(state, 'DiskUsed', 'disk_used'), total)
    }
    case 'disk_total': {
      const total = num(host, 'DiskTotal', 'disk_total') || num(state, 'DiskTotal', 'disk_total')
      return total > 0 ? total : null
    }
    default:
      return server.display_index
  }
}

export function resolveServerSortProp(prop: string | undefined | null): ServerSortProp {
  return prop && SORT_PROPS.has(prop as ServerSortProp) ? prop as ServerSortProp : 'display_index'
}

export function sortServers(
  list: readonly ServerRecord[],
  prop: ServerSortProp | string = 'display_index',
  order: ServerSortOrder = 'desc',
): ServerRecord[] {
  const direction = order === 'asc' ? 1 : -1
  const resolved = resolveServerSortProp(prop)
  return [...list].sort((a, b) => {
    const left = sortValue(a, resolved)
    const right = sortValue(b, resolved)
    if (left === null && right === null) {
      return (b.display_index - a.display_index) || (a.id - b.id)
    }
    if (left === null) return 1
    if (right === null) return -1
    if (typeof left === 'string' || typeof right === 'string') {
      return String(left).localeCompare(String(right)) * direction
    }
    return (left - right) * direction
  })
}
