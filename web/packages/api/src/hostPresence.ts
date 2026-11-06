/** 列表 / 公开站共用的主机在线判定输入。不要用浏览器墙钟去算 last_active。 */
export type HostPresenceInput = {
  online?: boolean
  /** 主端最近一次收到上报的时间；空串 / 缺省 = 从未上报。 */
  last_active?: string
  telemetry?: {
    host?: string | null
    connectivity?: string | null
    available?: boolean | null
  } | null
}

function hostState(server: HostPresenceInput) {
  return String(server.telemetry?.host || '')
}

function connectivity(server: HostPresenceInput) {
  return String(server.telemetry?.connectivity || '')
}

/**
 * 主机是否在线：跟主端收报间隔（后端 `online`），不跟可用性共识。
 * `online` 缺省（旧快照/测试夹具）时才退回 telemetry 字段。
 */
export function isHostOnline(server: HostPresenceInput): boolean {
  if (typeof server.online === 'boolean') return server.online
  const host = hostState(server)
  if (host === 'offline') return false
  const link = connectivity(server)
  if (link === 'unavailable') return false
  if (host === 'online') return true
  return link === 'full' || link === 'partial'
}
