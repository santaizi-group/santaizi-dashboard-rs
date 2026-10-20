/** 列表 / 公开站共用的主机在线判定输入。不要用浏览器墙钟去算 last_active。 */
export type HostPresenceInput = {
  online?: boolean
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
 * 主机是否在线：跟 V2 共识，任一健康观测点看到即在线。
 * telemetry 优先于 LastActive 算出的 `online` 标志；没有观测数据时才退回该标志。
 */
export function isHostOnline(server: HostPresenceInput): boolean {
  const host = hostState(server)
  if (host === 'offline') return false
  const link = connectivity(server)
  if (link === 'unavailable') return false
  if (host === 'online') return true
  if (link === 'full' || link === 'partial') return true
  if (server.telemetry?.available === true) return true
  if (server.telemetry?.available === false) return false
  return server.online === true
}
