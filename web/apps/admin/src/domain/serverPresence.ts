import { isHostOnline, type HostPresenceInput } from '@santaizi/api'

export type HostListTone = 'online' | 'degraded' | 'offline' | ''

export type HostListServer = HostPresenceInput

export function hostListTone(server: HostListServer): HostListTone {
  const connectivity = server.telemetry?.connectivity
  // 在线只看主端收报（后端 `online`）；可用性列单独展示观测点覆盖。
  if (server.online === false) {
    // 从未上报过的机器不是「离线」，保持灰点。
    return server.last_active ? 'offline' : ''
  }
  if (server.online === true) {
    return connectivity === 'partial' ? 'degraded' : 'online'
  }
  if (connectivity === 'full') return 'online'
  if (connectivity === 'partial') return 'degraded'
  if (connectivity === 'unavailable') return 'offline'
  const host = server.telemetry?.host
  if (host === 'offline') return 'offline'
  if (host === 'online') return 'online'
  return ''
}

export function hostCoverageTone(server: HostListServer): 'is-ok' | 'is-warn' | 'is-bad' | 'is-unknown' {
  const connectivity = server.telemetry?.connectivity
  if (connectivity === 'full') return 'is-ok'
  if (connectivity === 'partial') return 'is-warn'
  if (connectivity === 'unavailable') return 'is-bad'
  if (connectivity === 'unknown') return 'is-unknown'
  if (server.telemetry?.available === false) return 'is-bad'
  if (server.telemetry?.available === true) return 'is-ok'
  return 'is-unknown'
}

export function hostCoverageIcon(tone: ReturnType<typeof hostCoverageTone>): string {
  if (tone === 'is-ok') return 'ri-checkbox-circle-fill'
  if (tone === 'is-warn') return 'ri-error-warning-fill'
  if (tone === 'is-bad') return 'ri-close-circle-fill'
  return 'ri-question-fill'
}
