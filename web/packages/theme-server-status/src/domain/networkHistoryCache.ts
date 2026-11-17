import { getPublicNetwork, type MonitorHistory } from '@santaizi/api'

const CACHE_TTL_MS = 60_000

interface CacheEntry {
  rows: MonitorHistory[]
  expiresAt: number
}

const cache = new Map<number, CacheEntry>()
const inflight = new Map<number, Promise<MonitorHistory[]>>()
const waiting: Array<() => void> = []
const LIMIT = 4
let active = 0

function acquire() {
  if (active < LIMIT) {
    active += 1
    return Promise.resolve()
  }
  return new Promise<void>((resolve) => {
    waiting.push(() => {
      active += 1
      resolve()
    })
  })
}

function release() {
  active = Math.max(0, active - 1)
  waiting.shift()?.()
}

export function peekNetworkHistory(id: number, now = Date.now()) {
  const entry = cache.get(id)
  if (!entry) return undefined
  if (entry.expiresAt <= now) {
    cache.delete(id)
    return undefined
  }
  return entry.rows
}

export function rememberNetworkHistory(id: number, rows: MonitorHistory[], now = Date.now()) {
  cache.set(id, { rows, expiresAt: now + CACHE_TTL_MS })
}

export async function loadNetworkHistory(id: number, options?: { force?: boolean }) {
  if (!options?.force) {
    const hit = peekNetworkHistory(id)
    if (hit) return hit
    const pending = inflight.get(id)
    if (pending) return pending
  }
  const request = (async () => {
    await acquire()
    try {
      if (!options?.force) {
        const cached = peekNetworkHistory(id)
        if (cached) return cached
      }
      const rows = (await getPublicNetwork(id)).data || []
      rememberNetworkHistory(id, rows)
      return rows
    } finally {
      release()
      inflight.delete(id)
    }
  })()
  inflight.set(id, request)
  return request
}
