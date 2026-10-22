import { computed, reactive, ref } from 'vue'
import { getBootstrap, listPublicServers, type ServerRecord, type SiteBootstrap, websocketURL } from '@santaizi/api'
import { mergeServerSnapshot, normalizeServer, type StatusStoreState } from '@santaizi/status-core'

export { mergeServerSnapshot, normalizeServer } from '@santaizi/status-core'

const bootstrap = ref<SiteBootstrap | null>(null)
const servers = ref<ServerRecord[]>([])
const loading = ref(false)
const connected = ref(false)
const loadError = ref(false)

let socket: WebSocket | undefined
let retry = 0
let timer: number | undefined
let stopped = true
/// 连接代际：每次 `connect()` 自增。
///
/// 没有代际时，重复调用 `connect()` 会让**旧** socket 的 `onclose`/`onerror`
/// 继续跑：旧 error 关掉的是新 socket（互踢），旧 close 又排一条新的重连，
/// 于是重连链越滚越多；`stop()` 之后旧代际的定时器还会把连接拉起来。
/// 回调一律先比对代际，过期的一律当作没发生。
let generation = 0

function apply(message: unknown) {
  const value = message as Record<string, unknown>
  const list = Array.isArray(value.servers)
    ? value.servers
    : Array.isArray(value.Servers)
      ? value.Servers
      : Array.isArray(message)
        ? message
        : null
  if (list) {
    const prevById = new Map(servers.value.map((row) => [row.id, row]))
    servers.value = list.map((item) => {
      const row = normalizeServer(item as Record<string, unknown>)
      return mergeServerSnapshot(prevById.get(row.id), row)
    })
    return
  }
  const row = normalizeServer(value)
  const index = servers.value.findIndex((item) => item.id === row.id)
  if (index >= 0) {
    servers.value[index] = mergeServerSnapshot(servers.value[index], row)
  } else if (row.id) {
    servers.value.push(row)
  }
}

async function load() {
  loading.value = true
  try {
    bootstrap.value = await getBootstrap()
    const result = await listPublicServers()
    servers.value = result.data.map((v) => normalizeServer(v as unknown as Record<string, unknown>))
    loadError.value = false
  } catch {
    loadError.value = true
  } finally {
    loading.value = false
  }
}

function connect() {
  stopped = false
  generation += 1
  const current = generation
  // 只关自己这条：旧 socket 的 onerror 里不能去 close 新 socket。
  socket?.close()
  const next = new WebSocket(websocketURL('/ws/v2/public/runtime'))
  socket = next
  next.onopen = () => {
    if (current !== generation) return
    connected.value = true
    retry = 0
  }
  next.onmessage = (e) => {
    if (current !== generation) return
    try {
      apply(JSON.parse(String(e.data)))
    } catch { /* ignore malformed frames */ }
  }
  next.onclose = () => {
    if (current !== generation) return
    connected.value = false
    if (stopped) return
    window.clearTimeout(timer)
    timer = window.setTimeout(() => {
      if (current !== generation || stopped) return
      connect()
    }, Math.min(30000, 1000 * 2 ** retry++))
  }
  next.onerror = () => {
    if (current !== generation) return
    next.close()
  }
}

function stop() {
  stopped = true
  generation += 1
  window.clearTimeout(timer)
  socket?.close()
  socket = undefined
  // 代际已作废，onclose 不会再跑；这里必须自己把连接态清掉，
  // 否则界面会一直显示「已连接」。
  connected.value = false
}

const groups = computed(() => {
  const map = new Map<string, ServerRecord[]>()
  for (const server of servers.value) {
    const name = server.tag || 'default'
    map.set(name, [...(map.get(name) || []), server])
  }
  return [...map].map(([name, items]) => ({ name, items }))
})

const statusStore: StatusStoreState = reactive({
  bootstrap,
  servers,
  groups,
  loading,
  connected,
  loadError,
  load,
  connect,
  stop,
})

export function useStatusStore() {
  return statusStore
}
