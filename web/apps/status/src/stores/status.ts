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
  socket?.close()
  socket = new WebSocket(websocketURL('/ws/v2/public/runtime'))
  socket.onopen = () => {
    connected.value = true
    retry = 0
  }
  socket.onmessage = (e) => {
    try {
      apply(JSON.parse(String(e.data)))
    } catch { /* ignore malformed frames */ }
  }
  socket.onclose = () => {
    connected.value = false
    if (stopped) return
    window.clearTimeout(timer)
    timer = window.setTimeout(connect, Math.min(30000, 1000 * 2 ** retry++))
  }
  socket.onerror = () => socket?.close()
}

function stop() {
  stopped = true
  window.clearTimeout(timer)
  socket?.close()
  socket = undefined
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
