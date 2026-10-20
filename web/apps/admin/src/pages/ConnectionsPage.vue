<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import { AppDrawer, AppEmpty } from '@santaizi/ui'
import type { ConnectionLatencyBucket, ConnectionPath } from '@santaizi/api'
import { listCollectors, listConnectionLatency, listConnectionPaths, type CollectorRecord } from '@/api/adminApi'
import CopyableId from '@/components/CopyableId.vue'
import { formatAdminValue, formatClockTime, formatDateTime, formatProductVersion } from '@/composables/format'
import { notifyAPIError } from '@/composables/notify'
import { isProbeCollector } from '@/domain/collectorKind'
import {
  compareNodeCatalog,
  DEFAULT_NODE_SORT,
  groupFilterLabel,
  matchNodeCatalog,
  type NodeSortKey,
  uniqueNodeTags,
} from '@/domain/nodeCatalog'

const { t, te, locale } = useI18n()
const router = useRouter()
const route = useRoute()
const loading = ref(false)
const latencyLoading = ref(false)
const collectors = ref<CollectorRecord[]>([])
const paths = ref<ConnectionPath[]>([])
const collectorLatency = ref<ConnectionLatencyBucket[]>([])
const pathLatency = ref<ConnectionLatencyBucket[]>([])
const collectorLatencyMeta = reactive({ page: 1, page_size: 20, total: 0 })
const pathLatencyMeta = reactive({ page: 1, page_size: 20, total: 0 })
const observerFilter = ref('')
const linkFilter = ref('')
const nodeQuery = ref('')
const tagFilter = ref('')
const nodeSort = ref<NodeSortKey>(DEFAULT_NODE_SORT)
const collectorDrawer = ref(false)
const pathDrawer = ref(false)
const activeCollector = ref<CollectorRecord>()
const activePath = ref<ConnectionPath>()
const POLL_MS = 5000
let timer: ReturnType<typeof setInterval> | undefined
let inflight = false

type MatrixObserver = { id: string; name: string; kind: 'primary' | 'collector' }
type PathMatrixRow = {
  key: string
  server_id?: number
  node_uuid?: string
  server_name: string
  display_index: number
  tag: string
  cells: Record<string, ConnectionPath>
}
type MatrixCell = {
  path: ConnectionPath
  observerName: string
  connected: boolean
  hasError: boolean
  title?: string
  text: string
  sampled: string
}

function rowIdentity(serverId?: number, nodeUuid?: string) {
  if (serverId) return `s:${serverId}`
  return `n:${nodeUuid || ''}`
}

const observerCollectors = computed(() => collectors.value.filter(row => !row.revoked && !isProbeCollector(row)))

function isProbeObserverID(id: string) {
  return collectors.value.some(row => row.id === id && isProbeCollector(row))
}

const observerOptions = computed(() => {
  const items: MatrixObserver[] = [{ id: 'primary', name: t('observerKindPrimary'), kind: 'primary' }]
  const seen = new Set(['primary'])
  for (const row of observerCollectors.value) {
    seen.add(row.id)
    items.push({ id: row.id, name: row.name, kind: 'collector' })
  }
  for (const path of paths.value) {
    if (seen.has(path.observer_id) || isProbeObserverID(path.observer_id)) continue
    seen.add(path.observer_id)
    items.push({
      id: path.observer_id,
      name: path.observer_kind === 'primary' ? t('observerKindPrimary') : (path.observer_name || path.observer_id),
      kind: path.observer_kind === 'primary' ? 'primary' : 'collector',
    })
  }
  return items
})

const filteredPaths = computed(() => paths.value.filter((path) => {
  if (isProbeObserverID(path.observer_id)) return false
  if (observerFilter.value && path.observer_id !== observerFilter.value) return false
  if (linkFilter.value === 'connected' && !pathLive(path)) return false
  if (linkFilter.value === 'disconnected' && pathLive(path)) return false
  if (!matchNodeCatalog({
    server_id: path.server_id, server_name: path.server_name, display_index: path.display_index, tag: path.tag,
  }, nodeQuery.value, tagFilter.value)) return false
  return true
}))

const matrixObservers = computed(() => {
  if (!observerFilter.value) return observerOptions.value
  return observerOptions.value.filter((item) => item.id === observerFilter.value)
})

const groupOptions = computed(() => uniqueNodeTags(paths.value.filter(path => !isProbeObserverID(path.observer_id))))

const nodeCards = computed(() => {
  const matching = new Set<string>()
  for (const path of filteredPaths.value) matching.add(rowIdentity(path.server_id, path.node_uuid))
  const rows = new Map<string, PathMatrixRow>()
  const take = (path: ConnectionPath) => {
    const key = rowIdentity(path.server_id, path.node_uuid)
    let row = rows.get(key)
    if (!row) {
      row = {
        key, server_id: path.server_id, node_uuid: path.node_uuid, server_name: path.server_name || '',
        display_index: path.display_index || 0, tag: path.tag || '', cells: {},
      }
      rows.set(key, row)
    }
    if (!row.server_name && path.server_name) row.server_name = path.server_name
    if (!row.server_id && path.server_id) row.server_id = path.server_id
    if (!row.node_uuid && path.node_uuid) row.node_uuid = path.node_uuid
    if (!row.display_index && path.display_index) row.display_index = path.display_index
    if (!row.tag && path.tag) row.tag = path.tag
    return row
  }
  for (const path of paths.value) {
    if (isProbeObserverID(path.observer_id)) continue
    const key = rowIdentity(path.server_id, path.node_uuid)
    if (!matching.has(key)) continue
    take(path).cells[path.observer_id] = path
  }
  return [...rows.values()].sort((left, right) => compareNodeCatalog(left, right, nodeSort.value))
})

function pretty(value: unknown, key = '') {
  return formatAdminValue(value, key, locale.value, t as never, te)
}

function lastErrorText(value?: string | null) {
  return pretty(value, 'last_error')
}

function latencyText(ms?: number | null, sampled?: string | null) {
  if (!sampled) return '—'
  return pretty(ms, 'last_rtt_ms')
}

function sampledClock(sampled?: string | null) {
  return formatClockTime(sampled, locale.value)
}

function sampledTitle(sampled?: string | null) {
  if (!sampled) return undefined
  const text = formatDateTime(sampled, locale.value)
  return text === '—' ? undefined : text
}

function collectorVersionText(row: CollectorRecord) {
  return formatProductVersion(row.software_version) || '—'
}

function collectorStatus(row: CollectorRecord) {
  return row.revoked ? 'offline' : (row.status || 'unknown')
}

function collectorOnline(id: string) {
  const row = observerCollectors.value.find(item => item.id === id)
  return Boolean(row && collectorStatus(row) === 'online')
}

function pathLive(path: ConnectionPath) {
  if (!path.sink.connected) return false
  if (path.observer_kind === 'primary' || path.observer_id === 'primary') return true
  return collectorOnline(path.observer_id)
}

function collectorRttTone(row: CollectorRecord) {
  return collectorStatus(row) === 'online' ? 'is-connected' : 'is-disconnected'
}

function collectorRttText(row: CollectorRecord) {
  if (collectorStatus(row) !== 'online') return t('offline')
  return latencyText(row.heartbeat_rtt_ms, row.heartbeat_rtt_sampled_at)
}

function collectorRttSampled(row: CollectorRecord) {
  if (collectorStatus(row) !== 'online') return ''
  return sampledClock(row.heartbeat_rtt_sampled_at)
}

function collectorRttTitle(row: CollectorRecord) {
  if (collectorStatus(row) !== 'online') return undefined
  return sampledTitle(row.heartbeat_rtt_sampled_at)
}

function collectorReplicationText(row: CollectorRecord) {
  if (!row.pending_records) return t('caughtUp')
  const oldest = row.oldest_pending ? new Date(row.oldest_pending) : null
  if (!oldest || Number.isNaN(oldest.valueOf())) return t('replicating')
  const lagMs = Date.now() - oldest.getTime()
  if (lagMs < 0) return t('replicating')
  const hours = Math.floor(lagMs / 3_600_000)
  if (hours >= 1) return t('replicationLagHours', { n: hours })
  const minutes = Math.floor(lagMs / 60_000)
  if (minutes >= 1) return t('replicationLagMinutes', { n: minutes })
  return t('replicationLagSeconds', { n: Math.max(1, Math.floor(lagMs / 1000)) })
}

function pathRowKey(row: ConnectionPath) {
  return `${row.node_uuid}:${row.observer_id}`
}

function observerLabel(path: ConnectionPath) {
  if (path.observer_kind === 'primary') return t('observerKindPrimary')
  return path.observer_name || path.observer_id
}

function nodeEndCells(row: PathMatrixRow): MatrixCell[] {
  const cells: MatrixCell[] = []
  for (const observer of matrixObservers.value) {
    const path = row.cells[observer.id]
    if (!path) continue
    const live = pathLive(path)
    cells.push({
      path,
      observerName: observer.name,
      connected: live,
      hasError: Boolean(path.sink.last_error),
      title: path.sink.last_error ? lastErrorText(path.sink.last_error) : (live ? sampledTitle(path.sink.rtt_sampled_at) : undefined),
      text: live
        ? latencyText(path.sink.last_rtt_ms, path.sink.rtt_sampled_at)
        : t('disconnected'),
      sampled: live ? sampledClock(path.sink.rtt_sampled_at) : '',
    })
  }
  return cells
}

function cellTone(cell: MatrixCell) {
  return cell.connected ? 'is-connected' : 'is-disconnected'
}

async function loadCollectorLatency() {
  const collector = activeCollector.value
  if (!collector) return
  latencyLoading.value = true
  try {
    const result = await listConnectionLatency({
      collector_id: collector.id, page: collectorLatencyMeta.page, page_size: collectorLatencyMeta.page_size,
    })
    collectorLatency.value = result.data
    collectorLatencyMeta.total = result.meta.total || result.data.length
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    latencyLoading.value = false
  }
}

async function loadPathLatency() {
  const path = activePath.value
  if (!path) return
  latencyLoading.value = true
  try {
    const result = await listConnectionLatency({
      kind: 'path', server_id: path.server_id || undefined, observer_id: path.observer_id,
      page: pathLatencyMeta.page, page_size: pathLatencyMeta.page_size,
    })
    pathLatency.value = result.data
    pathLatencyMeta.total = result.meta.total || result.data.length
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    latencyLoading.value = false
  }
}

function openCollector(row: CollectorRecord) {
  activeCollector.value = row
  collectorLatencyMeta.page = 1
  collectorDrawer.value = true
  void loadCollectorLatency()
}

function openPath(row: ConnectionPath) {
  activePath.value = row
  pathLatencyMeta.page = 1
  pathDrawer.value = true
  void loadPathLatency()
}

async function load(quiet = false) {
  if (inflight) return
  inflight = true
  if (!quiet) loading.value = true
  try {
    const [collectorList, pathList] = await Promise.all([
      listCollectors(),
      listConnectionPaths(),
    ])
    collectors.value = collectorList.data
    paths.value = pathList.data
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    inflight = false
    loading.value = false
  }
}

function onVisibility() {
  if (!document.hidden) void load(true)
}

onMounted(async () => {
  const observerId = String(route.query.observer_id || '')
  if (observerId) observerFilter.value = observerId
  await load()
  timer = setInterval(() => { if (!document.hidden) void load(true) }, POLL_MS)
  document.addEventListener('visibilitychange', onVisibility)
})
onUnmounted(() => {
  if (timer) clearInterval(timer)
  document.removeEventListener('visibilitychange', onVisibility)
})
</script>

<template>
  <div class="connections-page">
  <div class="page-head">
    <h1>{{ t('connections') }}</h1>
    <el-button @click="load()"><i class="ri-refresh-line"></i>{{ t('refresh') }}</el-button>
  </div>

  <div class="page-stack">
    <section class="surface table-card connections-collectors">
      <div class="toolbar">
        <h2 class="table-card-title">{{ t('collectorLinks') }} <small>{{ observerCollectors.length }}</small></h2>
      </div>
      <div class="collector-grid" v-loading="loading">
        <AppEmpty v-if="!observerCollectors.length && !loading" class="empty-state" icon="ri-base-station-line" :title="t('noCollectorsTitle')" :description="t('noCollectorsHint')" />
        <article
          v-for="row in observerCollectors"
          :key="row.id"
          class="collector-tile"
          role="button"
          tabindex="0"
          @click="openCollector(row)"
          @keydown.enter.prevent="openCollector(row)"
          @keydown.space.prevent="openCollector(row)"
        >
          <span class="status-dot" :class="collectorStatus(row)"></span>
          <div class="collector-tile__id">
            <strong>{{ row.name }}</strong>
            <CopyableId :value="row.id" />
          </div>
          <span class="rtt-chip" :class="collectorRttTone(row)" :title="collectorRttTitle(row)">
            <span class="rtt-value">{{ collectorRttText(row) }}</span>
            <span v-if="collectorRttSampled(row)" class="rtt-sampled">{{ collectorRttSampled(row) }}</span>
          </span>
          <div class="collector-tile__metrics">
            <span>{{ pretty(row.connected_agents, 'connected_agents') }} {{ t('connectedAgents') }}</span>
            <span>{{ collectorReplicationText(row) }}</span>
          </div>
        </article>
      </div>
      <div v-if="!observerCollectors.length && !loading" class="pagination">
        <el-button type="primary" @click="router.push('/telemetry?create=1')"><i class="ri-add-line"></i>{{ t('createCollector') }}</el-button>
      </div>
    </section>

    <section class="surface table-card connections-nodes">
      <div class="toolbar">
        <h2 class="table-card-title">{{ t('nodeLinks') }} <small>{{ nodeCards.length }}</small></h2>
        <div class="toolbar-filters">
          <el-input v-model="nodeQuery" class="toolbar-filter" clearable :placeholder="t('searchServers')">
            <template #prefix><i class="ri-search-line"></i></template>
          </el-input>
          <el-select v-model="tagFilter" class="toolbar-filter" clearable :placeholder="t('allGroups')">
            <el-option v-for="item in groupOptions" :key="item" :label="groupFilterLabel(item)" :value="item" />
          </el-select>
          <el-select v-model="nodeSort" class="toolbar-filter">
            <el-option :label="t('sortDisplayIndexDesc')" value="display_index_desc" />
            <el-option :label="t('sortDisplayIndexAsc')" value="display_index_asc" />
            <el-option :label="t('sortNameAsc')" value="name_asc" />
            <el-option :label="t('sortNameDesc')" value="name_desc" />
          </el-select>
          <el-select v-model="observerFilter" class="toolbar-filter" clearable :placeholder="t('allObservers')">
            <el-option v-for="item in observerOptions" :key="item.id" :label="item.name" :value="item.id" />
          </el-select>
          <el-select v-model="linkFilter" class="toolbar-filter" clearable :placeholder="t('allLinkStatus')">
            <el-option :label="t('connected')" value="connected" />
            <el-option :label="t('disconnected')" value="disconnected" />
          </el-select>
        </div>
      </div>
      <div class="node-card-grid-wrap" v-loading="loading">
        <AppEmpty v-if="!nodeCards.length && !loading" class="empty-state" icon="ri-links-line" :title="t('noPathsTitle')" :description="t('noPathsHint')" />
        <div v-else class="node-card-grid">
          <article v-for="row in nodeCards" :key="row.key" class="node-card">
            <strong class="node-card__name" :title="row.server_name || undefined">{{ row.server_name || '—' }}</strong>
            <div class="node-card__ends">
              <button
                v-for="cell in nodeEndCells(row)"
                :key="pathRowKey(cell.path)"
                type="button"
                class="node-end-chip"
                :class="cellTone(cell)"
                :title="cell.title"
                @click="openPath(cell.path)"
              >
                <span class="node-end-chip__name">{{ cell.observerName }}</span>
                <span class="rtt-main">
                  <i v-if="cell.hasError" class="ri-error-warning-line" aria-hidden="true"></i>
                  <span class="rtt-value">{{ cell.text }}</span>
                </span>
                <span v-if="cell.sampled" class="rtt-sampled">{{ cell.sampled }}</span>
              </button>
            </div>
          </article>
        </div>
      </div>
    </section>
  </div>

  <AppDrawer v-model="collectorDrawer" :title="activeCollector?.name || t('collector')" mode="view">
    <div class="page-stack">
    <dl v-if="activeCollector" class="mobile-card-meta">
      <div><dt>{{ t('id') }}</dt><dd><CopyableId :value="activeCollector.id" :compact="false" /></dd></div>
      <div><dt>{{ t('collectorKind') }}</dt><dd>{{ t('collectorKindObserver') }}</dd></div>
      <div><dt>{{ t('status') }}</dt><dd>{{ t(collectorStatus(activeCollector)) }}</dd></div>
      <div><dt>{{ t('lastSeen') }}</dt><dd>{{ pretty(activeCollector.last_seen, 'last_seen') }}</dd></div>
      <div><dt>{{ t('lastSync') }}</dt><dd>{{ pretty(activeCollector.last_sync, 'last_sync') }}</dd></div>
      <div><dt>{{ t('lastPrimarySeen') }}</dt><dd>{{ pretty(activeCollector.last_primary_seen, 'last_primary_seen') }}</dd></div>
      <div><dt>{{ t('heartbeatLatency') }}</dt><dd>{{ collectorStatus(activeCollector) === 'online' ? latencyText(activeCollector.heartbeat_rtt_ms, activeCollector.heartbeat_rtt_sampled_at) : t('offline') }}</dd></div>
      <div><dt>{{ t('replicationLatency') }}</dt><dd>{{ collectorStatus(activeCollector) === 'online' ? latencyText(activeCollector.replication_rtt_ms, activeCollector.replication_rtt_sampled_at) : t('offline') }}</dd></div>
      <div><dt>{{ t('connectedAgents') }}</dt><dd>{{ pretty(activeCollector.connected_agents, 'connected_agents') }}</dd></div>
      <div><dt>{{ t('pendingRecords') }}</dt><dd>{{ pretty(activeCollector.pending_records, 'pending_records') }}</dd></div>
      <div><dt>{{ t('oldestPending') }}</dt><dd>{{ pretty(activeCollector.oldest_pending, 'oldest_pending') }}</dd></div>
      <div><dt>{{ t('replicationCursor') }}</dt><dd>{{ pretty(activeCollector.replication_cursor, 'replication_cursor') }}</dd></div>
      <div><dt>{{ t('protocolVersion') }}</dt><dd>{{ pretty(activeCollector.protocol_version, 'protocol_version') }}</dd></div>
      <div><dt>{{ t('collectorVersion') }}</dt><dd>{{ collectorVersionText(activeCollector) }}</dd></div>
    </dl>
    <el-table v-if="activeCollector" v-loading="latencyLoading" :data="collectorLatency" class="dataset-table">
      <el-table-column :label="t('bucketStart')" min-width="180"><template #default="{row}">{{ pretty(row.bucket_start, 'bucket_start') }}</template></el-table-column>
      <el-table-column :label="t('type')" width="90"><template #default="{row}">{{ pretty(row.kind, 'kind') }}</template></el-table-column>
      <el-table-column :label="t('minMs')" width="90"><template #default="{row}">{{ pretty(row.min_ms, 'min_ms') }}</template></el-table-column>
      <el-table-column :label="t('avgMs')" width="90"><template #default="{row}">{{ pretty(row.avg_ms, 'avg_ms') }}</template></el-table-column>
      <el-table-column :label="t('maxMs')" width="90"><template #default="{row}">{{ pretty(row.max_ms, 'max_ms') }}</template></el-table-column>
      <template #empty><AppEmpty icon="ri-timer-line" :description="t('noData')" /></template>
    </el-table>
    <div v-if="activeCollector && collectorLatencyMeta.total" class="pagination">
      <el-pagination v-model:current-page="collectorLatencyMeta.page" v-model:page-size="collectorLatencyMeta.page_size" layout="total, prev, pager, next" :total="collectorLatencyMeta.total" @change="loadCollectorLatency"/>
    </div>
    </div>
  </AppDrawer>

  <AppDrawer v-model="pathDrawer" :title="activePath ? (activePath.server_name || t('server')) : t('nodeLinks')" mode="view">
    <div class="page-stack">
    <dl v-if="activePath" class="mobile-card-meta">
      <div><dt>{{ t('observer') }}</dt><dd>{{ observerLabel(activePath) }}</dd></div>
      <div><dt>{{ t('linkStatus') }}</dt><dd>{{ t(pathLive(activePath) ? 'connected' : 'disconnected') }}</dd></div>
      <div><dt>{{ t('lastObservation') }}</dt><dd>{{ pretty(activePath.last_seen, 'last_seen') }}</dd></div>
      <div><dt>{{ t('latency') }}</dt><dd>{{ pathLive(activePath) ? latencyText(activePath.sink.last_rtt_ms, activePath.sink.rtt_sampled_at) : t('disconnected') }}</dd></div>
      <div><dt>{{ t('pendingEvents') }}</dt><dd>{{ pretty(activePath.sink.pending_events, 'pending_events') }}</dd></div>
      <div><dt>{{ t('ackThrough') }}</dt><dd>{{ pretty(activePath.sink.ack_through, 'ack_through') }}</dd></div>
      <div><dt>{{ t('lastError') }}</dt><dd><CopyableId :value="activePath.sink.last_error" :compact="false" /></dd></div>
      <div><dt>{{ t('nodeUUID') }}</dt><dd><CopyableId :value="activePath.node_uuid" :compact="false" /></dd></div>
    </dl>
    <el-table v-loading="latencyLoading" :data="pathLatency" class="dataset-table">
      <el-table-column :label="t('bucketStart')" min-width="180"><template #default="{row}">{{ pretty(row.bucket_start, 'bucket_start') }}</template></el-table-column>
      <el-table-column :label="t('minMs')" width="90"><template #default="{row}">{{ pretty(row.min_ms, 'min_ms') }}</template></el-table-column>
      <el-table-column :label="t('avgMs')" width="90"><template #default="{row}">{{ pretty(row.avg_ms, 'avg_ms') }}</template></el-table-column>
      <el-table-column :label="t('maxMs')" width="90"><template #default="{row}">{{ pretty(row.max_ms, 'max_ms') }}</template></el-table-column>
      <template #empty><AppEmpty icon="ri-timer-line" :description="t('noData')" /></template>
    </el-table>
    <div v-if="pathLatencyMeta.total" class="pagination">
      <el-pagination v-model:current-page="pathLatencyMeta.page" v-model:page-size="pathLatencyMeta.page_size" layout="total, prev, pager, next" :total="pathLatencyMeta.total" @change="loadPathLatency"/>
    </div>
    </div>
  </AppDrawer>
  </div>
</template>
