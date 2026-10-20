<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { AppDrawer, AppEmpty } from '@santaizi/ui'
import type { ProbePath } from '@santaizi/api'
import { listCollectors, listProbePaths, type CollectorRecord } from '@/api/adminApi'
import CopyableId from '@/components/CopyableId.vue'
import ProbePathDialog from '@/components/ProbePathDialog.vue'
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
import {
  probeHasNoTarget,
  probeICMPMetric,
  probeMTRMetric,
  probePathKey,
  probeTCPMetric,
  probeTargetText,
} from '@/domain/probePath'

const { t, te, locale } = useI18n()
const router = useRouter()
const loading = ref(false)
const collectors = ref<CollectorRecord[]>([])
const probePaths = ref<ProbePath[]>([])
const collectorFilter = ref('')
const statusFilter = ref('')
const nodeQuery = ref('')
const tagFilter = ref('')
const nodeSort = ref<NodeSortKey>(DEFAULT_NODE_SORT)
const collectorDrawer = ref(false)
const probeDialog = ref(false)
const activeCollector = ref<CollectorRecord>()
const activeProbe = ref<ProbePath>()
const POLL_MS = 5000
let timer: ReturnType<typeof setInterval> | undefined
let inflight = false

const probeCollectors = computed(() => collectors.value.filter(row => !row.revoked && isProbeCollector(row)))

const collectorOptions = computed(() => probeCollectors.value.map(row => ({ id: row.id, name: row.name })))

function probeStatus(path: ProbePath) {
  if (probeHasNoTarget(path)) return 'none'
  if (!collectorOnline(path.collector_id)) return 'offline'
  if (path.reachable && path.sampled_at) return 'reachable'
  return 'timeout'
}

const filteredProbePaths = computed(() => probePaths.value.filter((path) => {
  if (collectorFilter.value && path.collector_id !== collectorFilter.value) return false
  if (!matchNodeCatalog({
    server_id: path.server_id, server_name: path.server_name, display_index: path.display_index, tag: path.tag,
  }, nodeQuery.value, tagFilter.value)) return false
  if (!statusFilter.value) return true
  return probeStatus(path) === statusFilter.value
}))

const groupOptions = computed(() => uniqueNodeTags(probePaths.value))

const probeGroups = computed(() => {
  const rows = collectorFilter.value
    ? probeCollectors.value.filter(row => row.id === collectorFilter.value)
    : probeCollectors.value
  const groups = rows.map(collector => ({
    collector,
    paths: filteredProbePaths.value
      .filter(path => path.collector_id === collector.id)
      .sort((left, right) => compareNodeCatalog(left, right, nodeSort.value)),
  }))
  if (statusFilter.value || nodeQuery.value || tagFilter.value) return groups.filter(group => group.paths.length)
  return groups
})

function pretty(value: unknown, key = '') {
  return formatAdminValue(value, key, locale.value, t as never, te)
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
  const row = probeCollectors.value.find(item => item.id === id)
  return Boolean(row && collectorStatus(row) === 'online')
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

function icmpMetric(path: ProbePath) {
  return probeICMPMetric(path, locale.value, t('probeTimeout'))
}

function tcpMetric(path: ProbePath) {
  return probeTCPMetric(path, locale.value, t('probeTimeout'))
}

function mtrMetric(path: ProbePath) {
  return probeMTRMetric(path, locale.value)
}

function tcpLabel(path: ProbePath) {
  const metric = tcpMetric(path)
  return metric.port != null ? `${t('tcp')} :${metric.port}` : t('tcp')
}

function openCollector(row: CollectorRecord) {
  activeCollector.value = row
  collectorDrawer.value = true
}

function openProbe(row: ProbePath) {
  activeProbe.value = row
  probeDialog.value = true
}

async function load(quiet = false) {
  if (inflight) return
  inflight = true
  if (!quiet) loading.value = true
  try {
    const [collectorList, probeList] = await Promise.all([
      listCollectors(),
      listProbePaths(),
    ])
    collectors.value = collectorList.data
    probePaths.value = probeList.data
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
  <div class="probes-page">
  <div class="page-head">
    <h1>{{ t('probeObservation') }}</h1>
    <div class="page-actions">
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
      <el-select v-model="collectorFilter" class="toolbar-filter" clearable :placeholder="t('allProbes')">
        <el-option v-for="item in collectorOptions" :key="item.id" :label="item.name" :value="item.id" />
      </el-select>
      <el-select v-model="statusFilter" class="toolbar-filter" clearable :placeholder="t('allProbeStatus')">
        <el-option :label="t('probeReachable')" value="reachable" />
        <el-option :label="t('probeTimeout')" value="timeout" />
        <el-option :label="t('probeNoTarget')" value="none" />
      </el-select>
      <el-button @click="load()"><i class="ri-refresh-line"></i>{{ t('refresh') }}</el-button>
    </div>
  </div>

  <div class="probe-groups-wrap" v-loading="loading">
    <AppEmpty v-if="!probeCollectors.length && !loading" class="empty-state" icon="ri-radar-line" :title="t('noProbesTitle')" :description="t('noProbesHint')" />
    <div v-if="!probeCollectors.length && !loading" class="pagination">
      <el-button type="primary" @click="router.push('/telemetry?create=1')"><i class="ri-add-line"></i>{{ t('createCollector') }}</el-button>
    </div>
    <AppEmpty v-else-if="!probeGroups.length && !loading" class="empty-state" icon="ri-radar-line" :title="t('noProbePathsTitle')" :description="t('noProbePathsHint')" />
    <section v-for="group in probeGroups" :key="group.collector.id" class="surface probe-group">
      <article
        class="collector-tile"
        role="button"
        tabindex="0"
        @click="openCollector(group.collector)"
        @keydown.enter.prevent="openCollector(group.collector)"
        @keydown.space.prevent="openCollector(group.collector)"
      >
        <span class="status-dot" :class="collectorStatus(group.collector)"></span>
        <div class="collector-tile__id">
          <strong>{{ group.collector.name }}</strong>
          <CopyableId :value="group.collector.id" />
        </div>
        <span class="rtt-chip" :class="collectorRttTone(group.collector)" :title="collectorRttTitle(group.collector)">
          <span class="rtt-value">{{ collectorRttText(group.collector) }}</span>
          <span v-if="collectorRttSampled(group.collector)" class="rtt-sampled">{{ collectorRttSampled(group.collector) }}</span>
        </span>
        <div class="collector-tile__metrics">
          <span>{{ collectorVersionText(group.collector) }}</span>
        </div>
      </article>
      <div class="probe-card-grid">
        <AppEmpty v-if="!group.paths.length" class="empty-state" icon="ri-radar-line" :title="t('noProbePathsTitle')" :description="t('noProbePathsHint')" />
        <button
          v-for="path in group.paths"
          :key="probePathKey(path)"
          type="button"
          class="probe-card"
          @click="openProbe(path)"
        >
          <strong class="probe-card__name">{{ path.server_name || '—' }}</strong>
          <span class="probe-card__target">{{ probeTargetText(path) }}</span>
          <div class="probe-card__metrics">
            <span class="probe-metric" :class="icmpMetric(path).tone">
              <span class="probe-metric__label">{{ t('icmp') }}</span>
              <span class="probe-metric__value">{{ icmpMetric(path).text }}</span>
            </span>
            <span class="probe-metric" :class="tcpMetric(path).tone">
              <span class="probe-metric__label">{{ tcpLabel(path) }}</span>
              <span class="probe-metric__value">{{ tcpMetric(path).text }}</span>
            </span>
            <span class="probe-metric" :class="mtrMetric(path).tone">
              <span class="probe-metric__label">{{ t('probeTrace') }}</span>
              <span class="probe-metric__value">{{ mtrMetric(path).text }}</span>
            </span>
          </div>
        </button>
      </div>
    </section>
  </div>

  <AppDrawer v-model="collectorDrawer" :title="activeCollector?.name || t('collector')" mode="view">
    <div class="page-stack">
    <dl v-if="activeCollector" class="mobile-card-meta">
      <div><dt>{{ t('id') }}</dt><dd><CopyableId :value="activeCollector.id" :compact="false" /></dd></div>
      <div><dt>{{ t('collectorKind') }}</dt><dd>{{ t('collectorKindProbe') }}</dd></div>
      <div><dt>{{ t('status') }}</dt><dd>{{ t(collectorStatus(activeCollector)) }}</dd></div>
      <div><dt>{{ t('lastSeen') }}</dt><dd>{{ pretty(activeCollector.last_seen, 'last_seen') }}</dd></div>
      <div><dt>{{ t('lastPrimarySeen') }}</dt><dd>{{ pretty(activeCollector.last_primary_seen, 'last_primary_seen') }}</dd></div>
      <div><dt>{{ t('heartbeatLatency') }}</dt><dd>{{ collectorStatus(activeCollector) === 'online' ? latencyText(activeCollector.heartbeat_rtt_ms, activeCollector.heartbeat_rtt_sampled_at) : t('offline') }}</dd></div>
      <div><dt>{{ t('protocolVersion') }}</dt><dd>{{ pretty(activeCollector.protocol_version, 'protocol_version') }}</dd></div>
      <div><dt>{{ t('collectorVersion') }}</dt><dd>{{ collectorVersionText(activeCollector) }}</dd></div>
    </dl>
    </div>
  </AppDrawer>

  <ProbePathDialog v-model="probeDialog" :path="activeProbe" />
  </div>
</template>
