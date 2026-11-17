<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { listPublicCycleTransfer, type CycleTransfer } from '@santaizi/api'
import { AppEmpty } from '@santaizi/ui'
import {
  SERVER_SORT_COLUMNS,
  sortServers,
  useInjectedStatusStore,
  type ServerSortProp,
} from '@santaizi/status-core'
import { mapCycleTransfers, toServerStatusView } from '../domain/serverStatusView'
import { resolveStatusNoteColumns, type StatusTableColumns } from '../domain/statusTableColumns'
import { useStatusSort } from '../composables/useStatusSort'
import { useStatusDrawerRoute, useWorldMapOverlay } from '../composables/useStatusOverlays'
import ServerDetailDrawer from '../components/ServerDetailDrawer.vue'
import StatusGlobe from '../components/StatusGlobe.vue'
import StatusTable from '../components/StatusTable.vue'

const { t, locale } = useI18n()
const store = useInjectedStatusStore()
const grouped = ref(localStorage.getItem('santaizi-status-grouped') !== '0')
const mapDialog = ref<HTMLDialogElement>()
const cycleRows = ref<CycleTransfer[]>([])
const { sortProp, sortOrder, sortOption, setSortProp, toggleSort, toggleOrder } = useStatusSort()

const cycles = computed(() => mapCycleTransfers(cycleRows.value))
const sortedAll = computed(() => sortServers(store.servers, sortProp.value, sortOrder.value))
const sortedGroups = computed(() => store.groups.map((group) => ({
  ...group,
  items: sortServers(group.items, sortProp.value, sortOrder.value),
})))
const sortLabel = computed(() => t(sortOption.value?.labelKey || 'nazhua.sortWeight'))
const showAvailability = computed(() => store.bootstrap?.show_availability !== false)
const tableColumns = computed((): StatusTableColumns => ({
  availability: showAvailability.value,
  ...resolveStatusNoteColumns(store.servers, Date.now(), locale.value),
}))
const connectionClass = computed(() => {
  if (store.loadError) return 'failed'
  if (store.connected) return 'connected'
  return ''
})
const connectionLabel = computed(() => {
  if (store.loadError) return t('loadFailed')
  if (store.connected) return t('liveConnected')
  return t('reconnecting')
})
const emptyDescription = computed(() => {
  if (store.loadError) return t('requestFailed')
  if (store.loading) return t('loading')
  return t('noData')
})
const {
  selectedItem: selectedServer,
  drawerOpen,
  openDrawer,
  closeDrawer,
} = useStatusDrawerRoute({
  resolveItem: (id) => {
    const record = store.servers.find((row) => row.id === id)
    if (!record) return null
    return toServerStatusView(record, cycles.value, Date.now(), locale.value)
  },
  isLoaded: () => store.bootstrapped && !store.loading && !store.loadError,
})

const {
  mapOpen,
  showMap,
  closeMap,
  onMapClosed,
  onScrimClick,
} = useWorldMapOverlay(mapDialog, () => drawerOpen.value)

function toggle() {
  grouped.value = !grouped.value
  closeDrawer()
  localStorage.setItem('santaizi-status-grouped', grouped.value ? '1' : '0')
}

function selectRow(id: number) {
  void openDrawer(id)
}

async function loadCycles() {
  try {
    const result = await listPublicCycleTransfer()
    cycleRows.value = result.data || []
  } catch {
    cycleRows.value = []
  }
}

async function refreshHome() {
  await store.load()
  if (!store.loadError) await loadCycles()
}

function onGlobeSelect(id: number) {
  void openDrawer(id)
}

onMounted(loadCycles)
</script>

<template>
  <div class="status-container">
    <div class="status-toolbar">
      <span :class="['connection-state', connectionClass]" :aria-label="connectionLabel">
        <i></i><span class="connection-state__label">{{ connectionLabel }}</span>
      </span>
      <span />
      <div class="ss-sort-group">
        <el-dropdown trigger="click" popper-class="ss-sort-menu" @command="setSortProp($event as ServerSortProp)">
          <button type="button" class="ss-sort" :aria-label="`${t('nazhua.sort')}: ${sortLabel}`">
            <span>{{ sortLabel }}</span>
          </button>
          <template #dropdown>
            <el-dropdown-menu>
              <div v-for="column in SERVER_SORT_COLUMNS" :key="column.group" class="ss-sort-menu__column" role="none">
                <el-dropdown-item
                  v-for="option in column.options"
                  :key="option.prop"
                  :command="option.prop"
                  :class="{ 'is-current': option.prop === sortProp }"
                >{{ t(option.labelKey) }}</el-dropdown-item>
              </div>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <button
          type="button"
          class="ss-sort__order"
          :aria-label="`${t('nazhua.sort')} (${sortOrder === 'asc' ? '↑' : '↓'})`"
          @click="toggleOrder"
        ><i :class="sortOrder === 'asc' ? 'ri-arrow-up-line' : 'ri-arrow-down-line'"></i></button>
      </div>
      <button type="button" class="status-toolbar__page-action" :aria-label="t('refresh')" @click="refreshHome">
        <i class="ri-refresh-line"></i><span>{{ t('refresh') }}</span>
      </button>
      <button type="button" class="status-toolbar__page-action" :aria-label="t('worldMap')" @click="showMap">
        <i class="ri-earth-line"></i><span>{{ t('worldMap') }}</span>
      </button>
      <button
        type="button"
        class="status-toolbar__page-action"
        :aria-label="t(grouped ? 'flatView' : 'groupView')"
        @click="toggle"
      >
        <i :class="grouped ? 'ri-list-check-2' : 'ri-folder-chart-line'"></i>
        <span>{{ t(grouped ? 'flatView' : 'groupView') }}</span>
      </button>
    </div>

    <template v-if="store.servers.length">
      <template v-if="grouped">
        <StatusTable
          v-for="group in sortedGroups"
          :key="group.name"
          :title="group.name"
          :servers="group.items"
          :cycles="cycles"
          :columns="tableColumns"
          :sort-prop="sortProp"
          :sort-order="sortOrder"
          @select="selectRow"
          @sort="toggleSort"
        />
      </template>
      <StatusTable
        v-else
        :servers="sortedAll"
        :cycles="cycles"
        :columns="tableColumns"
        :sort-prop="sortProp"
        :sort-order="sortOrder"
        @select="selectRow"
        @sort="toggleSort"
      />
    </template>
    <div v-else class="empty-status status-page-empty">
      <AppEmpty
        :tone="store.loadError ? 'danger' : 'default'"
        :icon="store.loadError ? 'ri-error-warning-line' : 'ri-server-line'"
        :title="store.loadError ? t('loadFailed') : ''"
        :description="emptyDescription"
      />
      <el-button v-if="store.loadError" type="primary" @click="refreshHome">
        <i class="ri-refresh-line"></i>{{ t('refresh') }}
      </el-button>
    </div>

    <ServerDetailDrawer
      v-model="drawerOpen"
      :server="selectedServer"
      :show-availability="showAvailability"
    />

    <div
      v-if="mapOpen"
      class="map-scrim"
      @click="onScrimClick"
    ></div>
    <dialog ref="mapDialog" class="map-dialog" @close="onMapClosed">
      <header>
        <h2 class="icon-text"><i class="ri-earth-line"></i>{{ t('worldMap') }}</h2>
        <button type="button" :aria-label="t('close')" @click="closeMap"><i class="ri-close-line"></i></button>
      </header>
      <div class="world-map">
        <StatusGlobe
          v-if="mapOpen"
          :servers="store.servers"
          :locale="locale"
          :occluded="drawerOpen"
          @select="onGlobeSelect"
        />
      </div>
    </dialog>
  </div>
</template>
