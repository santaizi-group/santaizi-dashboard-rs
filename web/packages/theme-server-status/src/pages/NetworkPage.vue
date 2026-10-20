<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { AppDrawer, AppEmpty } from '@santaizi/ui'
import { useInjectedStatusStore } from '@santaizi/status-core'
import LatencyChart from '../components/LatencyChart.vue'
import NetworkHostCard from '../components/NetworkHostCard.vue'
import { networkGridColumns, networkGridDensity, toNetworkHostTiles } from '../domain/networkSparkline'

const { t } = useI18n()
const store = useInjectedStatusStore()
const selected = ref(0)
const drawerOpen = ref(false)

const tiles = computed(() => toNetworkHostTiles(store.servers))
const selectedTile = computed(() => tiles.value.find((tile) => tile.id === selected.value) || null)
const density = computed(() => networkGridDensity(tiles.value.length))
const gridStyle = computed(() => {
  const columns = networkGridColumns(tiles.value.length)
  return columns > 0 ? { '--network-cols': String(columns) } : undefined
})

function openTile(id: number) {
  selected.value = id
  drawerOpen.value = true
}

onMounted(async () => {
  if (!store.servers.length && !store.loading) await store.load()
})
</script>

<template>
  <div class="status-container">
    <section class="status-panel network-panel">
      <header class="network-header">
        <div class="network-header-title">
          <h1>{{ t('statusNetwork') }}</h1>
        </div>
      </header>

      <div v-if="tiles.length" class="network-grid" :data-density="density" :style="gridStyle">
        <NetworkHostCard
          v-for="tile in tiles"
          :key="tile.id"
          :server="tile"
          :selected="tile.id === selected && drawerOpen"
          @select="openTile(tile.id)"
        />
      </div>
      <div v-else class="empty-status network-empty">
        <AppEmpty
          :tone="store.loadError ? 'danger' : 'default'"
          :icon="store.loadError ? 'ri-error-warning-line' : store.loading ? 'ri-loader-4-line' : 'ri-server-line'"
          :title="store.loadError ? t('loadFailed') : ''"
          :description="t(store.loadError ? 'requestFailed' : store.loading ? 'loading' : 'noData')"
        />
        <el-button v-if="store.loadError" type="primary" @click="store.load()">
          <i class="ri-refresh-line"></i>{{ t('refresh') }}
        </el-button>
      </div>
    </section>

    <AppDrawer
      v-model="drawerOpen"
      :title="selectedTile?.name || t('statusNetwork')"
      mode="view"
      size="min(980px, 100vw)"
    >
      <LatencyChart v-if="selected && drawerOpen" :key="selected" :server-id="selected" hide-heading />
    </AppDrawer>
  </div>
</template>
