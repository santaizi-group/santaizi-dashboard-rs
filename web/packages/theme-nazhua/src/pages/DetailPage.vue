<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import { useInjectedStatusStore } from '@santaizi/status-core'
import { isHostOnline, type CycleTransfer } from '@santaizi/api'
import { AppEmpty, AppSkeleton } from '@santaizi/ui'
import { mapCycleTransfers } from '../domain/nazhuaServerView'
import { resolveServerLocation } from '../utils/worldMap'
import ServerNameBlock from '../components/detail/ServerNameBlock.vue'
import ServerStatusBox from '../components/detail/ServerStatusBox.vue'
import ServerResourceHistory from '../components/detail/ServerResourceHistory.vue'
import ServerInfoBox from '../components/detail/ServerInfoBox.vue'
import ServerCycleTransfer from '../components/detail/ServerCycleTransfer.vue'
import ServerMonitor from '../components/detail/ServerMonitor.vue'

const props = defineProps<{ serverId: string }>()
const { t } = useI18n()
const route = useRoute()
const router = useRouter()
const store = useInjectedStatusStore()
const cycleRows = ref<CycleTransfer[]>([])

const id = computed(() => Number(props.serverId || route.params.serverId))
const server = computed(() => store.servers.find((row) => row.id === id.value))
const location = computed(() => (server.value ? resolveServerLocation(server.value) : null))
const cycleMap = computed(() => mapCycleTransfers(cycleRows.value))

watch(id, () => {
  cycleRows.value = []
})

watch(
  [server, () => store.bootstrapped, () => store.loading, () => store.loadError],
  ([value, bootstrapped, loading, loadError]) => {
    if (bootstrapped && !loading && !loadError && !value) {
      router.replace({ name: 'home' })
    }
  },
  { immediate: true },
)
</script>

<template>
  <div class="nazhua-detail" :class="{ offline: server && !isHostOnline(server) }">
    <div class="nazhua-detail__nav">
      <RouterLink to="/" class="nazhua-detail__back">
        <i class="ri-arrow-left-line"></i><span>{{ t('back') }}</span>
      </RouterLink>
    </div>
    <template v-if="server">
      <ServerNameBlock :server="server" :location="location" />
      <ServerStatusBox :server="server" :cycles="cycleMap" />
      <ServerResourceHistory :server="server" />
      <ServerCycleTransfer :server="server" @loaded="cycleRows = $event" />
      <ServerInfoBox :server="server" />
      <ServerMonitor :server-id="server.id" />
    </template>
    <AppSkeleton v-else-if="store.loading || !store.bootstrapped" variant="page" />
    <div v-else-if="store.loadError" class="nazhua-empty">
      <AppEmpty
        tone="danger"
        icon="ri-error-warning-line"
        :title="t('nazhua.loadFailed')"
        :description="t('nazhua.requestFailed')"
      />
      <el-button type="primary" @click="store.load()"><i class="ri-refresh-line"></i>{{ t('nazhua.refresh') }}</el-button>
    </div>
  </div>
</template>
