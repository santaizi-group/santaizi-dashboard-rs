<script setup lang="ts">
import { computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useInjectedStatusStore } from '@santaizi/status-core'
import { isHostOnline } from '@santaizi/api'
import { resolveServerLocation } from '../utils/worldMap'
import ServerNameBlock from '../components/detail/ServerNameBlock.vue'
import ServerStatusBox from '../components/detail/ServerStatusBox.vue'
import ServerResourceHistory from '../components/detail/ServerResourceHistory.vue'
import ServerInfoBox from '../components/detail/ServerInfoBox.vue'
import ServerCycleTransfer from '../components/detail/ServerCycleTransfer.vue'
import ServerMonitor from '../components/detail/ServerMonitor.vue'

const props = defineProps<{ serverId: string }>()
const route = useRoute()
const router = useRouter()
const store = useInjectedStatusStore()

const id = computed(() => Number(props.serverId || route.params.serverId))
const server = computed(() => store.servers.find((row) => row.id === id.value))
const location = computed(() => (server.value ? resolveServerLocation(server.value) : null))

watch(server, (value) => {
  if (store.servers.length && !value) router.replace({ name: 'home' })
}, { immediate: true })
</script>

<template>
  <div v-if="server" class="nazhua-detail" :class="{ offline: !isHostOnline(server) }">
    <ServerNameBlock :server="server" :location="location" />
    <ServerStatusBox :server="server" />
    <ServerResourceHistory :server="server" />
    <ServerCycleTransfer :server="server" />
    <ServerInfoBox :server="server" />
    <ServerMonitor :server-id="server.id" />
  </div>
</template>
