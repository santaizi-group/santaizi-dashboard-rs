<script setup lang="ts">
import { AppDrawer } from '@santaizi/ui'
import type { ServerStatusView } from '../domain/serverStatusView'
import ServerDetail from './ServerDetail.vue'

defineProps<{
  modelValue: boolean
  server: ServerStatusView | null
  showAvailability: boolean
}>()

const emit = defineEmits<{
  'update:modelValue': [boolean]
}>()
</script>

<template>
  <AppDrawer
    :model-value="modelValue"
    :title="server?.name || ''"
    mode="view"
    size="min(980px, 100vw)"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <ServerDetail v-if="server" :key="server.id" :server="server" :show-availability="showAvailability" />
  </AppDrawer>
</template>
