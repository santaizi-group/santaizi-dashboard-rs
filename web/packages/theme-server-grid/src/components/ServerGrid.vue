<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { ServerRecord } from '@santaizi/api'
import {
  toServerStatusViews,
  type CycleTransferMap,
  type StatusTableColumns,
} from '@santaizi/theme-server-status'
import ServerTile from './ServerTile.vue'

const props = defineProps<{
  title?: string
  servers: readonly ServerRecord[]
  cycles?: CycleTransferMap
  columns: StatusTableColumns
}>()

const emit = defineEmits<{ select: [id: number] }>()
const { locale } = useI18n()
const views = computed(() => toServerStatusViews([...props.servers], props.cycles, Date.now(), locale.value))
</script>

<template>
  <section class="sg-group">
    <header v-if="title" class="sg-group__title">
      <span>{{ title }}</span>
      <small>{{ servers.length }}</small>
    </header>
    <div class="sg-grid">
      <ServerTile
        v-for="row in views"
        :key="row.id"
        :server="row"
        :columns="props.columns"
        @select="emit('select', row.id)"
      />
    </div>
  </section>
</template>
