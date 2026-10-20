<script setup lang="ts">
import { computed, nextTick, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import type { ServerRecord } from '@santaizi/api'
import { isHostOnline } from '@santaizi/api'
import { AppDialog } from '@santaizi/ui'

const props = defineProps<{ servers: ServerRecord[]; modelValue: string }>()
const emit = defineEmits<{ 'update:modelValue': [value: string] }>()
const { t } = useI18n()
const router = useRouter()
const open = ref(false)
const inputRef = ref<{ focus: () => void }>()

const results = computed(() => {
  const q = props.modelValue.trim().toLowerCase()
  if (!q) return props.servers.slice(0, 12)
  return props.servers.filter((server) => {
    const haystack = [
      server.name,
      server.tag,
      server.host?.Platform,
      server.host?.CountryCode,
    ].join(' ').toLowerCase()
    return haystack.includes(q)
  }).slice(0, 12)
})

async function activate() {
  open.value = true
  await nextTick()
  inputRef.value?.focus()
}

function openDetail(id: number) {
  open.value = false
  router.push({ name: 'public-detail', params: { serverId: String(id) } })
}
</script>

<template>
  <button type="button" class="nazhua-search-btn" :aria-label="t('nazhua.search')" @click="activate">
    <i class="ri-search-eye-line"></i>
  </button>
  <AppDialog v-model="open" mode="view" width="560px" :title="t('nazhua.search')">
    <div class="nazhua-search-box">
      <el-input
        ref="inputRef"
        :model-value="modelValue"
        clearable
        :placeholder="t('nazhua.searchPlaceholder')"
        @update:model-value="emit('update:modelValue', String($event))"
        @keyup.enter="results[0] && openDetail(results[0].id)"
      >
        <template #prefix><i class="ri-search-line"></i></template>
      </el-input>
    </div>
    <ul class="nazhua-search-results" :aria-label="t('nazhua.search')">
      <li v-for="server in results" :key="server.id">
        <el-button text @click="openDetail(server.id)">
          <i :class="isHostOnline(server) ? 'ri-checkbox-circle-fill online' : 'ri-indeterminate-circle-fill offline'"></i>
          <span>{{ server.name }}</span>
          <small>{{ server.tag || 'default' }}</small>
        </el-button>
      </li>
    </ul>
  </AppDialog>
</template>
