<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, ElMessageBox } from 'element-plus'
import { AppDialog, AppEmpty } from '@santaizi/ui'
import { listServerGroups, renameServerGroup } from '@/api/adminApi'
import { notifyAPIError } from '@/composables/notify'
import type { ServerGroup } from '@santaizi/api'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; changed: [] }>()
const { t, te } = useI18n()
const loading = ref(false)
const busy = ref(false)
const groups = ref<ServerGroup[]>([])

const open = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
})

function displayName(name: string) {
  return name || 'default'
}

async function load() {
  loading.value = true
  try {
    const result = await listServerGroups()
    groups.value = result.data
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    loading.value = false
  }
}

async function applyRename(from: string, to: string) {
  if (from === to) return
  busy.value = true
  try {
    await renameServerGroup({ from, to })
    ElMessage.success(t('saveSuccess'))
    emit('changed')
    await load()
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    busy.value = false
  }
}

async function rename(group: ServerGroup) {
  try {
    const { value } = await ElMessageBox.prompt(t('group'), t('renameGroup'), {
      inputValue: displayName(group.name),
      confirmButtonText: t('save'),
      cancelButtonText: t('cancel'),
      inputValidator: value => {
        const next = String(value ?? '').trim()
        if (!next) return t('required')
        return true
      },
    })
    const next = value.trim() === 'default' ? '' : value.trim()
    await applyRename(group.name, next)
  } catch { /* cancelled */ }
}

async function merge(group: ServerGroup) {
  const targets = groups.value.filter(item => item.name !== group.name)
  const firstTarget = targets[0]
  if (!firstTarget) {
    ElMessage.warning(t('noMergeTarget'))
    return
  }
  try {
    const { value } = await ElMessageBox.prompt(t('mergeGroupHint'), t('mergeGroup'), {
      inputValue: displayName(firstTarget.name),
      confirmButtonText: t('save'),
      cancelButtonText: t('cancel'),
      inputPlaceholder: targets.map(item => displayName(item.name)).join(', '),
      inputValidator: value => {
        const raw = String(value ?? '').trim()
        if (!raw) return t('required')
        const target = raw === 'default' ? '' : raw
        if (!targets.some(item => item.name === target)) return t('mergeTargetMissing')
        return true
      },
    })
    const target = value.trim() === 'default' ? '' : value.trim()
    await applyRename(group.name, target)
  } catch { /* cancelled */ }
}

async function clear(group: ServerGroup) {
  if (!group.name) return
  try {
    await ElMessageBox.confirm(t('clearGroupConfirm'), t('clearGroup'), { type: 'warning' })
    await applyRename(group.name, '')
  } catch { /* cancelled */ }
}

watch(() => props.modelValue, value => { if (value) void load() })
</script>

<template>
  <AppDialog v-model="open" :title="t('manageGroups')" mode="edit" :dirty="false" :submitting="busy" width="min(720px, 96vw)">
    <el-table v-loading="loading" :data="groups" row-key="name">
      <el-table-column :label="t('group')" min-width="180">
        <template #default="{ row }"><el-tag effect="plain">{{ displayName(row.name) }}</el-tag></template>
      </el-table-column>
      <el-table-column prop="count" :label="t('serverCount')" width="120" />
      <el-table-column :label="t('actions')" width="220" fixed="right">
        <template #default="{ row }">
          <div class="inline-actions">
            <el-button circle plain :aria-label="t('renameGroup')" :disabled="busy" @click="rename(row)"><i class="ri-edit-line"></i></el-button>
            <el-button circle plain :aria-label="t('mergeGroup')" :disabled="busy" @click="merge(row)"><i class="ri-folder-transfer-line"></i></el-button>
            <el-button circle type="danger" plain :aria-label="t('clearGroup')" :disabled="busy || !row.name" @click="clear(row)"><i class="ri-folder-reduce-line"></i></el-button>
          </div>
        </template>
      </el-table-column>
      <template #empty><AppEmpty icon="ri-folder-line" :description="t('noServerGroups')" /></template>
    </el-table>
    <template #footer="{ close }"><el-button :disabled="busy" @click="close()">{{ t('close') }}</el-button></template>
  </AppDialog>
</template>
