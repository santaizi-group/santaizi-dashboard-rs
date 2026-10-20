<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, type FormInstance } from 'element-plus'
import { AppDialog } from '@santaizi/ui'
import { createMonitor, listAllServers, listNotificationGroups, updateMonitor, type ServerRecord } from '@/api/adminApi'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'
import type { MonitorRecord } from '@/types/admin'

const props = defineProps<{ modelValue: boolean; value?: MonitorRecord }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: [] }>()
const { t, te } = useI18n()
const formRef = ref<FormInstance>()
const saving = ref(false)
const servers = ref<ServerRecord[]>([])
const groups = ref<string[]>([])
const scopeMode = ref<'all' | 'include' | 'exclude'>('all')
const scopeServers = ref<number[]>([])
const form = reactive<MonitorRecord>({
  id: 0, name: '', type: 'http', target: '', interval_seconds: 30, scope: { mode: 'all', server_ids: [] },
  notify: false, notification_tag: 'default', show_in_service: true,
  latency_notify: false, min_latency_ms: 0, max_latency_ms: 0,
})
const snapshotValue = computed(() => ({ form, scopeMode: scopeMode.value, scopeServers: scopeServers.value }))
const { dirty, capture } = useEditorSnapshot(snapshotValue, computed(() => props.modelValue))
const transferData = computed(() => servers.value.map(server => ({ key: server.id, label: server.name })))
const targetPlaceholder = computed(() => form.type === 'http' ? 'https://example.com/health' : form.type === 'icmp' ? '1.1.1.1' : 'example.com:443')

function clampNumber(value: unknown, min: number, max?: number, fallback = min) {
  const next = Number(value)
  if (!Number.isFinite(next)) return fallback
  if (max === undefined) return Math.max(min, next)
  return Math.min(max, Math.max(min, next))
}

function reset(value?: MonitorRecord) {
  Object.assign(form, {
    id: value?.id || 0, name: value?.name || '', type: value?.type || 'http', target: value?.target || '',
    interval_seconds: value?.interval_seconds || 30, scope: value?.scope || { mode: 'all', server_ids: [] },
    notify: value?.notify ?? false, notification_tag: value?.notification_tag || 'default',
    show_in_service: value?.show_in_service ?? true, latency_notify: value?.latency_notify ?? false,
    min_latency_ms: value?.min_latency_ms || 0, max_latency_ms: value?.max_latency_ms || 0,
  })
  scopeServers.value = [...(value?.scope?.server_ids || [])]
  scopeMode.value = value?.scope?.mode || 'all'
  nextTick(capture)
}
async function loadOptions() {
  try {
    const [serverResult, notificationGroups] = await Promise.all([listAllServers(), listNotificationGroups()])
    servers.value = serverResult.data
    groups.value = notificationGroups.length ? notificationGroups : ['default']
  } catch (error) { notifyAPIError(error, t as never, te) }
}
async function submit() {
  await formRef.value?.validate()
  saving.value = true
  try {
    const { id: _id, ...write } = form
    const payload = {
      ...write,
      scope: { mode: scopeMode.value, server_ids: scopeMode.value === 'all' ? [] : [...scopeServers.value] },
    }
    if (form.id) await updateMonitor(form.id, payload)
    else await createMonitor(payload)
    capture()
    emit('update:modelValue', false)
    emit('saved')
    ElMessage.success(t('saveSuccess'))
  } catch (error) { notifyAPIError(error, t as never, te) }
  finally { saving.value = false }
}
watch(() => props.modelValue, value => { if (value) { reset(props.value); void loadOptions() } })
</script>

<template>
  <AppDialog :model-value="modelValue" :title="form.id ? t('editMonitor') : t('createMonitor')" mode="edit" :dirty="dirty" :submitting="saving" width="min(920px, 96vw)" @update:model-value="emit('update:modelValue', $event)">
    <el-form ref="formRef" :model="form" label-position="top" @submit.prevent="submit">
      <div class="editor-grid">
        <el-form-item :label="t('name')" prop="name" :rules="[{ required: true, message: t('required') }]"><el-input v-model="form.name" /></el-form-item>
        <el-form-item :label="t('monitorType')" prop="type"><el-select v-model="form.type" class="field-full"><el-option :label="t('monitorHTTP')" value="http"/><el-option :label="t('monitorICMP')" value="icmp"/><el-option :label="t('monitorTCP')" value="tcp"/></el-select></el-form-item>
        <el-form-item class="span-2" :label="t('target')" prop="target" :rules="[{ required: true, message: t('required') }]"><el-input v-model="form.target" :placeholder="targetPlaceholder" /></el-form-item>
        <el-form-item :label="t('intervalSeconds')"><el-input v-model.number="form.interval_seconds" inputmode="numeric" class="field-full" @blur="form.interval_seconds = clampNumber(form.interval_seconds, 5, 86400, 30)" /></el-form-item>
        <el-form-item :label="t('showOnServicePage')"><el-switch v-model="form.show_in_service" /></el-form-item>
      </div>
      <div class="editor-section">
        <div class="editor-section-title"><h3>{{ t('notificationSettings') }}</h3><el-switch v-model="form.notify" /></div>
        <div class="editor-grid">
          <el-form-item :label="t('notificationGroup')"><el-select v-model="form.notification_tag" filterable allow-create default-first-option class="field-full"><el-option v-for="group in groups" :key="group" :label="group" :value="group"/></el-select></el-form-item>
          <el-form-item :label="t('latencyAlert')"><el-switch v-model="form.latency_notify" /></el-form-item>
          <el-form-item v-if="form.latency_notify" :label="t('minimumLatencyMs')"><el-input v-model.number="form.min_latency_ms" inputmode="numeric" class="field-full" @blur="form.min_latency_ms = clampNumber(form.min_latency_ms, 0, undefined, 0)" /></el-form-item>
          <el-form-item v-if="form.latency_notify" :label="t('maximumLatencyMs')"><el-input v-model.number="form.max_latency_ms" inputmode="numeric" class="field-full" @blur="form.max_latency_ms = clampNumber(form.max_latency_ms, 0, undefined, 0)" /></el-form-item>
        </div>
      </div>
      <div class="editor-section">
        <el-form-item :label="t('serverScope')" class="scope-field">
          <el-radio-group v-model="scopeMode" class="scope-mode">
            <el-radio-button value="all">{{ t('scopeAll') }}</el-radio-button>
            <el-radio-button value="include">{{ t('scopeSelectedServers') }}</el-radio-button>
            <el-radio-button value="exclude">{{ t('scopeExceptSelected') }}</el-radio-button>
          </el-radio-group>
          <el-transfer
            v-if="scopeMode !== 'all'"
            v-model="scopeServers"
            filterable
            :filter-placeholder="t('searchServers')"
            :data="transferData"
            :titles="[t('availableServers'), scopeMode === 'include' ? t('selectedServers') : t('excludedServers')]"
            class="server-transfer"
          />
        </el-form-item>
      </div>
    </el-form>
    <template #footer="{ close }"><el-button :disabled="saving" @click="close()">{{ t('cancel') }}</el-button><el-button type="primary" :loading="saving" @click="submit">{{ t('save') }}</el-button></template>
  </AppDialog>
</template>
