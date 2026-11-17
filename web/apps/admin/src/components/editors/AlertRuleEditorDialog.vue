<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, type FormInstance } from 'element-plus'
import { AppDialog } from '@santaizi/ui'
import { createAlertRule, listAllServers, listNotificationGroups, updateAlertRule, type ServerRecord } from '@/api/adminApi'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'
import {
  NET_SPEED_UNITS,
  defaultMaxForMetric,
  formatNetSpeedAmount,
  inferNetSpeedUnit,
  isNetSpeedMetric,
  resolveConditionThreshold,
  type NetSpeedUnit,
} from '@/domain/alertThreshold'
import type { AlertCondition, AlertMetric, AlertRuleRecord } from '@/types/admin'

interface VisualCondition extends AlertCondition {
  scope_mode: 'all' | 'include' | 'exclude'
  server_ids: number[]
  min_input: string
  max_input: string
  min_unit: NetSpeedUnit
  max_unit: NetSpeedUnit
}

const props = defineProps<{ modelValue: boolean; value?: AlertRuleRecord }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: [] }>()
const { t, te } = useI18n()
const saving = ref(false)
const formRef = ref<FormInstance>()
const servers = ref<ServerRecord[]>([])
const groups = ref<string[]>([])
const form = reactive<{ id: number; name: string; notification_tag: string; trigger_mode: 'always' | 'once'; enabled: boolean; conditions: VisualCondition[] }>({
  id: 0,
  name: '',
  notification_tag: 'default',
  trigger_mode: 'always',
  enabled: true,
  conditions: [],
})
const { dirty, capture } = useEditorSnapshot(form, computed(() => props.modelValue))
const transferData = computed(() => servers.value.map(server => ({ key: server.id, label: server.name })))
const metricGroups = computed(() => [
  { label: t('resourceUsage'), options: ['cpu', 'gpu', 'temperature_max', 'memory', 'swap', 'disk'] as AlertMetric[] },
  { label: t('networkPerformance'), options: ['net_in_speed', 'net_out_speed', 'net_all_speed', 'tcp_conn_count', 'udp_conn_count'] as AlertMetric[] },
  { label: t('systemHealth'), options: ['offline', 'load1', 'load5', 'load15', 'process_count'] as AlertMetric[] },
])
const metricIcons: Record<AlertMetric, string> = {
  cpu: 'ri-cpu-line', gpu: 'ri-cpu-line', temperature_max: 'ri-temp-hot-line', memory: 'ri-database-2-line', swap: 'ri-swap-line', disk: 'ri-hard-drive-3-line',
  net_in_speed: 'ri-download-cloud-2-line', net_out_speed: 'ri-upload-cloud-2-line', net_all_speed: 'ri-speed-up-line', offline: 'ri-wifi-off-line',
  load1: 'ri-pulse-line', load5: 'ri-pulse-line', load15: 'ri-pulse-line', tcp_conn_count: 'ri-links-line', udp_conn_count: 'ri-broadcast-line', process_count: 'ri-stack-line',
}

function unit(type: AlertMetric) {
  if (['cpu', 'gpu', 'memory', 'swap', 'disk'].includes(type)) return '(%)'
  if (type === 'temperature_max') return '(°C)'
  return ''
}

function durationHint(durationSeconds: number) {
  const seconds = Math.max(0, Number(durationSeconds) || 0)
  if (seconds >= 60) return t('durationHintMinutes', { minutes: (seconds / 60).toFixed(1).replace(/\.0$/, '') })
  return ''
}

function clampNumber(value: unknown, min: number, max?: number, fallback = min) {
  const next = Number(value)
  if (!Number.isFinite(next)) return fallback
  if (max === undefined) return Math.max(min, next)
  return Math.min(max, Math.max(min, next))
}

function normalizeThreshold(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null
  const next = Number(value)
  if (!Number.isFinite(next) || next <= 0) return null
  return next
}

function normalizeInputString(raw: string): string {
  const trimmed = String(raw ?? '').trim()
  if (!trimmed) return ''
  const num = Number(trimmed)
  if (!Number.isFinite(num) || num <= 0) return ''
  return String(num)
}

function syncThresholds(condition: VisualCondition) {
  condition.min = resolveConditionThreshold(condition.min_input, condition.type, condition.min_unit)
  condition.max = resolveConditionThreshold(condition.max_input, condition.type, condition.max_unit)
}

function onBlurThreshold(condition: VisualCondition, bound: 'min' | 'max') {
  if (bound === 'min') condition.min_input = normalizeInputString(condition.min_input)
  else condition.max_input = normalizeInputString(condition.max_input)
  syncThresholds(condition)
}

function onMetricChange(condition: VisualCondition) {
  condition.min = null
  condition.min_input = ''
  condition.min_unit = 'MB/s'
  condition.max_unit = 'MB/s'
  if (condition.type === 'offline') {
    condition.max = null
    condition.max_input = ''
  } else {
    const def = defaultMaxForMetric(condition.type)
    condition.max = def
    if (def === null) {
      condition.max_input = ''
    } else if (isNetSpeedMetric(condition.type)) {
      condition.max_unit = inferNetSpeedUnit(def)
      condition.max_input = formatNetSpeedAmount(def, condition.max_unit)
    } else {
      condition.max_input = String(def)
    }
  }
  formRef.value?.clearValidate()
}

function validateThreshold(condition: VisualCondition) {
  return (_rule: unknown, _value: unknown, callback: (error?: Error) => void) => {
    if (condition.type === 'offline') {
      callback()
      return
    }
    const min = resolveConditionThreshold(condition.min_input, condition.type, condition.min_unit)
    const max = resolveConditionThreshold(condition.max_input, condition.type, condition.max_unit)
    if (min === null && max === null) {
      callback(new Error(t('required')))
      return
    }
    if (min !== null && max !== null && min > max) {
      callback(new Error(t('alertThresholdOrderInvalid')))
      return
    }
    callback()
  }
}

function validateScope(condition: VisualCondition) {
  return (_rule: unknown, _value: unknown, callback: (error?: Error) => void) => {
    if (condition.scope_mode === 'include' && (!condition.server_ids || condition.server_ids.length === 0)) {
      callback(new Error(t('required')))
      return
    }
    callback()
  }
}

function blankCondition(): VisualCondition {
  return {
    type: 'cpu',
    min: null,
    max: 90,
    min_input: '',
    max_input: '90',
    min_unit: 'MB/s',
    max_unit: 'MB/s',
    duration_seconds: 30,
    scope: { mode: 'all', server_ids: [] },
    scope_mode: 'all',
    server_ids: [],
  }
}

function visualCondition(value: AlertCondition): VisualCondition {
  const minVal = value.type === 'offline' ? null : normalizeThreshold(value.min)
  const maxVal = value.type === 'offline' ? null : normalizeThreshold(value.max)
  const minUnit: NetSpeedUnit = isNetSpeedMetric(value.type) ? inferNetSpeedUnit(minVal) : 'MB/s'
  const maxUnit: NetSpeedUnit = isNetSpeedMetric(value.type) ? inferNetSpeedUnit(maxVal) : 'MB/s'
  const minInput = minVal === null
    ? ''
    : (isNetSpeedMetric(value.type) ? formatNetSpeedAmount(minVal, minUnit) : String(minVal))
  const maxInput = maxVal === null
    ? ''
    : (isNetSpeedMetric(value.type) ? formatNetSpeedAmount(maxVal, maxUnit) : String(maxVal))

  return {
    ...blankCondition(),
    ...value,
    min: minVal,
    max: maxVal,
    min_input: minInput,
    max_input: maxInput,
    min_unit: minUnit,
    max_unit: maxUnit,
    duration_seconds: clampNumber(value.duration_seconds, 3, 86400, 30),
    scope_mode: value.scope.mode,
    server_ids: [...value.scope.server_ids],
  }
}

function reset(value?: AlertRuleRecord) {
  Object.assign(form, {
    id: value?.id || 0,
    name: value?.name || '',
    notification_tag: value?.notification_tag || 'default',
    trigger_mode: value?.trigger_mode || 'always',
    enabled: value?.enabled ?? true,
  })
  form.conditions = value?.conditions?.length ? value.conditions.map(visualCondition) : [blankCondition()]
  nextTick(capture)
}

async function loadOptions() {
  try {
    const [serverResult, groupResult] = await Promise.all([listAllServers(), listNotificationGroups()])
    servers.value = serverResult.data
    groups.value = groupResult.length ? groupResult : ['default']
  } catch (error) { notifyAPIError(error, t as never, te) }
}

async function submit() {
  const formValid = await formRef.value?.validate().then(() => true, () => false)
  if (!formValid) return
  saving.value = true
  try {
    const conditions: AlertCondition[] = form.conditions.map(condition => ({
      type: condition.type,
      min: resolveConditionThreshold(condition.min_input, condition.type, condition.min_unit),
      max: resolveConditionThreshold(condition.max_input, condition.type, condition.max_unit),
      duration_seconds: clampNumber(condition.duration_seconds, 3, 86400, 30),
      scope: { mode: condition.scope_mode, server_ids: condition.scope_mode === 'all' ? [] : [...condition.server_ids] },
    }))
    const payload = {
      name: form.name.trim(),
      notification_tag: form.notification_tag.trim() || 'default',
      trigger_mode: form.trigger_mode,
      enabled: form.enabled,
      conditions,
    }
    if (form.id) await updateAlertRule(form.id, payload)
    else await createAlertRule(payload)
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
  <AppDialog :model-value="modelValue" :title="form.id ? t('editAlertRule') : t('createAlertRule')" mode="edit" :dirty="dirty" :submitting="saving" width="min(1040px, 97vw)" @update:model-value="emit('update:modelValue', $event)">
    <el-form ref="formRef" :model="form" label-position="top" @submit.prevent="submit">
      <div class="editor-grid">
        <el-form-item :label="t('name')" prop="name" :rules="[{ required: true, message: t('required') }]"><el-input v-model="form.name" /></el-form-item>
        <el-form-item :label="t('notificationGroup')"><el-select v-model="form.notification_tag" filterable allow-create default-first-option class="field-full"><el-option v-for="group in groups" :key="group" :label="group" :value="group"/></el-select></el-form-item>
        <el-form-item :label="t('triggerMode')"><el-radio-group v-model="form.trigger_mode"><el-radio-button value="always">{{ t('triggerAlways') }}</el-radio-button><el-radio-button value="once">{{ t('triggerOnce') }}</el-radio-button></el-radio-group></el-form-item>
        <el-form-item :label="t('enabled')"><el-switch v-model="form.enabled" /></el-form-item>
      </div>
      <div class="editor-section">
        <div class="editor-section-title"><h3>{{ t('alertConditions') }}</h3><el-button type="primary" plain @click="form.conditions.push(blankCondition())"><i class="ri-add-line"></i>{{ t('addCondition') }}</el-button></div>
        <div class="condition-list">
          <section v-for="(condition, index) in form.conditions" :key="index" class="condition-card">
            <div class="condition-head"><span class="condition-icon"><i :class="metricIcons[condition.type]"></i></span><strong>{{ t(`metric_${condition.type}`) }}</strong><el-button circle :disabled="form.conditions.length === 1" :aria-label="t('delete')" @click="form.conditions.splice(index, 1)"><i class="ri-delete-bin-6-line"></i></el-button></div>
            <div class="editor-grid condition-fields">
              <el-form-item :label="t('informationType')">
                <el-select v-model="condition.type" class="field-full" @change="onMetricChange(condition)">
                  <el-option-group v-for="group in metricGroups" :key="group.label" :label="group.label"><el-option v-for="metric in group.options" :key="metric" :label="t(`metric_${metric}`)" :value="metric"><div class="metric-option"><i :class="metricIcons[metric]"></i><span>{{ t(`metric_${metric}`) }}</span></div></el-option></el-option-group>
                </el-select>
              </el-form-item>
              <el-form-item :label="t('durationSeconds')">
                <el-input v-model.number="condition.duration_seconds" inputmode="numeric" class="field-full" @blur="condition.duration_seconds = clampNumber(condition.duration_seconds, 3, 86400, 30)" />
                <span v-if="durationHint(condition.duration_seconds)" class="condition-duration-hint">{{ durationHint(condition.duration_seconds) }}</span>
              </el-form-item>
              <el-form-item
                v-if="condition.type !== 'offline'"
                :label="`${t('alertAboveThreshold')} ${unit(condition.type)}`.trim()"
                :prop="`conditions.${index}.max_input`"
                :rules="[{ validator: validateThreshold(condition), trigger: 'blur' }]"
              >
                <div v-if="isNetSpeedMetric(condition.type)" class="compound-field">
                  <el-input
                    v-model="condition.max_input"
                    inputmode="decimal"
                    placeholder=">"
                    @update:model-value="syncThresholds(condition)"
                    @blur="onBlurThreshold(condition, 'max')"
                  />
                  <el-select v-model="condition.max_unit" @change="syncThresholds(condition)">
                    <el-option v-for="speedUnit in NET_SPEED_UNITS" :key="speedUnit" :label="speedUnit" :value="speedUnit" />
                  </el-select>
                </div>
                <el-input
                  v-else
                  v-model="condition.max_input"
                  inputmode="decimal"
                  placeholder=">"
                  class="field-full"
                  @update:model-value="syncThresholds(condition)"
                  @blur="onBlurThreshold(condition, 'max')"
                />
              </el-form-item>
              <el-form-item
                v-if="condition.type !== 'offline'"
                :label="`${t('alertBelowThreshold')} ${unit(condition.type)}`.trim()"
                :prop="`conditions.${index}.min_input`"
                :rules="[{ validator: validateThreshold(condition), trigger: 'blur' }]"
              >
                <div v-if="isNetSpeedMetric(condition.type)" class="compound-field">
                  <el-input
                    v-model="condition.min_input"
                    inputmode="decimal"
                    placeholder="<"
                    @update:model-value="syncThresholds(condition)"
                    @blur="onBlurThreshold(condition, 'min')"
                  />
                  <el-select v-model="condition.min_unit" @change="syncThresholds(condition)">
                    <el-option v-for="speedUnit in NET_SPEED_UNITS" :key="speedUnit" :label="speedUnit" :value="speedUnit" />
                  </el-select>
                </div>
                <el-input
                  v-else
                  v-model="condition.min_input"
                  inputmode="decimal"
                  placeholder="<"
                  class="field-full"
                  @update:model-value="syncThresholds(condition)"
                  @blur="onBlurThreshold(condition, 'min')"
                />
              </el-form-item>
            </div>
            <el-form-item :label="t('serverScope')" :prop="`conditions.${index}.server_ids`" :rules="[{ validator: validateScope(condition), trigger: 'change' }]" class="scope-field">
              <el-radio-group v-model="condition.scope_mode" class="scope-mode">
                <el-radio-button value="all">{{ t('scopeAll') }}</el-radio-button>
                <el-radio-button value="include">{{ t('scopeSelectedServers') }}</el-radio-button>
                <el-radio-button value="exclude">{{ t('scopeExceptSelected') }}</el-radio-button>
              </el-radio-group>
              <el-transfer
                v-if="condition.scope_mode !== 'all'"
                v-model="condition.server_ids"
                filterable
                :filter-placeholder="t('searchServers')"
                :data="transferData"
                :titles="[t('availableServers'), condition.scope_mode === 'include' ? t('selectedServers') : t('excludedServers')]"
                class="server-transfer"
              />
            </el-form-item>
          </section>
        </div>
      </div>
    </el-form>
    <template #footer="{ close }"><el-button :disabled="saving" @click="close()">{{ t('cancel') }}</el-button><el-button type="primary" :loading="saving" @click="submit">{{ t('save') }}</el-button></template>
  </AppDialog>
</template>
