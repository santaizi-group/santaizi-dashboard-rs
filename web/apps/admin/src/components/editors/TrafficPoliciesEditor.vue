<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import {
  DEFAULT_QUOTA_BYTES,
  MIN_QUOTA_BYTES,
  QUOTA_UNITS,
  QUOTA_UNIT_FACTOR,
  formatQuotaAmount,
  inferQuotaUnit,
  parsePositiveQuotaBytes,
  recalcQuotaBytesForNewUnit,
  type QuotaUnit,
} from '@/domain/trafficPolicyQuota'
import type { TrafficPolicyRecord } from '@/types/admin'

const props = defineProps<{ modelValue: TrafficPolicyRecord[]; notificationGroups: string[] }>()
const emit = defineEmits<{ 'update:modelValue': [TrafficPolicyRecord[]] }>()
const { t } = useI18n()

const quotaUnits = ref<QuotaUnit[]>([])
const quotaDrafts = ref<Record<number, string>>({})
let lastEmitted: TrafficPolicyRecord[] | null = null

watch(() => props.modelValue, (list) => {
  if (list === lastEmitted) return
  quotaUnits.value = list.map(policy => inferQuotaUnit(policy.quota_bytes))
  quotaDrafts.value = {}
}, { immediate: true })

function add() {
  const cycleStart = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z')
  quotaUnits.value = [...quotaUnits.value, 'GB']
  const next: TrafficPolicyRecord[] = [...props.modelValue, {
    name: '',
    direction: 'total',
    mode: 'recurring',
    cycle_start: cycleStart,
    cycle_interval: 1,
    cycle_unit: 'month',
    quota_bytes: DEFAULT_QUOTA_BYTES,
    warning_percent: 80,
    notification_tag: 'default',
    enabled: true,
  }]
  lastEmitted = next
  emit('update:modelValue', next)
}

function remove(index: number) {
  quotaUnits.value = quotaUnits.value.filter((_, current) => current !== index)
  const drafts: Record<number, string> = {}
  for (const [key, value] of Object.entries(quotaDrafts.value)) {
    const current = Number(key)
    if (current === index) continue
    drafts[current > index ? current - 1 : current] = value
  }
  quotaDrafts.value = drafts
  const next = props.modelValue.filter((_, current) => current !== index)
  lastEmitted = next
  emit('update:modelValue', next)
}

function byteLabel(value: number) {
  if (value >= QUOTA_UNIT_FACTOR.TB) return `${(value / QUOTA_UNIT_FACTOR.TB).toFixed(2)} TB`
  if (value >= QUOTA_UNIT_FACTOR.GB) return `${(value / QUOTA_UNIT_FACTOR.GB).toFixed(2)} GB`
  return `${(value / QUOTA_UNIT_FACTOR.MB).toFixed(2)} MB`
}

function clampNumber(value: unknown, min: number, max?: number, fallback = min) {
  const next = Number(value)
  if (!Number.isFinite(next)) return fallback
  if (max === undefined) return Math.max(min, next)
  return Math.min(max, Math.max(min, next))
}

function quotaDisplay(index: number, bytes: number) {
  if (quotaDrafts.value[index] !== undefined) return quotaDrafts.value[index]
  return formatQuotaAmount(bytes, quotaUnits.value[index] ?? 'GB')
}

function onQuotaInput(index: number, value: string | number, policy: TrafficPolicyRecord) {
  const raw = String(value)
  quotaDrafts.value[index] = raw
  const unit = quotaUnits.value[index] ?? 'GB'
  const parsed = parsePositiveQuotaBytes(raw, unit)
  if (parsed !== null) {
    policy.quota_bytes = parsed
  }
}

function commitQuota(index: number, policy: TrafficPolicyRecord) {
  const unit = quotaUnits.value[index] ?? 'GB'
  const draft = quotaDrafts.value[index]
  delete quotaDrafts.value[index]
  if (draft !== undefined) {
    const parsed = parsePositiveQuotaBytes(draft, unit)
    if (parsed !== null) {
      policy.quota_bytes = parsed
      return
    }
  }
  policy.quota_bytes = Math.max(MIN_QUOTA_BYTES, Number(policy.quota_bytes) || QUOTA_UNIT_FACTOR[unit])
}

function onQuotaUnitChange(index: number, nextUnit: QuotaUnit, policy: TrafficPolicyRecord) {
  const prevUnit = quotaUnits.value[index] ?? 'GB'
  const nextBytes = recalcQuotaBytesForNewUnit(policy.quota_bytes, prevUnit, nextUnit, quotaDrafts.value[index])
  quotaUnits.value[index] = nextUnit
  delete quotaDrafts.value[index]
  policy.quota_bytes = nextBytes
}
</script>

<template>
  <div class="editor-section-title"><h3>{{ t('trafficPolicies') }}</h3><el-button type="primary" plain @click="add"><i class="ri-add-line"></i>{{ t('addTrafficPolicy') }}</el-button></div>
  <div v-if="modelValue.length" class="traffic-policy-list">
    <section v-for="(policy, index) in modelValue" :key="policy.id || index" class="traffic-policy-card">
      <div class="condition-head"><span class="condition-icon"><i class="ri-exchange-2-line"></i></span><strong>{{ policy.name || t('unnamedPolicy') }}</strong><el-switch v-model="policy.enabled"/><el-button circle :aria-label="t('delete')" @click="remove(index)"><i class="ri-delete-bin-6-line"></i></el-button></div>
      <div v-if="policy.usage" class="traffic-progress"><el-progress :percentage="Math.min(100, Math.round(policy.usage.usage_percent))" :status="policy.usage.status === 'exceeded' ? 'exception' : undefined"/><span>{{ byteLabel(policy.usage.used_bytes) }} / {{ byteLabel(policy.quota_bytes) }}</span></div>
      <div class="editor-grid">
        <el-form-item :label="t('name')" required><el-input v-model="policy.name" /></el-form-item>
        <el-form-item :label="t('notificationGroup')"><el-select v-model="policy.notification_tag" filterable allow-create class="field-full"><el-option v-for="group in notificationGroups" :key="group" :label="group" :value="group" /></el-select></el-form-item>
        <el-form-item :label="t('trafficDirection')"><el-select v-model="policy.direction" class="field-full"><el-option :label="t('inbound')" value="inbound"/><el-option :label="t('outbound')" value="outbound"/><el-option :label="t('totalDirection')" value="total"/></el-select></el-form-item>
        <el-form-item :label="t('trafficMode')"><el-segmented v-model="policy.mode" :options="[{ label: t('cumulative'), value: 'cumulative' }, { label: t('recurring'), value: 'recurring' }]" /></el-form-item>
        <el-form-item :label="t('warningPercent')"><el-input v-model.number="policy.warning_percent" inputmode="numeric" class="field-full" @blur="policy.warning_percent = clampNumber(policy.warning_percent, 1, 99, 80)"><template #suffix>%</template></el-input></el-form-item>
        <el-form-item :label="t('quotaBytes')">
          <div class="compound-field">
            <el-input
              :model-value="quotaDisplay(index, policy.quota_bytes)"
              inputmode="decimal"
              @update:model-value="onQuotaInput(index, $event, policy)"
              @blur="commitQuota(index, policy)"
            />
            <el-select :model-value="quotaUnits[index] ?? 'GB'" @change="onQuotaUnitChange(index, $event as QuotaUnit, policy)">
              <el-option v-for="unit in QUOTA_UNITS" :key="unit" :label="unit" :value="unit" />
            </el-select>
          </div>
        </el-form-item>
        <el-form-item v-if="policy.mode === 'recurring'" :label="t('cycle')"><div class="compound-field"><el-input v-model.number="policy.cycle_interval" inputmode="numeric" @blur="policy.cycle_interval = clampNumber(policy.cycle_interval, 1, undefined, 1)" /><el-select v-model="policy.cycle_unit"><el-option :label="t('hour')" value="hour"/><el-option :label="t('day')" value="day"/><el-option :label="t('week')" value="week"/><el-option :label="t('month')" value="month"/><el-option :label="t('year')" value="year"/></el-select></div></el-form-item>
        <el-form-item v-if="policy.mode === 'recurring'" class="span-2" :label="t('cycleStart')" required><el-date-picker v-model="policy.cycle_start" type="datetime" value-format="YYYY-MM-DDTHH:mm:ssZ" :clearable="false" class="field-full" /></el-form-item>
      </div>
    </section>
  </div>
  <el-empty v-else :description="t('noTrafficPolicies')" :image-size="56" />
</template>
