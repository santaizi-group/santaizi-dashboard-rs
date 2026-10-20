<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import {
  emptyPublicNote,
  filterBillingCycleSuggestions,
  parsePublicNote,
  serializePublicNote,
} from '@/domain/publicNote'
import type { PublicNoteForm } from '@/types/admin'

const props = defineProps<{ modelValue: string }>()
const emit = defineEmits<{ 'update:modelValue': [string] }>()
const { t } = useI18n()
const active = ref('billing')
const base = ref<Record<string, unknown>>({})
const form = reactive<PublicNoteForm>(emptyPublicNote())
const raw = ref('')
const rawError = ref('')
let internal = false
const preview = computed(() => serializePublicNote(form, base.value) || '{}')

function queryBillingCycles(query: string, cb: (items: Array<{ value: string }>) => void) {
  cb(filterBillingCycleSuggestions(query))
}

function load(value: string) {
  const parsed = parsePublicNote(value)
  Object.assign(form, parsed.form)
  base.value = parsed.base
  raw.value = value || '{}'
  rawError.value = parsed.error
}
function emitStructured() {
  if (internal) return
  const value = serializePublicNote(form, base.value)
  raw.value = value || '{}'
  emit('update:modelValue', value)
}
function applyRaw() {
  const parsed = parsePublicNote(raw.value)
  if (parsed.error) { rawError.value = parsed.error; return }
  internal = true
  Object.assign(form, parsed.form)
  base.value = parsed.base
  internal = false
  rawError.value = ''
  emit('update:modelValue', serializePublicNote(form, base.value))
  ElMessage.success(t('jsonApplied'))
}
watch(() => props.modelValue, value => { if (value !== serializePublicNote(form, base.value)) load(value) }, { immediate: true })
watch(form, emitStructured, { deep: true })
</script>

<template>
  <el-tabs v-model="active" class="public-note-tabs">
    <el-tab-pane :label="t('billingInfo')" name="billing">
      <div class="editor-grid">
        <el-form-item :label="t('startDate')"><el-date-picker v-model="form.billing.startDate" type="date" value-format="YYYY-MM-DD" class="field-full" /></el-form-item>
        <el-form-item :label="t('endDate')"><div class="date-toggle"><el-checkbox v-model="form.unlimitedEnd">{{ t('unlimited') }}</el-checkbox><el-date-picker v-model="form.billing.endDate" type="date" value-format="YYYY-MM-DD" class="field-full" :disabled="form.unlimitedEnd" /></div></el-form-item>
        <el-form-item :label="t('amount')"><el-input v-model="form.billing.amount" class="field-full" /></el-form-item>
        <el-form-item :label="t('billingCycle')">
          <el-autocomplete
            v-model="form.billing.cycle"
            :fetch-suggestions="queryBillingCycles"
            :placeholder="t('billingCyclePlaceholder')"
            clearable
            fit-input-width
            popper-class="billing-cycle-suggest"
            class="field-full"
          />
        </el-form-item>
        <el-form-item :label="t('autoRenewal')"><el-switch v-model="form.billing.autoRenewal" active-value="1" inactive-value="0" /></el-form-item>
      </div>
    </el-tab-pane>
    <el-tab-pane :label="t('planInfo')" name="plan">
      <div class="editor-grid">
        <el-form-item :label="t('bandwidth')"><el-input v-model="form.plan.bandwidth" placeholder="1 Gbps" /></el-form-item>
        <el-form-item :label="t('trafficVolume')"><el-input v-model="form.plan.trafficVol" placeholder="2 TB" /></el-form-item>
        <el-form-item :label="t('trafficType')">
          <el-select v-model="form.plan.trafficType" clearable class="field-full">
            <!-- 与 aobobo / 上游 Nezha 一致：1 单向出、2 双向、3 单向取最大；空=不限制 -->
            <el-option :label="t('trafficOneWayOut')" value="1"/>
            <el-option :label="t('trafficBidirectional')" value="2"/>
            <el-option :label="t('trafficOneWayMax')" value="3"/>
          </el-select>
        </el-form-item>
        <el-form-item :label="t('ipSupport')"><el-checkbox v-model="form.plan.IPv4" true-value="1" false-value="0">IPv4</el-checkbox><el-checkbox v-model="form.plan.IPv6" true-value="1" false-value="0">IPv6</el-checkbox></el-form-item>
        <el-form-item class="span-2" :label="t('networkRoute')"><el-select v-model="form.plan.networkRoute" multiple filterable allow-create default-first-option class="field-full" /></el-form-item>
        <el-form-item class="span-2" :label="t('extraTags')"><el-select v-model="form.plan.extra" multiple filterable allow-create default-first-option class="field-full" /></el-form-item>
      </div>
    </el-tab-pane>
    <el-tab-pane :label="t('presentationInfo')" name="presentation">
      <div class="editor-grid">
        <el-form-item :label="t('locationCode')"><el-input v-model="form.presentation.location" placeholder="CN" /></el-form-item>
        <el-form-item :label="t('flagCode')"><el-input v-model="form.presentation.flag" placeholder="cn" /></el-form-item>
        <el-form-item class="span-2" :label="t('purchaseLink')"><el-input v-model="form.presentation.orderLink" /></el-form-item>
        <el-form-item :label="t('purchaseButtonText')"><el-input v-model="form.presentation.buyBtnText" /></el-form-item>
        <el-form-item :label="t('purchaseButtonIcon')"><el-select v-model="form.presentation.buyBtnIcon" filterable allow-create class="field-full"><el-option :label="t('purchaseIconShopping')" value="ri-shopping-bag-3-line"/><el-option :label="t('purchaseIconExternal')" value="ri-external-link-line"/></el-select></el-form-item>
        <el-form-item class="span-2" :label="t('slogan')"><el-input v-model="form.presentation.slogan" /></el-form-item>
        <el-form-item :label="t('latitude')"><el-input v-model="form.presentation.lat" /></el-form-item>
        <el-form-item :label="t('longitude')"><el-input v-model="form.presentation.lng" /></el-form-item>
        <el-form-item :label="t('coordinatePair')"><el-input v-model="form.presentation.latlng" placeholder="31.2304,121.4737" /></el-form-item>
        <el-form-item :label="t('locationLabel')"><el-input v-model="form.presentation.locationLabel" /></el-form-item>
      </div>
    </el-tab-pane>
    <el-tab-pane :label="t('advancedJSON')" name="advanced">
      <el-alert v-if="rawError" :title="t('invalidJSON')" :description="rawError" type="error" show-icon :closable="false" />
      <el-input v-model="raw" type="textarea" :rows="10" class="mono json-editor" />
      <el-button type="primary" plain @click="applyRaw"><i class="ri-check-line"></i>{{ t('applyJSON') }}</el-button>
    </el-tab-pane>
  </el-tabs>
  <el-collapse class="json-preview"><el-collapse-item :title="t('finalPreview')"><pre>{{ preview }}</pre></el-collapse-item></el-collapse>
</template>
