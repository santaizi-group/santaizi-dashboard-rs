<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox, type FormInstance } from 'element-plus'
import { AppEmpty } from '@santaizi/ui'
import AlertRuleEditorDialog from '@/components/editors/AlertRuleEditorDialog.vue'
import { deleteAlertRule, listAlertRules } from '@/api/adminApi'
import { cleanupOfflineHistory, getSettings, updateSettings } from '@santaizi/api'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'
import { PAGE_SIZES, readStoredPageSize, writeStoredPageSize } from '@/composables/pageSize'
import { isRowSelected, toggleRowSelection } from '@/composables/selection'
import type { AlertCondition, AlertMetric, AlertRuleRecord } from '@/types/admin'

const metricIcons: Record<string, string> = {
  cpu: 'ri-cpu-line', gpu: 'ri-cpu-line', temperature_max: 'ri-temp-hot-line', memory: 'ri-database-2-line', swap: 'ri-swap-line', disk: 'ri-hard-drive-3-line',
  net_in_speed: 'ri-download-cloud-2-line', net_out_speed: 'ri-upload-cloud-2-line', net_all_speed: 'ri-speed-up-line', offline: 'ri-wifi-off-line',
  load1: 'ri-pulse-line', load5: 'ri-pulse-line', load15: 'ri-pulse-line', tcp_conn_count: 'ri-links-line', udp_conn_count: 'ri-broadcast-line', process_count: 'ri-stack-line',
}
function formatMetricThreshold(type: AlertMetric, value: number): string {
  if (['cpu', 'gpu', 'memory', 'swap', 'disk'].includes(type)) return `${value}%`
  if (type === 'temperature_max') return `${value}°C`
  if (type.includes('net_')) {
    if (value >= 1073741824) return `${(value / 1073741824).toFixed(1).replace(/\.0$/, '')} GiB/s`
    if (value >= 1048576) return `${(value / 1048576).toFixed(1).replace(/\.0$/, '')} MiB/s`
    if (value >= 1024) return `${(value / 1024).toFixed(1).replace(/\.0$/, '')} KiB/s`
    return `${value} B/s`
  }
  return `${value}`
}
function formatConditionSummary(condition: AlertCondition): string {
  const label = t(`metric_${condition.type}`)
  const dur = condition.duration_seconds > 0 ? ` · ${condition.duration_seconds}s` : ''
  if (condition.type === 'offline') return `${label}${dur}`
  const bounds: string[] = []
  if (condition.min && condition.min > 0) bounds.push(`< ${formatMetricThreshold(condition.type, condition.min)}`)
  if (condition.max && condition.max > 0) bounds.push(`> ${formatMetricThreshold(condition.type, condition.max)}`)
  return bounds.length ? `${label} ${bounds.join(' / ')}${dur}` : `${label}${dur}`
}

const { t, te } = useI18n()
const route = useRoute()
const settingsFormRef = ref<FormInstance>()
const loading = ref(false), saving = ref(false), editor = ref(false), total = ref(0)
const items = ref<AlertRuleRecord[]>([]), selected = ref<AlertRuleRecord[]>([]), editing = ref<AlertRuleRecord>()
const query = reactive({ page: 1, page_size: readStoredPageSize(route.path), q: '', sort: 'id', order: 'desc' as const })
const SETTINGS_KEYS = [
  'enable_offline_history', 'offline_threshold', 'check_interval', 'merge_gap', 'retention_days',
  'notify_offline', 'notify_recovery', 'connectivity_notification', 'correction_notification',
  'collector_offline_notification', 'collector_online_notification', 'data_loss_notification', 'plain_ip_in_notification',
] as const
// 与后端 `config::MAX_OFFLINE_*` 及 `admin_settings.rs` 校验口径完全一致。
const MAX_THRESHOLD = 86400, MAX_INTERVAL = 86400, MAX_MERGE_GAP = 3600, MAX_RETENTION = 3650
const form = reactive<Record<string, unknown>>({
  enable_offline_history: true, offline_threshold: 30, check_interval: 5, merge_gap: 0, retention_days: 30,
  notify_offline: true, notify_recovery: true, connectivity_notification: false, correction_notification: false,
  collector_offline_notification: true, collector_online_notification: true, data_loss_notification: true, plain_ip_in_notification: false,
})
const settingsSnapshot = computed(() => Object.fromEntries(SETTINGS_KEYS.map(key => [key, form[key]])))
const { capture } = useEditorSnapshot(settingsSnapshot, ref(true))

function parseIntField(value: unknown, emptyFallback?: number): number {
  if ((value === '' || value === null || value === undefined) && emptyFallback !== undefined) return emptyFallback
  const n = Number(String(value ?? '').trim())
  return Number.isFinite(n) ? Math.round(n) : Number.NaN
}

const settingsRules = {
  offline_threshold: [{
    validator: (_rule: unknown, value: unknown, callback: (error?: Error) => void) => {
      const n = parseIntField(value)
      if (!Number.isFinite(n) || n < 10 || n > MAX_THRESHOLD) {
        callback(new Error(t('offlineThresholdRange')))
        return
      }
      callback()
    },
    trigger: 'blur',
  }],
  check_interval: [{
    validator: (_rule: unknown, value: unknown, callback: (error?: Error) => void) => {
      const n = parseIntField(value)
      const threshold = parseIntField(form.offline_threshold)
      const maxAllowed = Number.isFinite(threshold) ? Math.min(MAX_INTERVAL, threshold) : MAX_INTERVAL
      if (!Number.isFinite(n) || n < 5 || n > maxAllowed) {
        callback(new Error(t('checkIntervalRange')))
        return
      }
      callback()
    },
    trigger: 'blur',
  }],
  merge_gap: [{
    validator: (_rule: unknown, value: unknown, callback: (error?: Error) => void) => {
      const n = parseIntField(value, 0)
      if (!Number.isFinite(n) || n < 0 || n > MAX_MERGE_GAP) {
        callback(new Error(t('mergeGapRange')))
        return
      }
      callback()
    },
    trigger: 'blur',
  }],
  retention_days: [{
    validator: (_rule: unknown, value: unknown, callback: (error?: Error) => void) => {
      const n = parseIntField(value)
      if (!Number.isFinite(n) || n < 1 || n > MAX_RETENTION) {
        callback(new Error(t('retentionDaysRange')))
        return
      }
      callback()
    },
    trigger: 'blur',
  }],
}

function applySettingsToForm(settings: Record<string, unknown>) {
  for (const key of SETTINGS_KEYS) {
    if (key in settings) form[key] = settings[key]
  }
}

async function loadSettings() {
  try {
    const settings = await getSettings()
    applySettingsToForm(settings)
    await nextTick()
    capture()
  } catch (error) {
    notifyAPIError(error, t as never, te)
  }
}

async function loadRules() {
  writeStoredPageSize(route.path, query.page_size)
  loading.value = true
  try {
    const result = await listAlertRules(query)
    const nextTotal = result.meta.total ?? result.data.length
    if (result.data.length === 0 && query.page > 1 && nextTotal > 0) {
      query.page -= 1
      return await loadRules()
    }
    items.value = result.data
    total.value = nextTotal
    const visibleIds = new Set(result.data.map(row => row.id))
    selected.value = selected.value.filter(row => visibleIds.has(row.id))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    loading.value = false
  }
}

async function saveSettings() {
  if (form.merge_gap === '' || form.merge_gap === null || form.merge_gap === undefined) form.merge_gap = 0
  const valid = await settingsFormRef.value?.validate().then(() => true, () => false)
  if (!valid) return
  saving.value = true
  try {
    form.offline_threshold = parseIntField(form.offline_threshold)
    form.check_interval = parseIntField(form.check_interval)
    form.merge_gap = parseIntField(form.merge_gap, 0)
    form.retention_days = parseIntField(form.retention_days)
    const payload: Record<string, unknown> = {}
    for (const key of SETTINGS_KEYS) payload[key] = form[key]
    applySettingsToForm(await updateSettings(payload))
    await nextTick()
    capture()
    ElMessage.success(t('saveSuccess'))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    saving.value = false
  }
}

async function cleanupHistory() {
  try {
    await ElMessageBox.confirm(t('cleanupHistoryConfirm'), t('confirm'), { type: 'warning' })
  } catch {
    return
  }
  try {
    const result = await cleanupOfflineHistory()
    ElMessage.success(t('cleanupHistoryResult', { count: Number(result.deleted || 0) }))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  }
}

function open(item?: AlertRuleRecord) { editing.value = item; editor.value = true }
async function remove(rows: AlertRuleRecord[]) {
  try {
    await ElMessageBox.confirm(t('confirmDelete'), t('dangerousAction'), { type: 'warning' })
  } catch {
    return
  }
  try {
    await Promise.all(rows.map(row => deleteAlertRule(row.id)))
    selected.value = []
    await loadRules()
    ElMessage.success(t('deleteSuccess'))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  }
}
function onSelect(row: AlertRuleRecord, checked: boolean | string | number) { selected.value = toggleRowSelection(selected.value, row, !!checked) }
onMounted(() => {
  void Promise.all([loadSettings(), loadRules()])
})
</script>
<template>
  <div class="page-head">
    <h1>{{ t('alertRules') }}</h1>
    <div class="page-actions">
      <el-button @click="open()"><i class="ri-add-line"></i>{{ t('createAlertRule') }}</el-button>
      <el-button type="primary" :loading="saving" @click="saveSettings"><i class="ri-save-line"></i>{{ t('save') }}</el-button>
    </div>
  </div>
  <div class="page-stack">
  <el-form ref="settingsFormRef" :model="form" :rules="settingsRules" label-position="top" class="settings-stack">
    <section class="surface settings-section">
      <div class="settings-heading">
        <i class="ri-timer-flash-line"></i>
        <div><h2>{{ t('offlineSettings') }}</h2></div>
        <el-button plain @click="cleanupHistory"><i class="ri-delete-bin-5-line"></i>{{ t('cleanupHistory') }}</el-button>
      </div>
      <div class="form-grid">
        <el-form-item :label="t('enableOfflineHistory')"><el-switch v-model="form.enable_offline_history"/></el-form-item>
        <el-form-item :label="t('offlineThreshold')" prop="offline_threshold"><el-input v-model.number="form.offline_threshold" inputmode="numeric" style="width:100%" @blur="settingsFormRef?.validateField('check_interval').catch(() => {})"/></el-form-item>
        <el-form-item :label="t('checkInterval')" prop="check_interval"><el-input v-model.number="form.check_interval" inputmode="numeric" style="width:100%"/></el-form-item>
        <el-form-item :label="t('mergeGap')" prop="merge_gap"><el-input v-model.number="form.merge_gap" inputmode="numeric" style="width:100%"/></el-form-item>
        <el-form-item :label="t('retentionDays')" prop="retention_days"><el-input v-model.number="form.retention_days" inputmode="numeric" style="width:100%"/></el-form-item>
        <el-form-item :label="t('notifyOffline')"><el-switch v-model="form.notify_offline"/></el-form-item>
        <el-form-item :label="t('notifyRecovery')"><el-switch v-model="form.notify_recovery"/></el-form-item>
      </div>
    </section>
    <section class="surface settings-section">
      <div class="settings-heading"><i class="ri-notification-badge-line"></i><div><h2>{{ t('telemetryNotifications') }}</h2></div></div>
      <div class="setting-switches">
        <label><span>{{ t('connectivityNotification') }}</span><el-switch v-model="form.connectivity_notification"/></label>
        <label><span>{{ t('correctionNotification') }}</span><el-switch v-model="form.correction_notification"/></label>
        <label><span>{{ t('collectorOfflineNotification') }}</span><el-switch v-model="form.collector_offline_notification"/></label>
        <label><span>{{ t('collectorOnlineNotification') }}</span><el-switch v-model="form.collector_online_notification"/></label>
        <label><span>{{ t('dataLossNotification') }}</span><el-switch v-model="form.data_loss_notification"/></label>
        <label><span>{{ t('plainIPInNotification') }}</span><el-switch v-model="form.plain_ip_in_notification"/></label>
      </div>
    </section>
  </el-form>
  <section class="surface table-card"><div class="toolbar"><el-input v-model="query.q" class="search-input" clearable :placeholder="t('search')" @keyup.enter="query.page=1;loadRules()" @clear="query.page=1;loadRules()"><template #prefix><i class="ri-search-line"></i></template></el-input><el-button @click="query.page=1;loadRules()"><i class="ri-search-line"></i>{{ t('submitSearch') }}</el-button><el-button v-if="selected.length" type="danger" plain @click="remove(selected)"><i class="ri-delete-bin-6-line"></i>{{ t('batchDelete') }}</el-button><span class="toolbar-spacer"></span><el-button @click="loadRules"><i class="ri-refresh-line"></i>{{ t('refresh') }}</el-button></div>
    <el-table class="desktop-only" v-loading="loading" :data="items" row-key="id" @selection-change="selected=$event"><el-table-column type="selection" width="46"/><el-table-column prop="name" :label="t('name')" min-width="180"/><el-table-column prop="notification_tag" :label="t('notificationGroup')" width="160"><template #default="{row}"><el-tag effect="plain">{{ row.notification_tag }}</el-tag></template></el-table-column><el-table-column :label="t('informationTypes')" min-width="280"><template #default="{row}"><div class="metric-tags"><el-tag v-for="(condition, idx) in row.conditions" :key="`${idx}-${condition.type}`" effect="plain"><i :class="metricIcons[condition.type] || 'ri-pulse-line'"></i>{{ formatConditionSummary(condition) }}</el-tag></div></template></el-table-column><el-table-column :label="t('triggerMode')" width="130"><template #default="{row}">{{ t(row.trigger_mode === 'once' ? 'triggerOnce' : 'triggerAlways') }}</template></el-table-column><el-table-column :label="t('status')" width="100"><template #default="{row}"><el-tag :type="row.enabled ? 'success' : 'info'">{{ t(row.enabled ? 'enabled' : 'disabled') }}</el-tag></template></el-table-column><el-table-column :label="t('actions')" width="72" fixed="right"><template #default="{row}"><el-dropdown trigger="click"><el-button text class="actions-more" :aria-label="t('actions')"><i class="ri-more-fill"></i></el-button><template #dropdown><el-dropdown-menu><el-dropdown-item @click="open(row)"><i class="ri-edit-line"></i>{{ t('edit') }}</el-dropdown-item><el-dropdown-item divided @click="remove([row])"><i class="ri-delete-bin-6-line"></i>{{ t('delete') }}</el-dropdown-item></el-dropdown-menu></template></el-dropdown></template></el-table-column><template #empty><AppEmpty icon="ri-alarm-warning-line" :description="t('noData')"/></template></el-table>
    <div class="mobile-only" v-loading="loading">
      <AppEmpty v-if="!items.length && !loading" icon="ri-alarm-warning-line" :description="t('noData')"/>
      <div v-else class="mobile-card-list">
        <article v-for="row in items" :key="row.id" class="mobile-card">
          <div class="mobile-card-head">
            <el-checkbox :model-value="isRowSelected(selected, row)" @change="onSelect(row, $event)" />
            <div class="mobile-card-title"><strong>{{ row.name }}</strong></div>
            <div class="mobile-card-actions">
              <el-dropdown trigger="click">
                <el-button text class="actions-more" :aria-label="t('actions')"><i class="ri-more-fill"></i></el-button>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item @click="open(row)"><i class="ri-edit-line"></i>{{ t('edit') }}</el-dropdown-item>
                    <el-dropdown-item divided @click="remove([row])"><i class="ri-delete-bin-6-line"></i>{{ t('delete') }}</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </div>
          </div>
          <dl class="mobile-card-meta">
            <div><dt>{{ t('notificationGroup') }}</dt><dd><el-tag effect="plain">{{ row.notification_tag }}</el-tag></dd></div>
            <div><dt>{{ t('informationTypes') }}</dt><dd><div class="metric-tags"><el-tag v-for="(condition, idx) in row.conditions" :key="`${idx}-${condition.type}`" effect="plain"><i :class="metricIcons[condition.type] || 'ri-pulse-line'"></i>{{ formatConditionSummary(condition) }}</el-tag></div></dd></div>
            <div><dt>{{ t('triggerMode') }}</dt><dd>{{ t(row.trigger_mode === 'once' ? 'triggerOnce' : 'triggerAlways') }}</dd></div>
            <div><dt>{{ t('status') }}</dt><dd><el-tag :type="row.enabled ? 'success' : 'info'">{{ t(row.enabled ? 'enabled' : 'disabled') }}</el-tag></dd></div>
          </dl>
        </article>
      </div>
    </div>
    <div class="pagination"><el-pagination v-model:current-page="query.page" v-model:page-size="query.page_size" :page-sizes="PAGE_SIZES" layout="total, sizes, prev, pager, next" :total="total" @change="loadRules"/></div></section>
  </div>
  <AlertRuleEditorDialog v-model="editor" :value="editing" @saved="loadRules"/>
</template>
