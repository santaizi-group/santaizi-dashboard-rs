<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, type FormInstance } from 'element-plus'
import { AppDialog } from '@santaizi/ui'
import { createNotification, updateNotification } from '@/api/adminApi'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'
import {
  NOTIFICATION_PRESETS,
  applyNotificationPreset,
  composeNotificationFromPreset,
  detectNotificationPreset,
  emptyCredentialValues,
  extractPresetValues,
  firstMissingCredentialField,
  getNotificationPreset,
  isGuidedNotificationPreset,
  type NotificationCredentialKey,
  type NotificationPresetId,
} from '@/domain/notificationPresets'
import type { KeyValueRow, NotificationChannelRecord } from '@/types/admin'

const props = defineProps<{ modelValue: boolean; value?: NotificationChannelRecord }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; saved: [] }>()
const { t, te } = useI18n()
const formRef = ref<FormInstance>()
const saving = ref(false)
const activePreset = ref<NotificationPresetId | ''>('')
const headers = ref<KeyValueRow[]>([])
const credentials = reactive<Partial<Record<NotificationCredentialKey, string>>>({})
const form = reactive<NotificationChannelRecord>({
  id: 0,
  name: '',
  tag: 'default',
  url: '',
  method: 'post',
  request_type: 'json',
  headers: [],
  body: '',
  verify_tls: true,
})
const snapshotValue = computed(() => ({
  form,
  headers: headers.value,
  credentials: { ...credentials },
  activePreset: activePreset.value,
}))
const { dirty, capture } = useEditorSnapshot(snapshotValue, computed(() => props.modelValue))

const guided = computed(() => isGuidedNotificationPreset(activePreset.value))
const activePresetDef = computed(() =>
  activePreset.value ? getNotificationPreset(activePreset.value) : undefined,
)

function clearCredentials() {
  for (const key of Object.keys(credentials) as NotificationCredentialKey[]) {
    delete credentials[key]
  }
}

function reset(value?: NotificationChannelRecord) {
  Object.assign(form, {
    id: value?.id || 0,
    name: value?.name || '',
    tag: value?.tag || 'default',
    url: value?.url || '',
    method: value?.method || 'post',
    request_type: value?.request_type || 'json',
    headers: [...(value?.headers || [])],
    body: value?.body || '',
    verify_tls: value?.verify_tls ?? true,
  })
  headers.value = (value?.headers || []).map(item => ({ ...item }))
  clearCredentials()

  if (value?.id) {
    const detected = detectNotificationPreset(value.url || '', value.body || '')
    activePreset.value = detected
    const preset = getNotificationPreset(detected)
    if (preset && isGuidedNotificationPreset(detected)) {
      Object.assign(credentials, extractPresetValues(preset, value.url || '', value.body || ''))
    }
  } else {
    activePreset.value = ''
  }
  nextTick(capture)
}

function applyPreset(id: NotificationPresetId) {
  const preset = NOTIFICATION_PRESETS.find(item => item.id === id)
  if (!preset) return
  clearCredentials()
  Object.assign(credentials, emptyCredentialValues(preset))
  const patch = applyNotificationPreset(preset, form.name, key => t(key), credentials)
  form.url = patch.url
  form.method = patch.method
  form.request_type = patch.request_type
  form.body = patch.body
  headers.value = patch.headers
  if (patch.name) form.name = patch.name
  activePreset.value = id
}

function addHeader() {
  headers.value.push({ key: '', value: '' })
}

async function submit() {
  if (guided.value && activePresetDef.value) {
    const missing = firstMissingCredentialField(activePresetDef.value, credentials)
    if (missing) {
      ElMessage.warning(t('required'))
      return
    }
    const patch = composeNotificationFromPreset(activePresetDef.value, credentials)
    form.url = patch.url
    form.method = patch.method
    form.request_type = patch.request_type
    form.body = patch.body
    headers.value = patch.headers
  }

  await formRef.value?.validate()
  if (guided.value && !form.url.trim()) {
    ElMessage.warning(t('required'))
    return
  }

  saving.value = true
  try {
    const { id: _id, ...write } = form
    const payload = {
      ...write,
      headers: headers.value
        .filter(item => item.key.trim())
        .map(item => ({ key: item.key.trim(), value: item.value })),
    }
    if (form.id) await updateNotification(form.id, payload)
    else await createNotification(payload)
    capture()
    emit('update:modelValue', false)
    emit('saved')
    ElMessage.success(t('saveSuccess'))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    saving.value = false
  }
}

watch(
  () => props.modelValue,
  value => {
    if (value) reset(props.value)
  },
)
</script>

<template>
  <AppDialog
    :model-value="modelValue"
    :title="form.id ? t('editNotificationChannel') : t('createNotificationChannel')"
    mode="edit"
    :dirty="dirty"
    :submitting="saving"
    width="min(860px, 96vw)"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-form ref="formRef" :model="form" label-position="top" @submit.prevent="submit">
      <el-form-item :label="t('channelPreset')">
        <div class="channel-preset-grid" role="group" :aria-label="t('channelPreset')">
          <button
            v-for="preset in NOTIFICATION_PRESETS"
            :key="preset.id"
            type="button"
            class="channel-preset-chip"
            :class="{ 'is-active': activePreset === preset.id }"
            @click="applyPreset(preset.id)"
          >
            {{ t(preset.labelKey) }}
          </button>
        </div>
      </el-form-item>
      <div class="editor-grid">
        <el-form-item :label="t('name')" prop="name" :rules="[{ required: true, message: t('required') }]">
          <el-input v-model="form.name" />
        </el-form-item>
        <el-form-item :label="t('notificationGroup')">
          <el-input v-model="form.tag" />
        </el-form-item>

        <template v-if="guided && activePresetDef">
          <el-form-item
            v-for="field in activePresetDef.fields"
            :key="field.key"
            class="span-2"
            :label="t(field.labelKey)"
            :required="!field.optional"
          >
            <el-input
              v-model="credentials[field.key]"
              :type="field.secret ? 'password' : 'text'"
              :show-password="Boolean(field.secret)"
              :placeholder="field.defaultValue || undefined"
            />
          </el-form-item>
        </template>

        <template v-else>
          <el-form-item
            class="span-2"
            :label="t('requestURL')"
            prop="url"
            :rules="[{ required: true, message: t('required') }]"
          >
            <el-input v-model="form.url" placeholder="https://example.com/webhook" />
          </el-form-item>
          <el-form-item :label="t('requestMethod')">
            <el-segmented
              v-model="form.method"
              :options="[
                { label: 'GET', value: 'get' },
                { label: 'POST', value: 'post' },
              ]"
            />
          </el-form-item>
          <el-form-item v-if="form.method === 'post'" :label="t('requestType')">
            <el-segmented
              v-model="form.request_type"
              :options="[
                { label: t('bodyJSON'), value: 'json' },
                { label: t('bodyForm'), value: 'form' },
              ]"
            />
          </el-form-item>
        </template>
      </div>

      <template v-if="!guided">
        <div class="editor-section">
          <div class="editor-section-title">
            <h3>{{ t('requestHeaders') }}</h3>
            <el-button @click="addHeader">
              <i class="ri-add-line"></i>{{ t('addHeader') }}
            </el-button>
          </div>
          <div v-if="headers.length" class="key-value-list">
            <div v-for="(header, index) in headers" :key="index" class="key-value-row">
              <el-input v-model="header.key" :placeholder="t('headerName')" />
              <el-input v-model="header.value" :placeholder="t('headerValue')" />
              <el-button circle :aria-label="t('delete')" @click="headers.splice(index, 1)">
                <i class="ri-delete-bin-6-line"></i>
              </el-button>
            </div>
          </div>
          <el-empty v-else :description="t('noRequestHeaders')" :image-size="48" />
        </div>
        <el-form-item v-if="form.method === 'post'" :label="t('requestBody')">
          <el-input v-model="form.body" type="textarea" :rows="7" class="mono" />
        </el-form-item>
      </template>

      <div class="switch-grid">
        <label>
          <span>{{ t('verifyTLS') }}</span>
          <el-switch v-model="form.verify_tls" />
        </label>
      </div>
    </el-form>
    <template #footer="{ close }">
      <el-button :disabled="saving" @click="close()">{{ t('cancel') }}</el-button>
      <el-button type="primary" :loading="saving" @click="submit">{{ t('save') }}</el-button>
    </template>
  </AppDialog>
</template>
