<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, type FormInstance } from 'element-plus'
import { createApiToken, type APIToken } from '@santaizi/api'
import { AppDialog } from '@santaizi/ui'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'

type ExpiryPreset = 'never' | '7' | '30' | '90' | '365' | 'custom'
type TokenPermission = 'read' | 'write'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ 'update:modelValue': [boolean]; issued: [APIToken] }>()
const { t, te } = useI18n()
const saving = ref(false)
const formRef = ref<FormInstance>()
const form = reactive({
  note: '',
  permission: 'write' as TokenPermission,
  expiryPreset: 'never' as ExpiryPreset,
  customExpiresAt: '',
})
const snapshotValue = computed(() => ({ ...form }))
const { dirty, capture } = useEditorSnapshot(snapshotValue, computed(() => props.modelValue))

const permissionOptions = computed(() => [
  { label: t('tokenReadOnly'), value: 'read' },
  { label: t('tokenWrite'), value: 'write' },
])
const expiryOptions = computed(() => [
  { label: t('tokenNeverExpires'), value: 'never' },
  { label: t('tokenExpiry7d'), value: '7' },
  { label: t('tokenExpiry30d'), value: '30' },
  { label: t('tokenExpiry90d'), value: '90' },
  { label: t('tokenExpiry365d'), value: '365' },
  { label: t('tokenExpiryCustom'), value: 'custom' },
])

function reset() {
  Object.assign(form, { note: '', permission: 'write', expiryPreset: 'never', customExpiresAt: '' })
  nextTick(capture)
}

function resolveExpiresAt(): string | null | undefined {
  if (form.expiryPreset === 'never') return null
  if (form.expiryPreset === 'custom') return form.customExpiresAt || undefined
  const days = Number(form.expiryPreset)
  return new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString()
}

async function submit() {
  await formRef.value?.validate()
  if (form.expiryPreset === 'custom' && !form.customExpiresAt) {
    ElMessage.warning(t('required'))
    return
  }
  const expiresAt = resolveExpiresAt()
  saving.value = true
  try {
    const body: { note: string; permission: TokenPermission; expires_at?: string | null } = {
      note: form.note.trim(),
      permission: form.permission,
    }
    if (expiresAt === null) body.expires_at = null
    else if (expiresAt) body.expires_at = expiresAt
    const result = await createApiToken(body)
    capture()
    emit('update:modelValue', false)
    emit('issued', result)
    ElMessage.success(t('issueTokenSuccess'))
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    saving.value = false
  }
}

watch(() => props.modelValue, (value) => { if (value) reset() })
</script>

<template>
  <AppDialog
    :model-value="modelValue"
    :title="t('issueToken')"
    mode="edit"
    :dirty="dirty"
    :submitting="saving"
    width="min(560px, 94vw)"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <el-form ref="formRef" :model="form" label-position="top" class="token-issue-form" @submit.prevent="submit">
      <el-form-item :label="t('tokenNote')" prop="note" :rules="[{ required: true, message: t('required') }]">
        <el-input v-model="form.note" maxlength="200" show-word-limit />
      </el-form-item>
      <div class="form-grid">
        <el-form-item :label="t('tokenPermission')" prop="permission" :rules="[{ required: true, message: t('required') }]">
          <el-segmented v-model="form.permission" :options="permissionOptions" class="field-full" />
        </el-form-item>
        <el-form-item :label="t('tokenExpiresAt')" prop="expiryPreset" :rules="[{ required: true, message: t('required') }]">
          <el-select v-model="form.expiryPreset" class="field-full">
            <el-option v-for="option in expiryOptions" :key="option.value" :label="option.label" :value="option.value" />
          </el-select>
        </el-form-item>
      </div>
      <el-form-item
        v-if="form.expiryPreset === 'custom'"
        :label="t('tokenExpiryCustom')"
        prop="customExpiresAt"
        :rules="[{ required: true, message: t('required') }]"
      >
        <el-date-picker
          v-model="form.customExpiresAt"
          type="datetime"
          value-format="YYYY-MM-DDTHH:mm:ssZ"
          class="field-full"
        />
      </el-form-item>
    </el-form>
    <template #footer="{ close }">
      <el-button :disabled="saving" @click="close()">{{ t('cancel') }}</el-button>
      <el-button type="primary" :loading="saving" @click="submit">
        <i class="ri-key-2-line"></i>{{ t('issueToken') }}
      </el-button>
    </template>
  </AppDialog>
</template>
