<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { AppDialog } from '@santaizi/ui'
import { getCollectorInstallPreview, getCollectorToken, type CollectorRecord } from '@/api/adminApi'
import { useEditorSnapshot } from '@/composables/editorSnapshot'
import { notifyAPIError } from '@/composables/notify'

const props = defineProps<{ modelValue: boolean; collector?: CollectorRecord; token?: string }>()
const emit = defineEmits<{ 'update:modelValue': [boolean] }>()
const { t, te } = useI18n()
const loading = ref(false)
const token = ref('')
const command = ref('')
const form = reactive({
  primary_endpoint: '',
  primary_tls: false,
  primary_insecure_tls: false,
})
const snapshotValue = computed(() => ({ ...form }))
const { dirty, capture } = useEditorSnapshot(snapshotValue, computed(() => props.modelValue))

async function refreshPreview() {
  if (!props.collector) return
  const preview = await getCollectorInstallPreview(props.collector.id, {
    primary_endpoint: form.primary_endpoint || undefined,
    primary_tls: form.primary_tls,
    primary_insecure_tls: form.primary_insecure_tls,
  })
  command.value = preview.command
  if (!form.primary_endpoint) form.primary_endpoint = preview.primary_endpoint
  return preview
}

async function open() {
  loading.value = true
  command.value = ''
  form.primary_endpoint = ''
  form.primary_tls = false
  form.primary_insecure_tls = false
  try {
    if (props.token) {
      token.value = props.token
    } else if (props.collector) {
      const result = await getCollectorToken(props.collector.id)
      token.value = result.registration_token
    } else {
      token.value = ''
    }
    const preview = await refreshPreview()
    if (preview && preview.default_primary_tls !== form.primary_tls) {
      form.primary_tls = preview.default_primary_tls
      await refreshPreview()
    }
    await nextTick()
    capture()
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    loading.value = false
  }
}

async function copyToken() {
  await navigator.clipboard.writeText(token.value)
  ElMessage.success(t('copied'))
}

async function copyCommand() {
  if (!command.value) await refreshPreview()
  await navigator.clipboard.writeText(command.value)
  ElMessage.success(t('copied'))
}

watch(() => props.modelValue, value => { if (value) void open() })
watch(snapshotValue, () => {
  if (!props.modelValue || loading.value) return
  command.value = ''
  void refreshPreview().catch(error => notifyAPIError(error, t as never, te))
}, { deep: true })
</script>

<template>
  <AppDialog
    :model-value="modelValue"
    :title="`${t('installCollector')} · ${collector?.name || ''}`"
    mode="edit"
    :dirty="dirty"
    :submitting="loading"
    width="min(720px, 96vw)"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div v-loading="loading">
      <el-form label-position="top">
        <el-form-item :label="t('registrationToken')">
          <el-input :model-value="token" readonly class="mono">
            <template #append>
              <el-button :aria-label="t('copy')" @click="copyToken"><i class="ri-file-copy-line"></i></el-button>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item :label="t('primaryEndpoint')" required>
          <el-input v-model="form.primary_endpoint" placeholder="primary.example.com:5555" class="mono" />
        </el-form-item>
        <el-form-item :label="t('primaryTLS')">
          <el-switch v-model="form.primary_tls" />
        </el-form-item>
        <el-form-item :label="t('insecureTLS')">
          <el-switch v-model="form.primary_insecure_tls" />
        </el-form-item>
        <el-form-item :label="t('installCommand')">
          <el-input :model-value="command" readonly type="textarea" :rows="5" class="mono" />
        </el-form-item>
      </el-form>
    </div>
    <template #footer="{ close }">
      <el-button :disabled="loading" @click="close()">{{ t('close') }}</el-button>
      <el-button type="primary" :disabled="loading || !command" @click="copyCommand"><i class="ri-file-copy-line"></i>{{ t('copyCommand') }}</el-button>
    </template>
  </AppDialog>
</template>
