<script setup lang="ts">
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { AppDialog } from '@santaizi/ui'
import { getServerUpgradePreview, type ServerRecord } from '@/api/adminApi'
import { notifyAPIError } from '@/composables/notify'

const props = defineProps<{ modelValue: boolean; server?: ServerRecord }>()
const emit = defineEmits<{ 'update:modelValue': [boolean] }>()
const { t, te } = useI18n()
const loading = ref(false)
const platform = ref<'linux' | 'macos' | 'windows'>('linux')
const command = ref('')

async function refreshPreview() {
  if (!props.server) return
  const preview = await getServerUpgradePreview(props.server.id, { platform: platform.value })
  command.value = preview.command
}

async function open() {
  if (!props.server) return
  loading.value = true
  platform.value = 'linux'
  command.value = ''
  try {
    await refreshPreview()
  } catch (error) {
    notifyAPIError(error, t as never, te)
  } finally {
    loading.value = false
  }
}

async function copy() {
  if (!command.value) await refreshPreview()
  await navigator.clipboard.writeText(command.value)
  ElMessage.success(t('copied'))
}

watch(() => props.modelValue, value => { if (value) void open() })
watch(platform, () => {
  command.value = ''
  if (props.modelValue && !loading.value) void refreshPreview().catch(error => notifyAPIError(error, t as never, te))
})
</script>

<template>
  <AppDialog :model-value="modelValue" :title="`${t('upgradeAgent')} · ${server?.name || ''}`" mode="view" width="min(720px, 97vw)" @update:model-value="emit('update:modelValue', $event)">
    <div v-loading="loading">
      <el-tabs v-model="platform">
        <el-tab-pane :label="t('linux')" name="linux" />
        <el-tab-pane :label="t('macos')" name="macos" />
        <el-tab-pane :label="t('windows')" name="windows" />
      </el-tabs>
      <el-input :model-value="command" readonly type="textarea" :rows="4" class="mono" />
    </div>
    <template #footer="{ close }">
      <el-button :disabled="loading" @click="close()">{{ t('close') }}</el-button>
      <el-button type="primary" :disabled="loading || !command" @click="copy"><i class="ri-file-copy-line"></i>{{ t('copyUpgradeCommand') }}</el-button>
    </template>
  </AppDialog>
</template>
