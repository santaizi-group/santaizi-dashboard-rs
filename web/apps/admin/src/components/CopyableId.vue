<script setup lang="ts">
import { ElMessage } from 'element-plus'
import { useI18n } from 'vue-i18n'
import { shortId } from '@/composables/shortId'

const props = withDefaults(defineProps<{
  value?: string | null
  compact?: boolean
}>(), { compact: true })

const { t } = useI18n()

async function copy(event: MouseEvent) {
  event.stopPropagation()
  if (!props.value) return
  await navigator.clipboard.writeText(props.value)
  ElMessage.success(t('copied'))
}
</script>

<template>
  <span v-if="!value" class="muted">—</span>
  <span v-else class="token-cell" :class="{ 'copyable-id--full': !compact }" :title="value">
    <code class="mono" :class="compact ? 'copyable-id__text' : 'copyable-id__full'">{{ compact ? shortId(value) : value }}</code>
    <el-button text class="token-copy" :aria-label="t('copy')" @click="copy"><i class="ri-file-copy-line"></i></el-button>
  </span>
</template>
