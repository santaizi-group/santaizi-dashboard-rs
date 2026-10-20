<script setup lang="ts">
import { ElMessage } from 'element-plus'
import { useI18n } from 'vue-i18n'

const props = defineProps<{
  value?: string | null
}>()

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
  <button v-else type="button" class="copyable-text" :title="value" @click="copy">{{ value }}</button>
</template>
