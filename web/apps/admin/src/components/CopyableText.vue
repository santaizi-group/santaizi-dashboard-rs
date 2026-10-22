<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { copyText } from '@/composables/clipboard'

const props = defineProps<{
  value?: string | null
}>()

const { t } = useI18n()

async function copy(event: MouseEvent) {
  event.stopPropagation()
  await copyText(props.value, t)
}
</script>

<template>
  <span v-if="!value" class="muted">—</span>
  <button v-else type="button" class="copyable-text" :title="value" @click="copy">{{ value }}</button>
</template>
