<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { confirmDiscardChanges } from './confirmDiscard'

const props = withDefaults(
  defineProps<{
    modelValue: boolean
    title?: string
    mode?: 'edit' | 'view'
    dirty?: boolean
    submitting?: boolean
    width?: string
  }>(),
  {
    mode: 'edit',
    dirty: false,
    submitting: false,
    width: '520px',
  },
)

const emit = defineEmits<{
  'update:modelValue': [boolean]
  close: []
}>()

const { t, te } = useI18n()
const viewport = ref(typeof window === 'undefined' ? 1024 : window.innerWidth)
const isMobile = computed(() => viewport.value < 720)

function onResize() {
  viewport.value = window.innerWidth
}
onMounted(() => window.addEventListener('resize', onResize))
onUnmounted(() => window.removeEventListener('resize', onResize))

function label(key: string, fallback: string) {
  return te(key) ? String(t(key)) : fallback
}

async function guardClose(): Promise<boolean> {
  if (props.submitting) return false
  if (props.mode === 'edit' && props.dirty) {
    return confirmDiscardChanges({
      message: label('discardChangesConfirm', '有未保存的修改，确定放弃吗？'),
      title: label('discardChanges', '放弃修改'),
      confirm: label('discard', '放弃'),
      cancel: label('keepEditing', '继续编辑'),
    })
  }
  return true
}

async function handleBeforeClose(done: (cancel?: boolean) => void) {
  const ok = await guardClose()
  if (ok) {
    done()
    emit('close')
  }
  else done(true)
}

async function requestClose() {
  const ok = await guardClose()
  if (ok) {
    emit('update:modelValue', false)
    emit('close')
  }
}

function onUpdate(v: boolean) {
  emit('update:modelValue', v)
}

defineExpose({ requestClose })
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    :width="width"
    :fullscreen="isMobile"
    :close-on-click-modal="mode === 'view'"
    :close-on-press-escape="mode === 'view'"
    :show-close="!submitting"
    :before-close="handleBeforeClose"
    destroy-on-close
    class="app-dialog"
    @update:model-value="onUpdate"
  >
    <slot />
    <template v-if="$slots.footer" #footer>
      <slot name="footer" :close="requestClose" :submitting="submitting" />
    </template>
  </el-dialog>
</template>

<style scoped>
@media (max-width: 720px) {
  :deep(.el-dialog__footer) {
    position: sticky;
    bottom: 0;
    background: var(--bg-elevated);
    border-top: 1px solid var(--border);
  }
  :deep(.el-button) {
    min-height: var(--touch-min-mobile);
  }
}
</style>
