import { computed, ref, unref, watch, type Ref } from 'vue'
import { registerRouteDirtyEditor } from './routeDirtyGuard'

function normalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalize)
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return Object.fromEntries(Object.keys(record)
      .sort((left, right) => left.localeCompare(right))
      .map(key => [key, normalize(record[key])]))
  }
  return value
}

export function editorSnapshot(value: unknown) {
  return JSON.stringify(normalize(value))
}

export function useEditorSnapshot<T>(value: T | Ref<T>, visible: Ref<boolean>) {
  const baseline = ref<string | null>(null)
  watch(visible, (isOpen) => {
    if (!isOpen) baseline.value = null
  })
  const dirty = computed(() => visible.value && baseline.value !== null && baseline.value !== editorSnapshot(unref(value)))
  registerRouteDirtyEditor(dirty)
  function capture() { baseline.value = editorSnapshot(unref(value)) }
  return { dirty, capture }
}
