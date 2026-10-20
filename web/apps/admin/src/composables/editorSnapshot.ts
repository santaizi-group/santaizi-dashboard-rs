import { computed, ref, toRaw, unref, type Ref } from 'vue'
import { registerRouteDirtyEditor } from './routeDirtyGuard'

function normalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalize)
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, normalize(item)]))
  }
  return value
}

export function editorSnapshot(value: unknown) {
  return JSON.stringify(normalize(toRaw(value)))
}

export function useEditorSnapshot<T>(value: T | Ref<T>, visible: Ref<boolean>) {
  const baseline = ref('')
  const dirty = computed(() => visible.value && baseline.value !== editorSnapshot(unref(value)))
  registerRouteDirtyEditor(dirty)
  function capture() { baseline.value = editorSnapshot(unref(value)) }
  return { dirty, capture }
}
