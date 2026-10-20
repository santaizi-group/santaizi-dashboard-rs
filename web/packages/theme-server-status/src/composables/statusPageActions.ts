import { inject, onUnmounted, provide, ref, watchEffect, type InjectionKey, type Ref } from 'vue'

export type StatusPageAction = {
  id: string
  label: string
  icon: string
  run: () => void
}

export const statusPageActionsKey: InjectionKey<Ref<StatusPageAction[]>> = Symbol('ss-page-actions')

export function provideStatusPageActions() {
  const actions = ref<StatusPageAction[]>([])
  provide(statusPageActionsKey, actions)
  return actions
}

export function registerStatusPageActions(source: () => StatusPageAction[]) {
  const actions = inject(statusPageActionsKey)
  if (!actions) return
  watchEffect(() => {
    actions.value = source()
  })
  onUnmounted(() => {
    actions.value = []
  })
}
