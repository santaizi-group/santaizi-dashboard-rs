import { inject, type InjectionKey } from 'vue'
import type { StatusStoreState } from './types'

export const STATUS_STORE_KEY: InjectionKey<StatusStoreState> = Symbol('santaizi-status-store')

export function useInjectedStatusStore(): StatusStoreState {
  const store = inject(STATUS_STORE_KEY)
  if (!store) {
    throw new Error('@santaizi/status-core: status store was not provided by the shell app')
  }
  return store
}
