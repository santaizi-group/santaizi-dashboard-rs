import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

export interface AdminMessage {
  id: string
  createdAt: string
  title: string
  message: string
  code: string
  status?: number
  traceId?: string
  fields?: Record<string, string[]>
  detail?: string
  route: string
  read: boolean
}

const MAX_MESSAGES = 100

export const useMessageStore = defineStore('messages', () => {
  const items = ref<AdminMessage[]>([])
  const activeId = ref('')
  const panelOpen = ref(false)

  const unreadCount = computed(() => items.value.filter(item => !item.read).length)
  const activeMessage = computed(() => items.value.find(item => item.id === activeId.value) || null)

  function pushError(input: Omit<AdminMessage, 'id' | 'createdAt' | 'read'> & { id?: string }) {
    const id = input.id || `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const entry: AdminMessage = {
      id,
      createdAt: new Date().toISOString(),
      title: input.title,
      message: input.message,
      code: input.code || '',
      status: input.status,
      traceId: input.traceId,
      fields: input.fields,
      detail: input.detail,
      route: input.route || '',
      read: false,
    }
    items.value = [entry, ...items.value].slice(0, MAX_MESSAGES)
    return entry
  }

  function markRead(id: string) {
    const item = items.value.find(entry => entry.id === id)
    if (item) item.read = true
  }

  function markAllRead() {
    for (const item of items.value) item.read = true
  }

  function remove(id: string) {
    items.value = items.value.filter(item => item.id !== id)
    if (activeId.value === id) activeId.value = ''
  }

  function clear() {
    items.value = []
    activeId.value = ''
  }

  function openPanel() {
    panelOpen.value = true
  }

  function closePanel() {
    panelOpen.value = false
    activeId.value = ''
  }

  function openDetail(id: string) {
    activeId.value = id
    panelOpen.value = true
    markRead(id)
  }

  function closeDetail() {
    activeId.value = ''
  }

  return {
    items,
    activeId,
    panelOpen,
    unreadCount,
    activeMessage,
    pushError,
    markRead,
    markAllRead,
    remove,
    clear,
    openPanel,
    closePanel,
    openDetail,
    closeDetail,
  }
})
