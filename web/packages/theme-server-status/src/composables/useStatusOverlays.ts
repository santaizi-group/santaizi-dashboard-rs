import { computed, onBeforeUnmount, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

function parsePositiveInt(raw: unknown): number {
  const value = Array.isArray(raw) ? raw[0] : raw
  const n = Number(value)
  return Number.isInteger(n) && n > 0 ? n : 0
}

export function useStatusDrawerRoute<T>(options: {
  resolveItem: (id: number) => T | null
  isLoaded: () => boolean
  queryKey?: string
}) {
  const queryKey = options.queryKey ?? 'server'
  const route = useRoute()
  const router = useRouter()
  const selectedId = ref(parsePositiveInt(route.query[queryKey]))
  let pushedHistory = false
  let syncingRoute = false

  function stripQueryKey() {
    const nextQuery = { ...route.query }
    delete nextQuery[queryKey]
    return nextQuery
  }

  async function openDrawer(id: number) {
    if (id <= 0 || selectedId.value === id) {
      closeDrawer()
      return
    }
    const wasOpen = selectedId.value > 0
    selectedId.value = id
    syncingRoute = true
    try {
      const nextQuery = { ...route.query, [queryKey]: String(id) }
      if (wasOpen) {
        await router.replace({ query: nextQuery })
      } else {
        pushedHistory = true
        await router.push({ query: nextQuery })
      }
    } finally {
      syncingRoute = false
    }
  }

  function closeDrawer() {
    if (selectedId.value === 0 && !parsePositiveInt(route.query[queryKey])) return
    selectedId.value = 0
    if (!parsePositiveInt(route.query[queryKey])) {
      pushedHistory = false
      return
    }
    syncingRoute = true
    if (pushedHistory) {
      pushedHistory = false
      router.back()
      queueMicrotask(() => { syncingRoute = false })
      return
    }
    void router.replace({ query: stripQueryKey() }).finally(() => {
      syncingRoute = false
    })
  }

  watch(
    () => route.query[queryKey],
    (raw) => {
      const nextId = parsePositiveInt(raw)
      if (!syncingRoute && nextId === 0) {
        pushedHistory = false
      }
      if (selectedId.value !== nextId) {
        selectedId.value = nextId
      }
    },
  )

  const selectedItem: ComputedRef<T | null> = computed(() => {
    if (selectedId.value <= 0) return null
    return options.resolveItem(selectedId.value)
  })

  watch(
    [selectedId, selectedItem, () => options.isLoaded()],
    ([id, item, loaded]) => {
      if (id > 0 && loaded && !item) {
        // 主机从列表消失（删除或隐藏）时自动收起空白抽屉；用 replace 清理深链参数，避免退站
        pushedHistory = false
        closeDrawer()
      }
    },
    { immediate: true },
  )

  const drawerOpen = computed({
    get: () => selectedId.value > 0 && Boolean(selectedItem.value),
    set: (open: boolean) => {
      if (!open) closeDrawer()
    },
  })

  return {
    selectedId,
    selectedItem,
    drawerOpen,
    openDrawer,
    closeDrawer,
  }
}

export function useWorldMapOverlay(
  mapDialog: Ref<HTMLDialogElement | undefined>,
  isDrawerOpen: () => boolean,
) {
  const mapOpen = ref(false)
  let previousOverflow: string | null = null

  function lockBodyScroll() {
    if (typeof document === 'undefined' || previousOverflow !== null) return
    previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
  }

  function unlockBodyScroll() {
    if (typeof document === 'undefined' || previousOverflow === null) return
    document.body.style.overflow = previousOverflow
    previousOverflow = null
  }

  function onWindowKeydown(event: KeyboardEvent) {
    if (event.key !== 'Escape' || !mapOpen.value) return
    // 抽屉正开时让抽屉先接管 Esc 关闭；再按一次才关世界地图
    if (isDrawerOpen()) return
    event.preventDefault()
    closeMap()
  }

  function attachMapGuards() {
    lockBodyScroll()
    if (typeof window !== 'undefined') {
      window.addEventListener('keydown', onWindowKeydown, true)
    }
  }

  function detachMapGuards() {
    unlockBodyScroll()
    if (typeof window !== 'undefined') {
      window.removeEventListener('keydown', onWindowKeydown, true)
    }
  }

  function showMap() {
    mapOpen.value = true
    attachMapGuards()
    const dialog = mapDialog.value
    if (dialog && !dialog.open) dialog.show()
  }

  function closeMap() {
    const dialog = mapDialog.value
    if (dialog?.open) dialog.close()
    else onMapClosed()
  }

  function onMapClosed() {
    mapOpen.value = false
    detachMapGuards()
  }

  function onScrimClick() {
    if (isDrawerOpen()) return
    closeMap()
  }

  onBeforeUnmount(detachMapGuards)

  return {
    mapOpen,
    showMap,
    closeMap,
    onMapClosed,
    onScrimClick,
  }
}
