import { onBeforeUnmount, onMounted, ref, type Ref } from 'vue'

export function useInView(
  target: Ref<HTMLElement | undefined>,
  options?: { loadMargin?: string; keepMargin?: string },
) {
  const load = ref(false)
  const inView = ref(false)
  const keep = ref(false)
  let loadObserver: IntersectionObserver | undefined
  let keepObserver: IntersectionObserver | undefined

  onMounted(() => {
    const node = target.value
    if (!node || typeof IntersectionObserver === 'undefined') {
      load.value = true
      inView.value = true
      keep.value = true
      return
    }
    loadObserver = new IntersectionObserver(([entry]) => {
      const visible = Boolean(entry?.isIntersecting)
      inView.value = visible
      if (visible) load.value = true
    }, { rootMargin: options?.loadMargin ?? '200px' })
    keepObserver = new IntersectionObserver(([entry]) => {
      keep.value = Boolean(entry?.isIntersecting)
    }, { rootMargin: options?.keepMargin ?? '200%' })
    loadObserver.observe(node)
    keepObserver.observe(node)
  })

  onBeforeUnmount(() => {
    loadObserver?.disconnect()
    keepObserver?.disconnect()
  })

  return { load, inView, keep }
}
