<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useData, withBase } from 'vitepress'

type RedocApi = {
  init: (
    specOrUrl: string,
    options: Record<string, unknown>,
    element: HTMLElement,
    callback?: (error?: Error) => void,
  ) => void
  destroy?: (element?: HTMLElement | null) => void
}

const { isDark } = useData()
const shell = ref<HTMLElement | null>(null)
const host = ref<HTMLElement | null>(null)
const loading = ref(true)
const loadError = ref('')
let generation = 0
let scriptPromise: Promise<RedocApi> | null = null
let mountedReady = false

/** Desktop keeps three columns; only true phone widths stack. Use px to avoid rem/root-font jitter. */
const BREAKPOINTS = {
  small: '640px',
  medium: '800px',
  large: '1200px',
} as const

function navOffsetPx(): number {
  const nav =
    document.querySelector<HTMLElement>('.VPNav') ||
    document.querySelector<HTMLElement>('.VPNavBar')
  const height = nav?.getBoundingClientRect().height
  return height && height > 0 ? Math.ceil(height) : 64
}

function applyNavOffset() {
  const offset = navOffsetPx()
  const root = shell.value || document.querySelector<HTMLElement>('.api-reference-page')
  if (root) {
    root.style.setProperty('--sz-docs-nav-offset', `${offset}px`)
  }
  document.documentElement.style.setProperty('--sz-docs-nav-offset', `${offset}px`)
  return offset
}

function waitFrames(count = 2): Promise<void> {
  return new Promise((resolve) => {
    const step = (left: number) => {
      if (left <= 0) {
        resolve()
        return
      }
      requestAnimationFrame(() => step(left - 1))
    }
    step(count)
  })
}

async function waitForHostWidth(el: HTMLElement, gen: number): Promise<void> {
  for (let i = 0; i < 40; i++) {
    if (gen !== generation) return
    // Require a real desktop-ish width before init so layout CSS is stable.
    if (el.clientWidth >= 320) return
    await waitFrames(1)
  }
}

function destroyRedoc(el: HTMLElement | null | undefined) {
  if (!el) return
  const w = window as Window & { Redoc?: RedocApi }
  try {
    w.Redoc?.destroy?.(el)
  } catch {
    /* ignore stale roots */
  }
  // Drop React 18 root markers so the next createRoot(init) is clean.
  for (const key of Object.keys(el)) {
    if (key.startsWith('__react') || key.startsWith('_react')) {
      try {
        delete (el as unknown as Record<string, unknown>)[key]
      } catch {
        /* ignore non-configurable */
      }
    }
  }
  el.innerHTML = ''
}

function themeFor(dark: boolean) {
  if (dark) {
    return {
      breakpoints: { ...BREAKPOINTS },
      sidebar: {
        width: '260px',
        backgroundColor: '#111827',
        textColor: '#d0d7e4',
        activeTextColor: '#60a5fa',
      },
      rightPanel: {
        width: '40%',
        backgroundColor: '#0b1120',
        textColor: '#f8fafc',
      },
      colors: {
        primary: { main: '#60a5fa' },
        success: { main: '#34d399' },
        warning: { main: '#fbbf24' },
        error: { main: '#f87171' },
        text: { primary: '#f8fafc', secondary: '#d0d7e4' },
        border: { dark: '#1f2a3d', light: '#243044' },
        http: {
          get: '#34d399',
          post: '#60a5fa',
          put: '#fbbf24',
          patch: '#22d3ee',
          delete: '#f87171',
        },
      },
      typography: {
        fontSize: '14px',
        fontFamily: 'var(--sz-font-family, system-ui, sans-serif)',
        headings: {
          fontFamily: 'var(--sz-font-family, system-ui, sans-serif)',
        },
        code: {
          fontSize: '13px',
        },
      },
    }
  }
  return {
    breakpoints: { ...BREAKPOINTS },
    sidebar: {
      width: '260px',
      backgroundColor: '#ffffff',
      textColor: '#344054',
      activeTextColor: '#1d4ed8',
    },
    rightPanel: {
      width: '40%',
      backgroundColor: '#0f172a',
      textColor: '#f8fafc',
    },
    colors: {
      primary: { main: '#2563eb' },
      success: { main: '#059669' },
      warning: { main: '#d97706' },
      error: { main: '#dc2626' },
      text: { primary: '#172033', secondary: '#3f4b63' },
      border: { dark: '#e4e9f2', light: '#eef1f6' },
      http: {
        get: '#059669',
        post: '#2563eb',
        put: '#d97706',
        patch: '#0891b2',
        delete: '#dc2626',
      },
    },
    typography: {
      fontSize: '14px',
      fontFamily: 'var(--sz-font-family, system-ui, sans-serif)',
      headings: {
        fontFamily: 'var(--sz-font-family, system-ui, sans-serif)',
      },
      code: {
        fontSize: '13px',
      },
    },
  }
}

function redocOptions(dark: boolean) {
  return {
    scrollYOffset: () => navOffsetPx(),
    hideDownloadButton: false,
    expandResponses: '200,201',
    pathInMiddlePanel: true,
    hideHostname: false,
    nativeScrollbars: true,
    theme: themeFor(dark),
  }
}

function loadRedoc(): Promise<RedocApi> {
  const w = window as Window & { Redoc?: RedocApi }
  if (w.Redoc?.init) return Promise.resolve(w.Redoc)
  if (scriptPromise) return scriptPromise

  scriptPromise = new Promise<RedocApi>((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>('script[data-sz-redoc]')
    if (existing && w.Redoc?.init) {
      resolve(w.Redoc)
      return
    }
    const script = document.createElement('script')
    script.src = withBase('/redoc.standalone.js')
    script.async = true
    script.dataset.szRedoc = '1'
    script.onload = () => {
      if (w.Redoc?.init) resolve(w.Redoc)
      else reject(new Error('Redoc 未能初始化'))
    }
    script.onerror = () => reject(new Error('无法加载 Redoc 脚本'))
    document.head.appendChild(script)
  }).catch((err) => {
    scriptPromise = null
    throw err
  })

  return scriptPromise
}

async function mountRedoc() {
  const gen = ++generation
  loadError.value = ''
  loading.value = true
  await nextTick()
  applyNavOffset()
  await waitFrames(2)
  const el = host.value
  if (!el) return

  destroyRedoc(el)
  await waitForHostWidth(el, gen)
  if (gen !== generation) return

  try {
    const Redoc = await loadRedoc()
    if (gen !== generation) return
    applyNavOffset()
    // Capture appearance after VitePress hydrates localStorage theme.
    const dark = isDark.value
    await new Promise<void>((resolve, reject) => {
      Redoc.init('/openapi/v2.yaml', redocOptions(dark), el, (error?: Error) => {
        if (error) reject(error)
        else resolve()
      })
    })
    if (gen !== generation) return
    loading.value = false
    await waitFrames(1)
    applyNavOffset()
    window.dispatchEvent(new Event('resize'))
  } catch (err) {
    if (gen !== generation) return
    loading.value = false
    loadError.value = err instanceof Error ? err.message : '无法加载接口文档'
  }
}

onMounted(async () => {
  window.addEventListener('resize', applyNavOffset)
  // Let VitePress appearance settle before first init to avoid dark/light double-mount.
  await nextTick()
  await waitFrames(3)
  mountedReady = true
  void mountRedoc()
})

watch(isDark, () => {
  if (!mountedReady) return
  void mountRedoc()
})

onBeforeUnmount(() => {
  generation += 1
  mountedReady = false
  window.removeEventListener('resize', applyNavOffset)
  destroyRedoc(host.value)
})
</script>

<template>
  <div ref="shell" class="api-reference-shell">
    <div v-if="loadError" class="api-reference-status is-error">
      <p>{{ loadError }}</p>
      <a href="/openapi/v2.yaml">直接打开 /openapi/v2.yaml</a>
    </div>
    <div v-else-if="loading" class="api-reference-status is-overlay">正在加载接口规范…</div>
    <div ref="host" class="redoc-host" />
  </div>
</template>
