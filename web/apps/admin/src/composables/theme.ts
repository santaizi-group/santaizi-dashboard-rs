import { ref, watchEffect } from 'vue'
import type { ThemeMode } from '@santaizi/api/types'

const storageKey = 'santaizi-admin-theme'
const mode = ref<ThemeMode>((localStorage.getItem(storageKey) as ThemeMode) || 'system')
const media = window.matchMedia('(prefers-color-scheme: dark)')

function apply() {
  const dark = mode.value === 'dark' || (mode.value === 'system' && media.matches)
  document.documentElement.classList.toggle('dark', dark)
  document.documentElement.style.colorScheme = dark ? 'dark' : 'light'
}
media.addEventListener('change', apply)

export function useTheme() {
  watchEffect(() => { localStorage.setItem(storageKey, mode.value); apply() })
  return { mode, setMode: (next: ThemeMode) => { mode.value = next } }
}
