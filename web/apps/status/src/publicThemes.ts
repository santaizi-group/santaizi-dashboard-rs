import { ref, type Component } from 'vue'
import {
  HomePage as ServerGridHome,
  Shell as ServerGridShell,
} from '@santaizi/theme-server-grid'
import {
  HomePage as ServerStatusHome,
  NetworkPage as ServerStatusNetwork,
  ServicesPage as ServerStatusServices,
  Shell as ServerStatusShell,
} from '@santaizi/theme-server-status'
import {
  DetailPage as NazhuaDetail,
  HomePage as NazhuaHome,
  Shell as NazhuaShell,
} from '@santaizi/theme-nazhua'
import { normalizePublicTheme, resolvePublicTheme, type PublicThemeId } from './themeResolution'

export { normalizePublicTheme, resolvePublicTheme, type PublicThemeId } from './themeResolution'

export interface PublicThemeDefinition {
  id: PublicThemeId
  Shell: Component
  Home: Component
  Detail?: Component
  Services: Component
  Network: Component
}

const PUBLIC_THEME_STORAGE = 'santaizi-public-theme'

const definitions: Record<PublicThemeId, PublicThemeDefinition> = {
  'server-status': {
    id: 'server-status',
    Shell: ServerStatusShell,
    Home: ServerStatusHome,
    Services: ServerStatusServices,
    Network: ServerStatusNetwork,
  },
  'server-grid': {
    id: 'server-grid',
    Shell: ServerGridShell,
    Home: ServerGridHome,
    Services: ServerStatusServices,
    Network: ServerStatusNetwork,
  },
  nazhua: {
    id: 'nazhua',
    Shell: NazhuaShell,
    Home: NazhuaHome,
    Detail: NazhuaDetail,
    Services: ServerStatusServices,
    Network: ServerStatusNetwork,
  },
}

export const activePublicTheme = ref<PublicThemeId>(
  normalizePublicTheme(document.documentElement.dataset.publicTheme || localStorage.getItem(PUBLIC_THEME_STORAGE)),
)

export function readStoredPublicTheme(): PublicThemeId | null {
  const raw = localStorage.getItem(PUBLIC_THEME_STORAGE)
  if (raw === 'nazhua' || raw === 'server-status' || raw === 'server-grid') return raw
  return null
}

export function writeStoredPublicTheme(theme: PublicThemeId) {
  localStorage.setItem(PUBLIC_THEME_STORAGE, theme)
}

export function getActivePublicTheme() {
  return activePublicTheme.value
}

export function setActivePublicTheme(theme: PublicThemeId) {
  const next = normalizePublicTheme(theme)
  document.documentElement.dataset.publicTheme = next
  activePublicTheme.value = next
  return next
}

export function getPublicThemeDefinition(theme = getActivePublicTheme()) {
  return definitions[normalizePublicTheme(theme)]
}

export const publicThemeDefinitions = definitions
