export type PublicThemeId = 'server-status' | 'server-grid' | 'nazhua'

export function normalizePublicTheme(value: unknown): PublicThemeId {
  if (value === 'nazhua') return 'nazhua'
  if (value === 'server-grid') return 'server-grid'
  return 'server-status'
}

export function resolvePublicTheme(options: {
  siteTheme?: unknown
  allowSwitch?: boolean
  stored?: PublicThemeId | null
}): PublicThemeId {
  const site = normalizePublicTheme(options.siteTheme)
  if (options.allowSwitch === false) return site
  return options.stored || site
}
