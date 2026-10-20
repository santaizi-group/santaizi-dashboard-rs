import { describe, expect, it } from 'vitest'
import { normalizePublicTheme, resolvePublicTheme } from '../themeResolution'

describe('public theme resolution', () => {
  it('normalizes unsupported values to ServerStatus', () => {
    expect(normalizePublicTheme('nazhua')).toBe('nazhua')
    expect(normalizePublicTheme('server-grid')).toBe('server-grid')
    expect(normalizePublicTheme('remote-theme')).toBe('server-status')
  })

  it('honors the site theme when visitor switching is disabled', () => {
    expect(resolvePublicTheme({ siteTheme: 'nazhua', allowSwitch: false, stored: 'server-status' })).toBe('nazhua')
  })

  it('uses a saved visitor selection when switching is allowed', () => {
    expect(resolvePublicTheme({ siteTheme: 'server-status', allowSwitch: true, stored: 'nazhua' })).toBe('nazhua')
    expect(resolvePublicTheme({ siteTheme: 'server-status', allowSwitch: true, stored: 'server-grid' })).toBe('server-grid')
  })
})
