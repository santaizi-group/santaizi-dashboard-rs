import { describe, expect, it } from 'vitest'
import {
  resolveStatusNoteColumns,
  statusTableLayout,
} from '@santaizi/theme-server-status'

describe('resolveStatusNoteColumns', () => {
  it('hides location, price and remaining when nothing is filled', () => {
    expect(resolveStatusNoteColumns([{ public_note: {}, host: {} }])).toEqual({
      location: false,
      price: false,
      remaining: false,
    })
  })

  it('shows location from country code even without a public note', () => {
    expect(resolveStatusNoteColumns([{ public_note: {}, host: { CountryCode: 'US' } }]).location).toBe(true)
  })

  it('shows price and remaining independently', () => {
    const now = Date.parse('2026-08-14T00:00:00Z')
    expect(resolveStatusNoteColumns([{
      public_note: { billingDataMod: { amount: '12USD', cycle: '月' } },
      host: {},
    }]).price).toBe(true)
    expect(resolveStatusNoteColumns([{
      public_note: { billingDataMod: { endDate: '0000-00-00' } },
      host: {},
    }], now).remaining).toBe(true)
  })
})

describe('statusTableLayout', () => {
  it('drops optional tracks when those columns are hidden', () => {
    const all = statusTableLayout({
      location: true, price: true, availability: true, remaining: true,
    })
    const none = statusTableLayout({
      location: false, price: false, availability: false, remaining: false,
    })
    expect(all.columns).toContain('80px')
    expect(all.columns).toContain('72px')
    expect(none.columns).toContain('72px')
    expect(none.columns).not.toContain('80px')
    expect(none.count).toBe(all.count - 4)
    expect(none.minWidth).toBeLessThan(all.minWidth)
  })
})
