import { describe, expect, it } from 'vitest'
import { reactive } from 'vue'
import {
  BILLING_CYCLE_SUGGESTIONS,
  filterBillingCycleSuggestions,
  normalizeBillingCycle,
  parsePublicNote,
  serializePublicNote,
} from './publicNote'

describe('structured public note', () => {
  it('preserves unknown fields while updating known values', () => {
    const parsed = parsePublicNote('{"extension":{"keep":true},"billingDataMod":{"amount":"9","vendor":"x"}}')
    parsed.form.billing.amount = '12'
    const value = JSON.parse(serializePublicNote(parsed.form, parsed.base))
    expect(value.extension).toEqual({ keep: true })
    expect(value.billingDataMod.vendor).toBe('x')
    expect(value.billingDataMod.amount).toBe('12')
  })

  it('round trips tags and unlimited dates', () => {
    const parsed = parsePublicNote('{"billingDataMod":{"endDate":"0000-00-00"},"planDataMod":{"networkRoute":"CN2,CMI"}}')
    expect(parsed.form.unlimitedEnd).toBe(true)
    expect(parsed.form.plan.networkRoute).toEqual(['CN2', 'CMI'])
    expect(JSON.parse(serializePublicNote(parsed.form, parsed.base)).billingDataMod.endDate).toBe('0000-00-00')
  })

  it('serializes reactive editor state without a clone error', () => {
    const parsed = parsePublicNote('{"extension":{"keep":true}}')
    const form = reactive(parsed.form)
    const base = reactive(parsed.base)
    form.presentation.location = 'CN'

    expect(JSON.parse(serializePublicNote(form, base))).toMatchObject({
      extension: { keep: true },
      customData: { location: 'CN' },
    })
  })
})

describe('billing cycle aliases', () => {
  it('normalizes nezha aliases to 月/季/半年/年', () => {
    expect(normalizeBillingCycle('monthly')).toBe('月')
    expect(normalizeBillingCycle('M')).toBe('月')
    expect(normalizeBillingCycle('3m')).toBe('季')
    expect(normalizeBillingCycle('semi-annually')).toBe('半年')
    expect(normalizeBillingCycle('1y')).toBe('年')
    expect(normalizeBillingCycle('年付')).toBe('年')
  })

  it('keeps unknown free-text values as-is', () => {
    expect(normalizeBillingCycle('lifetime')).toBe('lifetime')
    expect(normalizeBillingCycle('一次性')).toBe('一次性')
  })

  it('exposes autocomplete templates and filters them', () => {
    expect(BILLING_CYCLE_SUGGESTIONS.length).toBeGreaterThan(10)
    expect(BILLING_CYCLE_SUGGESTIONS).toContain('月')
    expect(BILLING_CYCLE_SUGGESTIONS).toContain('买断')
    expect(filterBillingCycleSuggestions('').map(item => item.value)).toEqual([...BILLING_CYCLE_SUGGESTIONS])
    expect(filterBillingCycleSuggestions('年').map(item => item.value)).toEqual(
      expect.arrayContaining(['年', '年付', '2年', '3年', '1年半']),
    )
    expect(filterBillingCycleSuggestions('mo').map(item => item.value)).toEqual(
      expect.arrayContaining(['month', 'monthly', 'mo']),
    )
  })
})
