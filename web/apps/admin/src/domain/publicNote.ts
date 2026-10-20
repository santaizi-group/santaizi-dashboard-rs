import type { PublicNoteForm } from '@/types/admin'

type UnknownObject = Record<string, unknown>

/** Canonical buckets used by nezha-compatible auto-renewal normalization. */
export const BILLING_CYCLE_VALUES = ['月', '季', '半年', '年'] as const
export type BillingCycleValue = (typeof BILLING_CYCLE_VALUES)[number]

/** Autocomplete templates: nezha dashboard cycles + aobobo-style symbols/extensions. */
export const BILLING_CYCLE_SUGGESTIONS = [
  '月', 'month', 'monthly', 'm', 'mo', 'M', '月付',
  '季', 'quarterly', 'q', 'Q', '季付',
  '半年', '半', 'half', 'semi-annually', 'h', 'H',
  '年', 'year', 'annually', 'y', 'yr', 'Y', '年付',
  '1', '3', '6', '12',
  '2年', '3年', '1年半',
  '一次性', '买断', 'onetime',
] as const

// Aliases aligned with nezha-next status getBillingCycle + dashboard suggestions.
const BILLING_CYCLE_ALIASES: Record<string, BillingCycleValue> = {
  月: '月', 每月: '月', 月付: '月', 个月: '月',
  mo: '月', month: '月', months: '月', monthly: '月', m: '月', '1m': '月',
  季: '季', 季度: '季', 季付: '季',
  quarter: '季', quarterly: '季', q: '季', '3m': '季',
  半: '半年', 半年: '半年', 半年付: '半年',
  'half-year': '半年', half: '半年', semiannual: '半年', semiannually: '半年',
  semiannualy: '半年', 'semi-annually': '半年', h: '半年', '6m': '半年',
  年: '年', 年度: '年', 年付: '年',
  yr: '年', year: '年', years: '年', yearly: '年', annually: '年', y: '年', '12m': '年', '1y': '年',
}

/** Map known aliases to 月/季/半年/年; empty stays empty; unknown strings kept as-is. */
export function normalizeBillingCycle(raw: string): string {
  const trimmed = raw.trim()
  if (!trimmed) return ''
  const key = trimmed.toLowerCase()
  return BILLING_CYCLE_ALIASES[key] ?? BILLING_CYCLE_ALIASES[trimmed] ?? trimmed
}

export function isCanonicalBillingCycle(value: string): value is BillingCycleValue {
  return (BILLING_CYCLE_VALUES as readonly string[]).includes(value)
}

/** Filter autocomplete templates (case-insensitive substring). */
export function filterBillingCycleSuggestions(query: string): Array<{ value: string }> {
  const q = query.trim().toLowerCase()
  return BILLING_CYCLE_SUGGESTIONS
    .filter(item => !q || item.toLowerCase().includes(q))
    .map(value => ({ value }))
}

export function emptyPublicNote(): PublicNoteForm {
  return {
    billing: { startDate: '', endDate: '', autoRenewal: '0', cycle: '', amount: '' },
    plan: { bandwidth: '', trafficVol: '', trafficType: '', IPv4: '0', IPv6: '0', networkRoute: [], extra: [] },
    presentation: { location: '', flag: '', orderLink: '', buyBtnText: '', buyBtnIcon: '', slogan: '', lat: '', lng: '', latlng: '', locationLabel: '' },
    unlimitedEnd: false,
  }
}

function object(value: unknown): UnknownObject {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as UnknownObject : {}
}
function cloneJSON(value: UnknownObject): UnknownObject {
  // Public notes are JSON by contract. JSON round-tripping also unwraps Vue's
  // reactive proxies, which structuredClone rejects in browsers.
  return JSON.parse(JSON.stringify(value)) as UnknownObject
}
function text(value: unknown) { return value === undefined || value === null ? '' : String(value) }
function switchValue(value: unknown): '0' | '1' { return value === true || String(value) === '1' ? '1' : '0' }
function tags(value: unknown) {
  if (Array.isArray(value)) return value.map(String).filter(Boolean)
  return text(value).split(',').map(item => item.trim()).filter(Boolean)
}

export function parsePublicNote(raw: string): { form: PublicNoteForm; base: UnknownObject; error: string } {
  const form = emptyPublicNote()
  if (!raw.trim()) return { form, base: {}, error: '' }
  try {
    const base = object(JSON.parse(raw))
    const billing = object(base.billingDataMod)
    form.billing = {
      startDate: text(billing.startDate),
      endDate: text(billing.endDate) === '0000-00-00' ? '' : text(billing.endDate),
      autoRenewal: switchValue(billing.autoRenewal),
      cycle: text(billing.cycle),
      amount: text(billing.amount),
    }
    form.unlimitedEnd = text(billing.endDate) === '0000-00-00'
    const plan = object(base.planDataMod)
    form.plan = {
      bandwidth: text(plan.bandwidth), trafficVol: text(plan.trafficVol), trafficType: text(plan.trafficType),
      IPv4: switchValue(plan.IPv4), IPv6: switchValue(plan.IPv6), networkRoute: tags(plan.networkRoute), extra: tags(plan.extra),
    }
    const custom = object(base.customData)
    for (const key of Object.keys(form.presentation) as Array<keyof typeof form.presentation>) form.presentation[key] = text(custom[key])
    return { form, base: cloneJSON(base), error: '' }
  } catch (error) {
    return { form, base: {}, error: error instanceof Error ? error.message : String(error) }
  }
}

function compact(value: UnknownObject) {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => {
    if (Array.isArray(item)) return item.length > 0
    return item !== '' && item !== undefined && item !== null
  }))
}

export function serializePublicNote(form: PublicNoteForm, base: UnknownObject): string {
  const result = cloneJSON(base)
  const billingBase = object(result.billingDataMod)
  const billing = compact({
    ...billingBase,
    startDate: form.billing.startDate,
    endDate: form.unlimitedEnd ? '0000-00-00' : form.billing.endDate,
    autoRenewal: form.billing.autoRenewal,
    cycle: form.billing.cycle,
    amount: form.billing.amount,
  })
  if (Object.keys(billing).some(key => key !== 'autoRenewal') || form.billing.autoRenewal === '1' || 'autoRenewal' in billingBase) result.billingDataMod = billing
  else delete result.billingDataMod

  const planBase = object(result.planDataMod)
  const plan = compact({
    ...planBase,
    bandwidth: form.plan.bandwidth, trafficVol: form.plan.trafficVol, trafficType: form.plan.trafficType,
    IPv4: form.plan.IPv4, IPv6: form.plan.IPv6,
    networkRoute: form.plan.networkRoute.join(','), extra: form.plan.extra.join(','),
  })
  if (Object.keys(plan).some(key => !['IPv4', 'IPv6'].includes(key)) || form.plan.IPv4 === '1' || form.plan.IPv6 === '1' || 'IPv4' in planBase || 'IPv6' in planBase) result.planDataMod = plan
  else delete result.planDataMod

  const custom = compact({ ...object(result.customData), ...form.presentation })
  if (Object.keys(custom).length) result.customData = custom
  else delete result.customData
  return Object.keys(result).length ? JSON.stringify(result, null, 2) : ''
}
