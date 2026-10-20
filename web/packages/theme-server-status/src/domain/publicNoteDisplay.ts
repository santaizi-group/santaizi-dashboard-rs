import type { PublicNoteView } from './publicNoteView'

type Translate = (key: string, values?: Record<string, unknown>) => string
type TranslateExists = (key: string) => boolean

export function localizeCycle(cycleLabel: string, t: Translate, te: TranslateExists) {
  if (!cycleLabel) return ''
  return te(cycleLabel) ? t(cycleLabel) : cycleLabel
}

export function billingText(view: PublicNoteView, t: Translate, te: TranslateExists) {
  const { amountKind, amountValue, cycleLabel } = view.bill
  const cycle = localizeCycle(cycleLabel, t, te)
  if (amountKind === 'metered') return cycle ? `${t('everyCycle', { cycle })} ${t('meteredBilling')}` : t('meteredBilling')
  if (amountKind === 'free') return t('freeBilling')
  if (amountKind === 'priced') {
    if (cycleLabel === 'cycleOnetime') return amountValue
    return cycle ? `${amountValue} · ${t('cyclePay', { cycle })}` : amountValue
  }
  return ''
}

export function remainingText(view: PublicNoteView, t: Translate) {
  const { remainingKind, remainingDays } = view.bill
  if (remainingKind === 'infinity') return t('foreverValid')
  if (remainingKind === 'expired') return t('expired')
  if (remainingKind === 'days' && remainingDays !== null) return t('remainingDays', { n: remainingDays })
  return ''
}

export function trafficQuotaText(view: PublicNoteView, t: Translate) {
  if (!view.bill.trafficVol) return ''
  const type = Number(view.bill.trafficType)
  const typeKey = type === 1
    ? 'trafficOneWayOut'
    : type === 3
      ? 'trafficOneWayMax'
      : 'trafficBidirectionalQuota'
  return `${t(typeKey)} ${view.bill.trafficVol}`
}

export function planTagLabel(tag: string, t: Translate) {
  if (tag === '__dual_stack__') return t('dualStack')
  if (tag === '__ipv4_only__') return t('ipv4Only')
  if (tag === '__ipv6_only__') return t('ipv6Only')
  return tag
}

export function trafficUsageText(
  kind: string,
  valueLabel: string,
  t: Translate,
) {
  if (kind === 'cycle') return t('trafficRemaining', { value: valueLabel })
  if (kind === 'out') return t('trafficOutUsed', { value: valueLabel })
  if (kind === 'both') return t('trafficBothUsed', { value: valueLabel })
  if (kind === 'maxOut') return t('trafficMaxOutUsed', { value: valueLabel })
  if (kind === 'maxIn') return t('trafficMaxInUsed', { value: valueLabel })
  return t('trafficUnlimited')
}

export function cycleStatusLabel(level: string, t: Translate) {
  if (level === 'fine') return t('cycleStatusFine')
  if (level === 'warning') return t('cycleStatusWarning')
  if (level === 'alert') return t('cycleStatusAlert')
  if (level === 'over') return t('cycleStatusOver')
  return t('cycleStatusNeutral')
}
