import { resolveFlagCode, resolveRegionLabel } from './regionDisplay'

type UnknownObject = Record<string, unknown>

export type RemainingTone = 'danger' | 'warning' | 'success' | ''
export type RemainingKind = 'infinity' | 'days' | 'expired' | ''
export type AmountKind = 'metered' | 'free' | 'priced' | ''
export type TrafficType = '1' | '2' | '3' | ''

export interface PublicPresentation {
  slogan: string
  locationLabel: string
  location: string
  flag: string
  orderLink: string
  buyBtnText: string
  buyBtnIcon: string
}

export interface BillAndPlanView {
  amountKind: AmountKind
  amountValue: string
  cycleLabel: string
  remainingKind: RemainingKind
  remainingDays: number | null
  remainingTone: RemainingTone
  bandwidth: string
  trafficVol: string
  trafficType: TrafficType
}

export interface PublicNoteView {
  presentation: PublicPresentation
  bill: BillAndPlanView
  planTags: string[]
  hasBillMeta: boolean
  hasPlanMeta: boolean
  hasBuy: boolean
  hasDetail: boolean
}

const EMPTY_PRESENTATION: PublicPresentation = {
  slogan: '',
  locationLabel: '',
  location: '',
  flag: '',
  orderLink: '',
  buyBtnText: '',
  buyBtnIcon: '',
}

const EMPTY_BILL: BillAndPlanView = {
  amountKind: '',
  amountValue: '',
  cycleLabel: '',
  remainingKind: '',
  remainingDays: null,
  remainingTone: '',
  bandwidth: '',
  trafficVol: '',
  trafficType: '',
}

function object(value: unknown): UnknownObject {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as UnknownObject : {}
}

function text(value: unknown) {
  return value === undefined || value === null ? '' : String(value).trim()
}

function isSet(value: unknown) {
  return text(value) !== ''
}

function parseNoteRoot(raw: unknown): UnknownObject {
  if (!raw) return {}
  if (typeof raw === 'string') {
    const trimmed = raw.trim()
    if (!trimmed) return {}
    try {
      return object(JSON.parse(trimmed))
    } catch {
      return { text: trimmed }
    }
  }
  return object(raw)
}

export function getCycleMonths(cycle: unknown): number {
  const cycleStr = text(cycle)
  if (!cycleStr) return 1
  if (typeof cycle === 'number' && Number.isFinite(cycle) && cycle > 0) return cycle
  const asNum = Number(cycleStr)
  if (String(asNum) === cycleStr && Number.isFinite(asNum) && asNum > 0) return asNum

  const lower = cycleStr.toLowerCase()
  const yearMatch = lower.match(/^(\d+(?:\.\d+)?)\s*(?:年|y(?:r)?|year(?:s)?|annual)(?:付)?$/)
  if (yearMatch) return Math.round(Number(yearMatch[1]) * 12)
  const yearHalf = lower.match(/^(\d+(?:\.\d+)?)\s*年\s*半(?:付)?$/)
  if (yearHalf) return Math.round(Number(yearHalf[1]) * 12) + 6
  const quarter = lower.match(/^(\d+(?:\.\d+)?)\s*(?:季|quarter(?:ly)?)(?:付)?$/)
  if (quarter) return Math.round(Number(quarter[1]) * 3)
  const half = lower.match(/^(\d+(?:\.\d+)?)\s*(?:半年|half|semi-annually)(?:付)?$/)
  if (half) return Math.round(Number(half[1]) * 6)
  const month = lower.match(/^(\d+(?:\.\d+)?)\s*(?:个?)(?:月|m(?:o)?|month(?:ly)?)(?:付)?$/)
  if (month) return Math.round(Number(month[1]))

  switch (lower) {
    case '年':
    case 'y':
    case 'yr':
    case 'year':
    case 'annual':
    case 'annually':
    case '年付':
      return 12
    case '季':
    case 'quarterly':
    case 'q':
    case '季付':
      return 3
    case '半':
    case '半年':
    case 'h':
    case 'half':
    case 'semi-annually':
    case '半年付':
      return 6
    case '月':
    case 'm':
    case 'mo':
    case 'month':
    case 'monthly':
    case '月付':
    default:
      return 1
  }
}

export function getCycleLabel(cycle: unknown): string {
  const cycleStr = text(cycle)
  if (!cycleStr) return ''
  const lower = cycleStr.toLowerCase()
  if (['一次性', '买断', 'onetime', 'one-time', 'one time'].includes(lower)) return 'cycleOnetime'
  switch (lower) {
    case '月':
    case 'm':
    case 'mo':
    case 'month':
    case 'monthly':
    case '月付':
      return 'cycleMonth'
    case '年':
    case 'y':
    case 'yr':
    case 'year':
    case 'annual':
    case 'annually':
    case '年付':
      return 'cycleYear'
    case '季':
    case 'quarterly':
    case 'q':
    case '季付':
      return 'cycleQuarter'
    case '半':
    case '半年':
    case 'h':
    case 'half':
    case 'semi-annually':
    case '半年付':
      return 'cycleHalfYear'
    default:
      return cycleStr
  }
}

export function isInfinityEndDate(endDate: unknown) {
  const value = text(endDate)
  return value === '0000-00-00' || value.startsWith('0000-00-00')
}

export function isAutoRenewalEnabled(autoRenewal: unknown) {
  return autoRenewal === true || text(autoRenewal) === '1'
}

function addMonths(date: Date, months: number) {
  const next = new Date(date.getTime())
  const day = next.getDate()
  next.setMonth(next.getMonth() + months)
  if (next.getDate() < day) next.setDate(0)
  return next
}

/** Advance from start by `months` until strictly after `now`. */
export function getNextCycleTime(startMs: number, months: number, nowMs: number) {
  if (!Number.isFinite(startMs) || months <= 0) return nowMs
  let next = new Date(startMs)
  let guard = 0
  while (next.getTime() <= nowMs && guard < 1200) {
    next = addMonths(next, months)
    guard += 1
  }
  return next.getTime()
}

function daysUntil(endMs: number, nowMs = Date.now()) {
  return Math.floor((endMs - nowMs) / 86_400_000) + 1
}

function remainingTone(kind: RemainingKind, days: number | null): RemainingTone {
  if (kind === 'expired') return 'danger'
  if (kind === 'infinity') return 'success'
  if (kind === 'days' && days !== null) {
    if (days <= 7) return 'danger'
    if (days <= 30) return 'warning'
    return 'success'
  }
  return ''
}

export function getPresentation(note: unknown): PublicPresentation {
  const root = parseNoteRoot(note)
  const custom = object(root.customData)
  const fallback = object(root.presentation)
  const source = Object.keys(custom).length ? custom : fallback
  return {
    slogan: text(source.slogan),
    locationLabel: text(source.locationLabel),
    location: text(source.location),
    flag: text(source.flag).toLowerCase(),
    // 直接存收敛后的链接：`hasBuy` 由此派生，非法协议连按钮都不会渲染。
    orderLink: decodeOrderLink(text(source.orderLink)),
    buyBtnText: text(source.buyBtnText),
    buyBtnIcon: safeBuyBtnIcon(source.buyBtnIcon),
  }
}

export function getBillAndPlan(note: unknown, nowMs = Date.now()): BillAndPlanView {
  const root = parseNoteRoot(note)
  const billing = object(root.billingDataMod)
  const plan = object(root.planDataMod)
  const result: BillAndPlanView = { ...EMPTY_BILL }

  const cycleLabel = getCycleLabel(billing.cycle)
  const months = getCycleMonths(billing.cycle)
  result.cycleLabel = cycleLabel

  if (isSet(billing.amount)) {
    const amount = text(billing.amount)
    if (amount === '-1') {
      result.amountKind = 'metered'
      result.amountValue = '-1'
    } else if (amount === '0') {
      result.amountKind = 'free'
      result.amountValue = '0'
    } else {
      result.amountKind = 'priced'
      result.amountValue = amount
    }
  }

  if (isSet(billing.endDate)) {
    const endDate = text(billing.endDate)
    if (isInfinityEndDate(endDate)) {
      result.remainingKind = 'infinity'
      result.remainingDays = null
    } else {
      const endMs = Date.parse(endDate)
      if (!Number.isNaN(endMs)) {
        if (isAutoRenewalEnabled(billing.autoRenewal)) {
          const targetMs = endMs > nowMs ? endMs : getNextCycleTime(endMs, months, nowMs)
          const days = daysUntil(targetMs, nowMs)
          result.remainingKind = 'days'
          result.remainingDays = days
        } else if (endMs > nowMs) {
          result.remainingKind = 'days'
          result.remainingDays = daysUntil(endMs, nowMs)
        } else {
          result.remainingKind = 'expired'
          result.remainingDays = null
        }
      }
    }
  }
  result.remainingTone = remainingTone(result.remainingKind, result.remainingDays)

  result.bandwidth = text(plan.bandwidth)
  result.trafficVol = text(plan.trafficVol)
  const trafficType = text(plan.trafficType)
  if (trafficType === '1' || trafficType === '2' || trafficType === '3') {
    result.trafficType = trafficType
  }

  return result
}

export function getPlanTags(note: unknown): string[] {
  const root = parseNoteRoot(note)
  const plan = object(root.planDataMod)
  const tags: string[] = []
  for (const part of text(plan.networkRoute).split(',')) {
    const item = part.trim()
    if (item) tags.push(item)
  }
  for (const part of text(plan.extra).split(',')) {
    const item = part.trim()
    if (item) tags.push(item)
  }
  const v4 = text(plan.IPv4) === '1'
  const v6 = text(plan.IPv6) === '1'
  if (v4 && v6) tags.push('__dual_stack__')
  else if (v4) tags.push('__ipv4_only__')
  else if (v6) tags.push('__ipv6_only__')
  return tags
}

/// 购买按钮图标默认值（Remix，见 AGENTS.md §4.5：操作图标只用 Remix）。
const DEFAULT_BUY_ICON = 'ri-shopping-bag-3-line'

/// 收敛购买按钮图标类名。
///
/// 该值存在公开站可见的 `public_note` 里，经 `el-select` 的 allow-create
/// 可以填进任意字符串，最终原样进 `<i :class>`。虽然 Vue 会把它关在 class
/// 属性内（不会 breakout 成属性注入），但任意类名足以借既有 CSS 改布局、
/// 或者借将来新增的类制造意外效果。这里只放行 Remix 图标类名形态。
export function safeBuyBtnIcon(raw: unknown): string {
  const value = typeof raw === 'string' ? raw.trim() : ''
  if (!/^ri-[a-z0-9-]+$/.test(value)) return DEFAULT_BUY_ICON
  return value
}

/// 购买链接允许的协议白名单。
///
/// 这个值来自公开站可见的 `public_note.customData`，运营/被拿下账号都能改。
/// 不限协议时 `javascript:` / `data:` / `vbscript:` 会被原样塞进 `href`，
/// 访客点「购买」即执行脚本——公开站 XSS，不需要任何登录态。
const ORDER_LINK_PROTOCOLS = ['http:', 'https:']

/// 判断链接是否可安全地当 `href` 用。
///
/// 站内相对路径（`/`、`./`、`../` 开头，或不含协议分隔符）一律放行，
/// 这样换域名/换 CDN 不用改代码；带协议的只认白名单。
export function isSafeOrderLink(raw: string): boolean {
  const link = raw.trim()
  if (!link) return false
  // 先剥掉可能藏在空白/控制字符里的协议（`java\nscript:` 之类的老套路）。
  const compact = link.replace(/[\u0000-\u0020\u007f]/g, '')
  // `scheme[0]` 含结尾冒号，与白名单条目同形，直接比。
  let scheme = compact.match(/^([a-z][a-z0-9+.-]*):/i)
  if (!scheme) return true // 相对路径
  return ORDER_LINK_PROTOCOLS.includes((scheme[0] ?? '').toLowerCase())
}

/// 解码并收敛购买链接：百分号解码 + 协议白名单。
///
/// 不合法的链接一律返回空串——调用方用空串即可让按钮不渲染，
/// 比留一个「点了没反应」的死链更诚实。
export function decodeOrderLink(raw: string) {
  if (!raw) return ''
  let decoded = raw
  try {
    decoded = decodeURIComponent(raw)
  } catch {
    decoded = raw
  }
  return isSafeOrderLink(decoded) ? decoded.trim() : ''
}

export function flagCode(note: unknown, countryCode?: unknown) {
  const presentation = getPresentation(note)
  return resolveFlagCode(presentation.flag, presentation.location, countryCode)
}

export function buildPublicNoteView(note: unknown, nowMs = Date.now()): PublicNoteView {
  const presentation = getPresentation(note)
  const bill = getBillAndPlan(note, nowMs)
  const planTags = getPlanTags(note)
  const hasBillMeta = Boolean(bill.amountKind || bill.remainingKind)
  const hasPlanMeta = Boolean(bill.bandwidth || bill.trafficVol || planTags.length)
  const hasBuy = Boolean(presentation.orderLink)
  return {
    presentation,
    bill,
    planTags,
    hasBillMeta,
    hasPlanMeta,
    hasBuy,
    hasDetail: hasBillMeta || hasPlanMeta || hasBuy,
  }
}

export function publicSubtitle(note: unknown) {
  const { slogan, locationLabel } = getPresentation(note)
  return slogan || locationLabel
}

export function publicLocation(note: unknown, countryCode?: unknown, locale = 'zh-CN') {
  const { locationLabel, location } = getPresentation(note)
  return resolveRegionLabel(locationLabel, location, countryCode, locale)
}

export { EMPTY_BILL, EMPTY_PRESENTATION }
