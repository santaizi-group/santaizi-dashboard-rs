import { describe, expect, it } from 'vitest'
import {
  buildPublicNoteView,
  decodeOrderLink,
  flagCode,
  getBillAndPlan,
  getCycleMonths,
  getPresentation,
  isSafeOrderLink,
  getNextCycleTime,
  getPlanTags,
  publicLocation,
  publicSubtitle,
  safeBuyBtnIcon,
} from '@santaizi/theme-server-status'

const sample = {
  billingDataMod: {
    startDate: '2024-10-01',
    endDate: '2024-11-01',
    autoRenewal: '0',
    cycle: '月',
    amount: '$3.99',
  },
  planDataMod: {
    bandwidth: '30Mbps',
    trafficVol: '1TB/月',
    trafficType: '1',
    IPv4: '1',
    IPv6: '1',
    networkRoute: 'CN2,GIA',
    extra: '传家宝',
  },
  customData: {
    location: 'HKG',
    slogan: '香港节点',
    orderLink: 'https%3A%2F%2Fbuy.example.com',
    buyBtnText: '官网',
    buyBtnIcon: 'ri-gift-2-line',
    flag: 'cn',
    locationLabel: '香港',
  },
}

describe('publicNoteView', () => {
  it('reads customData for subtitle/location/flag', () => {
    expect(publicSubtitle(sample)).toBe('香港节点')
    expect(publicLocation(sample, 'us')).toBe('香港')
    expect(flagCode(sample, 'us')).toBe('cn')
  })

  it('maps location codes to readable region names instead of raw IATA/ISO', () => {
    expect(publicLocation({ customData: { location: 'HKG' } }, 'HK')).toBe('香港')
    expect(publicLocation({ customData: { location: 'SGP' } }, '')).toBe('新加坡')
    expect(publicLocation({}, 'UK')).toBe('英国')
    expect(publicLocation({}, 'us', 'en-US')).toBe('United States')
    expect(publicLocation({ customData: { location: 'HKG' } }, 'HK', 'en-US')).toBe('Hong Kong')
    expect(publicLocation({}, 'ZZZ')).toBe('')
  })

  it('normalizes flags to ISO2 and ignores invalid three-letter codes', () => {
    expect(flagCode({ customData: { location: 'HKG' } }, 'US')).toBe('hk')
    expect(flagCode({}, 'UK')).toBe('gb')
    expect(flagCode({ customData: { flag: 'hkg', location: 'HKG' } }, '')).toBe('hk')
    expect(flagCode({ customData: { flag: 'gb-eng' } }, 'us')).toBe('gb-eng')
  })

  it('falls back when presentation keys wrongly used', () => {
    expect(publicSubtitle({ presentation: { slogan: 'x' } })).toBe('x')
  })

  it('computes remaining days and infinity', () => {
    const now = Date.parse('2024-10-20T00:00:00+08:00')
    const bill = getBillAndPlan(sample, now)
    expect(bill.amountKind).toBe('priced')
    expect(bill.amountValue).toBe('$3.99')
    expect(bill.remainingKind).toBe('days')
    expect(bill.remainingDays).toBeGreaterThan(0)
    expect(bill.remainingTone).toMatch(/success|warning|danger/)

    const forever = getBillAndPlan({
      billingDataMod: { endDate: '0000-00-00', amount: '0' },
    }, now)
    expect(forever.amountKind).toBe('free')
    expect(forever.remainingKind).toBe('infinity')
    expect(forever.remainingTone).toBe('success')
  })

  it('marks expired without auto renewal', () => {
    const now = Date.parse('2025-01-01T00:00:00Z')
    const bill = getBillAndPlan({
      billingDataMod: { endDate: '2024-01-01', autoRenewal: '0', amount: '-1' },
    }, now)
    expect(bill.amountKind).toBe('metered')
    expect(bill.remainingKind).toBe('expired')
    expect(bill.remainingTone).toBe('danger')
  })

  it('builds plan tags with dual stack sentinel', () => {
    expect(getPlanTags(sample)).toEqual(['CN2', 'GIA', '传家宝', '__dual_stack__'])
  })

  it('decodes order link and cycle months', () => {
    expect(decodeOrderLink(sample.customData.orderLink)).toBe('https://buy.example.com')
    expect(getCycleMonths('年')).toBe(12)
    expect(getCycleMonths('季')).toBe(3)
  })

  it('drops order links with a disallowed protocol', () => {
    // 公开站 XSS：`href` 不设白名单时，访客点「购买」即执行脚本。
    expect(decodeOrderLink('javascript:alert(1)')).toBe('')
    expect(decodeOrderLink('JavaScript:alert(1)')).toBe('')
    expect(decodeOrderLink('  javascript:alert(1)  ')).toBe('')
    expect(decodeOrderLink('java%0Ascript:alert(1)')).toBe('')
    expect(decodeOrderLink('data:text/html,<script>alert(1)</script>')).toBe('')
    expect(decodeOrderLink('vbscript:msgbox(1)')).toBe('')
    expect(decodeOrderLink('file:///etc/passwd')).toBe('')
    // 白名单内的照常放行。
    expect(decodeOrderLink('http://buy.example.com')).toBe('http://buy.example.com')
    expect(decodeOrderLink('https://buy.example.com')).toBe('https://buy.example.com')
    // 站内相对路径不带协议，也要放行（换域名不用改代码）。
    expect(decodeOrderLink('/order')).toBe('/order')
    expect(decodeOrderLink('order.html')).toBe('order.html')
    expect(decodeOrderLink('')).toBe('')
    expect(isSafeOrderLink('https://buy.example.com')).toBe(true)
    expect(isSafeOrderLink('javascript:alert(1)')).toBe(false)
  })

  it('falls back to the default icon for a non-Remix class', () => {
    // 图标类名来自 allow-create 的 el-select，可填任意字符串；
    // 只放行 Remix 形态，其余回落默认值。
    expect(getPresentation({ customData: { buyBtnIcon: 'ri-gift-2-line' } }).buyBtnIcon)
      .toBe('ri-gift-2-line')
    expect(getPresentation({ customData: { buyBtnIcon: 'ri-shopping-bag-3-line' } }).buyBtnIcon)
      .toBe('ri-shopping-bag-3-line')
    expect(getPresentation({ customData: { buyBtnIcon: 'not-an-icon' } }).buyBtnIcon)
      .toBe('ri-shopping-bag-3-line')
    expect(getPresentation({ customData: { buyBtnIcon: 'ri-x" onmouseover="alert(1)' } }).buyBtnIcon)
      .toBe('ri-shopping-bag-3-line')
    expect(getPresentation({ customData: { buyBtnIcon: '' } }).buyBtnIcon)
      .toBe('ri-shopping-bag-3-line')
    expect(getPresentation({ customData: { buyBtnIcon: 42 } }).buyBtnIcon)
      .toBe('ri-shopping-bag-3-line')
    expect(safeBuyBtnIcon('ri-external-link-line')).toBe('ri-external-link-line')
    expect(safeBuyBtnIcon('fl-ubuntu')).toBe('ri-shopping-bag-3-line')
  })

  it('does not surface a buy button for an unsafe order link', () => {
    const view = buildPublicNoteView({
      customData: { orderLink: 'javascript:alert(document.cookie)' },
    })
    expect(view.presentation.orderLink).toBe('')
    expect(view.hasBuy).toBe(false)
    expect(view.hasDetail).toBe(false)
  })

  it('advances auto-renewal past now', () => {
    const start = Date.parse('2024-01-01T00:00:00Z')
    const now = Date.parse('2024-06-15T00:00:00Z')
    const next = getNextCycleTime(start, 1, now)
    expect(next).toBeGreaterThan(now)
  })

  it('empty note has no detail shell', () => {
    const view = buildPublicNoteView({})
    expect(view.hasDetail).toBe(false)
    expect(view.hasBuy).toBe(false)
  })

  it('amount-only billing has no remaining kind', () => {
    const bill = getBillAndPlan({
      billingDataMod: { amount: '109.00CNY', cycle: '月' },
    })
    expect(bill.amountKind).toBe('priced')
    expect(bill.remainingKind).toBe('')
  })
})
