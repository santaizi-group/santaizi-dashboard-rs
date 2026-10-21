import { describe, expect, it } from 'vitest'
import { trafficUsageParts, trafficUsageText } from '@santaizi/theme-server-status'

// 卡片第 4 行把「标签」与「数值」分开着色（标签次要色、数值加粗），
// 表格行仍用 trafficUsageText 整串；两边的 kind→文案映射必须一致。
const dict: Record<string, string> = {
  trafficLabelRemaining: '剩余',
  trafficLabelOut: '单向出',
  trafficLabelBoth: '双向',
  trafficLabelMaxOut: '最大出',
  trafficLabelMaxIn: '最大入',
  trafficUnlimited: '不限制',
  trafficRemaining: '剩余',
  trafficOutUsed: '单向出',
  trafficBothUsed: '双向',
  trafficMaxOutUsed: '最大出',
  trafficMaxInUsed: '最大入',
}
const t = (key: string, values?: Record<string, unknown>) =>
  String(values?.value !== undefined ? `${dict[key] ?? key} ${values.value}` : (dict[key] ?? key))

describe('trafficUsageParts', () => {
  it('splits label and value per kind', () => {
    expect(trafficUsageParts('both', '16.96G', t)).toEqual({ label: '双向', value: '16.96G' })
    expect(trafficUsageParts('cycle', '500G', t)).toEqual({ label: '剩余', value: '500G' })
    expect(trafficUsageParts('out', '1.2T', t)).toEqual({ label: '单向出', value: '1.2T' })
    expect(trafficUsageParts('maxOut', '900G', t)).toEqual({ label: '最大出', value: '900G' })
    expect(trafficUsageParts('maxIn', '300G', t)).toEqual({ label: '最大入', value: '300G' })
  })

  it('unlimited has label only', () => {
    expect(trafficUsageParts('unlimited', '', t)).toEqual({ label: '不限制', value: '' })
    expect(trafficUsageParts('whatever', '', t)).toEqual({ label: '不限制', value: '' })
  })

  it('unknown kind falls back to unlimited', () => {
    expect(trafficUsageParts('mystery', '1G', t)).toEqual({ label: '不限制', value: '' })
  })

  it('label matches the table row text prefix', () => {
    for (const kind of ['both', 'cycle', 'out', 'maxOut', 'maxIn']) {
      const parts = trafficUsageParts(kind, '9.9G', t)
      expect(trafficUsageText(kind, '9.9G', t)).toBe(`${parts.label} 9.9G`)
    }
  })
})
