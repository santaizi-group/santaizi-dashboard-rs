import { describe, expect, it } from 'vitest'
import { createTween, easeInOutCubic, tweenDoneAt, tweenValueAt } from '@santaizi/theme-server-status'

describe('easeInOutCubic', () => {
  it('pins both endpoints', () => {
    expect(easeInOutCubic(0)).toBe(0)
    expect(easeInOutCubic(1)).toBe(1)
  })

  it('ramps in and out symmetrically, fastest in the middle', () => {
    // 0.25 / 0.5 / 0.75 的缓动值都是二进制精确小数
    expect([0, 0.25, 0.5, 0.75, 1].map(easeInOutCubic)).toEqual([0, 0.0625, 0.5, 0.9375, 1])
    expect(easeInOutCubic(0.25) + easeInOutCubic(0.75)).toBe(1)
    // 起步慢：前四分之一时间只走 6.25% 路程（ease-out 会走 58%）
    expect(easeInOutCubic(0.25)).toBeLessThan(0.1)
  })

  it('clamps out-of-range input', () => {
    expect(easeInOutCubic(-1)).toBe(0)
    expect(easeInOutCubic(2)).toBe(1)
    expect(easeInOutCubic(Number.NaN)).toBe(1)
  })
})

describe('tweenValueAt', () => {
  const state = createTween(10, 20, 1000, 1600)

  it('starts at the previous value and ends exactly at the target', () => {
    expect(tweenValueAt(state, 1000)).toBe(10)
    expect(tweenValueAt(state, 2600)).toBe(20)
    expect(tweenValueAt(state, 9000)).toBe(20)
  })

  it('holds the previous value before the tween clock begins', () => {
    expect(tweenValueAt(state, 900)).toBe(10)
  })

  it('covers the distance slowly at the edges and fastest in the middle', () => {
    expect(tweenValueAt(state, 1400)).toBeCloseTo(10.625, 10) // 25% 时间 → 6.25% 路程
    expect(tweenValueAt(state, 1800)).toBe(15) // 中点
    expect(tweenValueAt(state, 2200)).toBeCloseTo(19.375, 10) // 75% 时间 → 93.75% 路程
  })

  it('snaps to the target for zero or invalid duration', () => {
    expect(tweenValueAt(createTween(10, 20, 1000, 0), 1000)).toBe(20)
    expect(tweenValueAt(createTween(10, 20, 1000, -5), 1000)).toBe(20)
    expect(tweenValueAt(createTween(10, 20, 1000, Number.NaN), 1000)).toBe(20)
  })

  it('treats a non-finite clock as completed instead of leaking NaN', () => {
    expect(tweenValueAt(state, Number.NaN)).toBe(20)
  })
})

describe('tweenDoneAt', () => {
  const state = createTween(10, 20, 1000, 1600)

  it('reports completion at or after the end', () => {
    expect(tweenDoneAt(state, 1000)).toBe(false)
    expect(tweenDoneAt(state, 2599)).toBe(false)
    expect(tweenDoneAt(state, 2600)).toBe(true)
  })

  it('treats zero duration as already done', () => {
    expect(tweenDoneAt(createTween(10, 20, 1000, 0), 1000)).toBe(true)
  })
})
