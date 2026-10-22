import { describe, expect, it } from 'vitest'
import { createTween, easeOutCubic, tweenDoneAt, tweenValueAt } from '@santaizi/theme-server-status'

describe('easeOutCubic', () => {
  it('pins both endpoints', () => {
    expect(easeOutCubic(0)).toBe(0)
    expect(easeOutCubic(1)).toBe(1)
  })

  it('decelerates monotonically and clamps out-of-range input', () => {
    // 0.25 / 0.5 / 0.75 的缓动值都是二进制精确小数
    expect([0, 0.25, 0.5, 0.75, 1].map(easeOutCubic)).toEqual([0, 0.578125, 0.875, 0.984375, 1])
    expect(easeOutCubic(-1)).toBe(0)
    expect(easeOutCubic(2)).toBe(1)
    expect(easeOutCubic(Number.NaN)).toBe(1)
  })
})

describe('tweenValueAt', () => {
  const state = createTween(10, 20, 1000, 700)

  it('starts at the previous value and ends exactly at the target', () => {
    expect(tweenValueAt(state, 1000)).toBe(10)
    expect(tweenValueAt(state, 1700)).toBe(20)
    expect(tweenValueAt(state, 5000)).toBe(20)
  })

  it('holds the previous value before the tween clock begins', () => {
    expect(tweenValueAt(state, 900)).toBe(10)
  })

  it('interpolates between endpoints mid-flight, decelerating', () => {
    const mid = tweenValueAt(state, 1350)
    expect(mid).toBeGreaterThan(10)
    expect(mid).toBeLessThan(20)
    // ease-out：一半时间已走完大半路程
    expect(mid).toBeGreaterThan(15)
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
  const state = createTween(10, 20, 1000, 700)

  it('reports completion at or after the end', () => {
    expect(tweenDoneAt(state, 1000)).toBe(false)
    expect(tweenDoneAt(state, 1699)).toBe(false)
    expect(tweenDoneAt(state, 1700)).toBe(true)
  })

  it('treats zero duration as already done', () => {
    expect(tweenDoneAt(createTween(10, 20, 1000, 0), 1000)).toBe(true)
  })
})
