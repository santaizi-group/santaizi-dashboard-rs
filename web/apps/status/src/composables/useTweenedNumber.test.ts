import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { effectScope, nextTick, ref } from 'vue'
import { useTweenedNumber } from '@santaizi/theme-server-status'

/** 手动驱动 rAF：node 环境没有 requestAnimationFrame，补间进度必须可控才测得了。 */
describe('useTweenedNumber', () => {
  const frames: FrameRequestCallback[] = []
  let original: unknown

  beforeEach(() => {
    frames.length = 0
    original = (globalThis as unknown as Record<string, unknown>).requestAnimationFrame
    ;(globalThis as unknown as Record<string, unknown>).requestAnimationFrame =
      (callback: FrameRequestCallback) => {
        frames.push(callback)
        return frames.length
      }
  })

  afterEach(() => {
    const globals = globalThis as unknown as Record<string, unknown>
    if (original === undefined) delete globals.requestAnimationFrame
    else globals.requestAnimationFrame = original
  })

  function flushFrame(timestamp: number) {
    const callback = frames.shift()
    expect(callback).toBeTypeOf('function')
    callback?.(timestamp)
  }

  it('glides from the previous value to the new one and settles exactly', async () => {
    const scope = effectScope()
    const source = ref<number | null>(100)
    const shown = scope.run(() => useTweenedNumber(() => source.value))
    expect(shown?.value).toBe(100) // 首值直接落位，不玩 0 → 100 的假滚动

    const startedAt = performance.now()
    source.value = 200
    await nextTick()
    expect(frames).toHaveLength(1)

    flushFrame(startedAt + 350)
    expect(shown?.value).toBeGreaterThan(100)
    expect(shown?.value).toBeLessThan(110) // 1800ms ease-in-out：前 19% 时间只走约 3% 路程

    flushFrame(startedAt + 900)
    expect(shown?.value).toBeGreaterThan(145)
    expect(shown?.value).toBeLessThan(155) // 中点：一半路程

    flushFrame(startedAt + 5000)
    expect(shown?.value).toBe(200)
    expect(frames).toHaveLength(0)
    scope.stop()
  })

  it('snaps from unknown (null) to a number without a tween', async () => {
    const scope = effectScope()
    const source = ref<number | null>(null)
    const shown = scope.run(() => useTweenedNumber(() => source.value))
    expect(shown?.value).toBeNull()

    source.value = 42
    await nextTick()
    expect(shown?.value).toBe(42)
    expect(frames).toHaveLength(0)
    scope.stop()
  })

  it('retargets mid-flight from the value currently on screen', async () => {
    const scope = effectScope()
    const source = ref(100)
    const shown = scope.run(() => useTweenedNumber(() => source.value))
    const startedAt = performance.now()

    source.value = 200
    await nextTick()
    flushFrame(startedAt + 350)
    const midway = shown?.value ?? 0
    expect(midway).toBeGreaterThan(100)

    source.value = 0
    await nextTick()
    expect(frames).toHaveLength(1) // 新目标从当前显示值续接，不回退到 100
    flushFrame(performance.now() + 5000)
    expect(shown?.value).toBe(0)
    scope.stop()
  })

  it('stops the tween when the scope is disposed', async () => {
    const scope = effectScope()
    const source = ref(100)
    const shown = scope.run(() => useTweenedNumber(() => source.value))

    source.value = 200
    await nextTick()
    expect(frames).toHaveLength(1)
    scope.stop()

    flushFrame(performance.now() + 5000)
    expect(shown?.value).toBe(100) // 已摘除，不再写显示值
    expect(frames).toHaveLength(0)
  })
})
