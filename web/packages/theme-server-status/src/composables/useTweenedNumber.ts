import { onScopeDispose, ref, watch, type Ref } from 'vue'
import { createTween, tweenDoneAt, tweenValueAt, type TweenState } from '../domain/numberTween'

/** 与 WS 帧间隔（2s）错开：补间在下一帧到达前停稳，不产生追赶感。 */
const DEFAULT_DURATION = 700

interface ActiveTween {
  state: TweenState
  emit: (value: number) => void
}

/* 单条共享 rAF 循环：一张卡片就有五个补间，29 张卡各自开循环会把主线程排满。 */
const active = new Set<ActiveTween>()
let frame: number | undefined

function nowMs() {
  return typeof performance !== 'undefined' && typeof performance.now === 'function'
    ? performance.now()
    : Date.now()
}

function schedule() {
  if (frame !== undefined || typeof requestAnimationFrame !== 'function') return
  frame = requestAnimationFrame(tick)
}

function tick(timestamp: number) {
  frame = undefined
  const now = Number.isFinite(timestamp) ? timestamp : nowMs()
  for (const item of active) {
    if (tweenDoneAt(item.state, now)) {
      active.delete(item)
      item.emit(item.state.to)
    } else {
      item.emit(tweenValueAt(item.state, now))
    }
  }
  if (active.size) schedule()
}

function reducedMotion() {
  return typeof window !== 'undefined'
    && typeof window.matchMedia === 'function'
    && window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

function numeric(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

/**
 * 数值从上一帧平滑滚到新帧。
 *
 * 首值直接落位（不玩 0 → x 的假滚动）；`null`（未知用量）与数字互切直接落位；
 * `prefers-reduced-motion` 或无 rAF 环境（测试）瞬跳。补间途中来新目标时从
 * 当前显示值续接，不回退。
 */
export function useTweenedNumber(
  source: () => number | null | undefined,
  options?: { duration?: number },
): Ref<number | null> {
  const duration = options?.duration ?? DEFAULT_DURATION
  const shown = ref<number | null>(numeric(source()))
  let entry: ActiveTween | null = null

  function stop() {
    if (!entry) return
    active.delete(entry)
    entry = null
  }

  function snap(value: number | null) {
    stop()
    shown.value = value
  }

  watch(source, (next) => {
    const target = numeric(next)
    const current = shown.value
    if (
      target === null
      || current === null
      || target === current
      || !(duration > 0)
      || reducedMotion()
      || typeof requestAnimationFrame !== 'function'
    ) {
      snap(target)
      return
    }
    stop()
    const started: ActiveTween = {
      state: createTween(current, target, nowMs(), duration),
      emit: (value) => { shown.value = value },
    }
    active.add(started)
    entry = started
    schedule()
  })

  onScopeDispose(stop)
  return shown
}
