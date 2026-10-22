/** 数值补间核心：纯函数、无 Vue/DOM 依赖，node 环境可直接单测。
 *
 *  WS 每 2s 推一帧（`src/api/ws.rs` FRAME_INTERVAL），这里只负责把「当前可见值 →
 *  新帧目标值」在约 1.6s 内滚完；调度（共享 rAF 循环）在 `useTweenedNumber`。
 *  曲线用 ease-in-out：两端速度为零；ease-out 起点速度最大，看起来是「一窜再爬」。 */

export interface TweenState {
  from: number
  to: number
  /** 起始毫秒，与 performance.now / rAF 时间戳同源 */
  start: number
  duration: number
}

/** 端点精确：0 → 0，1 → 1，中段最快（峰值速度为平均速度的 1.5 倍）。
 *  非法进度按已完成处理，避免 NaN 渗进显示值。 */
export function easeInOutCubic(progress: number): number {
  const t = Math.min(1, Math.max(0, Number.isFinite(progress) ? progress : 1))
  return t < 0.5 ? 4 * t ** 3 : 1 - (-2 * t + 2) ** 3 / 2
}

/** 零/负时长视为瞬跳（`progressAt` 直接返回 1）。 */
export function createTween(from: number, to: number, start: number, duration: number): TweenState {
  return { from, to, start, duration: Number.isFinite(duration) ? Math.max(0, duration) : 0 }
}

function progressAt(state: TweenState, now: number): number {
  if (!(state.duration > 0)) return 1
  const elapsed = now - state.start
  if (!Number.isFinite(elapsed)) return 1
  return Math.min(1, Math.max(0, elapsed / state.duration))
}

export function tweenValueAt(state: TweenState, now: number): number {
  return state.from + (state.to - state.from) * easeInOutCubic(progressAt(state, now))
}

export function tweenDoneAt(state: TweenState, now: number): boolean {
  return progressAt(state, now) >= 1
}
