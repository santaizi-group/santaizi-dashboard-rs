import { ElMessage } from 'element-plus'
import type { Translate } from '@/composables/format'

/// 复制结果：调用方只需知道成没成，失败原因已在这里消化成用户可见的提示。
export type CopyOutcome = 'copied' | 'empty' | 'failed'

/**
 * 复制文本到剪贴板，并把失败变成用户可见的提示。
 *
 * 裸调 `navigator.clipboard.writeText` 有两处会让按钮「点了没反应」：
 * 1. 面板跑在明文 HTTP（自托管常见）时 `navigator.clipboard` 整个是 undefined；
 * 2. 用户拒绝剪贴板权限时 promise reject。
 * 两者都会变成未处理的 rejection——控制台一条红字，界面零反馈。
 *
 * 明文上下文补一条 `execCommand('copy')` 兜底；再不行才告知用户手动复制。
 */
export async function copyText(value: string | null | undefined, t: Translate): Promise<CopyOutcome> {
  if (!value) return 'empty'
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value)
      ElMessage.success(t('copied'))
      return 'copied'
    } catch {
      // 权限被拒：落到下面的兜底，不在这里返回。
    }
  }
  if (legacyCopy(value)) {
    ElMessage.success(t('copied'))
    return 'copied'
  }
  ElMessage.warning(t('copyFailedManual'))
  return 'failed'
}

/// 非安全上下文（明文 HTTP）下用 textarea + execCommand 兜底。
function legacyCopy(value: string): boolean {
  if (typeof document === 'undefined' || !document.body) return false
  const area = document.createElement('textarea')
  area.value = value
  area.setAttribute('readonly', '')
  area.style.position = 'fixed'
  area.style.opacity = '0'
  document.body.appendChild(area)
  try {
    area.select()
    return document.execCommand('copy')
  } catch {
    return false
  } finally {
    area.remove()
  }
}
