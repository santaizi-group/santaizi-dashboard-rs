import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ElMessage } from 'element-plus'
import { copyText } from './clipboard'

/// 只关心 copyText 用到的两个键；完整词条在 @santaizi/i18n。
const t = ((key: string) => ({ copied: '已复制', copyFailedManual: '复制失败，请手动选择并复制' })[key] ?? key) as never

/// vitest 默认 environment = node，没有 DOM；这里手搭一个够用的假 document，
/// 只为覆盖「明文 HTTP 下 execCommand 兜底」这条分支。
function stubDocument(execResult: boolean) {
  const appended: unknown[] = []
  const execCommand = vi.fn().mockReturnValue(execResult)
  const area = {
    value: '',
    style: {} as Record<string, string>,
    setAttribute: vi.fn(),
    select: vi.fn(),
    remove: vi.fn(),
  }
  vi.stubGlobal('document', {
    body: { appendChild: (node: unknown) => appended.push(node) },
    createElement: () => area,
    execCommand,
  })
  return { appended, execCommand, area }
}

describe('copyText', () => {
  beforeEach(() => {
    vi.unstubAllGlobals()
    // spy 的调用记录跨用例累积，先清干净再断言 not.toHaveBeenCalled()。
    vi.clearAllMocks()
    // ElMessage 需要真实容器；node 环境下只验证它被调用过。
    vi.spyOn(ElMessage, 'success').mockImplementation(() => ({} as never))
    vi.spyOn(ElMessage, 'warning').mockImplementation(() => ({} as never))
  })

  it('uses the async clipboard when available', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { clipboard: { writeText } })
    await expect(copyText('token-abc', t)).resolves.toBe('copied')
    expect(writeText).toHaveBeenCalledWith('token-abc')
    expect(ElMessage.success).toHaveBeenCalledWith('已复制')
    expect(ElMessage.warning).not.toHaveBeenCalled()
  })

  it('treats an empty value as a no-op without touching the clipboard', async () => {
    const writeText = vi.fn()
    vi.stubGlobal('navigator', { clipboard: { writeText } })
    await expect(copyText('', t)).resolves.toBe('empty')
    await expect(copyText(null, t)).resolves.toBe('empty')
    await expect(copyText(undefined, t)).resolves.toBe('empty')
    expect(writeText).not.toHaveBeenCalled()
  })

  it('reports failure instead of rejecting when the permission is denied', async () => {
    // 明文 HTTP 部署下 navigator.clipboard 整个是 undefined；
    // 权限被拒时 writeText reject。两者都必须是「有提示」而不是
    // 未处理 rejection（按钮点了没反应）。
    vi.stubGlobal('navigator', {
      clipboard: { writeText: vi.fn().mockRejectedValue(new Error('NotAllowedError')) },
    })
    const doc = stubDocument(true)
    await expect(copyText('token-abc', t)).resolves.toBe('copied')
    expect(doc.execCommand).toHaveBeenCalledWith('copy')
  })

  it('falls back to execCommand when the async clipboard is missing', async () => {
    vi.stubGlobal('navigator', {})
    const doc = stubDocument(true)
    await expect(copyText('token-abc', t)).resolves.toBe('copied')
    expect(doc.execCommand).toHaveBeenCalledWith('copy')
    // 临时 textarea 必须挂进 body 又摘掉，不能残留在页面上。
    expect(doc.appended).toEqual([doc.area])
    expect(doc.area.remove).toHaveBeenCalled()
  })

  it('reports failure when neither path works', async () => {
    vi.stubGlobal('navigator', {})
    stubDocument(false)
    await expect(copyText('token-abc', t)).resolves.toBe('failed')
    // 关键：失败必须变成用户可见的提示，而不是一句未处理 rejection。
    expect(ElMessage.warning).toHaveBeenCalledWith('复制失败，请手动选择并复制')
    expect(ElMessage.success).not.toHaveBeenCalled()
  })
})
