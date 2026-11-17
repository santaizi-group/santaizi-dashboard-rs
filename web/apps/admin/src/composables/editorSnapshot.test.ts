import { effectScope, nextTick, reactive, ref } from 'vue'
import { describe, expect, it } from 'vitest'
import { useEditorSnapshot } from './editorSnapshot'

describe('useEditorSnapshot', () => {
  it('stays clean before initial capture and resets baseline when closed', async () => {
    const scope = effectScope()
    await scope.run(async () => {
      const visible = ref(true)
      const form = reactive({ name: 'alpha' })
      const { dirty, capture } = useEditorSnapshot(form, visible)

      // 异步加载选项期间尚未调用 capture()，点取消不应误报脏数据
      expect(dirty.value).toBe(false)
      form.name = 'loaded'
      expect(dirty.value).toBe(false)

      capture()
      expect(dirty.value).toBe(false)

      form.name = 'edited'
      expect(dirty.value).toBe(true)

      // 关闭后再打开另一个对象的编辑弹窗，在 capture() 前同样不应误报脏数据
      visible.value = false
      await nextTick()
      expect(dirty.value).toBe(false)

      visible.value = true
      form.name = 'next-server'
      await nextTick()
      expect(dirty.value).toBe(false)

      capture()
      form.name = 'next-server-edited'
      expect(dirty.value).toBe(true)
    })
    scope.stop()
  })
})
