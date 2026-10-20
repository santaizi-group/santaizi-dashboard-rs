import { onScopeDispose, type Ref } from 'vue'
import type { Router } from 'vue-router'
import type { Composer } from 'vue-i18n'
import { confirmDiscardChanges } from '@santaizi/ui'

const activeEditors = new Set<Readonly<Ref<boolean>>>()

export function registerRouteDirtyEditor(dirty: Readonly<Ref<boolean>>) {
  activeEditors.add(dirty)
  onScopeDispose(() => activeEditors.delete(dirty))
}

export function installRouteDirtyGuard(router: Router, i18n: Composer) {
  router.beforeEach(async () => {
    if (![...activeEditors].some(editor => editor.value)) return true
    const { t } = i18n
    return confirmDiscardChanges({
      message: String(t('discardChangesConfirm')),
      title: String(t('discardChanges')),
      confirm: String(t('discard')),
      cancel: String(t('keepEditing')),
    })
  })
}
