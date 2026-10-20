import { ElNotification } from 'element-plus'
import { extractAPIError, formatAPIError, type Translate } from '@/composables/format'
import { useMessageStore } from '@/stores/messages'

export function notifyAPIError(error: unknown, t: Translate, te: (key: string) => boolean) {
  const extracted = extractAPIError(error)
  const message = formatAPIError(error, t, te)
  const title = t('requestFailed')
  const store = useMessageStore()
  const entry = store.pushError({
    title,
    message,
    code: extracted.code,
    status: extracted.status,
    traceId: extracted.traceId,
    fields: extracted.fields,
    detail: extracted.detail && extracted.detail !== message ? extracted.detail : undefined,
    route: typeof location !== 'undefined' ? `${location.pathname}${location.search}` : '',
  })
  ElNotification.error({
    title,
    message,
    duration: 6000,
    showClose: true,
    onClick: () => store.openDetail(entry.id),
  })
  return entry
}
