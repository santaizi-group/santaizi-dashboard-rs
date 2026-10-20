import type { CollectorRecord } from '@santaizi/api'

export function isProbeCollector(row?: Pick<CollectorRecord, 'kind'> | null) {
  return row?.kind === 'probe'
}
