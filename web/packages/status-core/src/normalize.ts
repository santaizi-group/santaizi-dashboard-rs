import { isHostOnline, type ServerHost, type ServerRecord, type ServerState } from '@santaizi/api'

function pick(object: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    if (object[key] !== undefined && object[key] !== null) return object[key]
  }
  return undefined
}

function publicNote(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object') return value as Record<string, unknown>
  if (typeof value === 'string' && value.trim()) {
    try {
      return JSON.parse(value) as Record<string, unknown>
    } catch {
      return { text: value }
    }
  }
  return {}
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' ? value as Record<string, unknown> : undefined
}

function asNumber(raw: unknown, fallback = 0) {
  const n = typeof raw === 'number' ? raw : Number(raw)
  return Number.isFinite(n) ? n : fallback
}

function asBoolean(raw: unknown): boolean | undefined {
  if (typeof raw === 'boolean') return raw
  return undefined
}

function telemetryPresentation(raw: Record<string, unknown> | undefined) {
  if (!raw) return undefined
  return {
    host: String(pick(raw, 'host', 'Host') ?? ''),
    connectivity: String(pick(raw, 'connectivity', 'Connectivity') ?? ''),
    available: (pick(raw, 'available', 'Available') as boolean | null | undefined) ?? null,
    coverage: String(pick(raw, 'coverage', 'Coverage') ?? ''),
  }
}

const HAS_EXPLICIT_TAG = Symbol('hasExplicitTag')

type NormalizedServerRecord = ServerRecord & {
  [HAS_EXPLICIT_TAG]?: boolean
}

function hasOwnField(object: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(object, key) && object[key] !== undefined && object[key] !== null) {
      return true
    }
  }
  return false
}

export function normalizeServer(raw: Record<string, unknown>): ServerRecord {
  const value = (
    raw.data && typeof raw.data === 'object' && !Array.isArray(raw.data)
    && (pick(raw.data as Record<string, unknown>, 'id', 'ID', 'name', 'Name') !== undefined)
      ? raw.data as Record<string, unknown>
      : raw
  )
  const lastActive = String(pick(value, 'last_active', 'LastActive') ?? '')
  const telemetry = telemetryPresentation(asRecord(pick(value, 'telemetry', 'Telemetry')))
  const onlineFlag = asBoolean(pick(value, 'online', 'Online'))
  const explicitTag = hasOwnField(value, 'tag', 'Tag')
  const explicitNote = hasOwnField(value, 'public_note', 'PublicNote')
  const record: NormalizedServerRecord = {
    id: asNumber(pick(value, 'id', 'ID')),
    name: String(pick(value, 'name', 'Name') ?? ''),
    tag: String(pick(value, 'tag', 'Tag') ?? ''),
    display_index: asNumber(pick(value, 'display_index', 'DisplayIndex')),
    hide_for_guest: Boolean(pick(value, 'hide_for_guest', 'HideForGuest')),
    enable_ddns: Boolean(pick(value, 'enable_ddns', 'EnableDDNS')),
    public_note: explicitNote ? publicNote(pick(value, 'public_note', 'PublicNote')) : undefined,
    host: asRecord(pick(value, 'host', 'Host')) as ServerHost | undefined,
    state: asRecord(pick(value, 'state', 'State')) as ServerState | undefined,
    last_active: lastActive,
    online: telemetry || onlineFlag !== undefined ? isHostOnline({ online: onlineFlag, telemetry }) : undefined,
    telemetry,
  }
  Object.defineProperty(record, HAS_EXPLICIT_TAG, {
    value: explicitTag,
    enumerable: false,
    configurable: true,
  })
  return record
}

function isEmptyRecord(value: ServerHost | ServerState | Record<string, unknown> | undefined) {
  return !value || Object.keys(value).length === 0
}

function resolveMergedTag(prev: ServerRecord, next: NormalizedServerRecord): string {
  if (typeof next[HAS_EXPLICIT_TAG] === 'boolean') {
    return next[HAS_EXPLICIT_TAG] ? next.tag : prev.tag
  }
  return next.tag !== undefined ? next.tag : prev.tag
}

/**
 * 增量 WS 帧缺省字段（undefined）时回退已有快照；
 * 完整快照明确携带 tag（含空串 ""）或 public_note 时以最新快照为准覆盖，
 * 避免后台清空分组或删除备注字段后前台永久残留旧值。
 */
export function mergeServerSnapshot(prev: ServerRecord | undefined, next: ServerRecord): ServerRecord {
  if (!prev) {
    return {
      ...next,
      public_note: next.public_note ?? {},
      online: isHostOnline(next),
    }
  }
  const telemetry = next.telemetry || prev.telemetry
  const merged: NormalizedServerRecord = {
    ...prev,
    ...next,
    name: next.name || prev.name,
    tag: resolveMergedTag(prev, next as NormalizedServerRecord),
    public_note: next.public_note !== undefined ? next.public_note : (prev.public_note ?? {}),
    host: isEmptyRecord(next.host) ? prev.host : next.host,
    state: isEmptyRecord(next.state) ? prev.state : next.state,
    telemetry,
    last_active: next.last_active || prev.last_active,
  }
  Object.defineProperty(merged, HAS_EXPLICIT_TAG, {
    value: true,
    enumerable: false,
    configurable: true,
  })
  merged.online = isHostOnline({
    online: next.online !== undefined ? next.online : prev.online,
    telemetry,
  })
  return merged
}
