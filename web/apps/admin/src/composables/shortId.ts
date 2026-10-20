export const LONG_ID_MIN = 16

const prefixedHex = /^([a-z][a-z0-9]*-)([0-9a-f]{16,})$/i

export function shortId(value: string, keep = 8) {
  const text = value.trim()
  if (text.length < LONG_ID_MIN) return text
  const prefixed = text.match(prefixedHex)
  if (prefixed?.[1] && prefixed[2]) return `${prefixed[1]}${prefixed[2].slice(0, keep)}…`
  return `${text.slice(0, keep)}…`
}

export function isLongId(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length >= LONG_ID_MIN
}
