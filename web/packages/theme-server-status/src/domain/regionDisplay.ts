import { IATA_COUNTRY, LOCATION_ALIAS } from '@santaizi/geo'

/**
 * ServerStatus 国家/地区展示：位置码与 ISO2 归一成可读名和合法旗帜。
 * 与 `@santaizi/geo` 共享全量城市 IATA → ISO2 映射表，确保非首都城市码也能准确反查国旗与地区名。
 */

const FLAG_SUBDIVISION = /^[a-z]{2}-[a-z]{2,8}$/
const LETTERS_2 = /^[A-Za-z]{2}$/
const LETTERS_2_3 = /^[A-Za-z]{2,3}$/

function text(value: unknown) {
  return value === undefined || value === null ? '' : String(value).trim()
}

function canonicalize(raw: string) {
  let code = raw.trim().toUpperCase()
  const seen = new Set<string>()
  while (code && !seen.has(code)) {
    seen.add(code)
    const next = LOCATION_ALIAS[code]
    if (!next || next === code) break
    code = next
  }
  return code
}

/** 2–3 字母或 `gb-eng` 这类旗帜细分码，不当作给人看的地名。 */
export function isRegionCode(value: unknown) {
  const raw = text(value)
  if (!raw) return false
  if (FLAG_SUBDIVISION.test(raw.toLowerCase())) return true
  return LETTERS_2_3.test(raw)
}

/** flag-icons 用的 ISO2 / 细分码；`uk` → `gb`。三字母位置码返回空。 */
export function normalizeFlagCode(value: unknown) {
  const raw = text(value).toLowerCase()
  if (!raw) return ''
  if (FLAG_SUBDIVISION.test(raw)) return raw
  if (LETTERS_2.test(raw)) return raw === 'uk' ? 'gb' : raw
  return ''
}

/** 位置码、ISO2、俗称 → 小写 ISO2（或 `gb-eng`）。无法识别则空。 */
export function resolveIso2(value: unknown) {
  const raw = text(value)
  if (!raw) return ''
  const asFlag = normalizeFlagCode(raw)
  if (asFlag) return asFlag
  const canon = canonicalize(raw)
  if (canon.length === 2) return normalizeFlagCode(canon)
  const iso = IATA_COUNTRY[canon] || IATA_COUNTRY[raw.toUpperCase()]
  return iso ? normalizeFlagCode(iso) : ''
}

export function regionDisplayName(iso2: unknown, locale = 'zh-CN') {
  const iso = text(iso2)
  if (!iso) return ''
  const region = iso.includes('-') ? iso.slice(0, 2).toUpperCase() : iso.toUpperCase()
  if (!LETTERS_2.test(region)) return ''
  try {
    const short = new Intl.DisplayNames([locale], { type: 'region', style: 'short' }).of(region) || ''
    if (short && !isRegionCode(short)) return short
    const long = new Intl.DisplayNames([locale], { type: 'region', style: 'long' }).of(region) || ''
    if (long && !isRegionCode(long)) return long
    return long || short
  } catch {
    return ''
  }
}

export function resolveRegionLabel(
  locationLabel: unknown,
  location: unknown,
  countryCode?: unknown,
  locale = 'zh-CN',
) {
  const label = text(locationLabel)
  if (label && !isRegionCode(label)) return label
  const loc = text(location)
  if (loc && !isRegionCode(loc)) return loc
  const iso = resolveIso2(label) || resolveIso2(loc) || resolveIso2(countryCode)
  return regionDisplayName(iso, locale)
}

export function resolveFlagCode(flag: unknown, location?: unknown, countryCode?: unknown) {
  return normalizeFlagCode(flag) || resolveIso2(location) || resolveIso2(countryCode)
}
