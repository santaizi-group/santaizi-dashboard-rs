import { COUNTRY_IATA, IATA_COORDS, IATA_COUNTRY, IATA_NAMES, LOCATION_ALIAS, REGION_COORDS } from './locationMaps'

export interface GeoPoint {
  lon: number
  lat: number
}

/** 无任何节点可推算时，地球默认朝向。 */
export const DEFAULT_VIEW: GeoPoint = { lon: 0, lat: 20 }

const CONTINENTS = new Set(['AN', 'EU', 'OC'])

/** lon, lat。含 ISO2 与不冲突的大洲码（EU/OC/AN）。 */
const COORDS: Record<string, readonly [number, number]> = {
  AN: [0, -80], EU: [15, 50], OC: [145, -20],
  AD: [1.52, 42.51], AE: [55.27, 25.2], AF: [69.17, 34.53], AL: [19.82, 41.33], AM: [44.51, 40.18], AO: [13.23, -8.84], AR: [-58.38, -34.6],
  AT: [16.37, 48.21], AU: [151.21, -33.87], AZ: [49.87, 40.41], BA: [18.41, 43.86], BD: [90.41, 23.81],
  BE: [4.35, 50.85], BF: [-1.52, 12.37], BG: [23.32, 42.7], BH: [50.59, 26.23], BI: [29.36, -3.38], BJ: [2.63, 6.5], BM: [-64.75, 32.31], BN: [114.94, 4.9],
  BO: [-68.15, -16.5], BR: [-46.63, -23.55], BS: [-77.35, 25.05], BT: [89.64, 27.47], BW: [25.91, -24.65], BY: [27.57, 53.9], BZ: [-88.77, 17.25], CA: [-79.38, 43.65], CD: [15.31, -4.32], CF: [18.56, 4.36], CG: [15.28, -4.27], CH: [8.54, 47.38],
  CI: [-4.01, 5.35], CL: [-70.67, -33.45], CM: [11.52, 3.87], CN: [116.41, 39.9], CO: [-74.07, 4.71], CR: [-84.09, 9.93], CU: [-82.37, 23.11], CY: [33.38, 35.19],
  CZ: [14.44, 50.08], DE: [8.68, 50.11], DJ: [43.15, 11.59], DK: [12.57, 55.68], DO: [-69.9, 18.48], DZ: [3.06, 36.75], EC: [-78.47, -0.18],
  EE: [24.75, 59.44], EG: [31.24, 30.04], ER: [38.93, 15.33], ES: [-3.7, 40.42], ET: [38.75, 9.02], FI: [24.94, 60.17], FJ: [178.44, -18.14],
  FO: [-6.77, 62.01], FR: [2.35, 48.86], GA: [9.45, 0.39], GB: [-0.13, 51.51], GE: [44.83, 41.72], GG: [-2.59, 49.47], GH: [-0.19, 5.56],
  GI: [-5.35, 36.14], GL: [-51.72, 64.18], GM: [-16.58, 13.45], GN: [-13.68, 9.54], GR: [23.73, 37.98], GT: [-90.51, 14.63], GY: [-58.16, 6.8], HK: [114.17, 22.32], HN: [-87.21, 14.08], HR: [15.98, 45.81], HT: [-72.34, 18.54], HU: [19.04, 47.5],
  ID: [106.85, -6.21], IE: [-6.26, 53.35], IL: [34.78, 32.09], IM: [-4.55, 54.24], IN: [77.21, 28.61],
  IQ: [44.37, 33.32], IR: [51.39, 35.69], IS: [-21.83, 64.15], IT: [9.19, 45.46], JE: [-2.13, 49.21], JM: [-76.79, 18.0],
  JO: [35.91, 31.95], JP: [139.65, 35.68], KE: [36.82, -1.29], KG: [74.59, 42.87], KH: [104.92, 11.56], KP: [125.75, 39.03],
  KR: [126.98, 37.57], KW: [47.98, 29.38], KY: [-81.25, 19.31], KZ: [76.93, 43.24], LA: [102.63, 17.98], LB: [35.5, 33.89],
  LI: [9.52, 47.14], LK: [79.86, 6.93], LR: [-10.8, 6.3], LS: [27.48, -29.31], LT: [25.28, 54.69], LU: [6.13, 49.61], LV: [24.11, 56.95], LY: [13.19, 32.89],
  MA: [-6.85, 34.02], MC: [7.42, 43.74], MD: [28.86, 47.01], ME: [19.26, 42.43], MG: [47.51, -18.88], MK: [21.43, 41.1], ML: [-8.0, 12.64],
  MM: [96.16, 16.84], MN: [106.91, 47.92], MO: [113.54, 22.2], MR: [-15.98, 18.09], MT: [14.51, 35.9], MU: [57.5, -20.16],
  MV: [73.51, 4.18], MW: [33.79, -13.96], MX: [-99.13, 19.43], MY: [101.69, 3.14], MZ: [32.57, -25.97], NA: [17.08, -22.56], NC: [166.46, -22.28], NE: [2.11, 13.51], NG: [3.38, 6.52], NI: [-86.25, 12.14], NL: [4.9, 52.37],
  NO: [10.75, 59.91], NP: [85.32, 27.72], NZ: [174.76, -36.85], OM: [58.41, 23.59], PA: [-79.52, 8.98],
  PE: [-77.04, -12.05], PG: [147.18, -9.44], PH: [120.98, 14.6], PK: [73.05, 33.68], PL: [21.01, 52.23], PR: [-66.11, 18.47], PS: [35.2, 31.9],
  PT: [-9.14, 38.72], PY: [-57.58, -25.26], QA: [51.53, 25.29], RO: [26.1, 44.43], RS: [20.46, 44.82],
  RU: [37.62, 55.76], RW: [30.06, -1.94], SA: [46.68, 24.71], SB: [159.95, -9.43], SD: [32.56, 15.5], SE: [18.07, 59.33], SG: [103.82, 1.35], SI: [14.51, 46.06],
  SK: [17.11, 48.15], SL: [-13.23, 8.48], SM: [12.46, 43.94], SN: [-17.44, 14.69], SO: [45.32, 2.05], SR: [-55.17, 5.85], SS: [31.58, 4.85], SV: [-89.22, 13.69], SY: [36.28, 33.51],
  TD: [15.04, 12.13], TG: [1.22, 6.14], TH: [100.5, 13.76], TJ: [68.79, 38.56], TL: [125.57, -8.56], TM: [58.38, 37.96], TN: [10.18, 36.81], TR: [28.98, 41.01], TT: [-61.52, 10.65], TW: [121.57, 25.03], TZ: [39.27, -6.79],
  UA: [30.52, 50.45], UG: [32.58, 0.35], US: [-118.24, 34.05], UY: [-56.16, -34.9], UZ: [69.24, 41.3],
  VA: [12.45, 41.9], VE: [-66.9, 10.48], VG: [-64.62, 18.43], VI: [-64.9, 18.34], VN: [105.83, 21.03], VU: [168.32, -17.73],
  XK: [21.17, 42.66], YE: [44.21, 15.35], ZA: [28.05, -26.2], ZM: [28.29, -15.42], ZW: [31.05, -17.83],
  UK: [-0.13, 51.51],
}

const COORD_RE = /^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$/

export function parseLocation(raw?: string | null): GeoPoint | null {
  const value = String(raw || '').trim()
  if (!value) return null
  const coord = value.match(COORD_RE)
  if (coord) {
    const lat = Number(coord[1])
    const lon = Number(coord[2])
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null
    return { lon, lat }
  }
  const code = value.toUpperCase()
  const seen = new Set<string>()
  let cursor = code
  while (LOCATION_ALIAS[cursor] && !seen.has(cursor)) {
    seen.add(cursor)
    cursor = LOCATION_ALIAS[cursor]!
  }
  const iata = IATA_COORDS[cursor] || IATA_COORDS[COUNTRY_IATA[code] || ''] || IATA_COORDS[COUNTRY_IATA[cursor] || '']
  if (iata) return { lon: iata[0], lat: iata[1] }
  const region = REGION_COORDS[code] || REGION_COORDS[cursor]
  if (region) return { lon: region[0], lat: region[1] }
  const pair = COORDS[code]
  if (!pair) return null
  return { lon: pair[0], lat: pair[1] }
}

export function sphericalMean(points: GeoPoint[]): GeoPoint | null {
  if (!points.length) return null
  let x = 0
  let y = 0
  let z = 0
  for (const point of points) {
    const lon = point.lon * Math.PI / 180
    const lat = point.lat * Math.PI / 180
    const cos = Math.cos(lat)
    x += cos * Math.cos(lon)
    y += cos * Math.sin(lon)
    z += Math.sin(lat)
  }
  const n = points.length
  x /= n
  y /= n
  z /= n
  return {
    lon: Math.atan2(y, x) * 180 / Math.PI,
    lat: Math.atan2(z, Math.sqrt(x * x + y * y)) * 180 / Math.PI,
  }
}

export function locationKey(point: GeoPoint): string {
  return `${point.lon.toFixed(1)},${point.lat.toFixed(1)}`
}

export function canonicalIata(raw?: string | null): string {
  const code = String(raw || '').trim().toUpperCase()
  if (!code) return ''
  const seen = new Set<string>()
  let cursor = code
  while (LOCATION_ALIAS[cursor] && !seen.has(cursor)) {
    seen.add(cursor)
    cursor = LOCATION_ALIAS[cursor]!
  }
  if (IATA_COORDS[cursor]) return cursor
  return COUNTRY_IATA[code] || COUNTRY_IATA[cursor] || ''
}

export function cityLabel(code: string, locale: string): string {
  const names = IATA_NAMES[code]
  if (!names) return code
  return locale.toLowerCase().startsWith('zh') ? names.zh : names.en
}

export function locationOptions(locale: string): Array<{ code: string; label: string }> {
  return Object.keys(IATA_COORDS).map(code => ({
    code,
    label: `${cityLabel(code, locale)} · ${code}`,
  })).sort((a, b) => a.label.localeCompare(b.label, locale))
}

export function formatLocation(raw?: string | null, locale = 'zh-CN'): string {
  const value = String(raw || '').trim()
  if (!value) return ''
  const coord = value.match(COORD_RE)
  if (coord) return `${Number(coord[1]).toFixed(2)}, ${Number(coord[2]).toFixed(2)}`
  const iata = canonicalIata(value)
  if (iata) {
    const name = cityLabel(iata, locale)
    return name === iata ? iata : `${name} · ${iata}`
  }
  const code = value.toUpperCase()
  if (CONTINENTS.has(code)) return code
  try {
    const name = new Intl.DisplayNames([locale], { type: 'region' }).of(code === 'UK' ? 'GB' : code)
    if (name && name !== code) return name
  } catch { /* ignore */ }
  return value
}

function asObject(value: unknown): Record<string, unknown> {
  if (typeof value === 'string') {
    try { return asObject(JSON.parse(value) as unknown) }
    catch { return {} }
  }
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function text(value: unknown) {
  return value === undefined || value === null ? '' : String(value).trim()
}

export function publicNoteCustom(note: unknown): Record<string, unknown> {
  return asObject(asObject(note).customData)
}

/** 位置码 / ISO2 / IATA → ISO2（`UK`→`GB`）。无法识别则空。 */
export function iso2FromLocation(raw?: string | null): string {
  const upper = text(raw).toUpperCase()
  if (!upper) return ''
  const base = upper.includes('-') ? upper.slice(0, 2) : upper
  if (base.length === 2 && /^[A-Z]{2}$/.test(base) && !CONTINENTS.has(base)) {
    return base === 'UK' ? 'GB' : base
  }
  const seen = new Set<string>()
  let cursor = upper
  while (LOCATION_ALIAS[cursor] && !seen.has(cursor)) {
    seen.add(cursor)
    cursor = LOCATION_ALIAS[cursor]!
  }
  if (cursor.length === 2 && /^[A-Z]{2}$/.test(cursor) && !CONTINENTS.has(cursor)) {
    return cursor === 'UK' ? 'GB' : cursor
  }
  const direct = IATA_COUNTRY[cursor] || IATA_COUNTRY[upper]
  if (direct) return direct === 'UK' ? 'GB' : direct
  for (const [iso, iata] of Object.entries(COUNTRY_IATA)) {
    if (iata === cursor) return iso === 'UK' ? 'GB' : iso
  }
  for (const [from, to] of Object.entries(LOCATION_ALIAS)) {
    if (to === cursor && from.length === 2) return from === 'UK' ? 'GB' : from
  }
  return ''
}

export interface ResolvedLocation {
  point: GeoPoint
  iso2: string
  label: string
}

/** 统一解析坐标、ISO2 国家码与可读展示名（覆盖 IATA 城市码与 110+ 国家/地区）。 */
export function resolveLocation(raw?: string | null, locale = 'zh-CN'): ResolvedLocation | null {
  const point = parseLocation(raw)
  if (!point) return null
  return {
    point,
    iso2: iso2FromLocation(raw),
    label: formatLocation(raw, locale),
  }
}

/** 世界面数据无 HK/MO/TW 独立板块，点亮并入 CN。 */
export function highlightCountry(iso: string): string {
  if (iso === 'HK' || iso === 'MO' || iso === 'TW') return 'CN'
  return iso
}

/** 公开备注 latlng / 地区码优先，其次 host.CountryCode。对齐 Nazhua / aobobo。 */
export function resolveServerGeo(server: {
  host?: { CountryCode?: string }
  public_note?: unknown
}): { point: GeoPoint; country: string } | null {
  const custom = publicNoteCustom(server.public_note)
  const lat = text(custom.lat)
  const lng = text(custom.lng)
  const point = parseLocation(text(custom.latlng))
    || (lat && lng ? parseLocation(`${lat},${lng}`) : null)
    || parseLocation(text(custom.location))
    || parseLocation(server.host?.CountryCode)
  if (!point) return null
  const iso = iso2FromLocation(text(custom.location)) || iso2FromLocation(server.host?.CountryCode)
  return { point, country: iso ? highlightCountry(iso) : '' }
}
