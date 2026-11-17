import type { ServerHost } from '@santaizi/api'
import {
  iso2FromLocation,
  parseLocation,
  publicNoteCustom,
  regionDisplayName,
  resolveLocation,
} from '@santaizi/theme-server-status'
import CODE_MAPS, { aliasMapping, countryCodeMapping, regionGeoPresets } from '../data/code-maps'

type CodeInfo = { x?: number; y?: number; lon?: number; lat?: number; name?: string; country?: string }

export const ALIAS_CODE: Record<string, string> = {
  ...aliasMapping,
  ...countryCodeMapping,
}

const LOCATION_MAPS = CODE_MAPS as Record<string, CodeInfo>
const ALIAS_MAP = aliasMapping as Record<string, string>
const REGION_PRESETS = regionGeoPresets as Record<string, CodeInfo>

export function alias2code(code: unknown): string | undefined {
  if (code === undefined || code === null) return undefined
  const key = String(code)
  return ALIAS_CODE[key] || ALIAS_CODE[key.toUpperCase()]
}

export function locationCode2Info(code: unknown) {
  const key = String(code)
  let info = LOCATION_MAPS[key] || LOCATION_MAPS[key.toUpperCase()]
  const aliasCode = ALIAS_MAP[key] || ALIAS_MAP[key.toUpperCase()]
  if (!info && aliasCode) {
    info = LOCATION_MAPS[aliasCode]
  }
  return info
}

export function locationCode2GeoInfo(code: unknown) {
  const normalizedCode = typeof code === 'string' ? code.toUpperCase() : String(code)
  const aliasCode = ALIAS_MAP[normalizedCode]
  return LOCATION_MAPS[String(code)]
    || LOCATION_MAPS[normalizedCode]
    || (aliasCode ? LOCATION_MAPS[aliasCode] : undefined)
    || REGION_PRESETS[normalizedCode]
}

export function count2size(count: number) {
  if (count < 3) return 4
  if (count < 5) return 6
  return 8
}

export interface MapPoint {
  key: string
  left: number
  top: number
  size: number
  label: string
  type: 'single' | 'group'
}

export function findIntersectingGroups<T extends {
  key: string
  topLeft: { left: number; top: number }
  bottomRight: { left: number; top: number }
  parent?: T
  children?: T[]
}>(coordinates: T[]) {
  const groups: Record<string, T[]> = {}
  const n = -2
  coordinates.forEach((coordinate, index) => {
    const intersects: T[] = []
    coordinates.forEach((otherCoordinate, otherIndex) => {
      if (index !== otherIndex) {
        if (
          coordinate.topLeft.top - otherCoordinate.bottomRight.top < n
          && coordinate.topLeft.left - otherCoordinate.bottomRight.left < n
          && coordinate.bottomRight.top - otherCoordinate.topLeft.top > -n
          && coordinate.bottomRight.left - otherCoordinate.topLeft.left > -n
        ) {
          intersects.push(otherCoordinate)
        }
      }
    })
    if (intersects.length > 0) groups[coordinate.key] = intersects
  })
  return groups
}

export interface ServerLocation {
  code: string
  name?: string
  x?: number
  y?: number
  lon: number
  lat: number
  countryCode: string
}

/** 将经纬度投射到 1280×621 点阵世界地图画布坐标。 */
function projectMapXY(lon: number, lat: number): { x: number; y: number } {
  const x = Math.round(Math.max(8, Math.min(1272, 571.5 + lon * 3.96)))
  const y = Math.round(Math.max(8, Math.min(613, 360 - lat * 4.54)))
  return { x, y }
}

function cleanText(value: unknown): string {
  return value === undefined || value === null ? '' : String(value).trim()
}

export function resolveServerLocation(
  server: { host?: ServerHost; public_note?: Record<string, unknown> },
): ServerLocation | null {
  const custom = publicNoteCustom(server.public_note)
  const rawLocation = cleanText(custom.location)
  const rawLocationLabel = cleanText(custom.locationLabel)
  const hostCountry = cleanText(server.host?.CountryCode)
  const resolvedIso = (
    iso2FromLocation(rawLocation)
    || iso2FromLocation(cleanText(custom.flag))
    || iso2FromLocation(hostCountry)
  ).toLowerCase()

  // 1. 公开备注显式经纬度优先（latlng 或 lat + lng）
  const customLat = cleanText(custom.lat)
  const customLng = cleanText(custom.lng)
  const explicitPoint = parseLocation(cleanText(custom.latlng))
    || (customLat && customLng ? parseLocation(`${customLat},${customLng}`) : null)
  if (explicitPoint) {
    const projected = projectMapXY(explicitPoint.lon, explicitPoint.lat)
    const codeKey = rawLocation.toUpperCase() || hostCountry.toUpperCase() || `${explicitPoint.lat.toFixed(2)},${explicitPoint.lon.toFixed(2)}`
    const presetInfo = codeKey ? (locationCode2Info(codeKey) || locationCode2GeoInfo(codeKey)) : undefined
    return {
      code: codeKey,
      name: rawLocationLabel || String(presetInfo?.name || '') || regionDisplayName(resolvedIso, 'zh-CN'),
      x: typeof presetInfo?.x === 'number' ? presetInfo.x : projected.x,
      y: typeof presetInfo?.y === 'number' ? presetInfo.y : projected.y,
      lon: explicitPoint.lon,
      lat: explicitPoint.lat,
      countryCode: resolvedIso,
    }
  }

  let aliasCode: string | undefined
  let locationCode: string | undefined
  if (rawLocation) {
    aliasCode = rawLocation
    locationCode = rawLocation
  } else if (hostCountry) {
    aliasCode = hostCountry.toUpperCase()
  }
  const normalizedAliasCode = typeof aliasCode === 'string' ? aliasCode.toUpperCase() : aliasCode
  const code = alias2code(normalizedAliasCode) || locationCode || normalizedAliasCode
  if (!code) return null

  // 2. 命中内置点阵城市表 / 地区预设
  const locationInfo = locationCode2Info(code) || {}
  const geoInfo = locationCode2GeoInfo(code) || locationCode2GeoInfo(normalizedAliasCode) || {}
  const merged = { ...geoInfo, ...locationInfo } as Record<string, unknown>
  const x = merged.x as number | undefined
  const y = merged.y as number | undefined
  const lon = merged.lon as number | undefined
  const lat = merged.lat as number | undefined
  const hasMapCoord = typeof x === 'number' && typeof y === 'number'
  const hasGeoCoord = typeof lon === 'number' && typeof lat === 'number'
  if (hasMapCoord || hasGeoCoord) {
    const finalLon = hasGeoCoord ? lon! : (x! / 1280) * 360 - 180
    const finalLat = hasGeoCoord ? lat! : 90 - (y! / 621) * 180
    const projected = hasMapCoord ? { x: x!, y: y! } : projectMapXY(finalLon, finalLat)
    return {
      code: String(code).toUpperCase(),
      name: rawLocationLabel || String(merged.name || ''),
      x: projected.x,
      y: projected.y,
      lon: finalLon,
      lat: finalLat,
      countryCode: resolvedIso || iso2FromLocation(String(code)).toLowerCase(),
    }
  }

  // 3. 回退 @santaizi/geo（覆盖 110+ 国家/地区与坐标串）
  const fallback = resolveLocation(rawLocation || hostCountry, 'zh-CN')
  if (!fallback) return null
  const projected = projectMapXY(fallback.point.lon, fallback.point.lat)
  const fallbackIso = (fallback.iso2 || resolvedIso).toLowerCase()
  return {
    code: (fallback.iso2 || String(code)).toUpperCase(),
    name: rawLocationLabel || regionDisplayName(fallbackIso, 'zh-CN') || fallback.label,
    x: projected.x,
    y: projected.y,
    lon: fallback.point.lon,
    lat: fallback.point.lat,
    countryCode: fallbackIso,
  }
}
