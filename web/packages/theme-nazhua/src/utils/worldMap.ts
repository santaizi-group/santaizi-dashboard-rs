import type { ServerHost } from '@santaizi/api'
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

export function resolveServerLocation(
  server: { host?: ServerHost; public_note?: Record<string, unknown> },
): ServerLocation | null {
  let aliasCode: string | undefined
  let locationCode: string | undefined
  const custom = server.public_note?.customData as Record<string, unknown> | undefined
  if (custom?.location) {
    aliasCode = String(custom.location)
    locationCode = String(custom.location)
  } else if (server.host?.CountryCode) {
    aliasCode = server.host.CountryCode.toUpperCase()
  }
  const normalizedAliasCode = typeof aliasCode === 'string' ? aliasCode.toUpperCase() : aliasCode
  const code = alias2code(normalizedAliasCode) || locationCode || normalizedAliasCode
  if (!code) return null
  const locationInfo = locationCode2Info(code) || {}
  const geoInfo = locationCode2GeoInfo(code) || locationCode2GeoInfo(normalizedAliasCode) || {}
  const merged = { ...geoInfo, ...locationInfo } as Record<string, unknown>
  const x = merged.x as number | undefined
  const y = merged.y as number | undefined
  const lon = merged.lon as number | undefined
  const lat = merged.lat as number | undefined
  const hasMapCoord = typeof x === 'number' && typeof y === 'number'
  const hasGeoCoord = typeof lon === 'number' && typeof lat === 'number'
  if (!hasMapCoord && !hasGeoCoord) return null
  return {
    code,
    name: String(merged.name || ''),
    x,
    y,
    lon: hasGeoCoord ? lon! : (x! / 1280) * 360 - 180,
    lat: hasGeoCoord ? lat! : 90 - (y! / 621) * 180,
    countryCode: (server.host?.CountryCode || '').toLowerCase(),
  }
}
