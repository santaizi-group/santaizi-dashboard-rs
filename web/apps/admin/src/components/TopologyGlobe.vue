<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { geoDistance, geoGraticule, geoInterpolate, geoOrthographic, geoPath, geoRotation } from 'd3-geo'
import type { ScreenOffset, TopologyGraph, TopologyLink, TopologyMarker } from '@/domain/topology'
import { allMarkers, isCollectorPath, siteOffsets, visibleLinks } from '@/domain/topology'
// 随 admin 打包：/static 在本地开发时被代理到线上面板，拿不到新资产。
import worldUrl from '@/assets/world.geo.json?url'

const props = defineProps<{ graph: TopologyGraph; highlightId?: string }>()
const emit = defineEmits<{ select: [marker: TopologyMarker]; clear: [] }>()

const boxRef = ref<HTMLElement>()
const canvasRef = ref<HTMLCanvasElement>()
const tooltip = ref<{ x: number; y: number; marker: TopologyMarker } | null>(null)

type WorldGeo = { type: 'FeatureCollection'; features: Array<{ type: 'Feature'; geometry: object; properties?: { iso_a2?: string } | null }> }
type Palette = ReturnType<typeof palette>
type Projection = ReturnType<typeof geoOrthographic>

let world: WorldGeo | null = null
let resizeObserver: ResizeObserver | undefined
let themeObserver: MutationObserver | undefined
let frame = 0
let pulse = 0
/** 海洋 / 经纬网 / 陆地缓存：只随视角、尺寸、主题变化重画。不点亮国家。 */
let baseLayer: HTMLCanvasElement | null = null
let baseKey = ''
let haloDiameter = 0
let dragging = false
let centered = false
let lastSpin = 0
let lambda = 0
let phi = -20
let dragStart: { x: number; y: number; lambda: number; phi: number } | null = null
let hits: Array<{ marker: TopologyMarker; x: number; y: number; r: number }> = []

const PULSE_MS = 2400
const PULSE_LEN = 0.1
const PULSE_CAP = 80
const GLOBE_RATIO = 0.5
const GLOBE_FIT_PAD = 0
const GLOBE_SCALE_MIN = 0.64
const GLOBE_SCALE_DEFAULT = 0.82
const SPIN_STORE = 'santaizi.globe.spin'
const RAY_STORE = 'santaizi.globe.rays'
const SPIN_SPEED_MIN = 4
const SPIN_SPEED_MAX = 36
const SPIN_SPEED_DEFAULT = 12
let scaleMul = GLOBE_SCALE_DEFAULT
let markerOffsets = new Map<string, ScreenOffset>()

function loadSpin() {
  try {
    const raw = localStorage.getItem(SPIN_STORE)
    if (!raw) return { on: false, speed: SPIN_SPEED_DEFAULT }
    const parsed = JSON.parse(raw) as { on?: unknown; speed?: unknown }
    const speed = Number(parsed.speed)
    return {
      on: parsed.on === true,
      speed: Number.isFinite(speed)
        ? Math.min(SPIN_SPEED_MAX, Math.max(SPIN_SPEED_MIN, speed))
        : SPIN_SPEED_DEFAULT,
    }
  }
  catch {
    return { on: false, speed: SPIN_SPEED_DEFAULT }
  }
}

const savedSpin = loadSpin()
const autoRotate = ref(savedSpin.on)
const rotateSpeed = ref(savedSpin.speed)

function persistSpin() {
  try {
    localStorage.setItem(SPIN_STORE, JSON.stringify({ on: autoRotate.value, speed: rotateSpeed.value }))
  }
  catch { /* quota / private mode */ }
}

function loadRays() {
  try {
    const raw = localStorage.getItem(RAY_STORE)
    if (!raw) return { on: true, path: true, collectorPath: true, replication: true }
    const parsed = JSON.parse(raw) as { on?: unknown; path?: unknown; collectorPath?: unknown; replication?: unknown }
    return {
      on: parsed.on !== false,
      path: parsed.path !== false,
      collectorPath: parsed.collectorPath !== false,
      replication: parsed.replication !== false,
    }
  }
  catch {
    return { on: true, path: true, collectorPath: true, replication: true }
  }
}

const savedRays = loadRays()
const showRays = ref(savedRays.on)
const showPath = ref(savedRays.path)
const showCollectorPath = ref(savedRays.collectorPath)
const showReplication = ref(savedRays.replication)

function persistRays() {
  try {
    localStorage.setItem(RAY_STORE, JSON.stringify({
      on: showRays.value,
      path: showPath.value,
      collectorPath: showCollectorPath.value,
      replication: showReplication.value,
    }))
  }
  catch { /* quota / private mode */ }
}

function offsetOf(id: string): ScreenOffset {
  return markerOffsets.get(id) || { x: 0, y: 0 }
}
/** 3D 径向高度：贴地为 0，中点 sin 抬到半径的这一比例。短弧低、长弧高。 */
const ARC_PEAK_MIN = 0.09
const ARC_PEAK_MAX = 0.23
const ARC_STEPS = 56

function cssVar(name: string, fallback: string) {
  const host = boxRef.value || document.documentElement
  return getComputedStyle(host).getPropertyValue(name).trim() || fallback
}

/** Canvas 渐变淡出必须同色改 alpha；淡到 rgba(0,0,0,0) 会插值出泥色圈。 */
function withAlpha(color: string, alpha: number) {
  const value = color.trim()
  const comma = value.match(/^rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(?:\s*,\s*([\d.]+))?\s*\)$/i)
  if (comma) return `rgba(${comma[1]}, ${comma[2]}, ${comma[3]}, ${alpha})`
  const space = value.match(/^rgba?\(\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)(?:\s*\/\s*[\d.%]+)?\s*\)$/i)
  if (space) return `rgba(${space[1]}, ${space[2]}, ${space[3]}, ${alpha})`
  return `rgba(56, 189, 248, ${alpha})`
}

function colorAlpha(color: string) {
  const value = color.trim()
  const comma = value.match(/^rgba?\(\s*[\d.]+\s*,\s*[\d.]+\s*,\s*[\d.]+(?:\s*,\s*([\d.]+))?\s*\)$/i)
  if (comma) return comma[1] === undefined ? 1 : Number(comma[1])
  return 1
}

function palette() {
  return {
    ocean: cssVar('--sz-globe-ocean', '#d0e6fa'),
    land: cssVar('--sz-globe-land', '#eef6ff'),
    landStroke: cssVar('--sz-globe-land-stroke', 'rgba(96, 140, 184, .36)'),
    sphere: cssVar('--sz-globe-sphere-stroke', 'rgba(96, 140, 184, .45)'),
    graticule: cssVar('--sz-globe-graticule', 'rgba(96, 140, 184, .18)'),
    limb: cssVar('--sz-globe-limb', 'rgba(168, 204, 234, .28)'),
    primary: cssVar('--sz-globe-primary', '#3b82f6'),
    collector: cssVar('--sz-globe-collector', '#44cef6'),
    node: cssVar('--sz-globe-node', '#f97316'),
    pulse: cssVar('--sz-globe-pulse', '#f59e0b'),
    replication: cssVar('--sz-globe-replication', '#7c3aed'),
    danger: cssVar('--sz-danger', '#dc2626'),
    warning: cssVar('--sz-warning', '#d97706'),
    surface: cssVar('--sz-surface', '#ffffff'),
  }
}

function kindColor(marker: TopologyMarker, colors: Palette) {
  if (marker.kind === 'primary') return colors.primary
  if (marker.kind === 'collector') return colors.collector
  return colors.node
}

function markerRadius(marker: TopologyMarker, size: number) {
  if (marker.kind === 'primary') return Math.max(14, size * 0.036)
  if (marker.kind === 'collector') return Math.max(8, size * 0.02)
  return nodeVisual(marker.count).visual / 2
}

/** 同位置节点数量档位对齐 aobobo globe-earth。 */
function nodeVisual(count: number) {
  if (count > 9) return { visual: 26, dot: 8, large: true }
  if (count >= 7) return { visual: 22, dot: 6, large: false }
  if (count >= 5) return { visual: 20, dot: 6, large: false }
  if (count >= 3) return { visual: 18, dot: 6, large: false }
  if (count === 2) return { visual: 16, dot: 6, large: false }
  return { visual: 12, dot: 8, large: false }
}

function reduceMotion() {
  return typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches
}

function shouldPulse() {
  return !document.hidden && !reduceMotion() && visibleGraphLinks().some(link => link.connected)
}

function visibleGraphLinks() {
  return visibleLinks(props.graph.links, {
    highlightId: props.highlightId,
    showRays: showRays.value,
    showPath: showPath.value,
    showCollectorPath: showCollectorPath.value,
    showReplication: showReplication.value,
  })
}

function spinning() {
  return autoRotate.value && !dragging && !reduceMotion()
}

function shouldLoop() {
  return !document.hidden && (spinning() || shouldPulse())
}

function loRes() {
  return dragging || spinning()
}

/** 合帧：一帧内多次失效只画一次。拖拽时也靠它把 pointermove 压到每帧一次。 */
function schedule() {
  if (frame) return
  if (pulse) return
  frame = requestAnimationFrame(() => {
    frame = 0
    render()
    ensurePulse()
  })
}

function ensurePulse() {
  if (pulse || !shouldLoop()) return
  lastSpin = performance.now()
  const tick = (now: number) => {
    pulse = 0
    if (spinning()) {
      const dt = Math.min(0.05, (now - lastSpin) / 1000)
      lastSpin = now
      lambda = ((lambda - rotateSpeed.value * dt) % 360 + 360) % 360
    }
    else {
      lastSpin = now
    }
    render()
    if (shouldLoop()) pulse = requestAnimationFrame(tick)
  }
  pulse = requestAnimationFrame(tick)
}

function stopPulse() {
  if (!pulse) return
  cancelAnimationFrame(pulse)
  pulse = 0
}

function invalidateBase() {
  baseKey = ''
  schedule()
}

function projectionFor(width: number, height: number, size: number) {
  return geoOrthographic()
    .translate([width / 2, height / 2])
    .scale(size * GLOBE_RATIO * scaleMul)
    .rotate([lambda, phi])
    .clipAngle(90)
    .precision(loRes() ? 1.2 : 0.4)
}

function projectVisible(projection: Projection, lon: number, lat: number) {
  const rotated = projection.rotate()
  const center: [number, number] = [-(rotated[0] ?? 0), -(rotated[1] ?? 0)]
  if (geoDistance(center, [lon, lat]) > Math.PI / 2 - 0.02) return null
  const point = projection([lon, lat])
  if (!point) return null
  return { x: point[0], y: point[1], fade: Math.min(1, Math.max(0.15, 1 - geoDistance(center, [lon, lat]) / (Math.PI / 2))) }
}

function drawDot(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  radius: number,
  fill: string,
  ring: string,
  glow: boolean,
) {
  ctx.save()
  if (glow) {
    ctx.shadowColor = fill
    ctx.shadowBlur = Math.max(3, radius * 0.9)
  }
  ctx.beginPath()
  ctx.arc(x, y, radius, 0, Math.PI * 2)
  ctx.fillStyle = fill
  ctx.fill()
  ctx.shadowBlur = 0
  ctx.strokeStyle = ring
  ctx.lineWidth = 1
  ctx.stroke()
  ctx.restore()
}

function drawPulseRing(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  radius: number,
  color: string,
  now: number,
) {
  if (reduceMotion()) return
  const t = (now / 2400) % 1
  ctx.save()
  ctx.beginPath()
  ctx.arc(x, y, radius * (0.92 + t * 0.78), 0, Math.PI * 2)
  ctx.strokeStyle = color
  ctx.globalAlpha = 0.32 * (1 - t)
  ctx.lineWidth = 1.25
  ctx.stroke()
  ctx.restore()
}

/** remixicon@4.9.1 `icons/Device/base-station-fill.svg`，Canvas 用 Path2D，不走 Image。 */
const BASE_STATION = 'M12 13L18 22H6L12 13ZM10.9393 10.5606C10.3536 9.97486 10.3536 9.02511 10.9393 8.43933C11.5251 7.85354 12.4749 7.85354 13.0607 8.43933C13.6464 9.02511 13.6464 9.97486 13.0607 10.5606C12.4749 11.1464 11.5251 11.1464 10.9393 10.5606ZM5.28249 2.78247L6.6967 4.19668C3.76777 7.12562 3.76777 11.8744 6.6967 14.8033L5.28249 16.2175C1.5725 12.5075 1.5725 6.49245 5.28249 2.78247ZM18.7175 2.78247C22.4275 6.49245 22.4275 12.5075 18.7175 16.2175L17.3033 14.8033C20.2322 11.8744 20.2322 7.12562 17.3033 4.19668L18.7175 2.78247ZM8.11091 5.6109L9.52513 7.02511C8.15829 8.39195 8.15829 10.608 9.52513 11.9749L8.11091 13.3891C5.96303 11.2412 5.96303 7.75878 8.11091 5.6109H8.11091ZM15.8891 5.6109C18.037 7.75878 18.037 11.2412 15.8891 13.3891L14.4749 11.9749C15.8417 10.608 15.8417 8.39195 14.4749 7.02511L15.8891 5.6109Z'

function drawBaseStation(ctx: CanvasRenderingContext2D, x: number, y: number, size: number, color: string) {
  const scale = size / 24
  ctx.save()
  ctx.translate(x - size / 2, y - size / 2)
  ctx.scale(scale, scale)
  ctx.fillStyle = color
  ctx.fill(new Path2D(BASE_STATION))
  ctx.restore()
}

const LOGO_ARC_A = 'M61 31A80 80 0 0 1 165 62'
const LOGO_ARC_B = 'M177 91A80 80 0 0 1 112 177'
const LOGO_ARC_C = 'M79 176A80 80 0 0 1 23 80'
const LOGO_PULSE = 'M66 101h16l9-19 14 37 11-27 9 9h9'

/** 三轨守望：跟 resource/static/logo.svg 同路径，不走 Image——损坏/未解码的 SVG 会退化成黑点。 */
function drawLogo(ctx: CanvasRenderingContext2D, x: number, y: number, r: number) {
  const scale = (r * 2) / 200
  ctx.save()
  ctx.translate(x - r, y - r)
  ctx.scale(scale, scale)
  ctx.lineCap = 'round'
  ctx.lineJoin = 'round'
  ctx.lineWidth = 18
  ctx.strokeStyle = '#38BDF8'
  ctx.stroke(new Path2D(LOGO_ARC_A))
  ctx.strokeStyle = '#1473E6'
  ctx.stroke(new Path2D(LOGO_ARC_B))
  ctx.strokeStyle = '#4338CA'
  ctx.stroke(new Path2D(LOGO_ARC_C))
  ctx.beginPath()
  ctx.arc(100, 100, 43, 0, Math.PI * 2)
  ctx.fillStyle = '#0F172A'
  ctx.fill()
  ctx.strokeStyle = '#F8FAFC'
  ctx.lineWidth = 9
  ctx.stroke(new Path2D(LOGO_PULSE))
  ctx.restore()
}

function drawLand(
  ctx: CanvasRenderingContext2D,
  path: (feature: never) => void,
  features: WorldGeo['features'],
  fill: string,
  stroke: string,
) {
  if (!features.length) return
  ctx.beginPath()
  for (const feature of features) path(feature as never)
  ctx.fillStyle = fill
  ctx.fill()
  ctx.strokeStyle = stroke
  ctx.lineWidth = 0.6
  ctx.stroke()
}

function buildBase(width: number, height: number, size: number, ratio: number, colors: Palette) {
  const canvas = baseLayer || (baseLayer = document.createElement('canvas'))
  const bitmapW = Math.round(width * ratio)
  const bitmapH = Math.round(height * ratio)
  // 尺寸没变就只清像素：每帧重新分配位图会让拖拽时整层闪一下。
  if (canvas.width !== bitmapW || canvas.height !== bitmapH) {
    canvas.width = bitmapW
    canvas.height = bitmapH
  }
  const ctx = canvas.getContext('2d')
  if (!ctx) return
  ctx.setTransform(1, 0, 0, 1, 0, 0)
  ctx.clearRect(0, 0, bitmapW, bitmapH)
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0)
  const path = geoPath(projectionFor(width, height, size), ctx)
  const radius = size * GLOBE_RATIO * scaleMul
  const cx = width / 2
  const cy = height / 2

  ctx.save()
  ctx.beginPath()
  path({ type: 'Sphere' })
  ctx.fillStyle = colors.ocean
  ctx.fill()
  ctx.strokeStyle = colors.sphere
  ctx.lineWidth = 1
  ctx.stroke()
  ctx.clip()

  ctx.beginPath()
  path(geoGraticule()())
  ctx.strokeStyle = colors.graticule
  ctx.lineWidth = 0.45
  ctx.stroke()

  if (world) drawLand(ctx, path, world.features, colors.land, colors.landStroke)

  // 球内 limb 是内阴影。深色关掉（--sz-globe-limb alpha=0）；外围光晕走 CSS halo。
  if (colorAlpha(colors.limb) > 0.01) {
    const limb = ctx.createRadialGradient(cx, cy, radius * 0.62, cx, cy, radius)
    limb.addColorStop(0, withAlpha(colors.limb, 0))
    limb.addColorStop(1, colors.limb)
    ctx.beginPath()
    path({ type: 'Sphere' })
    ctx.fillStyle = limb
    ctx.fill()
  }
  ctx.restore()
}

function linkShift(fromId: string, toId: string) {
  let hash = 2166136261
  const key = `${fromId}\0${toId}`
  for (let i = 0; i < key.length; i++) hash = Math.imul(hash ^ key.charCodeAt(i), 16777619)
  return (hash >>> 0) / 4294967296
}

function pulseWindows(phase: number, length: number): Array<[number, number]> {
  const end = phase + length
  if (end <= 1) return [[phase, end]]
  return [[phase, 1], [0, end - 1]]
}

type ArcPoint = { x: number; y: number }

function arcPeak(from: TopologyMarker, to: TopologyMarker) {
  const dist = geoDistance([from.lon, from.lat], [to.lon, to.lat])
  return ARC_PEAK_MIN + (ARC_PEAK_MAX - ARC_PEAK_MIN) * Math.min(1, dist / 1.2)
}

/**
 * 大圆逐点径向抬高后投影。标点仍按半球隐藏。
 * 射线不跟标点整段裁掉；只有落在球面轮廓里的背面采样被地球挡住，飞出轮廓的弧保留。
 */
function sampleArc(
  projection: Projection,
  from: TopologyMarker,
  to: TopologyMarker,
  t0: number,
  t1: number,
  steps: number,
): Array<ArcPoint | null> {
  const interpolate = geoInterpolate([from.lon, from.lat], [to.lon, to.lat])
  const peak = arcPeak(from, to)
  const rotate = geoRotation(projection.rotate())
  const k = projection.scale()
  const [tx, ty] = projection.translate()
  const ox = tx ?? 0
  const oy = ty ?? 0
  const diskR2 = k * k
  const out: Array<ArcPoint | null> = []
  const n = Math.max(2, steps)
  for (let i = 0; i <= n; i++) {
    const t = t0 + (t1 - t0) * (i / n)
    const coord = interpolate(t)
    const lon = coord[0]
    const lat = coord[1]
    if (lon === undefined || lat === undefined) {
      out.push(null)
      continue
    }
    const rotated = rotate([lon, lat])
    const lambdaDeg = rotated[0]
    const phiDeg = rotated[1]
    if (lambdaDeg === undefined || phiDeg === undefined) {
      out.push(null)
      continue
    }
    const lambda = (lambdaDeg * Math.PI) / 180
    const phi = (phiDeg * Math.PI) / 180
    const cosPhi = Math.cos(phi)
    const x = cosPhi * Math.sin(lambda)
    const y = Math.sin(phi)
    const z = cosPhi * Math.cos(lambda)
    const radiusMul = 1 + Math.sin(Math.PI * t) * peak
    const px = ox + k * x * radiusMul
    const py = oy - k * y * radiusMul
    const behind = z < 0
    const inside = (px - ox) * (px - ox) + (py - oy) * (py - oy) < diskR2
    if (behind && inside) {
      out.push(null)
      continue
    }
    const fromOff = offsetOf(from.id)
    const toOff = offsetOf(to.id)
    out.push({
      x: px + (1 - t) * fromOff.x + t * toOff.x,
      y: py + (1 - t) * fromOff.y + t * toOff.y,
    })
  }
  return out
}

function forEachRun(points: Array<ArcPoint | null>, visit: (run: ArcPoint[]) => void) {
  let run: ArcPoint[] = []
  const flush = () => {
    if (run.length >= 2) visit(run)
    run = []
  }
  for (const point of points) {
    if (point) run.push(point)
    else flush()
  }
  flush()
}

function strokeRuns(ctx: CanvasRenderingContext2D, points: Array<ArcPoint | null>) {
  forEachRun(points, (run) => {
    const start = run[0]
    if (!start) return
    ctx.beginPath()
    ctx.moveTo(start.x, start.y)
    for (let i = 1; i < run.length; i++) {
      const point = run[i]
      if (point) ctx.lineTo(point.x, point.y)
    }
    ctx.stroke()
  })
}

function lastVisible(points: Array<ArcPoint | null>): ArcPoint | null {
  for (let i = points.length - 1; i >= 0; i--) {
    const point = points[i]
    if (point) return point
  }
  return null
}

function drawPulseSegment(
  ctx: CanvasRenderingContext2D,
  projection: Projection,
  from: TopologyMarker,
  to: TopologyMarker,
  start: number,
  end: number,
  color: string,
  glow: boolean,
) {
  if (end - start < 0.01) return
  const points = sampleArc(projection, from, to, start, end, Math.max(6, Math.round((end - start) * ARC_STEPS)))
  ctx.strokeStyle = color
  ctx.lineWidth = 1.55
  ctx.lineCap = 'round'
  ctx.lineJoin = 'round'
  ctx.globalAlpha = 0.92
  if (glow) {
    ctx.shadowColor = color
    ctx.shadowBlur = 8
  }
  strokeRuns(ctx, points)
  ctx.shadowBlur = 0
  const head = lastVisible(points)
  if (head) {
    ctx.beginPath()
    ctx.arc(head.x, head.y, 1.7, 0, Math.PI * 2)
    ctx.fillStyle = color
    ctx.globalAlpha = 1
    ctx.fill()
  }
}

function drawLink(
  ctx: CanvasRenderingContext2D,
  projection: Projection,
  link: TopologyLink,
  from: TopologyMarker,
  to: TopologyMarker,
  colors: Palette,
  now: number,
  pulseIndex: number,
) {
  const track = sampleArc(projection, from, to, 0, 1, ARC_STEPS)
  if (!lastVisible(track)) return
  const live = link.kind === 'replication'
    ? colors.replication
    : isCollectorPath(link) ? colors.collector : colors.pulse
  ctx.save()
  ctx.strokeStyle = link.connected ? live : colors.danger
  ctx.globalAlpha = link.connected ? (link.kind === 'replication' ? 0.62 : 0.48) : 0.4
  ctx.lineWidth = link.kind === 'replication' ? 1.6 : 1.35
  ctx.lineCap = 'round'
  ctx.lineJoin = 'round'
  if (!link.connected) ctx.setLineDash([4, 4])
  strokeRuns(ctx, track)
  ctx.restore()

  if (!link.connected || reduceMotion()) return
  if (pulseIndex >= PULSE_CAP) return

  const phase = ((now / PULSE_MS) + linkShift(link.fromId, link.toId)) % 1
  ctx.save()
  for (const [start, end] of pulseWindows(phase, PULSE_LEN)) {
    drawPulseSegment(ctx, projection, from, to, start, end, live, pulseIndex < 24)
  }
  ctx.restore()
}

function drawMarker(
  ctx: CanvasRenderingContext2D,
  marker: TopologyMarker,
  projected: { x: number; y: number; fade: number },
  size: number,
  colors: Palette,
  now: number,
  clustered = false,
) {
  const shrink = clustered ? (marker.kind === 'primary' ? 0.88 : 0.8) : 1
  const r = markerRadius(marker, size) * shrink
  const kind = kindColor(marker, colors)
  const offline = marker.status === 'offline'
  const mixed = marker.status === 'mixed'
  const x = projected.x
  const y = projected.y
  const ring = document.documentElement.classList.contains('dark')
    ? 'rgba(248, 250, 252, .55)'
    : 'rgba(255, 255, 255, .8)'
  const muted = colors.danger
  ctx.save()
  ctx.globalAlpha = marker.kind === 'primary' ? 1 : projected.fade * (offline ? 0.72 : 1)

  const glow = !loRes()

  if (marker.kind === 'primary') {
    drawLogo(ctx, x, y, r)
  }
  else if (marker.kind === 'collector') {
    drawBaseStation(ctx, x, y, r * 1.7, offline ? muted : kind)
  }
  else {
    const layout = nodeVisual(marker.count)
    const visual = layout.visual * shrink
    const dot = layout.dot * shrink
    const flags = marker.onlines.length ? marker.onlines : Array.from({ length: marker.count }, () => !offline)
    if (!offline) drawPulseRing(ctx, x, y, visual / 2, kind, now)
    if (layout.large) {
      ctx.save()
      ctx.globalAlpha = projected.fade * 0.16
      ctx.beginPath()
      ctx.arc(x, y, visual / 2 + 3.5, 0, Math.PI * 2)
      ctx.fillStyle = kind
      ctx.fill()
      ctx.restore()
      const fill = offline ? muted : kind
      const stroke = mixed ? colors.warning : ring
      drawDot(ctx, x, y, visual / 2, fill, stroke, glow)
    }
    else if (marker.count === 1) {
      drawDot(ctx, x, y, dot / 2, flags[0] ? kind : muted, ring, glow)
    }
    else {
      const radius = (visual - dot) / 2 - 1
      const start = marker.count > 6 ? 1 : 0
      const surrounding = marker.count > 6 ? marker.count - 1 : marker.count
      if (marker.count > 6) {
        drawDot(ctx, x, y, dot / 2, flags[0] ? kind : muted, ring, glow)
      }
      for (let i = 0; i < surrounding; i++) {
        const angle = (Math.PI * 2 * i) / surrounding - Math.PI / 2
        const online = flags[start + i] ?? !offline
        drawDot(
          ctx,
          x + Math.cos(angle) * radius,
          y + Math.sin(angle) * radius,
          dot / 2,
          online ? kind : muted,
          ring,
          glow,
        )
      }
    }
  }
  ctx.restore()
  hits.push({ marker, x, y, r: r + 6 })
}

function render() {
  const canvas = canvasRef.value
  const box = boxRef.value
  if (!canvas || !box) return
  const width = Math.max(160, box.clientWidth)
  const height = Math.max(160, box.clientHeight)
  const size = Math.min(width, height)
  scaleMul = Math.min(maxScale(size), Math.max(GLOBE_SCALE_MIN, scaleMul))
  const ratio = Math.min(window.devicePixelRatio || 1, 2)
  const nextW = Math.round(width * ratio)
  const nextH = Math.round(height * ratio)
  if (canvas.width !== nextW || canvas.height !== nextH) {
    canvas.width = nextW
    canvas.height = nextH
  }
  const cssW = `${width}px`
  const cssH = `${height}px`
  if (canvas.style.width !== cssW) canvas.style.width = cssW
  if (canvas.style.height !== cssH) canvas.style.height = cssH
  const ctx = canvas.getContext('2d')
  if (!ctx) return
  const colors = palette()

  const diameter = Math.round(size * GLOBE_RATIO * scaleMul * 2)
  if (diameter !== haloDiameter) {
    haloDiameter = diameter
    box.style.setProperty('--sz-globe-d', `${diameter}px`)
  }

  const key = [
    nextW, nextH, lambda.toFixed(2), phi.toFixed(2), scaleMul.toFixed(3),
    loRes() ? 'drag' : 'still', world ? 'geo' : 'bare',
    colors.ocean, colors.land, colors.limb,
  ].join('|')
  if (key !== baseKey) {
    buildBase(width, height, size, ratio, colors)
    baseKey = key
  }

  ctx.setTransform(1, 0, 0, 1, 0, 0)
  ctx.clearRect(0, 0, nextW, nextH)
  if (baseLayer) ctx.drawImage(baseLayer, 0, 0)
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0)

  const projection = projectionFor(width, height, size)
  const markerById = new Map(allMarkers(props.graph).map(item => [item.id, item]))
  markerOffsets = siteOffsets([...markerById.values()], size * GLOBE_RATIO * scaleMul)
  const now = performance.now()
  let pulseIndex = 0

  const links = [...visibleGraphLinks()].sort((a, b) => Number(b.kind === 'replication') - Number(a.kind === 'replication'))
  for (const link of links) {
    const from = markerById.get(link.fromId)
    const to = markerById.get(link.toId)
    if (!from || !to) continue
    drawLink(ctx, projection, link, from, to, colors, now, pulseIndex)
    if (link.connected) pulseIndex += 1
  }

  hits = []
  const drawOrder = [...props.graph.nodes, ...props.graph.collectors, props.graph.primary]
  for (const marker of drawOrder) {
    const projected = projectVisible(projection, marker.lon, marker.lat)
    if (!projected) continue
    const shift = offsetOf(marker.id)
    drawMarker(ctx, marker, { x: projected.x + shift.x, y: projected.y + shift.y, fade: projected.fade }, size, colors, now, Boolean(shift.clustered))
  }
}

/** 没有自转，初次进来必须把有内容的半球转到正面。 */
function centerOnPrimary() {
  if (centered) return
  const primary = props.graph.primary
  const ready = props.graph.nodes.length > 0 || props.graph.collectors.length > 0 || !primary.derived
  if (!ready || !Number.isFinite(primary.lon) || !Number.isFinite(primary.lat)) return
  lambda = -primary.lon
  phi = Math.max(-60, Math.min(60, -primary.lat))
  centered = true
  invalidateBase()
}

function hitTest(x: number, y: number) {
  for (let i = hits.length - 1; i >= 0; i--) {
    const item = hits[i]
    if (!item) continue
    const dx = x - item.x
    const dy = y - item.y
    if (dx * dx + dy * dy <= item.r * item.r) return item
  }
  return null
}

function localPoint(event: PointerEvent) {
  const canvas = canvasRef.value
  if (!canvas) return { x: 0, y: 0 }
  const rect = canvas.getBoundingClientRect()
  return { x: event.clientX - rect.left, y: event.clientY - rect.top }
}

function onPointerDown(event: PointerEvent) {
  if (event.button !== 0) return
  dragging = true
  centered = true
  const point = localPoint(event)
  dragStart = { x: point.x, y: point.y, lambda, phi }
  canvasRef.value?.setPointerCapture(event.pointerId)
}

function onPointerMove(event: PointerEvent) {
  const point = localPoint(event)
  if (dragging && dragStart) {
    tooltip.value = null
    lambda = dragStart.lambda + (point.x - dragStart.x) * 0.35
    phi = Math.max(-80, Math.min(80, dragStart.phi - (point.y - dragStart.y) * 0.25))
    schedule()
    return
  }
  const hit = hitTest(point.x, point.y)
  canvasRef.value?.style.setProperty('cursor', hit ? 'pointer' : 'grab')
  tooltip.value = hit ? { x: hit.x, y: hit.y, marker: hit.marker } : null
}

function onPointerUp(event: PointerEvent) {
  const point = localPoint(event)
  const moved = dragStart ? Math.hypot(point.x - dragStart.x, point.y - dragStart.y) > 4 : false
  dragging = false
  dragStart = null
  schedule()
  if (!moved) {
    const hit = hitTest(point.x, point.y)
    if (hit) emit('select', hit.marker)
    else emit('clear')
  }
}

function onPointerLeave() {
  tooltip.value = null
  if (!dragging) canvasRef.value?.style.setProperty('cursor', 'grab')
}

function maxScale(size: number) {
  return Math.max(GLOBE_SCALE_MIN, (size - GLOBE_FIT_PAD) / (size * GLOBE_RATIO * 2))
}

function onWheel(event: WheelEvent) {
  event.preventDefault()
  const box = boxRef.value
  const size = box ? Math.min(Math.max(160, box.clientWidth), Math.max(160, box.clientHeight)) : 160
  scaleMul = Math.min(maxScale(size), Math.max(GLOBE_SCALE_MIN, scaleMul * (event.deltaY > 0 ? 0.92 : 1.08)))
  schedule()
}

function onVisibility() {
  if (document.hidden) stopPulse()
  else ensurePulse()
}

async function loadGeo() {
  try {
    const response = await fetch(worldUrl)
    if (!response.ok) return
    world = await response.json() as WorldGeo
    invalidateBase()
  }
  catch {
    world = null
  }
}

onMounted(() => {
  void loadGeo()
  centerOnPrimary()
  const canvas = canvasRef.value
  canvas?.addEventListener('wheel', onWheel, { passive: false })
  document.addEventListener('visibilitychange', onVisibility)
  if (boxRef.value && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => schedule())
    resizeObserver.observe(boxRef.value)
  }
  themeObserver = new MutationObserver(() => invalidateBase())
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
  schedule()
})

watch(() => props.graph, () => {
  centerOnPrimary()
  schedule()
}, { deep: true })
watch(() => props.highlightId, () => schedule())
watch(autoRotate, () => {
  persistSpin()
  if (autoRotate.value) ensurePulse()
  else schedule()
})
watch(rotateSpeed, persistSpin)
watch([showRays, showPath, showCollectorPath, showReplication], () => {
  persistRays()
  schedule()
})

onBeforeUnmount(() => {
  if (frame) cancelAnimationFrame(frame)
  stopPulse()
  canvasRef.value?.removeEventListener('wheel', onWheel)
  document.removeEventListener('visibilitychange', onVisibility)
  resizeObserver?.disconnect()
  themeObserver?.disconnect()
})
</script>

<template>
  <div ref="boxRef" class="topology-globe">
    <div class="topology-globe__halo" aria-hidden="true" />
    <canvas
      ref="canvasRef"
      class="topology-globe__canvas"
      @pointerdown="onPointerDown"
      @pointermove="onPointerMove"
      @pointerup="onPointerUp"
      @pointerleave="onPointerLeave"
    />
    <div v-if="tooltip" class="topology-globe__tip" :style="{ left: `${tooltip.x}px`, top: `${tooltip.y}px` }">
      <strong>{{ tooltip.marker.name }}</strong>
      <span v-if="tooltip.marker.derived">{{ $t('derivedLocation') }}</span>
      <span v-if="tooltip.marker.coverage">{{ tooltip.marker.coverage }}</span>
    </div>
    <div
      v-if="$slots.legend"
      class="topology-globe__legend"
      @pointerdown.stop
      @pointerup.stop
      @pointermove.stop
      @wheel.stop.prevent
    >
      <slot name="legend" />
      <div class="topology-legend-controls">
        <label class="topology-legend-rays__toggle">
          <span>{{ $t('globeRays') }}</span>
          <el-switch v-model="showRays" size="small" />
        </label>
        <div class="topology-globe__spin">
          <label class="topology-legend-rays__toggle">
            <span>{{ $t('globeSpin') }}</span>
            <el-switch v-model="autoRotate" size="small" />
          </label>
          <el-slider
            v-if="autoRotate"
            v-model="rotateSpeed"
            :min="SPIN_SPEED_MIN"
            :max="SPIN_SPEED_MAX"
            :show-tooltip="false"
            :aria-label="$t('globeSpinSpeed')"
          />
        </div>
      </div>
      <div v-if="showRays" class="topology-legend-rays">
        <button
          type="button"
          class="topology-legend-rays__kind"
          :class="{ 'is-off': !showPath }"
          :aria-pressed="showPath"
          @click="showPath = !showPath"
        >
          <i class="topology-legend-rays__swatch is-path" aria-hidden="true"></i>{{ $t('nodeLinks') }}
        </button>
        <button
          type="button"
          class="topology-legend-rays__kind"
          :class="{ 'is-off': !showCollectorPath }"
          :aria-pressed="showCollectorPath"
          @click="showCollectorPath = !showCollectorPath"
        >
          <i class="topology-legend-rays__swatch is-collector-path" aria-hidden="true"></i>{{ $t('nodeCollectorLinks') }}
        </button>
        <button
          type="button"
          class="topology-legend-rays__kind"
          :class="{ 'is-off': !showReplication }"
          :aria-pressed="showReplication"
          @click="showReplication = !showReplication"
        >
          <i class="topology-legend-rays__swatch is-replication" aria-hidden="true"></i>{{ $t('collectorLinks') }}
        </button>
      </div>
    </div>
    <div v-if="$slots.note" class="topology-globe__note">
      <slot name="note" />
    </div>
  </div>
</template>
