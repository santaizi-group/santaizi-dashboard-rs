<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { geoGraticule, geoOrthographic, geoPath } from 'd3-geo'
import type { ServerLocation } from '../../utils/worldMap'
import countryNameMap from '../../data/country-name-map'

const props = defineProps<{ location: ServerLocation | null }>()
const boxRef = ref<HTMLElement>()
const canvasRef = ref<HTMLCanvasElement>()
const worldGeoJson = ref<GeoJSON.FeatureCollection | null>(null)
const loaded = ref(false)
let resizeObserver: ResizeObserver | undefined
let themeObserver: MutationObserver | undefined

const ready = computed(() => loaded.value && props.location && Number.isFinite(props.location.lon) && Number.isFinite(props.location.lat))
const targetCountryName = computed(() => {
  const code = props.location?.countryCode?.toLowerCase() || ''
  return (countryNameMap as Record<string, string>)[code] || ''
})

function isDarkTheme() {
  const root = document.documentElement
  return root.classList.contains('dark') || root.getAttribute('data-theme') === 'dark'
}

function palette() {
  const shell = document.querySelector('.nazhua-shell')
  const style = getComputedStyle(shell instanceof HTMLElement ? shell : document.documentElement)
  const dark = isDarkTheme()
  const read = (token: string, fallback: string) => style.getPropertyValue(token).trim() || fallback
  return {
    ocean: read('--nazhua-globe-ocean', dark ? '#0b2833' : '#cfe6f7'),
    land: read('--nazhua-globe-land', dark ? '#5e8fa3' : '#ffffff'),
    landStroke: read('--nazhua-globe-land-stroke', dark ? 'rgba(180, 236, 248, .35)' : 'rgba(147, 197, 222, .45)'),
    highlight: read('--nazhua-globe-highlight', dark ? '#2ee0f0' : '#168be5'),
    sphereStroke: read('--nazhua-globe-sphere-stroke', dark ? 'rgba(144, 242, 255, .5)' : 'rgba(147, 197, 222, .55)'),
    graticule: read('--nazhua-globe-graticule', dark ? 'rgba(144, 242, 255, .16)' : 'rgba(147, 197, 222, .2)'),
    marker: read('--nazhua-globe-marker', dark ? '#e0fcff' : '#c69c00'),
    markerGlow: read('--nazhua-globe-marker-glow', dark ? '#00dcff' : 'rgba(198, 156, 0, .4)'),
    sphereGlow: read('--nazhua-globe-glow', dark ? 'rgba(0, 212, 255, .42)' : 'rgba(110, 170, 210, .28)'),
  }
}

function featureName(feature: GeoJSON.Feature) {
  return feature.properties && typeof feature.properties === 'object'
    ? String((feature.properties as Record<string, unknown>).name || '')
    : ''
}

function renderGlobe() {
  const canvas = canvasRef.value
  const box = boxRef.value
  const location = props.location
  const geoJson = worldGeoJson.value
  if (!canvas || !box || !location || !geoJson) return
  const size = Math.max(128, Math.min(box.clientWidth || 170, box.clientHeight || 170) || 170)
  const ratio = Math.min(window.devicePixelRatio || 1, 2)
  canvas.width = Math.round(size * ratio)
  canvas.height = Math.round(size * ratio)
  canvas.style.width = `${size}px`
  canvas.style.height = `${size}px`
  const ctx = canvas.getContext('2d')
  if (!ctx) return
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0)
  ctx.clearRect(0, 0, size, size)

  const colors = palette()
  const projection = geoOrthographic()
    .translate([size / 2, size / 2])
    .scale(size * .46)
    .rotate([-location.lon, -location.lat])
    .clipAngle(90)
    .precision(.2)
  const path = geoPath(projection, ctx)

  ctx.save()
  ctx.beginPath()
  path({ type: 'Sphere' })
  ctx.fillStyle = colors.ocean
  ctx.shadowColor = colors.sphereGlow
  ctx.shadowBlur = size * .08
  ctx.fill()
  ctx.shadowBlur = 0
  ctx.strokeStyle = colors.sphereStroke
  ctx.lineWidth = 1
  ctx.stroke()
  ctx.clip()

  ctx.beginPath()
  path(geoGraticule()())
  ctx.strokeStyle = colors.graticule
  ctx.lineWidth = .45
  ctx.stroke()

  for (const feature of geoJson.features) {
    ctx.beginPath()
    path(feature)
    ctx.fillStyle = featureName(feature) === targetCountryName.value ? colors.highlight : colors.land
    ctx.fill()
    ctx.strokeStyle = colors.landStroke
    ctx.lineWidth = .6
    ctx.stroke()
  }

  ctx.beginPath()
  ctx.arc(size / 2, size / 2, Math.max(3.5, size * .028), 0, Math.PI * 2)
  ctx.fillStyle = colors.marker
  ctx.shadowColor = colors.markerGlow
  ctx.shadowBlur = size * .1
  ctx.fill()
  ctx.restore()
}

async function loadGeo() {
  try {
    const response = await fetch('/static/theme-nazhua/maps/world.geo.json')
    if (!response.ok) return
    worldGeoJson.value = await response.json()
    loaded.value = true
    await nextTick()
    requestAnimationFrame(renderGlobe)
  } catch {
    loaded.value = false
  }
}

onMounted(() => {
  loadGeo()
  if (boxRef.value && typeof ResizeObserver !== 'undefined') {
    resizeObserver = new ResizeObserver(() => requestAnimationFrame(renderGlobe))
    resizeObserver.observe(boxRef.value)
  }
  themeObserver = new MutationObserver(() => requestAnimationFrame(renderGlobe))
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class', 'data-theme'] })
})

watch(canvasRef, (canvas) => {
  if (canvas) requestAnimationFrame(renderGlobe)
})
watch(() => [props.location?.lon, props.location?.lat, targetCountryName.value, ready.value], () => {
  requestAnimationFrame(renderGlobe)
})
onBeforeUnmount(() => {
  resizeObserver?.disconnect()
  themeObserver?.disconnect()
})
</script>

<template>
  <div ref="boxRef" class="nazhua-globe" :class="{ 'nazhua-globe--ready': ready }">
    <canvas ref="canvasRef" class="nazhua-globe__chart" aria-hidden="true"></canvas>
  </div>
</template>
