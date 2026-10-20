<script setup lang="ts">
import { osLogoClass } from '../domain/osLogo'

defineProps<{ platform?: string }>()
</script>

<template>
  <span :class="osLogoClass(platform)" class="ss-os-logo" aria-hidden="true" />
</template>
