<script setup lang="ts">
import { computed } from 'vue'
import { osLogoClass } from '@santaizi/theme-server-status'

const props = defineProps<{ platform?: string }>()
const className = computed(() => osLogoClass(props.platform))
</script>

<template>
  <span :class="className" class="nazhua-os-logo" aria-hidden="true" />
</template>
