<script setup lang="ts">
withDefaults(
  defineProps<{
    width?: string
    height?: string
    radius?: string
  }>(),
  {
    width: '100%',
    height: '14px',
    radius: 'var(--skeleton-radius)',
  },
)
</script>

<template>
  <span
    class="skeleton-bone"
    aria-hidden="true"
    :style="{ width, height, borderRadius: radius }"
  />
</template>
