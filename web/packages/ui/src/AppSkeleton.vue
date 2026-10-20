<script setup lang="ts">
import AppSkeletonBone from './AppSkeletonBone.vue'
import { useI18n } from 'vue-i18n'

withDefaults(
  defineProps<{
    variant?: 'page' | 'table' | 'boot'
  }>(),
  { variant: 'page' },
)

const { t, te } = useI18n()
const loadingLabel = te('loading') ? String(t('loading')) : '加载中'
const pageRows = 5
const tableRows = 6
</script>

<template>
  <div class="app-skeleton" :class="`is-${variant}`" role="status" aria-busy="true">
    <span class="sr-only">{{ loadingLabel }}</span>

    <div v-if="variant === 'boot'" class="boot-wrap">
      <div class="boot-card">
        <AppSkeletonBone width="40%" height="18px" radius="var(--radius)" />
        <AppSkeletonBone width="100%" height="12px" />
        <AppSkeletonBone width="88%" height="12px" />
        <AppSkeletonBone width="72%" height="12px" />
      </div>
    </div>

    <div v-else-if="variant === 'table'" class="table-sk">
      <AppSkeletonBone width="100%" height="40px" radius="var(--radius)" />
      <AppSkeletonBone
        v-for="i in tableRows"
        :key="i"
        width="100%"
        height="28px"
        class="sk-gap"
      />
    </div>

    <div v-else class="page-sk">
      <AppSkeletonBone width="28%" height="18px" radius="var(--radius)" />
      <AppSkeletonBone
        v-for="i in pageRows"
        :key="i"
        :width="i % 2 === 0 ? '92%' : '100%'"
        height="14px"
        class="sk-gap"
      />
    </div>
  </div>
</template>

<style scoped>
.app-skeleton { width: 100%; min-height: 0; }
.is-boot { height: 100%; min-height: 100%; background: var(--bg); }
.boot-wrap {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 40vh;
  padding: var(--space-7);
}
.boot-card {
  width: min(360px, 100%);
  display: flex;
  flex-direction: column;
  gap: var(--layout-gap);
  padding: 28px var(--space-7);
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-overlay);
}
.page-sk,
.table-sk {
  display: flex;
  flex-direction: column;
  padding: var(--space-5);
  background: var(--bg-elevated);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow);
}
.sk-gap { margin-top: var(--space-4); }
</style>
