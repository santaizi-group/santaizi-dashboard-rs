<script setup lang="ts">
withDefaults(
  defineProps<{
    description: string
    title?: string
    icon?: string
    tone?: 'default' | 'danger'
  }>(),
  {
    title: '',
    icon: 'ri-inbox-line',
    tone: 'default',
  },
)
</script>

<template>
  <div class="app-empty" :class="`tone-${tone}`">
    <i :class="icon" class="empty-icon" aria-hidden="true" />
    <p v-if="title" class="title">{{ title }}</p>
    <p class="desc">{{ description }}</p>
  </div>
</template>

<style scoped>
.app-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 32px var(--space-5, 20px);
  color: var(--text-muted, #6c7788);
  min-height: 200px;
}
.empty-icon {
  font-size: 40px;
  line-height: 1;
  color: var(--text-muted, #6c7788);
  opacity: 0.72;
  margin-bottom: 4px;
}
.title {
  margin: 0;
  font-size: 15px;
  font-weight: 650;
  line-height: 1.3;
  text-align: center;
  color: var(--text, #172033);
}
html.dark .title,
[data-theme='dark'] .title {
  color: var(--text, #f1f3f5);
}
.desc {
  margin: 0;
  font-size: 13px;
  line-height: 1.4;
  text-align: center;
}
.tone-danger .empty-icon {
  color: #ef4444;
  opacity: 0.95;
}
</style>
