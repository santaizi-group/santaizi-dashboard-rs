<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { NETWORK_RANGES } from '../composables/useNetworkMonitor'

// 六档预设时间片：网络监控与资源历史共用。null = 无选中（自定义范围生效时）。
defineProps<{ hours: number | null; label?: boolean }>()
const emit = defineEmits<{ 'update:hours': [hours: number] }>()
const { t } = useI18n()
</script>

<template>
  <div class="ss-range-chips" role="group" :aria-label="t('nazhua.recent')">
    <span v-if="label" class="ss-range-chips__label">{{ t('nazhua.recent') }}</span>
    <button
      v-for="item in NETWORK_RANGES"
      :key="item.hours"
      type="button"
      :class="{ 'is-active': item.hours === hours }"
      :aria-pressed="item.hours === hours"
      @click="emit('update:hours', item.hours)"
    >{{ t(item.labelKey) }}</button>
  </div>
</template>
