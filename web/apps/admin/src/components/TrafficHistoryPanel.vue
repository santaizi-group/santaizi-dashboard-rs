<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { AppEmpty } from '@santaizi/ui'
import type { TrafficPolicyHistory } from '@santaizi/api'
import { formatBytes } from '@/composables/format'
import TrafficUsageBars from '@/components/TrafficUsageBars.vue'

const props = defineProps<{
  items: TrafficPolicyHistory[]
  loading: boolean
}>()

const { t, locale } = useI18n()
const policyId = ref<number>()

watch(() => props.items, (items) => {
  if (!items.some(item => item.policy_id === policyId.value)) {
    policyId.value = items[0]?.policy_id
  }
}, { immediate: true })

const current = computed(() => props.items.find(item => item.policy_id === policyId.value) || props.items[0])
const policyOptions = computed(() => props.items.map(item => ({ label: item.name, value: item.policy_id })))

function byteLabel(value: number) {
  return formatBytes(value, locale.value)
}
</script>

<template>
  <div v-loading="loading" class="traffic-history-panel">
    <AppEmpty v-if="!items.length && !loading" icon="ri-exchange-2-line" :description="t('noTrafficPolicies')" />
    <template v-else-if="current">
      <div v-if="items.length > 1" class="traffic-history-toolbar">
        <el-segmented v-if="items.length <= 4" v-model="policyId" :options="policyOptions" />
        <el-select v-else v-model="policyId" class="toolbar-filter">
          <el-option v-for="item in items" :key="item.policy_id" :label="item.name" :value="item.policy_id" />
        </el-select>
      </div>
      <div class="traffic-progress">
        <el-progress
          :percentage="Math.min(100, Math.round(current.usage.usage_percent))"
          :status="current.usage.status === 'exceeded' ? 'exception' : undefined"
        />
        <span>{{ byteLabel(current.usage.used_bytes) }} / {{ byteLabel(current.usage.quota_bytes) }}</span>
      </div>
      <section class="traffic-chart">
        <h3>{{ t('trafficHourly') }}</h3>
        <TrafficUsageBars :points="current.hourly" grain="hour" />
      </section>
      <section class="traffic-chart">
        <h3>{{ t('trafficDaily') }}</h3>
        <TrafficUsageBars :points="current.daily" grain="day" />
      </section>
    </template>
  </div>
</template>

<style scoped>
.traffic-history-panel { min-height: 160px; }
.traffic-history-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; margin: 0 0 12px; }
.traffic-chart { margin-top: 18px; }
.traffic-chart h3 { margin: 0 0 10px; font-size: 13px; font-weight: 650; }
</style>
