<script setup lang="ts">
import { toRef } from 'vue'
import { useI18n } from 'vue-i18n'
import { AppEmpty } from '@santaizi/ui'
import { useNetworkMonitor } from '../composables/useNetworkMonitor'
import MonitorLineChart from './MonitorLineChart.vue'
import TimeRangeChips from './TimeRangeChips.vue'

const props = defineProps<{ serverId: number; hideHeading?: boolean }>()
const { t } = useI18n()
const {
  loading,
  failed,
  empty,
  aggregated,
  autoRefresh,
  cutPeak,
  hours,
  series,
  load,
} = useNetworkMonitor(toRef(props, 'serverId'))
</script>

<template>
  <section class="ss-latency">
    <header class="ss-latency__head">
      <h3 v-if="!hideHeading">{{ t('nazhua.networkMonitor') }}</h3>
      <div class="ss-latency__toolbar">
        <div class="ss-latency__toggles">
          <label class="ss-latency__switch">
            <span>{{ t('nazhua.aggregate') }}</span>
            <el-switch v-model="aggregated" size="small" />
          </label>
          <label class="ss-latency__switch">
            <span>{{ t('nazhua.autoRefresh') }}</span>
            <el-switch v-model="autoRefresh" size="small" />
          </label>
          <label class="ss-latency__switch">
            <span>{{ t('nazhua.cutPeak') }}</span>
            <el-switch v-model="cutPeak" size="small" />
          </label>
        </div>
        <TimeRangeChips :hours="hours" label @update:hours="hours = $event" />
      </div>
    </header>
    <div v-if="failed || empty || loading" class="ss-latency__empty">
      <AppEmpty
        :tone="failed ? 'danger' : 'default'"
        :icon="failed ? 'ri-error-warning-line' : loading ? 'ri-loader-4-line' : 'ri-line-chart-line'"
        :title="failed ? t('loadFailed') : ''"
        :description="t(failed ? 'requestFailed' : loading ? 'loading' : 'noData')"
      />
      <button v-if="failed" type="button" @click="load()">
        <i class="ri-refresh-line"></i>{{ t('refresh') }}
      </button>
    </div>
    <MonitorLineChart v-else-if="aggregated" :series="series" />
    <div v-else class="ss-latency__grid">
      <article v-for="item in series" :key="item.name" class="ss-latency__card">
        <header>
          <strong>{{ item.name }}</strong>
          <span>{{ item.average.toFixed(2) }} ms</span>
        </header>
        <MonitorLineChart :series="[item]" compact lazy />
      </article>
    </div>
  </section>
</template>
