<script setup lang="ts">
import { toRef } from 'vue'
import { useI18n } from 'vue-i18n'
import { AppEmpty } from '@santaizi/ui'
import { NETWORK_RANGES, useNetworkMonitorChart } from '../../composables/useNetworkMonitorChart'
import MonitorLineChart from './MonitorLineChart.vue'

const props = defineProps<{ serverId: number }>()
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
} = useNetworkMonitorChart(toRef(props, 'serverId'))
</script>

<template>
  <section class="nazhua-monitor">
    <header class="nazhua-monitor__head">
      <h2>{{ t('nazhua.networkMonitor') }}</h2>
      <div class="nazhua-monitor__toolbar">
        <div class="nazhua-monitor__toggles">
          <label class="nazhua-monitor__switch">
            <span>{{ t('nazhua.aggregate') }}</span>
            <el-switch v-model="aggregated" size="small" />
          </label>
          <label class="nazhua-monitor__switch">
            <span>{{ t('nazhua.autoRefresh') }}</span>
            <el-switch v-model="autoRefresh" size="small" />
          </label>
          <label class="nazhua-monitor__switch">
            <span>{{ t('nazhua.cutPeak') }}</span>
            <el-switch v-model="cutPeak" size="small" />
          </label>
        </div>
        <div class="nazhua-monitor__ranges" role="group" :aria-label="t('nazhua.recent')">
          <span>{{ t('nazhua.recent') }}</span>
          <el-button-group>
            <el-button
              v-for="item in NETWORK_RANGES"
              :key="item.hours"
              :type="hours === item.hours ? 'primary' : 'default'"
              @click="hours = item.hours"
            >{{ t(item.labelKey) }}</el-button>
          </el-button-group>
        </div>
      </div>
    </header>
    <div v-if="failed || empty || loading" class="nazhua-monitor__empty">
      <AppEmpty
        :tone="failed ? 'danger' : 'default'"
        :icon="failed ? 'ri-error-warning-line' : loading ? 'ri-loader-4-line' : 'ri-line-chart-line'"
        :title="failed ? t('nazhua.loadFailed') : ''"
        :description="t(failed ? 'nazhua.requestFailed' : loading ? 'nazhua.loading' : 'nazhua.noData')"
      />
      <button v-if="failed" type="button" @click="load"><i class="ri-refresh-line"></i>{{ t('nazhua.refresh') }}</button>
    </div>
    <MonitorLineChart v-else-if="aggregated" :series="series" />
    <div v-else class="nazhua-monitor__grid">
      <article v-for="item in series" :key="item.name" class="nazhua-monitor__card">
        <header>
          <strong>{{ item.name }}</strong>
          <span>{{ item.average.toFixed(2) }} ms</span>
        </header>
        <MonitorLineChart :series="[item]" compact />
      </article>
    </div>
  </section>
</template>
