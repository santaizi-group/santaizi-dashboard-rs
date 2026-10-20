<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { listPublicServices, type ResourceRecord } from '@santaizi/api'
import { AppEmpty } from '@santaizi/ui'
import DelaySparkline from '../components/DelaySparkline.vue'
import { toServiceStatusViews } from '../domain/serviceStatusView'

const { t } = useI18n()
const services = ref<ResourceRecord[]>([])
const loading = ref(false)
const failed = ref(false)
const cards = computed(() => toServiceStatusViews(services.value))

async function load() {
  loading.value = true
  try {
    services.value = (await listPublicServices()).data
    failed.value = false
  } catch {
    failed.value = true
  } finally {
    loading.value = false
  }
}
onMounted(load)
</script>

<template>
  <div class="status-container">
    <section class="status-panel service-panel">
      <header class="group-title"><span>{{ t('statusServices') }}</span></header>
      <div v-if="cards.length" class="service-list">
        <article v-for="card in cards" :key="card.id" class="svc-card">
          <div class="svc-card__head">
            <div class="svc-card__name">
              <i class="live-dot" :class="card.live ? 'online' : 'offline'"></i>
              <strong>{{ card.name }}</strong>
            </div>
            <b class="svc-card__rate">{{ card.uptimeLabel }}</b>
          </div>
          <div v-if="card.days.length" class="svc-days">
            <span
              v-for="(day, index) in card.days"
              :key="index"
              class="svc-days__bar"
              :class="day.tone"
              :title="`${day.percent.toFixed(2)}%`"
            />
          </div>
          <div class="svc-card__latency">
            <span>{{ t('averageLatency') }}</span>
            <b>{{ card.latencyLabel ? `${card.latencyLabel} ms` : '—' }}</b>
          </div>
          <DelaySparkline v-if="card.delayPoints.length" :points="card.delayPoints" />
        </article>
      </div>
      <div v-else class="empty-status status-page-empty">
        <AppEmpty
          :tone="failed ? 'danger' : 'default'"
          :icon="failed ? 'ri-error-warning-line' : 'ri-heart-pulse-line'"
          :title="failed ? t('loadFailed') : ''"
          :description="t(failed ? 'requestFailed' : loading ? 'loading' : 'noData')"
        />
        <el-button v-if="failed" type="primary" @click="load">
          <i class="ri-refresh-line"></i>{{ t('refresh') }}
        </el-button>
      </div>
    </section>
  </div>
</template>
