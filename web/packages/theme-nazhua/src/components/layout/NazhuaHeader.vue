<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { formatHeaderStat } from '../../utils/host'

const props = defineProps<{
  brand?: string
  total: number
  online: number
  offline: number
  transferIn: number
  transferOut: number
  speedIn: number
  speedOut: number
}>()

const { t } = useI18n()
const title = computed(() => props.brand || t('nazhua.title'))
const inTransfer = computed(() => formatHeaderStat(props.transferIn))
const outTransfer = computed(() => formatHeaderStat(props.transferOut))
const inSpeed = computed(() => formatHeaderStat(props.speedIn))
const outSpeed = computed(() => formatHeaderStat(props.speedOut))
</script>

<template>
  <header class="nazhua-header">
    <div class="nazhua-header__inner">
      <RouterLink to="/" class="nazhua-header__brand">{{ title }}</RouterLink>
      <div class="nazhua-header__stats">
        <div v-if="total > 0" class="nazhua-server-count">
          <span>{{ t('nazhua.totalPrefix') }} <strong>{{ total }}</strong> {{ t('nazhua.serverCount') }}</span>
          <span>{{ t('nazhua.online') }} <strong class="online">{{ online }}</strong></span>
          <span>{{ t('nazhua.offline') }} <strong class="offline">{{ offline }}</strong></span>
        </div>
        <div class="nazhua-server-stat">
          <div>
            <span class="nazhua-server-stat__label">{{ t('nazhua.transfer') }}</span>
            <span><i class="ri-download-line"></i>{{ inTransfer.value }}{{ inTransfer.unit }}</span>
            <span><i class="ri-upload-line"></i>{{ outTransfer.value }}{{ outTransfer.unit }}</span>
          </div>
          <div>
            <span class="nazhua-server-stat__label">{{ t('nazhua.netSpeed') }}</span>
            <span><i class="ri-arrow-down-line"></i>{{ inSpeed.value }}{{ inSpeed.unit }}</span>
            <span><i class="ri-arrow-up-line"></i>{{ outSpeed.value }}{{ outSpeed.unit }}</span>
          </div>
        </div>
      </div>
      <div class="nazhua-header__actions"><slot name="actions" /></div>
    </div>
  </header>
</template>
