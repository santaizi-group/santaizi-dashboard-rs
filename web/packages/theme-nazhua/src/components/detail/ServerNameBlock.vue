<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { ServerRecord } from '@santaizi/api'
import type { ServerLocation } from '../../utils/worldMap'
import { toNazhuaServerView } from '../../domain/nazhuaServerView'
import OsLogo from '../common/OsLogo.vue'
import ServerGlobe from './ServerGlobe.vue'

const props = defineProps<{ server: ServerRecord; location: ServerLocation | null }>()
const { t } = useI18n()
const view = computed(() => toNazhuaServerView(props.server))
</script>

<template>
  <section class="nazhua-detail-name">
    <div class="nazhua-detail-name__main">
      <div class="nazhua-detail-name__flag" aria-hidden="true">
        <span v-if="view.flagClass" :class="view.flagClass" />
        <i v-else class="ri-global-line"></i>
      </div>
      <div class="nazhua-detail-name__text">
        <div class="nazhua-detail-name__title">
          <h1>{{ server.name }}</h1>
          <span v-if="view.spec" class="nazhua-detail-name__spec">
            <OsLogo :platform="view.platform" />{{ view.spec }}
          </span>
          <span v-if="!view.online" class="offline">
            <i class="ri-indeterminate-circle-fill"></i>
            {{ t('nazhua.offline') }}
          </span>
        </div>
        <p v-if="view.slogan">“{{ view.slogan }}”</p>
      </div>
    </div>
    <ServerGlobe :location="location" />
  </section>
</template>
