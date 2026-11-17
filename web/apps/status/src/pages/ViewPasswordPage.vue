<script setup lang="ts">
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { verifyViewPassword } from '@santaizi/api'
import { safeRedirectTarget } from '../router'
import { useStatusStore } from '../stores/status'

const { t } = useI18n()
const route = useRoute()
const router = useRouter()
const store = useStatusStore()
const password = ref('')
const loading = ref(false)

async function submit() {
  loading.value = true
  try {
    await verifyViewPassword(password.value)
    await store.load()
    store.connect()
    await router.replace(safeRedirectTarget(route.query.redirect))
  } catch (error) {
    ElMessage.error(error instanceof Error ? error.message : t('loadFailed'))
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="password-page">
    <section class="status-panel password-card">
      <img :src="store.bootstrap?.logo_url || '/static/logo.svg'" alt="">
      <h1>{{ t('viewPasswordTitle') }}</h1>
      <el-form label-position="top" @submit.prevent="submit">
        <el-form-item :label="t('password')">
          <el-input v-model="password" type="password" show-password autocomplete="current-password" autofocus />
        </el-form-item>
        <el-button type="primary" native-type="submit" :loading="loading">
          <i v-if="!loading" class="ri-lock-unlock-line"></i><span>{{ t('verify') }}</span>
        </el-button>
      </el-form>
    </section>
  </div>
</template>
