<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import { useSessionStore } from '@/stores/session'
const { t } = useI18n()
const session = useSessionStore()
</script>
<template>
  <main id="main-content" class="auth-page">
    <section class="surface auth-card">
      <a class="auth-brand icon-text" href="/"><img :src="'/static/logo.svg'" alt=""><span>{{ t('appName') }}</span></a>
      <div class="auth-icon"><i class="ri-shield-user-line"></i></div>
      <h1>{{ t('loginTitle') }}</h1>
      <a class="el-button el-button--primary auth-submit" :href="session.state.login_url"><i class="ri-login-circle-line"></i>{{ t('loginButton') }}</a>
    </section>
  </main>
</template>
