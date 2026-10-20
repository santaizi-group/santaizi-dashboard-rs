import { defineStore } from 'pinia'
import { ref } from 'vue'
import { getSession, logout as apiLogout, type SessionState } from '@santaizi/api'

export const useSessionStore = defineStore('session', () => {
  const state = ref<SessionState>({ authenticated: false, csrf_token: '', login_url: '/oauth2/login', capabilities: [], version: '' })
  const loading = ref(false)
  async function load() {
    loading.value = true
    try {
      state.value = await getSession()
      if (!state.value.authenticated && !location.pathname.endsWith('/login')) location.assign('/admin/login')
    } finally { loading.value = false }
  }
  async function logout() { await apiLogout(); location.assign('/') }
  return { state, loading, load, logout }
})
