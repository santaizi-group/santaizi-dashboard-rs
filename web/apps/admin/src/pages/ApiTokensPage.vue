<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, ElMessageBox } from 'element-plus'
import { deleteApiToken, getApiToken, listApiTokens, patchApiToken, type APIToken } from '@santaizi/api'
import { AppDialog, AppEmpty } from '@santaizi/ui'
import IssueApiTokenDialog from '@/components/editors/IssueApiTokenDialog.vue'
import { notifyAPIError } from '@/composables/notify'
import { formatDateTime } from '@/composables/format'

const { t, te, locale } = useI18n()
const items = ref<APIToken[]>([])
const loading = ref(false)
const togglingId = ref<number | null>(null)
const editor = ref(false)
const viewDialog = ref(false)
const token = ref('')

async function load() {
  loading.value = true
  try {
    items.value = (await listApiTokens()).data
  } catch (e) {
    notifyAPIError(e, t as never, te)
  } finally {
    loading.value = false
  }
}

function onIssued(result: APIToken) {
  token.value = result.token || ''
  viewDialog.value = true
  void load()
}

async function reveal(row: APIToken) {
  try {
    token.value = (await getApiToken(row.id)).token || ''
    viewDialog.value = true
  } catch (e) {
    notifyAPIError(e, t as never, te)
  }
}

async function copyRow(row: APIToken) {
  try {
    const plain = (await getApiToken(row.id)).token || ''
    await navigator.clipboard.writeText(plain)
    ElMessage.success(t('copied'))
  } catch (e) {
    notifyAPIError(e, t as never, te)
  }
}

async function toggleEnabled(row: APIToken, enabled: boolean) {
  const previous = row.enabled
  row.enabled = enabled
  togglingId.value = row.id
  try {
    const updated = await patchApiToken(row.id, { enabled })
    Object.assign(row, updated)
  } catch (e) {
    row.enabled = previous
    notifyAPIError(e, t as never, te)
  } finally {
    togglingId.value = null
  }
}

async function remove(row: APIToken) {
  await ElMessageBox.confirm(t('confirmDelete'), t('dangerousAction'), { type: 'warning' })
  try {
    await deleteApiToken(row.id)
    await load()
  } catch (e) {
    notifyAPIError(e, t as never, te)
  }
}

async function copy() {
  await navigator.clipboard.writeText(token.value)
  ElMessage.success(t('copied'))
}

function permissionLabel(value: string | undefined) {
  return value === 'read' ? t('tokenReadOnly') : t('tokenWrite')
}

function expiresLabel(row: APIToken) {
  if (row.expired) return t('tokenExpired')
  if (!row.expires_at) return t('tokenNeverExpires')
  return formatDateTime(row.expires_at, locale.value)
}

onMounted(load)
</script>

<template>
  <div class="page-head">
    <h1>{{ t('apiTokens') }}</h1>
    <div class="page-actions">
      <a class="el-button" href="/docs/api/" target="_blank"><i class="ri-book-open-line"></i>{{ t('apiDocs') }}</a>
      <el-button type="primary" @click="editor = true"><i class="ri-key-2-line"></i>{{ t('issueToken') }}</el-button>
    </div>
  </div>

  <section class="surface table-card">
    <el-table class="desktop-only" v-loading="loading" :data="items">
      <el-table-column prop="note" :label="t('tokenNote')" min-width="120" />
      <el-table-column prop="token_prefix" :label="t('token')" width="190">
        <template #default="{ row }">
          <div class="token-cell">
            <span class="mono">{{ row.token_prefix }}</span>
            <el-button text class="token-copy" :aria-label="t('copyToken')" @click="copyRow(row)">
              <i class="ri-file-copy-line"></i>
            </el-button>
          </div>
        </template>
      </el-table-column>
      <el-table-column :label="t('tokenPermission')" width="100">
        <template #default="{ row }">
          <el-tag size="small" :type="row.permission === 'read' ? 'info' : 'primary'" effect="plain">{{ permissionLabel(row.permission) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('tokenExpiresAt')" min-width="160">
        <template #default="{ row }">
          <el-tag v-if="row.expired" size="small" type="danger" effect="plain">{{ t('tokenExpired') }}</el-tag>
          <span v-else>{{ expiresLabel(row) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('status')" width="100">
        <template #default="{ row }">
          <el-switch
            :model-value="row.enabled"
            :disabled="togglingId === row.id || row.expired"
            :aria-label="row.enabled ? t('enabled') : t('disabled')"
            @change="(value: string | number | boolean) => toggleEnabled(row, Boolean(value))"
          />
        </template>
      </el-table-column>
      <el-table-column :label="t('createdAt')" width="180">
        <template #default="{ row }">{{ formatDateTime(row.created_at, locale) }}</template>
      </el-table-column>
      <el-table-column :label="t('actions')" width="72" fixed="right">
        <template #default="{ row }">
          <el-dropdown trigger="click">
            <el-button text class="actions-more" :aria-label="t('actions')"><i class="ri-more-fill"></i></el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item @click="reveal(row)"><i class="ri-eye-line"></i>{{ t('viewToken') }}</el-dropdown-item>
                <el-dropdown-item divided @click="remove(row)"><i class="ri-delete-bin-line"></i>{{ t('delete') }}</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </template>
      </el-table-column>
      <template #empty><AppEmpty class="empty-state" icon="ri-key-2-line" :description="t('noData')" /></template>
    </el-table>

    <div class="mobile-only" v-loading="loading">
      <AppEmpty v-if="!items.length && !loading" class="empty-state" icon="ri-key-2-line" :description="t('noData')" />
      <div v-else class="mobile-card-list">
        <article v-for="row in items" :key="row.id" class="mobile-card">
          <div class="mobile-card-head">
            <div class="mobile-card-title">
              <strong>{{ row.note }}</strong>
              <div class="token-cell">
                <small class="mono">{{ row.token_prefix }}</small>
                <el-button text class="token-copy" :aria-label="t('copyToken')" @click="copyRow(row)">
                  <i class="ri-file-copy-line"></i>
                </el-button>
              </div>
            </div>
            <div class="mobile-card-actions inline-actions">
              <el-dropdown trigger="click">
                <el-button text class="actions-more" :aria-label="t('actions')"><i class="ri-more-fill"></i></el-button>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item @click="reveal(row)"><i class="ri-eye-line"></i>{{ t('viewToken') }}</el-dropdown-item>
                    <el-dropdown-item divided @click="remove(row)"><i class="ri-delete-bin-line"></i>{{ t('delete') }}</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </div>
          </div>
          <dl class="mobile-card-meta">
            <div><dt>{{ t('tokenPermission') }}</dt><dd>{{ permissionLabel(row.permission) }}</dd></div>
            <div><dt>{{ t('tokenExpiresAt') }}</dt><dd>{{ expiresLabel(row) }}</dd></div>
            <div>
              <dt>{{ t('status') }}</dt>
              <dd>
                <el-switch
                  :model-value="row.enabled"
                  :disabled="togglingId === row.id || row.expired"
                  :aria-label="row.enabled ? t('enabled') : t('disabled')"
                  @change="(value: string | number | boolean) => toggleEnabled(row, Boolean(value))"
                />
              </dd>
            </div>
            <div><dt>{{ t('createdAt') }}</dt><dd>{{ formatDateTime(row.created_at, locale) }}</dd></div>
          </dl>
        </article>
      </div>
    </div>
  </section>

  <IssueApiTokenDialog v-model="editor" @issued="onIssued" />
  <AppDialog v-model="viewDialog" :title="t('token')" mode="view">
    <el-input v-model="token" readonly class="mono token-display">
      <template #append>
        <el-button :aria-label="t('copy')" @click="copy"><i class="ri-file-copy-line"></i></el-button>
      </template>
    </el-input>
  </AppDialog>
</template>
