<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { AppDrawer, AppEmpty } from '@santaizi/ui'
import { listScriptCommands, type ScriptCommand } from '@/api/adminApi'
import { notifyAPIError } from '@/composables/notify'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ 'update:modelValue': [boolean] }>()
const { t, te } = useI18n()
const loading = ref(false)
const commands = ref<ScriptCommand[]>([])

const commandTitleKeys: Record<string, string> = {
  dashboard_install: 'scriptCmdDashboardInstall',
  dashboard_upgrade: 'scriptCmdDashboardUpgrade',
  collector_upgrade: 'scriptCmdCollectorUpgrade',
  collector_remove: 'scriptCmdCollectorRemove',
  agent_upgrade_linux: 'scriptCmdAgentUpgradeLinux',
  agent_upgrade_macos: 'scriptCmdAgentUpgradeMacos',
  agent_upgrade_windows: 'scriptCmdAgentUpgradeWindows',
  agent_uninstall_posix: 'scriptCmdAgentUninstallPosix',
  agent_uninstall_windows: 'scriptCmdAgentUninstallWindows',
}

const groupMeta = [
  { id: 'dashboard', titleKey: 'scriptGroupDashboard', icon: 'ri-window-line' },
  { id: 'collector', titleKey: 'scriptGroupCollector', icon: 'ri-radar-line' },
  { id: 'agent', titleKey: 'scriptGroupAgent', icon: 'ri-server-line' },
] as const

const platformLabelKeys: Record<string, string> = {
  linux: 'linux',
  macos: 'macos',
  windows: 'windows',
  posix: 'posix',
}

const grouped = computed(() => groupMeta.map(section => ({
  ...section,
  items: commands.value.filter(item => item.group === section.id),
})).filter(section => section.items.length))

function titleFor(id: string) {
  const key = commandTitleKeys[id]
  return key ? t(key) : id
}

function platformLabel(platform: string) {
  const key = platformLabelKeys[platform]
  return key ? t(key) : platform
}

async function load() {
  loading.value = true
  try {
    const result = await listScriptCommands()
    commands.value = result.commands || []
  } catch (error) {
    notifyAPIError(error, t as never, te)
    commands.value = []
  } finally {
    loading.value = false
  }
}

async function copy(command: string) {
  await navigator.clipboard.writeText(command)
  ElMessage.success(t('copied'))
}

watch(() => props.modelValue, value => { if (value) void load() })
</script>

<template>
  <AppDrawer
    :model-value="modelValue"
    :title="t('scriptCommands')"
    mode="view"
    size="min(520px,94vw)"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div v-loading="loading" class="script-panel">
      <AppEmpty v-if="!loading && !grouped.length" icon="ri-terminal-box-line" :description="t('noData')" />
      <section v-for="section in grouped" :key="section.id" class="script-group">
        <h2 class="script-group-title"><i :class="section.icon" aria-hidden="true"></i>{{ t(section.titleKey) }}</h2>
        <article v-for="item in section.items" :key="item.id" class="script-command">
          <div class="script-command-head">
            <strong>{{ titleFor(item.id) }}</strong>
            <el-tag size="small">{{ platformLabel(item.platform) }}</el-tag>
            <el-tag v-if="item.destructive" type="danger" size="small">{{ t('irreversible') }}</el-tag>
            <el-button text class="token-copy" :aria-label="t('copy')" @click="copy(item.command)"><i class="ri-file-copy-line"></i></el-button>
          </div>
          <el-input :model-value="item.command" readonly type="textarea" :rows="3" class="mono" />
        </article>
      </section>
    </div>
  </AppDrawer>
</template>
