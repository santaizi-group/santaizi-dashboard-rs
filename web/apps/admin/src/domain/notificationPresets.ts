import type { KeyValueRow, NotificationChannelRecord } from '@/types/admin'

export type NotificationPresetId =
  | 'custom'
  | 'wecom'
  | 'dingtalk'
  | 'feishu'
  | 'telegram'
  | 'bark'
  | 'discord'
  | 'slack'

export type NotificationCredentialKey =
  | 'apiKey'
  | 'accessToken'
  | 'hookToken'
  | 'botToken'
  | 'chatId'
  | 'deviceKey'
  | 'serverHost'
  | 'webhookUrl'

export interface NotificationPresetField {
  key: NotificationCredentialKey
  /** i18n key for the field label */
  labelKey: string
  /** Real secret — use password input + show-password */
  secret?: boolean
  /** Optional default when the field is blank (e.g. Bark host) */
  defaultValue?: string
  /** Whether blank is allowed on submit (optional fields) */
  optional?: boolean
}

export interface NotificationPreset {
  id: NotificationPresetId
  /** i18n key for the chip label */
  labelKey: string
  /** i18n key for default channel name (applied only when name is empty) */
  nameKey: string
  method: NotificationChannelRecord['method']
  request_type: NotificationChannelRecord['request_type']
  headers: KeyValueRow[]
  /** Guided credential fields; empty for custom webhook */
  fields: readonly NotificationPresetField[]
  /**
   * URL template with `{fieldKey}` slots.
   * For custom / webhookUrl-style presets, may be a plain URL or `{webhookUrl}`.
   */
  urlTemplate: string
  /**
   * Body template with `{fieldKey}` slots (JSON string for form/json).
   * Empty for GET presets.
   */
  bodyTemplate: string
}

const pretty = (value: unknown) => `${JSON.stringify(value, null, 2)}\n`

function fillTemplate(template: string, values: Record<string, string>): string {
  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (_, key: string) => values[key] ?? '')
}

/** Escape values for substitution inside JSON string literals. */
function fillJsonTemplate(template: string, values: Record<string, string>): string {
  return template.replace(/\{([a-zA-Z0-9_]+)\}/g, (_, key: string) => {
    const value = values[key] ?? ''
    return JSON.stringify(value).slice(1, -1)
  })
}

export const NOTIFICATION_PRESETS: readonly NotificationPreset[] = [
  {
    id: 'custom',
    labelKey: 'presetCustom',
    nameKey: 'presetCustomName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [],
    urlTemplate: 'https://example.com/webhook',
    bodyTemplate: '',
  },
  {
    id: 'wecom',
    labelKey: 'presetWeCom',
    nameKey: 'presetWeComName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [{ key: 'apiKey', labelKey: 'notificationApiKey', secret: true }],
    urlTemplate: 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={apiKey}',
    bodyTemplate: pretty({
      msgtype: 'text',
      text: { content: '#SANTAIZI#' },
    }),
  },
  {
    id: 'dingtalk',
    labelKey: 'presetDingTalk',
    nameKey: 'presetDingTalkName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [{ key: 'accessToken', labelKey: 'notificationAccessToken', secret: true }],
    urlTemplate: 'https://oapi.dingtalk.com/robot/send?access_token={accessToken}',
    bodyTemplate: pretty({
      msgtype: 'text',
      text: { content: '三太子：\n#SANTAIZI#' },
    }),
  },
  {
    id: 'feishu',
    labelKey: 'presetFeishu',
    nameKey: 'presetFeishuName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [{ key: 'hookToken', labelKey: 'notificationHookToken', secret: true }],
    urlTemplate: 'https://open.feishu.cn/open-apis/bot/v2/hook/{hookToken}',
    bodyTemplate: pretty({
      msg_type: 'text',
      content: { text: '#SANTAIZI#\n#DATETIME#' },
    }),
  },
  {
    id: 'telegram',
    labelKey: 'presetTelegram',
    nameKey: 'presetTelegramName',
    method: 'post',
    request_type: 'form',
    headers: [],
    fields: [
      { key: 'botToken', labelKey: 'notificationBotToken', secret: true },
      { key: 'chatId', labelKey: 'notificationChatId' },
    ],
    urlTemplate: 'https://api.telegram.org/bot{botToken}/sendMessage',
    // Form bodies are stored as a JSON string map (backend GjsonParseStringMap).
    bodyTemplate: pretty({
      chat_id: '{chatId}',
      text: '#SANTAIZI#',
    }),
  },
  {
    id: 'bark',
    labelKey: 'presetBark',
    nameKey: 'presetBarkName',
    method: 'get',
    request_type: 'json',
    headers: [],
    fields: [
      { key: 'deviceKey', labelKey: 'notificationDeviceKey', secret: true },
      {
        key: 'serverHost',
        labelKey: 'notificationBarkServer',
        defaultValue: 'api.day.app',
        optional: true,
      },
    ],
    urlTemplate: 'https://{serverHost}/{deviceKey}/#SANTAIZI#',
    bodyTemplate: '',
  },
  {
    id: 'discord',
    labelKey: 'presetDiscord',
    nameKey: 'presetDiscordName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [{ key: 'webhookUrl', labelKey: 'notificationWebhookUrl', secret: true }],
    urlTemplate: '{webhookUrl}',
    bodyTemplate: pretty({ content: '#SANTAIZI#' }),
  },
  {
    id: 'slack',
    labelKey: 'presetSlack',
    nameKey: 'presetSlackName',
    method: 'post',
    request_type: 'json',
    headers: [],
    fields: [{ key: 'webhookUrl', labelKey: 'notificationWebhookUrl', secret: true }],
    urlTemplate: '{webhookUrl}',
    bodyTemplate: pretty({ text: '#SANTAIZI#' }),
  },
]

export function getNotificationPreset(id: NotificationPresetId): NotificationPreset | undefined {
  return NOTIFICATION_PRESETS.find(item => item.id === id)
}

export function isGuidedNotificationPreset(id: NotificationPresetId | ''): boolean {
  return Boolean(id && id !== 'custom')
}

/** Resolve field values with defaults for optional slots. */
export function resolveCredentialValues(
  preset: NotificationPreset,
  values: Partial<Record<NotificationCredentialKey, string>>,
): Record<string, string> {
  const resolved: Record<string, string> = {}
  for (const field of preset.fields) {
    const raw = (values[field.key] ?? '').trim()
    resolved[field.key] = raw || field.defaultValue || ''
  }
  return resolved
}

export interface NotificationPresetFormPatch {
  url: string
  method: NotificationChannelRecord['method']
  request_type: NotificationChannelRecord['request_type']
  body: string
  headers: KeyValueRow[]
  /** Present only when the current name is empty and should be filled */
  name?: string
}

/** Compose final webhook fields from a guided preset + credential values. */
export function composeNotificationFromPreset(
  preset: NotificationPreset,
  values: Partial<Record<NotificationCredentialKey, string>> = {},
): NotificationPresetFormPatch {
  const resolved = resolveCredentialValues(preset, values)
  const url = fillTemplate(preset.urlTemplate, resolved)
  const body =
    preset.method === 'get' ? '' : fillJsonTemplate(preset.bodyTemplate, resolved)
  return {
    url,
    method: preset.method,
    request_type: preset.request_type,
    body,
    headers: preset.headers.map(item => ({ ...item })),
  }
}

/** Build form fields for a preset. Name is filled only when `currentName` is blank. */
export function applyNotificationPreset(
  preset: NotificationPreset,
  currentName: string,
  resolveName: (nameKey: string) => string,
  values: Partial<Record<NotificationCredentialKey, string>> = {},
): NotificationPresetFormPatch {
  const patch = composeNotificationFromPreset(preset, values)
  if (!currentName.trim()) patch.name = resolveName(preset.nameKey)
  return patch
}

export function emptyCredentialValues(
  preset: NotificationPreset,
): Record<NotificationCredentialKey, string> {
  const values = {} as Record<NotificationCredentialKey, string>
  for (const field of preset.fields) {
    values[field.key] = field.defaultValue || ''
  }
  return values
}

/** Detect which guided preset an existing channel matches (if any). */
export function detectNotificationPreset(
  url: string,
  body: string,
): NotificationPresetId | 'custom' {
  const trimmed = url.trim()
  if (/qyapi\.weixin\.qq\.com\/cgi-bin\/webhook\/send/.test(trimmed)) return 'wecom'
  if (/oapi\.dingtalk\.com\/robot\/send/.test(trimmed)) return 'dingtalk'
  if (/open\.feishu\.cn\/open-apis\/bot\/v2\/hook\//.test(trimmed)) return 'feishu'
  if (/api\.telegram\.org\/bot[^/]+\/sendMessage/.test(trimmed)) return 'telegram'
  if (/^https?:\/\/[^/]+\/[^/]+\/#SANTAIZI#\/?$/.test(trimmed) || /api\.day\.app\//.test(trimmed)) {
    if (trimmed.includes('#SANTAIZI#')) return 'bark'
  }
  if (/discord(?:app)?\.com\/api\/webhooks\//.test(trimmed)) return 'discord'
  if (/hooks\.slack\.com\/services\//.test(trimmed)) return 'slack'
  void body
  return 'custom'
}

/** Extract credential field values from a stored url/body for a known preset. */
export function extractPresetValues(
  preset: NotificationPreset,
  url: string,
  body: string,
): Partial<Record<NotificationCredentialKey, string>> {
  const values: Partial<Record<NotificationCredentialKey, string>> = {}
  const trimmed = url.trim()

  switch (preset.id) {
    case 'wecom': {
      const match = trimmed.match(/[?&]key=([^&]+)/)
      if (match?.[1]) values.apiKey = decodeURIComponent(match[1])
      break
    }
    case 'dingtalk': {
      const match = trimmed.match(/[?&]access_token=([^&]+)/)
      if (match?.[1]) values.accessToken = decodeURIComponent(match[1])
      break
    }
    case 'feishu': {
      const match = trimmed.match(/\/hook\/([^/?#]+)/)
      if (match?.[1]) values.hookToken = decodeURIComponent(match[1])
      break
    }
    case 'telegram': {
      const urlMatch = trimmed.match(/\/bot([^/]+)\/sendMessage/)
      if (urlMatch?.[1]) values.botToken = decodeURIComponent(urlMatch[1])
      try {
        const parsed = JSON.parse(body) as Record<string, unknown>
        if (typeof parsed.chat_id === 'string' || typeof parsed.chat_id === 'number') {
          values.chatId = String(parsed.chat_id)
        }
      } catch {
        /* ignore malformed body */
      }
      break
    }
    case 'bark': {
      try {
        const parsed = new URL(trimmed)
        values.serverHost = parsed.host
        const segments = parsed.pathname.split('/').filter(Boolean)
        if (segments[0]) values.deviceKey = decodeURIComponent(segments[0])
      } catch {
        /* ignore */
      }
      break
    }
    case 'discord':
    case 'slack': {
      values.webhookUrl = trimmed
      break
    }
    default:
      break
  }

  return values
}

/** Validate required guided fields; returns i18n labelKey of the first missing field. */
export function firstMissingCredentialField(
  preset: NotificationPreset,
  values: Partial<Record<NotificationCredentialKey, string>>,
): NotificationPresetField | undefined {
  for (const field of preset.fields) {
    if (field.optional) continue
    const raw = (values[field.key] ?? '').trim()
    if (!raw) return field
  }
  return undefined
}
