import { createI18n } from 'vue-i18n'
import { messages } from './messages'

export type Locale = 'zh-CN' | 'zh-TW' | 'en-US' | 'es-ES'

export { messages }

export function createSantaiziI18n(locale: Locale = 'zh-CN') {
  return createI18n({ legacy: false, locale, fallbackLocale: 'zh-CN', messages })
}
