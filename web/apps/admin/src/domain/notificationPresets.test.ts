import { describe, expect, it } from 'vitest'
import {
  NOTIFICATION_PRESETS,
  applyNotificationPreset,
  composeNotificationFromPreset,
  detectNotificationPreset,
  extractPresetValues,
  firstMissingCredentialField,
  getNotificationPreset,
} from './notificationPresets'

describe('notificationPresets', () => {
  it('has unique ids', () => {
    const ids = NOTIFICATION_PRESETS.map(item => item.id)
    expect(new Set(ids).size).toBe(ids.length)
  })

  it('keeps form bodies as JSON object maps', () => {
    const telegram = getNotificationPreset('telegram')
    expect(telegram).toBeDefined()
    expect(telegram!.request_type).toBe('form')
    const composed = composeNotificationFromPreset(telegram!, {
      botToken: '123:ABC',
      chatId: 'xxxxxx',
    })
    const parsed = JSON.parse(composed.body) as Record<string, string>
    expect(parsed.chat_id).toBe('xxxxxx')
    expect(parsed.text).toBe('#SANTAIZI#')
  })

  it('clears body for GET presets', () => {
    const bark = getNotificationPreset('bark')!
    const patch = applyNotificationPreset(bark, '', key => key, {
      deviceKey: 'device-key',
    })
    expect(patch.method).toBe('get')
    expect(patch.body).toBe('')
    expect(patch.url).toBe('https://api.day.app/device-key/#SANTAIZI#')
  })

  it('fills name only when empty', () => {
    const wecom = getNotificationPreset('wecom')!
    const empty = applyNotificationPreset(wecom, '  ', key => `translated:${key}`)
    expect(empty.name).toBe('translated:presetWeComName')
    const kept = applyNotificationPreset(wecom, '已有名称', key => `translated:${key}`)
    expect(kept.name).toBeUndefined()
    expect(kept.url).toContain('qyapi.weixin.qq.com')
  })

  it('includes 三太子 keyword in DingTalk body', () => {
    const body = composeNotificationFromPreset(getNotificationPreset('dingtalk')!, {
      accessToken: 'tok',
    }).body
    expect(body).toContain('三太子')
    expect(body).toContain('#SANTAIZI#')
  })

  it('composes Telegram url and body from credentials', () => {
    const telegram = getNotificationPreset('telegram')!
    const patch = composeNotificationFromPreset(telegram, {
      botToken: '111:AA-BB',
      chatId: '-10042',
    })
    expect(patch.url).toBe('https://api.telegram.org/bot111:AA-BB/sendMessage')
    expect(patch.method).toBe('post')
    expect(patch.request_type).toBe('form')
    expect(JSON.parse(patch.body)).toEqual({
      chat_id: '-10042',
      text: '#SANTAIZI#',
    })
  })

  it('composes WeCom url from api key', () => {
    const wecom = getNotificationPreset('wecom')!
    const patch = composeNotificationFromPreset(wecom, { apiKey: 'secret-key' })
    expect(patch.url).toBe(
      'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=secret-key',
    )
  })

  it('escapes special characters in JSON body fields', () => {
    const telegram = getNotificationPreset('telegram')!
    const patch = composeNotificationFromPreset(telegram, {
      botToken: 't',
      chatId: 'a"b\\c',
    })
    expect(JSON.parse(patch.body).chat_id).toBe('a"b\\c')
  })

  it('detects and extracts guided presets', () => {
    const wecomUrl = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc123'
    expect(detectNotificationPreset(wecomUrl, '')).toBe('wecom')
    expect(extractPresetValues(getNotificationPreset('wecom')!, wecomUrl, '')).toEqual({
      apiKey: 'abc123',
    })

    const tgUrl = 'https://api.telegram.org/bot111:TOK/sendMessage'
    const tgBody = JSON.stringify({ chat_id: '42', text: '#SANTAIZI#' })
    expect(detectNotificationPreset(tgUrl, tgBody)).toBe('telegram')
    expect(extractPresetValues(getNotificationPreset('telegram')!, tgUrl, tgBody)).toEqual({
      botToken: '111:TOK',
      chatId: '42',
    })

    const barkUrl = 'https://api.day.app/my-device/#SANTAIZI#'
    expect(detectNotificationPreset(barkUrl, '')).toBe('bark')
    expect(extractPresetValues(getNotificationPreset('bark')!, barkUrl, '')).toEqual({
      serverHost: 'api.day.app',
      deviceKey: 'my-device',
    })
  })

  it('reports missing required credential fields', () => {
    const telegram = getNotificationPreset('telegram')!
    expect(firstMissingCredentialField(telegram, {})?.key).toBe('botToken')
    expect(
      firstMissingCredentialField(telegram, { botToken: 'x', chatId: '1' }),
    ).toBeUndefined()
  })
})
