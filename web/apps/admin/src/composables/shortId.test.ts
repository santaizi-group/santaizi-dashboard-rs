import { describe, expect, it } from 'vitest'
import { isLongId, shortId } from './shortId'

describe('shortId', () => {
  it('keeps short values and truncates opaque ids', () => {
    expect(shortId('collector-1')).toBe('collector-1')
    expect(shortId('dacee892aabbccddeeff001122334455')).toBe('dacee892…')
    expect(isLongId('dacee892aabbccddeeff001122334455')).toBe(true)
    expect(isLongId('collector-1')).toBe(false)
  })

  it('keeps a short prefix before truncating a long hex id', () => {
    expect(shortId('collector-2aee9892d4c14e4c8b9a112233445566')).toBe('collector-2aee9892…')
  })
})
