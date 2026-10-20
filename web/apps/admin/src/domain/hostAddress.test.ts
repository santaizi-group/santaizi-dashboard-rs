import { describe, expect, it } from 'vitest'
import { hostAddresses } from './hostAddress'

describe('hostAddresses', () => {
  it('reads ipv4 and ipv6 fields', () => {
    expect(hostAddresses({ ipv4: '192.0.2.10', ipv6: '2001:db8::10' })).toEqual({
      ipv4: '192.0.2.10', ipv6: '2001:db8::10',
    })
  })

  it('splits bundled ip when family fields are missing', () => {
    expect(hostAddresses({ ip: '192.0.2.10/2001:db8::10' })).toEqual({
      ipv4: '192.0.2.10', ipv6: '2001:db8::10',
    })
    expect(hostAddresses({ ip: '192.0.2.10' })).toEqual({ ipv4: '192.0.2.10', ipv6: '' })
    expect(hostAddresses({ ip: '2001:db8::10' })).toEqual({ ipv4: '', ipv6: '2001:db8::10' })
  })

  it('returns empty strings without a host', () => {
    expect(hostAddresses()).toEqual({ ipv4: '', ipv6: '' })
    expect(hostAddresses(null)).toEqual({ ipv4: '', ipv6: '' })
  })
})
