import { describe, expect, it } from 'vitest'
import { collectorAccessHost, collectorAccessPort, collectorListenPort, joinHostPort, parsePort, splitHostPort } from './collectorAddress'

describe('collectorAddress', () => {
  it('splits host and access port, including IPv6', () => {
    expect(splitHostPort('edge.example.com:443')).toEqual({ host: 'edge.example.com', port: '443' })
    expect(splitHostPort('[2001:db8::1]:5556')).toEqual({ host: '2001:db8::1', port: '5556' })
    expect(splitHostPort('edge.example.com')).toEqual({ host: 'edge.example.com', port: '' })
  })

  it('joins host and port', () => {
    expect(joinHostPort('edge.example.com', 443)).toBe('edge.example.com:443')
    expect(joinHostPort('2001:db8::1', 5556)).toBe('[2001:db8::1]:5556')
  })

  it('prefers listen_port over the access port in address', () => {
    expect(collectorAccessHost({ address: 'edge.example.com:443' })).toBe('edge.example.com')
    expect(collectorAccessPort({ address: 'edge.example.com:443' })).toBe(443)
    expect(collectorListenPort({ address: 'edge.example.com:443', listen_port: 5556 })).toBe(5556)
    expect(collectorListenPort({ address: 'edge.example.com:443', listen_port: 0 })).toBe(443)
    expect(parsePort('')).toBeNull()
  })
})
