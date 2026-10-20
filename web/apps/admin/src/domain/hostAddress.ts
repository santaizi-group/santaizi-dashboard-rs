import type { ServerHost } from '@santaizi/api'

export function hostAddresses(host?: ServerHost | null): { ipv4: string; ipv6: string } {
  if (!host) return { ipv4: '', ipv6: '' }
  const text = (value: unknown) => typeof value === 'string' ? value.trim() : ''
  let ipv4 = text(host.ipv4)
  let ipv6 = text(host.ipv6)
  const bundled = text(host.ip)
  if ((!ipv4 || !ipv6) && bundled) {
    const slash = bundled.indexOf('/')
    if (slash >= 0) {
      ipv4 = ipv4 || bundled.slice(0, slash)
      ipv6 = ipv6 || bundled.slice(slash + 1)
    } else if (bundled.includes(':')) {
      ipv6 = ipv6 || bundled
    } else {
      ipv4 = ipv4 || bundled
    }
  }
  return { ipv4, ipv6 }
}
