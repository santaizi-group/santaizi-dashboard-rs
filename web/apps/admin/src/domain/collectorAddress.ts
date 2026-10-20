export function splitHostPort(address: string): { host: string; port: string } {
  const value = address.trim()
  if (!value) return { host: '', port: '' }
  if (value.startsWith('[')) {
    const close = value.indexOf(']')
    if (close === -1) return { host: value, port: '' }
    const host = value.slice(1, close)
    const rest = value.slice(close + 1)
    return { host, port: rest.startsWith(':') ? rest.slice(1) : '' }
  }
  const colon = value.lastIndexOf(':')
  if (colon === -1) return { host: value, port: '' }
  const host = value.slice(0, colon)
  if (host.includes(':')) return { host: value, port: '' }
  return { host, port: value.slice(colon + 1) }
}

export function joinHostPort(host: string, port: number): string {
  const value = host.trim()
  if (value.includes(':') && !value.startsWith('[')) return `[${value}]:${port}`
  return `${value}:${port}`
}

export function parsePort(value: unknown): number | null {
  if (value === '' || value === null || value === undefined) return null
  const n = typeof value === 'number' ? value : Number(String(value).trim())
  if (!Number.isInteger(n) || n < 1 || n > 65535) return null
  return n
}

export function collectorAccessHost(row: { address?: string }): string {
  return splitHostPort(row.address || '').host || (row.address || '').trim()
}

export function collectorAccessPort(row: { address?: string }): number | null {
  return parsePort(splitHostPort(row.address || '').port)
}

export function collectorListenPort(row: { address?: string; listen_port?: number }): number | null {
  const listen = parsePort(row.listen_port)
  if (listen != null) return listen
  return collectorAccessPort(row)
}
