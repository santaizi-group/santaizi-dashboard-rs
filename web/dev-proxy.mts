import type { ClientRequest, IncomingMessage, ServerResponse } from 'node:http'
import { fileURLToPath } from 'node:url'

export function dashboardRoot(): string {
  return fileURLToPath(new URL('..', import.meta.url))
}

function truthy(value: string | undefined): boolean {
  return value === '1' || value === 'true' || value === 'yes'
}

function isLoopback(url: string): boolean {
  try {
    const { hostname } = new URL(url)
    return hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '::1'
  } catch {
    return false
  }
}

function stripCookieFlags(cookie: string): string {
  return cookie.replace(/;\s*Secure/gi, '').replace(/;\s*Domain=[^;]*/gi, '')
}

function headerValue(headers: IncomingMessage['headers'], name: string): string {
  const raw = headers[name]
  if (Array.isArray(raw)) {
    return raw[0] || ''
  }
  return raw || ''
}

function isAccessLoginRedirect(status: number, location: string): boolean {
  if (!location || (status !== 301 && status !== 302 && status !== 303 && status !== 307 && status !== 308)) {
    return false
  }
  try {
    if (new URL(location).hostname.endsWith('.cloudflareaccess.com')) {
      return true
    }
  } catch {
    /* fall through */
  }
  return location.includes('/cdn-cgi/access/')
}

function proxyHeaders(env: Record<string, string>, app: 'admin' | 'status'): Record<string, string> {
  const headers: Record<string, string> = {}
  const accessId = env.CF_ACCESS_CLIENT_ID?.trim()
  const accessSecret = env.CF_ACCESS_CLIENT_SECRET?.trim()
  if (accessId && accessSecret) {
    headers['CF-Access-Client-Id'] = accessId
    headers['CF-Access-Client-Secret'] = accessSecret
  }
  const jwt = env.CF_AUTHORIZATION?.trim()
  if (jwt) {
    headers.Cookie = `CF_Authorization=${jwt}`
  }
  const injectBearer = app === 'admin' || truthy(env.SANTAIZI_DEV_STATUS_BEARER)
  const token = env.SANTAIZI_API_TOKEN?.trim()
  if (injectBearer && token) {
    headers.Authorization = `Bearer ${token}`
  }
  return headers
}

type ProxyServer = {
  on(event: string, listener: (...args: never[]) => void): void
}

const accessDeniedBody = JSON.stringify({
  type: 'https://santaizi.dev/problems/cloudflare_access',
  title: 'Unauthorized',
  status: 401,
  code: 'cloudflare_access',
  detail: 'Cloudflare Access 拒绝了服务令牌。应用策略需要单独一条 Action = Service Auth，或在 .env.local 提供 CF_AUTHORIZATION。',
})

function attachOutgoingHeaders(proxy: ProxyServer, headers: Record<string, string>, origin?: string) {
  const apply = (proxyReq: ClientRequest) => {
    for (const [name, value] of Object.entries(headers)) {
      if (name.toLowerCase() === 'cookie') {
        const existing = proxyReq.getHeader('cookie')
        const parts = [existing, value].flatMap((item) => (item == null ? [] : String(item))).filter(Boolean)
        proxyReq.setHeader('cookie', parts.join('; '))
        continue
      }
      proxyReq.setHeader(name, value)
    }
    if (origin) {
      proxyReq.setHeader('Origin', origin)
    }
  }
  proxy.on('proxyReq', apply as (...args: never[]) => void)
  proxy.on('proxyReqWs', apply as (...args: never[]) => void)
  proxy.on('error', ((error: NodeJS.ErrnoException, _req: IncomingMessage, socket: { destroyed?: boolean; destroy?: () => void }) => {
    if (error.code === 'EPIPE' || error.code === 'ECONNRESET') {
      if (socket && !socket.destroyed) {
        socket.destroy?.()
      }
      return
    }
    console.error('[santaizi-dev-proxy]', error.message)
  }) as (...args: never[]) => void)
}

function interceptAccessRedirects(proxy: ProxyServer) {
  proxy.on('proxyRes', ((proxyRes: IncomingMessage, _req: IncomingMessage, res: ServerResponse) => {
    const location = headerValue(proxyRes.headers, 'location')
    if (isAccessLoginRedirect(proxyRes.statusCode || 0, location)) {
      res.writeHead(401, { 'content-type': 'application/problem+json' })
      res.end(accessDeniedBody)
      proxyRes.resume()
      return
    }
    const cookies = proxyRes.headers['set-cookie']
    if (cookies) {
      const list = Array.isArray(cookies) ? cookies : [cookies]
      proxyRes.headers['set-cookie'] = list.map(stripCookieFlags)
    }
    res.writeHead(proxyRes.statusCode || 502, proxyRes.headers)
    proxyRes.pipe(res)
  }) as (...args: never[]) => void)
}

export function createDashboardDevProxy(env: Record<string, string>, app: 'admin' | 'status') {
  const target = (env.SANTAIZI_DEV_UPSTREAM || 'http://127.0.0.1:8000').replace(/\/$/, '')
  const remote = Boolean(env.SANTAIZI_DEV_UPSTREAM?.trim()) && !isLoopback(target)
  const headers = proxyHeaders(env, app)

  if (remote) {
    console.info(
      `[santaizi-dev-proxy] ${app} remote=on access=${headers['CF-Access-Client-Id'] ? 'on' : 'off'} cookie=${headers.Cookie ? 'on' : 'off'} bearer=${headers.Authorization ? 'on' : 'off'}`,
    )
  }

  const httpEntry = (extra: Record<string, unknown> = {}) => ({
    target,
    changeOrigin: remote,
    headers,
    selfHandleResponse: remote,
    configure: (proxy: ProxyServer) => {
      attachOutgoingHeaders(proxy, headers, remote ? target : undefined)
      if (remote) {
        interceptAccessRedirects(proxy)
      }
    },
    ...extra,
  })

  const proxy: Record<string, ReturnType<typeof httpEntry>> = {
    '/api': httpEntry(),
    '/static': httpEntry(),
    '/ws': httpEntry({
      target,
      ws: true,
      selfHandleResponse: false,
      configure: (proxyServer: ProxyServer) => attachOutgoingHeaders(proxyServer, headers, remote ? target : undefined),
    }),
  }
  if (app === 'admin') {
    proxy['/oauth2'] = httpEntry()
  }
  return proxy
}

export function blockRemoteLogout(env: Record<string, string>) {
  const upstream = env.SANTAIZI_DEV_UPSTREAM?.trim() || ''
  const remote = Boolean(upstream) && !isLoopback(upstream)
  return {
    name: 'santaizi-block-remote-logout',
    configureServer(server: { middlewares: { use: (fn: (req: { method?: string; url?: string }, res: { statusCode: number; end: () => void }, next: () => void) => void) => void } }) {
      if (!remote) {
        return
      }
      server.middlewares.use((req, res, next) => {
        const path = req.url?.split('?')[0]
        if (req.method === 'POST' && path === '/api/v2/auth/logout') {
          res.statusCode = 204
          res.end()
          return
        }
        next()
      })
    },
  }
}
