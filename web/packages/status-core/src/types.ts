import type { ServerRecord, SiteBootstrap } from '@santaizi/api'

export interface StatusServerGroup {
  name: string
  items: ServerRecord[]
}

/** reactive store 访问时会自动解包 ref/computed */
export interface StatusStoreState {
  bootstrap: SiteBootstrap | null
  servers: ServerRecord[]
  groups: StatusServerGroup[]
  loading: boolean
  connected: boolean
  loadError: boolean
  load: () => Promise<void>
  connect: () => void
  stop: () => void
}
