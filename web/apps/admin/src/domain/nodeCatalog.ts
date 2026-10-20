export type NodeSortKey = 'display_index_desc' | 'display_index_asc' | 'name_asc' | 'name_desc'

export type NodeCatalogItem = {
  server_id?: number
  server_name?: string
  display_index?: number
  tag?: string
}

export const DEFAULT_NODE_SORT: NodeSortKey = 'display_index_desc'
export const EMPTY_GROUP_FILTER = '__empty__'

export function nodeTag(value?: string | null) {
  return (value || '').trim()
}

export function groupFilterValue(tag?: string | null) {
  const value = nodeTag(tag)
  return value || EMPTY_GROUP_FILTER
}

export function groupFilterLabel(tag?: string | null, fallback = 'default') {
  if (!tag || tag === EMPTY_GROUP_FILTER) return fallback
  return tag
}

export function matchNodeCatalog(item: NodeCatalogItem, query: string, tagFilter: string) {
  const needle = query.trim().toLowerCase()
  if (needle && !(item.server_name || '').toLowerCase().includes(needle)) return false
  if (!tagFilter) return true
  const tag = nodeTag(item.tag)
  if (tagFilter === EMPTY_GROUP_FILTER) return tag === ''
  return tag === tagFilter
}

export function compareNodeCatalog(left: NodeCatalogItem, right: NodeCatalogItem, sort: NodeSortKey) {
  const name = (left.server_name || '').localeCompare(right.server_name || '', undefined, { numeric: true, sensitivity: 'base' })
  const weight = (left.display_index ?? 0) - (right.display_index ?? 0)
  const id = (left.server_id ?? 0) - (right.server_id ?? 0)
  switch (sort) {
    case 'display_index_asc':
      return weight || name || id
    case 'name_asc':
      return name || -weight || id
    case 'name_desc':
      return -name || -weight || id
    default:
      return -weight || name || id
  }
}

export function uniqueNodeTags(items: Iterable<NodeCatalogItem>) {
  const seen = new Set<string>()
  const out: string[] = []
  for (const item of items) {
    const key = groupFilterValue(item.tag)
    if (seen.has(key)) continue
    seen.add(key)
    out.push(key)
  }
  return out.sort((left, right) => {
    if (left === EMPTY_GROUP_FILTER) return -1
    if (right === EMPTY_GROUP_FILTER) return 1
    return left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' })
  })
}
