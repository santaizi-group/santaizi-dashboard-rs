import { describe, expect, it } from 'vitest'
import {
  compareNodeCatalog,
  DEFAULT_NODE_SORT,
  EMPTY_GROUP_FILTER,
  groupFilterLabel,
  groupFilterValue,
  matchNodeCatalog,
  uniqueNodeTags,
} from './nodeCatalog'

const nodes = [
  { server_id: 1, server_name: 'alpha', display_index: 10, tag: 'edge' },
  { server_id: 2, server_name: 'bravo', display_index: 30, tag: '' },
  { server_id: 3, server_name: 'charlie', display_index: 30, tag: 'core' },
]

describe('nodeCatalog', () => {
  it('defaults to weight descending then name', () => {
    const sorted = [...nodes].sort((left, right) => compareNodeCatalog(left, right, DEFAULT_NODE_SORT))
    expect(sorted.map(item => item.server_id)).toEqual([2, 3, 1])
  })

  it('filters by name and group including empty tag', () => {
    expect(nodes.filter(item => matchNodeCatalog(item, 'alp', '')).map(item => item.server_id)).toEqual([1])
    expect(nodes.filter(item => matchNodeCatalog(item, '', 'edge')).map(item => item.server_id)).toEqual([1])
    expect(nodes.filter(item => matchNodeCatalog(item, '', EMPTY_GROUP_FILTER)).map(item => item.server_id)).toEqual([2])
  })

  it('lists unique group filter values with empty first', () => {
    expect(uniqueNodeTags(nodes)).toEqual([EMPTY_GROUP_FILTER, 'core', 'edge'])
    expect(groupFilterValue('')).toBe(EMPTY_GROUP_FILTER)
    expect(groupFilterLabel('', 'default')).toBe('default')
    expect(groupFilterLabel(EMPTY_GROUP_FILTER, 'default')).toBe('default')
  })
})
