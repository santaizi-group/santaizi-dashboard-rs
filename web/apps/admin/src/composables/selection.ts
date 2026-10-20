/** Toggle a row in an Admin list `selected` array (mobile cards + desktop table). */
export function isRowSelected<T extends { id: number | string }>(selected: readonly T[], row: T): boolean {
  return selected.some(item => item.id === row.id)
}

export function toggleRowSelection<T extends { id: number | string }>(selected: readonly T[], row: T, checked: boolean): T[] {
  if (checked) return selected.some(item => item.id === row.id) ? [...selected] : [...selected, row]
  return selected.filter(item => item.id !== row.id)
}
