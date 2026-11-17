export function resolveTokenHost(el?: HTMLElement | null): HTMLElement {
  if (el) {
    const shell = el.closest('.server-status-shell, .nazhua-shell')
    if (shell instanceof HTMLElement) return shell
    return el
  }
  if (typeof document !== 'undefined') {
    const shell = document.querySelector('.server-status-shell, .nazhua-shell')
    if (shell instanceof HTMLElement) return shell
  }
  return document.documentElement
}

export function readServiceToken(el: HTMLElement | null | undefined, name: string, fallback: string): string {
  if (typeof getComputedStyle !== 'function') return fallback
  const host = resolveTokenHost(el)
  return getComputedStyle(host).getPropertyValue(name).trim() || fallback
}
