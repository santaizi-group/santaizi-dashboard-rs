/// <reference types="vite/client" />

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent
  export default component
}

interface Window {
  Redoc?: {
    init: (
      specOrUrl: string,
      options: Record<string, unknown>,
      element: HTMLElement,
      callback?: (error?: Error) => void,
    ) => void
  }
}
