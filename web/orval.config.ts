import { defineConfig } from 'orval'

export default defineConfig({
  santaizi: {
    input: '../openapi/v2.yaml',
    output: {
      target: './packages/api/src/generated/santaizi.ts',
      schemas: './packages/api/src/generated/model',
      client: 'axios',
      mode: 'split',
      clean: true,
      prettier: false,
      override: {
        mutator: {
          path: './packages/api/src/request.ts',
          name: 'santaiziRequest',
        },
      },
    },
  },
})
