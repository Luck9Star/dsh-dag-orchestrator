/**
 * Vitest wiring: host tests run in node, client tests in jsdom (the mount
 * tests drive MutationObserver + DOM injection). Environment picked per
 * project; the tests dir mirrors the split (tests/host, tests/client).
 */
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/**/*.test.{ts,tsx}'],
    environment: 'node',
    projects: [
      {
        // Host-half route tests: fake ctx + fake face, node streams.
        test: {
          name: 'host',
          include: ['tests/host/**/*.test.{ts,tsx}'],
          environment: 'node',
        },
      },
      {
        // Browser-half tests: jsdom (fetch mock, MutationObserver, DOM).
        test: {
          name: 'client',
          include: ['tests/client/**/*.test.{ts,tsx}'],
          environment: 'jsdom',
        },
      },
    ],
  },
})
