/**
 * dag-view surface plugin, browser half: the sidebar entry + the
 * placeholder center panel (DOM extension; no slot exists for either).
 * The real views (runs list, DAG graph, task detail, events, logs,
 * outputs) mount in T3/T4 through the seams exported from mount.tsx and
 * api.ts; this entry stays a thin shell that owns only the mount
 * lifecycle.
 *
 * All data flows through the host /dag-view/* routes registered by this
 * package's own host half (same-origin fetch); nothing else is injected.
 * @module dsh-dag-view/client
 */

import { createMount, type DagViewMount } from './mount.tsx'

/** Minimal client context shape this shell needs (structural; grows in T3/T4). */
export interface ClientContext {
  /** cordis effect registration; the disposer runs on plugin unload. */
  effect(callback: () => (() => void) | undefined | void, label?: string): (() => void) | undefined | void
}

/** Required services: none yet — the placeholder mount needs no client service. */
export const inject: readonly string[] = []

/** The active mount handle (single instance per loaded plugin). */
let mount: DagViewMount | undefined

/**
 * Client plugin body: mount the placeholder surface, disposed on unload.
 * @param ctx - client root context.
 */
export function apply(ctx: ClientContext): void {
  ctx.effect(() => {
    const current = createMount()
    mount = current
    current.start()
    return () => {
      current.stop()
      mount = undefined
    }
  }, 'dsh-dag-view: placeholder mount')
}

/** The active mount handle, for T3/T4 view wiring. */
export function activeMount(): DagViewMount | undefined {
  return mount
}
