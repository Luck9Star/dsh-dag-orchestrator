/**
 * dsh-dag-view — host half: the /dag-view/* read-only JSON routes on the
 * shared webserver, fed by the dagOrchestrator service face (provided by
 * the dsh-dag-orchestrator core plugin) resolved lazily per request. The
 * browser half (exports "./client") is served from this same package's
 * dsh.client declaration by the host's client-modules scanner.
 *
 * The host half owns no state and never touches the orchestrator's
 * database: every read goes through the service face. Control operations
 * stay on the conversation's dag_* tools — this surface is view-only.
 * @module dsh-dag-view
 */

import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-host-webserver'
import { registerDagViewRoutes } from './host/routes.ts'

/** Required services: the shared webserver's route registry. */
export const inject = ['webServer']

/**
 * Mount the /dag-view routes.
 * @param ctx - context carrying webServer.
 */
export function apply(ctx: Context): void {
  ctx.effect(() => registerDagViewRoutes(ctx), 'dsh-dag-view: /dag-view routes')
}
