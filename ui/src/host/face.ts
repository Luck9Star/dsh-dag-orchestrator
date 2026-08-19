/**
 * The lazy dagOrchestrator service-face accessor plus the structural
 * TypeScript interface mirroring dsh-dag-orchestrator's lib/dag-face.js
 * (method names, argument shapes, and return shapes mirrored exactly —
 * type-only knowledge, no runtime import from the orchestrator package).
 *
 * The UI host half NEVER injects the service: the orchestrator core plugin
 * may be absent (the UI must mount and degrade gracefully), so the face is
 * resolved opportunistically per request via ctx.get('dagOrchestrator').
 * @module dsh-dag-view/host/face
 */

import type { Context } from '@deepseek-ai/cordis'
import type {
  AttemptRow, AttemptSummary, EventRow, OutputRow, RunSummary, SpecRecord, TaskRow,
} from '../core/types.ts'

/**
 * Options accepted by face.status — a structural mirror of the engine's
 * status options (detail depth plus the events-window filters).
 */
export interface FaceStatusOptions {
  readonly detail?: 'summary' | 'tasks' | 'attempts' | 'events'
  readonly taskId?: string
  readonly limit?: number
  readonly afterSeq?: number
}

/**
 * The read-only dagOrchestrator service face, mirrored structurally from
 * dsh-dag-orchestrator lib/dag-face.js (createDagFace's frozen return).
 * Method-for-method and shape-for-shape identical to the live object.
 */
export interface DagServiceFace {
  /**
   * engine.status passthrough (four depths). Omitted/empty runId means the
   * all-runs summary `{kind:'status',detail:'summary',runs:[...]}`; a runId
   * yields `{kind:'status',run_id,name,state,counts,created_at,updated_at}`
   * plus `tasks` (detail 'tasks'+), `attempts` (detail 'attempts'), or
   * `events` (detail 'events').
   */
  status(
    runId?: string | null,
    options?: FaceStatusOptions,
  ): unknown

  /** Parsed runs.spec_json plus spec_hash; unknown run throws dag.run_not_found. */
  getSpec(runId: string): SpecRecord

  /** Validated outputs of one run, value_json parsed. */
  listOutputs(runId: string): OutputRow[]

  /** Attempts rows of one run (optionally one task) with parsed result summaries. */
  attemptSummaries(runId: string, taskId?: string): AttemptSummary[]

  /**
   * All-runs summary rows filtered to the planning session (runs.planner_session
   * = sessionId) — the GUI tab's "this conversation's runs" section. Rows share
   * the status(detail:'summary') shape; an unknown session returns `{runs: []}`
   * (an empty result is a state, not an error).
   */
  runsForSession(sessionId: string): { runs: readonly RunSummary[] }
}

/** Structural narrowing of the status(detail:'summary') projection. */
export interface StatusSummary {
  readonly kind: 'status'
  readonly detail: 'summary'
  readonly runs: readonly RunSummary[]
}

/** Structural narrowing of the status(detail:'attempts') projection. */
export interface StatusAttempts {
  readonly kind: 'status'
  readonly detail: 'attempts'
  readonly run_id: string
  readonly name: string
  readonly state: RunSummary['state']
  readonly counts: RunSummary['counts']
  readonly created_at: number
  readonly updated_at: number
  readonly tasks: readonly TaskRow[]
  readonly attempts: readonly AttemptRow[]
}

/** Structural narrowing of the status(detail:'events') projection. */
export interface StatusEvents {
  readonly kind: 'status'
  readonly detail: 'events'
  readonly run_id: string
  readonly name: string
  readonly state: RunSummary['state']
  readonly counts: RunSummary['counts']
  readonly created_at: number
  readonly updated_at: number
  readonly tasks: readonly TaskRow[]
  readonly events: readonly EventRow[]
}

/** Envelope error for a structurally unusable resolved face value. */
const UNUSABLE = { code: 'dag_view.unavailable', message: 'dagOrchestrator service is not usable' } as const

/**
 * Resolve the dagOrchestrator service face for one request, or an envelope
 * error when the core plugin is absent or did not provide a usable face.
 *
 * Lazy by design: the orchestrator may load/unload at any time relative to
 * this UI plugin, so nothing is cached — each request re-reads the context.
 */
export function resolveFace(ctx: Context): { ok: true; face: DagServiceFace } | { ok: false; error: { code: string; message: string } } {
  // Guard the lookup itself: a Context always carries .get in cordis, but a
  // hostile/odd host must degrade to the unavailable envelope, not throw.
  if (typeof ctx.get !== 'function') {
    return { ok: false, error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' } }
  }
  let raw: unknown
  try {
    raw = ctx.get('dagOrchestrator')
  } catch {
    return { ok: false, error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' } }
  }
  if (raw === null || raw === undefined) {
    return { ok: false, error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' } }
  }
  const face = raw as Partial<Record<keyof DagServiceFace, unknown>>
  const usable = typeof face.status === 'function'
    && typeof face.getSpec === 'function'
    && typeof face.listOutputs === 'function'
    && typeof face.attemptSummaries === 'function'
    // runsForSession is a newer face member: an older core plugin without it
    // still serves the four original routes; only /session-runs degrades.
    && typeof face.runsForSession === 'function'
  if (!usable) return { ok: false, error: UNUSABLE }
  return { ok: true, face: raw as DagServiceFace }
}
