/**
 * Shared browser/host-safe data types for the dag-view surface. These are
 * structural mirrors of the dsh-dag-orchestrator projections (engine.status
 * rows, the dag-face shapes) — type-only knowledge, never a runtime import
 * from the orchestrator package. All cross-package collaboration at runtime
 * goes through the cordis service `ctx.get('dagOrchestrator')` and the
 * /dag-view/* HTTP routes.
 *
 * Conditional expansion rule mirrored from the orchestrator: optional row
 * fields are simply absent (never undefined-valued, never null) when not
 * applicable.
 * @module dsh-dag-view/core/types
 */

// ---------------------------------------------------------------------------
// State enums (dsh-dag-orchestrator task/run/attempt state machines)
// ---------------------------------------------------------------------------

/** Task lifecycle states (engine task-state diagram). */
export type TaskState =
  | 'pending'
  | 'ready'
  | 'queued'
  | 'running'
  | 'retry_wait'
  | 'blocked'
  | 'succeeded'
  | 'failed'
  | 'cancelled'

/** Run lifecycle states. */
export type RunState =
  | 'running'
  | 'pausing'
  | 'paused'
  | 'cancelling'
  | 'succeeded'
  | 'failed'
  | 'cancelled'

/** Attempt lifecycle states. */
export type AttemptState =
  | 'claimed'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled'
  | 'orphaned'

/** Task kinds accepted by the spec validator. */
export type TaskKind = 'agent' | 'approval' | 'merge'

/** Condition operator on a dependsOn edge. */
export type DependencyCondition = 'succeeded' | 'completed'

// ---------------------------------------------------------------------------
// Run/task/attempt/event rows (engine.status projections)
// ---------------------------------------------------------------------------

/** Aggregate task-state counts carried by every run row. */
export interface RunCounts {
  readonly pending: number
  readonly ready: number
  readonly running: number
  readonly succeeded: number
  readonly failed: number
  readonly blocked: number
}

/** One run summary row (all-runs /run list, and the /run aggregate base). */
export interface RunSummary {
  readonly run_id: string
  readonly name: string
  readonly state: RunState
  readonly counts: RunCounts
  readonly created_at: number
  readonly updated_at: number
}

/** One task row from status detail 'tasks' (and the /run aggregate). */
export interface TaskRow {
  readonly id: string
  readonly state: TaskState
  readonly attempts: number
  readonly ordinal: number
  readonly blocked_reason?: unknown
  readonly last_stop_reason?: string
  readonly retry_not_before?: number
}

/** One attempt row from status detail 'attempts' (and the /run aggregate). */
export interface AttemptRow {
  readonly attempt_id: string
  readonly task_id: string
  readonly ordinal: number
  readonly state: AttemptState
  readonly backend: string
  readonly started_at: number
  readonly child_session?: string
  readonly stop_reason?: string
  readonly failure?: unknown
}

/** One event row from status detail 'events' (the tail window). */
export interface EventRow {
  readonly seq: number
  readonly type: string
  readonly at: number
  readonly task_id?: string
  readonly attempt_id?: string
  readonly payload: unknown
}

// ---------------------------------------------------------------------------
// Parsed spec view (face.getSpec → spec, cropped to UI needs)
// ---------------------------------------------------------------------------

/** One dependsOn edge of a spec task (UI draws graph edges from these). */
export interface SpecDependency {
  readonly taskId: string
  readonly condition: DependencyCondition
  readonly gate?: {
    readonly artifact: string
    readonly expect: string
    readonly value?: string
  }
}

/** One spec task, cropped to the fields the UI renders. */
export interface SpecTask {
  readonly id: string
  readonly kind: TaskKind
  readonly prompt?: string
  readonly dependsOn: readonly SpecDependency[]
  readonly model?: string
  readonly provider?: string
}

/** The parsed WorkflowSpec view (top-level fields the UI needs). */
export interface SpecView {
  readonly version: 1
  readonly name: string
  readonly description?: string
  readonly tasks: readonly SpecTask[]
}

/** The face.getSpec return shape (spec_hash plus the parsed spec). */
export interface SpecRecord {
  readonly run_id: string
  readonly name: string
  readonly spec_hash: string
  readonly spec: SpecView
}

// ---------------------------------------------------------------------------
// Outputs and attempt summaries (face.listOutputs / face.attemptSummaries)
// ---------------------------------------------------------------------------

/** One validated task output row. */
export interface OutputRow {
  readonly task_id: string
  readonly name: string
  readonly value: unknown
  readonly produced_by_attempt: string
}

/** One attempt summary row (attempts projection + parsed result summary). */
export interface AttemptSummary {
  readonly attempt_id: string
  readonly task_id: string
  readonly ordinal: number
  readonly state: AttemptState
  readonly backend: string
  readonly started_at: number
  readonly child_session?: string
  readonly stop_reason?: string
  readonly summary?: unknown
}

// ---------------------------------------------------------------------------
// Route payloads (the /dag-view/* value shapes)
// ---------------------------------------------------------------------------

/** POST /dag-view/runs value. */
export interface RunsView {
  readonly runs: readonly RunSummary[]
}

/** POST /dag-view/session-runs value — the planning session's runs (same row shape). */
export interface SessionRunsView {
  readonly runs: readonly RunSummary[]
}

/** POST /dag-view/run value — one aggregate snapshot for the main view. */
export interface RunAggregate {
  readonly run: RunSummary
  readonly spec: SpecView
  readonly tasks: readonly TaskRow[]
  readonly attempts: readonly AttemptRow[]
  readonly outputs: readonly OutputRow[]
}

/** POST /dag-view/events value. */
export interface EventsView {
  readonly events: readonly EventRow[]
}

/** POST /dag-view/attempt-logs value. */
export interface AttemptLogsView {
  readonly items: readonly AttemptSummary[]
}

// ---------------------------------------------------------------------------
// JSON envelope (every /dag-view response)
// ---------------------------------------------------------------------------

/** Stable error shape carried by every failed envelope. */
export interface DagViewError {
  readonly code: string
  readonly message: string
}

/**
 * The response envelope: success carries the route value, failure carries a
 * stable error code (dag_view.* for transport/shape faults, dag.* passed
 * through from the service face).
 */
export type Envelope<T> =
  | { ok: true; value: T }
  | { ok: false; error: DagViewError }
