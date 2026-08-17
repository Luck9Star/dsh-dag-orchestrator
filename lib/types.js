/**
 * Scheduler frozen types — Cross-Track Frozen Interfaces.
 *
 * Ported from task-weaver packages/scheduler/src/types.ts (L1-62). JS has no
 * `interface`; the four frozen shapes are exported as JSDoc typedefs so
 * `lib/**.js` (and editors via JSDoc `@type` annotations) reference the same
 * contract. Semantics of each field are unchanged from the source (DESIGN §9.1).
 *
 * These types are the EXACT signatures the engine/evaluator/store modules
 * compile against; changing a shape is a cross-module decision, not a local
 * one (source "Cross-Track Frozen Interfaces" rule, kept as family law).
 */

/**
 * A stable, machine-readable reason a Task is not runnable right now.
 * Codes include "upstream_failed", "artifact_missing", "workspace_unavailable",
 * "approval_service_unavailable", "merge_unavailable", "verify_unavailable".
 *
 * @typedef {object} BlockedReason
 * @property {string} code
 * @property {Readonly<Record<string, unknown>>} details
 */

/**
 * The output of a ready-evaluation pass over a TaskGraph: which tasks are
 * runnable now, and which are blocked (with a stable reason).
 *
 * @typedef {object} ReadyEvaluation
 * @property {string} runId
 * @property {readonly string[]} readyTaskIds
 * @property {ReadonlyArray<{ taskId: string, reason: BlockedReason }>} blockedTasks
 */

/**
 * One task row in a TaskGraphSnapshot.
 *
 * `retryNotBeforeMs` is the epoch-ms when a `retry_wait` task becomes eligible
 * for ready promotion (backoff expiry). Omitted for non-retry states; when
 * missing on `retry_wait`, the evaluator treats the task as not yet due
 * (fail-closed — stays out of readyTaskIds until a concrete deadline is set).
 *
 * Task lifecycle states (DESIGN §6.2, task-weaver vocabulary narrowed):
 * pending | ready | queued | running | succeeded | failed | retry_wait |
 * blocked | cancelled.
 *
 * @typedef {object} TaskGraphTaskView
 * @property {string} taskId
 * @property {string} state
 * @property {number} version
 * @property {number} [retryNotBeforeMs] Epoch-ms when a `retry_wait` task may be re-evaluated for ready.
 * @property {string} [blockedReasonCode] When state is `blocked`, the machine-readable reason code (if known). Used so `approval_pending` parks are NOT re-evaluated into a claim loop.
 */

/**
 * A self-consistent snapshot of one TaskGraph at a committed transaction.
 * Used as the input to ready-evaluation and the reconcile loop.
 *
 * @typedef {object} TaskGraphSnapshot
 * @property {string} runId
 * @property {ReadonlyArray<TaskGraphTaskView>} tasks
 */

export {};
