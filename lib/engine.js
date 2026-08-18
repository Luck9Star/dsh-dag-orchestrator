/**
 * DagEngine — the state-machine heart (DESIGN §5, TASKS.md T07).
 *
 * oneRound = the ten-step single reconcile pass (§5.1 table), a narrowing of
 * task-weaver scheduler-loop.ts's 14-step `tick` (L552-690):
 *
 *   1. reconcileApprovals      (T12, landed: decided approvals promote
 *                               their task out of the approval park)
 *   2. promoteReady            (readiness.ts L71-159 直搬改造)
 *   3. admission gate          (isAdmissionClosed L2347-2352)
 *   4. buildQueue              (readiness.ts L209-231 直搬)
 *   5. dispatchLoop            (claimSpawnDrainCommit narrowed; claimTask
 *                               L1943-2056 with the H1/TOCTOU fix)
 *   6. harvestSettled          (D8: promise is the terminal carrier)
 *   7. propagateDownstream     (readiness.ts L165-203 直搬)
 *   8. finalizeRunIfDone       (scheduler-driver.ts L401-489 直搬)
 *   9. drainToPaused           (scheduler-loop.ts L758-810 直搬)
 *  10. oneRound summary
 *
 * tick = the bounded multi-round loop (§5.2 pseudo-code, D10): clamped
 * maxRounds 1..16, a WHOLE-CALL settle budget (never reset per round — the
 * 16×60s self-spin guard), noSettleStreak >= 2 early exit, and the §8.3
 * tickSummary with the waiting_on classification.
 *
 * autoTick = the optional no-dispatch Timer pump (T14 / §5.5): the
 * projection-only subset of oneRound over EVERY non-terminal run, for when
 * nobody is pumping (see the method's honest-boundary comment).
 *
 * Three hard invariants (scheduler-loop.ts L20-35, all inherited — enforced
 * here and in terminal-commit.js):
 *   #1  No Promise.all batched sibling terminals; EVERY attempt commits its
 *       terminal in its OWN transaction (harvestSettled loops one by one).
 *   #3  Ready evaluation + state updates share one transaction; dispatch and
 *       all other I/O stay OUTSIDE transactions.
 *   #6  Every projection change emits its event in the SAME transaction.
 *
 * Discipline: the engine is the ONLY tx orchestrator (red line 2); sqlite is
 * reachable only through the injected store (R2/R7); `now` and `random` are
 * injectable for deterministic tests.
 */

import { randomUUID } from 'node:crypto'

import { BoundedQueue } from './bounded-queue.js'
import { criticalPathDepth } from './critical-path.js'
import { runMergeTask } from './executors/merge.js'
import { commitTerminalAndRelease } from './terminal-commit.js'
import { evaluateGate, evaluateReady, READY_BLOCKED_CODES } from './ready-evaluator.js'
import { reflect, promiseSettledSync } from './reflect.js'
import { validateSpec, specHash } from './spec-validate.js'
import { evaluateVerifyGate } from './verify-gate.js'

/** Run states from which nothing further is derived (paused is resumable). */
const RUN_TERMINAL_STATES = new Set(['succeeded', 'failed', 'cancelled'])
/** Task states with no further transitions. */
const TASK_TERMINAL_STATES = new Set(['succeeded', 'failed', 'cancelled'])
/** controlIntents that close admission (no new dispatch; drain continues). */
const ADMISSION_CLOSED_INTENTS = new Set(['pause', 'stop'])
/**
 * Intents whose terminal is owned by a dedicated drain — finalize must NOT
 * derive the Run terminal from aggregate task outcomes while one is active.
 * stop is NOT here: its `cancelled` terminal is correctly derived (source
 * comment L405-412 carried over).
 */
const FINALIZE_BLOCKING_INTENTS = { pause: true }

/** tick clamps (DESIGN §5.2 / §8.3). */
const MAX_ROUNDS_FLOOR = 1
const MAX_ROUNDS_CEIL = 16
const SETTLE_MS_FLOOR = 0
const SETTLE_MS_CEIL = 60_000
const DEFAULT_MAX_ROUNDS = 4
const DEFAULT_SETTLE_MS = 10_000

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/** Parse the blocked-reason JSON blob's code (readiness.ts L55-62). */
function parseBlockedReasonCode(reason) {
  if (typeof reason !== 'string') return undefined
  try {
    return JSON.parse(reason).code
  } catch {
    return undefined
  }
}

/** Parse the blocked-reason JSON blob's retryNotBeforeMs (readiness.ts L45-52). */
function parseRetryNotBefore(reason) {
  if (typeof reason !== 'string') return undefined
  try {
    const parsed = JSON.parse(reason)
    return typeof parsed.retryNotBeforeMs === 'number' ? parsed.retryNotBeforeMs : undefined
  } catch {
    return undefined
  }
}

/**
 * Map a failureType to the retry-policy key (scheduler-loop.ts L2909-2913
 * verbatim): the harvest taxonomy is `transient`/`timeout`; the policy
 * domain is `transient_network`. Everything else passes through unchanged.
 */
export function failureTypeToPolicyKey(failureType) {
  if (failureType === 'transient') return 'transient_network'
  if (failureType === 'timeout') return 'transient_network'
  return failureType
}

/**
 * boundedRace — wait up to `ms` for ANY of the promises to settle (§5.2).
 * Returns the actual elapsed ms and whether at least one settled. Uses the
 * never-rejecting reflect() wrapper, so a rejecting in-flight attempt counts
 * as a settlement (its harvest classification happens on the next round).
 *
 * @param {Promise<unknown>[]} promises
 * @param {number} ms
 * @returns {Promise<{anySettled: boolean, actualMs: number}>}
 */
async function boundedRace(promises, ms) {
  if (promises.length === 0 || ms <= 0) return { anySettled: false, actualMs: 0 }
  const startAt = Date.now()
  const reflected = promises.map((p) => reflect(p))
  let timer = undefined
  // The race timer MUST stay ref'd: it bounds an in-flight await, so keeping
  // the event loop alive until it fires (or is cleared) is the contract. An
  // unref'd timer lets the loop drain mid-await — a bare process driving the
  // engine (and quiet CI runners) then dies inside tick() with the promise
  // still pending.
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve('timeout'), ms)
  })
  try {
    await Promise.race([Promise.all(reflected).then(() => 'all'), timeout])
  } finally {
    clearTimeout(timer)
  }
  const anySettled = reflected.some((p) => promiseSettledSync(p) === 'fulfilled')
    && promises.some((p) => promiseSettledSync(p) === 'fulfilled')
  return { anySettled, actualMs: Date.now() - startAt }
}

/** `dag_<yyyymmdd>_<8hex>` run id (task brief: self-chosen, unique). */
function newRunId(now) {
  const d = new Date(now)
  const ymd = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`
  return `dag_${ymd}_${randomUUID().replace(/-/g, '').slice(0, 8)}`
}

// ---------------------------------------------------------------------------
// engine factory
// ---------------------------------------------------------------------------

/**
 * @typedef {object} CreateEngineOptions
 * @property {object} store DagStore handle (the only persistence outlet).
 * @property {object} executor createExecutor() handle.
 * @property {object} admission createAdmission() handle.
 * @property {object} [config] {defaultMaxRunningAgents?, defaultQueueCapacity?}
 * @property {{debug?: Function, warn?: Function, error?: Function, info?: Function}} [logger]
 * @property {() => number} [now] injectable clock (defaults to Date.now).
 * @property {() => number} [random] injectable jitter source (shouldRetry tests).
 */

/**
 * @param {CreateEngineOptions} options
 */
export function createEngine({ store, executor, admission, config, logger, now, random } = {}) {
  if (!store || typeof store.tx !== 'function') throw new TypeError('createEngine: a store handle is required')
  if (!executor || typeof executor.dispatch !== 'function') throw new TypeError('createEngine: an executor handle is required')
  if (!admission || typeof admission.tryAcquireSlot !== 'function') throw new TypeError('createEngine: an admission handle is required')
  const clockNow = typeof now === 'function' ? now : Date.now
  const randomFn = typeof random === 'function' ? random : Math.random
  const log = logger ?? {}
  const defaultMaxRunning = config?.defaultMaxRunningAgents ?? 4
  const defaultQueueCapacity = config?.defaultQueueCapacity ?? 16

  /** attemptId → {runId, taskId, ordinal, sessionKey?} — the engine's
   * in-memory bookkeeping for in-flight handles (cleared on harvest). */
  const attemptMeta = new Map()

  // ---- spec access -----------------------------------------------------------

  function specOfRun(run) {
    // The run row carries the validated, normalized spec verbatim.
    return JSON.parse(run.spec_json)
  }

  function requireRun(runId) {
    const run = store.findRun(runId)
    if (run === null) {
      const error = new Error(`engine: run ${JSON.stringify(runId)} not found`)
      error.code = 'dag.run_not_found'
      throw error
    }
    return run
  }

  /** Loud control-plane error (§8.4 error contract: stable `dag.*` codes). */
  function controlError(code, message) {
    const error = new Error(message)
    error.code = code
    return error
  }

  // ==========================================================================
  // oneRound step 1 — reconcileApprovals (T12; DESIGN §5.1 row 2 / §8.5)
  // ==========================================================================

  /**
   * Promote decided approvals into task transitions, one approval per
   * transaction (T12). The source is the store's DECIDED set (rows
   * dag_approve's decideApproval CAS-flipped to approved/rejected — a
   * decided row no longer appears in the pending query, so the decided set
   * is the only complete source). Each decided approval promotes its PARKED
   * task in ONE tx:
   *
   *   approved  → task CAS blocked(approval_pending) → succeeded +
   *               `task.succeeded` event (payload approvalId) — approval IS
   *               the work, the task goes straight terminal, NO new attempt
   *               (the parked attempt is already failed; source semantics:
   *               reconcile promotes the task, never re-runs it).
   *   rejected  → task CAS → failed + `task.failed` event with the permanent
   *               `dag.policy_denied` classification — propagateDownstream
   *               (step 7, same round) then blocks its succeeded-condition
   *               dependents blocked(upstream_failed).
   *
   * The task CAS guards make the promotion idempotent: only a task still
   * parked blocked(approval_pending) is promotable; anything else (already
   * promoted, cancelled, re-parked on a NEW approval id) loses the race and
   * is skipped without an event — re-ticking a reconciled run is a no-op,
   * not a duplicate promotion.
   *
   * @param {string} runId
   * @returns {{promoted: number, pending: number}} promoted = tasks moved
   *   out of the approval park; pending = approvals still awaiting a
   *   decision (feeds the waiting_on classification).
   */
  function reconcileApprovals(runId) {
    const decided = store.findDecidedApprovals(runId)
    const pending = store.findPendingApprovals(runId).length
    if (decided.length === 0) return { promoted: 0, pending }

    let promoted = 0
    for (const approval of decided) {
      store.tx(() => {
        const nowTs = clockNow()
        const taskRow = store.findTasks(runId).find((t) => t.task_id === approval.task_id)
        if (taskRow === undefined) return
        if (taskRow.state !== 'blocked') return
        if (parseBlockedReasonCode(taskRow.blocked_reason) !== 'approval_pending') return

        if (approval.state === 'approved') {
          const cas = store.casTaskState(
            runId, approval.task_id, taskRow.state, taskRow.version, 'succeeded',
            { blocked_reason: null, retry_not_before: null },
          )
          if (!cas.ok) return
          store.insertEvent(runId, {
            type: 'task.succeeded',
            taskId: approval.task_id,
            payload: {
              from: taskRow.state,
              to: 'succeeded',
              reason: 'approval_approved',
              approvalId: approval.approval_id,
            },
            at: nowTs,
          })
          promoted++
          return
        }

        // approval.state === 'rejected'
        const cas = store.casTaskState(
          runId, approval.task_id, taskRow.state, taskRow.version, 'failed',
          { blocked_reason: null, retry_not_before: null },
        )
        if (!cas.ok) return
        store.insertEvent(runId, {
          type: 'task.failed',
          taskId: approval.task_id,
          payload: {
            from: taskRow.state,
            to: 'failed',
            reason: 'approval_rejected',
            failureType: 'permanent',
            code: 'dag.policy_denied',
            approvalId: approval.approval_id,
          },
          at: nowTs,
        })
        promoted++
      })
    }

    return { promoted, pending }
  }

  // ==========================================================================
  // oneRound step 2 — promoteReady (readiness.ts L71-159 直搬改造)
  // ==========================================================================

  /**
   * Evaluate readiness and CAS pending/retry_wait/blocked → ready/blocked in
   * ONE transaction, emitting task.ready / task.blocked events alongside the
   * projection change (invariants #3 + #6). Returns the number of tasks
   * promoted (ready + blocked).
   */
  function promoteReady(runId, spec) {
    const tasks = store.findTasks(runId)
    if (tasks.length === 0) return 0

    const snapshot = {
      runId,
      tasks: tasks.map((t) => {
        const view = { taskId: t.task_id, state: t.state, version: t.version }
        if (t.blocked_reason !== null && t.blocked_reason !== undefined) {
          const code = parseBlockedReasonCode(t.blocked_reason)
          const retryNotBeforeMs = parseRetryNotBefore(t.blocked_reason)
          if (code !== undefined) view.blockedReasonCode = code
          if (retryNotBeforeMs !== undefined) view.retryNotBeforeMs = retryNotBeforeMs
        }
        if (t.state === 'retry_wait' && t.retry_not_before !== null && t.retry_not_before !== undefined) {
          view.retryNotBeforeMs = t.retry_not_before
        }
        return view
      }),
    }

    const evaluation = evaluateReady(snapshot, spec, {
      now: clockNow(),
      // task://producer/name → outputs row presence (task brief's sanctioned
      // simplification: a present output implies its producer's terminal
      // success — outputs rows are only written by terminal commits).
      outputResolver: (ref) => {
        const match = /^task:\/\/([^/]+)\/([^/]+)$/.exec(ref)
        if (!match) return false
        return store.findOutput(runId, match[1], match[2]) !== null
      },
      // T18: the dependency-gate content seam (evaluateReady is pure — the
      // parsed value_json read happens HERE). A missing row reads as null
      // (exists → false / not_exists → true, evaluateGate's table).
      gateEvaluator: (gate) => evaluateGate(gate, (ref) => {
        const match = /^task:\/\/([^/]+)\/([^/]+)$/.exec(ref)
        if (!match) return null
        const row = store.findOutput(runId, match[1], match[2])
        return row === null ? null : JSON.parse(row.value_json)
      }),
    })
    if (evaluation.readyTaskIds.length === 0 && evaluation.blockedTasks.length === 0) {
      return 0
    }

    let promoted = 0
    store.tx(() => {
      const viewById = new Map(tasks.map((t) => [t.task_id, t]))
      const nowTs = clockNow()

      for (const taskId of evaluation.readyTaskIds) {
        const row = viewById.get(taskId)
        if (!row) continue
        if (row.state !== 'pending' && row.state !== 'retry_wait' && row.state !== 'blocked') continue
        const cas = store.casTaskState(runId, taskId, row.state, row.version, 'ready', { blocked_reason: null, retry_not_before: null })
        if (cas.ok) {
          promoted++
          store.insertEvent(runId, {
            type: 'task.ready',
            taskId,
            payload: { from: row.state, to: 'ready', reason: 'ready' },
            at: nowTs,
          })
        }
      }

      for (const { taskId, reason } of evaluation.blockedTasks) {
        const row = viewById.get(taskId)
        if (!row) continue
        if (row.state !== 'pending' && row.state !== 'retry_wait' && row.state !== 'blocked') continue
        // Same-state rewrites are the crash/replay safety net (DESIGN §6.2
        // "同态重写…崩溃/重放的合法安全网"), never a per-tick routine write:
        // re-CASing a blocked row with the IDENTICAL reason would bump the
        // version and emit a duplicate task.blocked event on every tick —
        // phantom `promoted` progress that defeats the quiescence detection
        // (M1 review M3). The store's CAS accepts same-state rewrites
        // mechanically; the ENGINE only performs one when the reason
        // actually CHANGED.
        if (row.state === 'blocked') {
          const currentCode = parseBlockedReasonCode(row.blocked_reason)
          if (currentCode === reason.code) continue
        }
        const cas = store.casTaskState(
          runId, taskId, row.state, row.version, 'blocked',
          { blocked_reason: JSON.stringify(reason) },
        )
        if (cas.ok) {
          promoted++
          store.insertEvent(runId, {
            type: 'task.blocked',
            taskId,
            payload: { from: row.state, to: 'blocked', reason },
            at: nowTs,
          })
          if (reason.code === READY_BLOCKED_CODES.dependencyGateNotMet) {
            // Source readiness.ts L144-153: a gate-not-met block additionally
            // emits task.condition_not_met (T18 reopened the code path — a
            // declared gate now exists and can fail).
            store.insertEvent(runId, {
              type: 'task.condition_not_met',
              taskId,
              payload: { reason },
              at: nowTs,
            })
          }
        }
      }
    })

    return promoted
  }

  // ==========================================================================
  // oneRound step 3 — admission gate (scheduler-loop.ts L2347-2352)
  // ==========================================================================

  function isAdmissionClosed(runId) {
    const run = store.findRun(runId)
    if (!run) return false
    const intent = run.control_intent
    return intent !== null && intent !== undefined && ADMISSION_CLOSED_INTENTS.has(intent)
  }

  // ==========================================================================
  // oneRound step 4 — buildQueue (readiness.ts L209-231 直搬)
  // ==========================================================================

  function buildQueue(runId, spec) {
    const readyTasks = store.findReadyTasks(runId)
    const queue = new BoundedQueue(spec.limits?.queueCapacity ?? defaultQueueCapacity)
    const versionByTask = new Map()
    for (const t of readyTasks) {
      const specTask = spec.tasks.find((s) => s.id === t.task_id)
      const entry = {
        taskId: t.task_id,
        humanUnblocked: false,
        criticalPathDepth: criticalPathDepth(t.task_id, spec),
        priority: specTask?.priority ?? 0,
        readyAt: t.updated_at,
      }
      if (queue.tryEnqueue(entry)) {
        versionByTask.set(t.task_id, t.version)
      }
    }
    return { queue, versionByTask, readyTasks }
  }

  // ==========================================================================
  // T12 — approval executor branch (runApprovalTask L191-256 直搬语义)
  // ==========================================================================

  /**
   * The three-branch approval semantics (source task-executors.ts
   * L191-256), entered from dispatchLoop AFTER the claim, BEFORE any
   * subagent dispatch:
   *
   *   1. existing APPROVED decision → the gate is already open: commit the
   *      attempt+task terminal SUCCESS (commitTerminalAndRelease with
   *      failure null; source L203-206).
   *   2. existing REJECTED decision → permanent `dag.approval_rejected`
   *      (source L208-215; task failed → downstream propagates
   *      blocked(upstream_failed) — approval tasks cannot retry, the spec
   *      matrix caps maxAttempts at 1).
   *   3. no decision → insert the pending approval row (action =
   *      specTask.approval?.action ?? `approve_task_<id>`, prompt recorded
   *      on the row's note for the dag_approve echo) + `approval.requested`
   *      event in ONE tx, then PARK in a SECOND single tx: attempt
   *      commitTerminal(failed) + `attempt.failed` event (payload reason
   *      'approval_pending' + approvalId) + task CAS running→blocked
   *      {code:'approval_pending', approvalId} + `task.blocked` event with
   *      the same payload (source L239-254's exact shape). Slot/sessionKey
   *      release happens in the CALLER, outside the tx (source L255).
   *
   * The parked task is excluded from ready re-evaluation (T05's
   * isReadySource park exclusion) until dag_approve decides and the next
   * tick's reconcileApprovals promotes it.
   *
   * Re-run semantics (source L218-235): when a PENDING approval already
   * exists (e.g. a re-tick after a crash between the two txs left the task
   * un-parked), branch 3 REUSES it — insertApproval is skipped, the park
   * tx runs with the existing approvalId.
   *
   * @returns {'parked' | 'terminal'} 'terminal' = resolved via an existing
   *   decision (attempt+task reached a terminal state through
   *   commitTerminalAndRelease, which also released the slot/sessionKey);
   *   'parked' = the approval row exists and the attempt/task are parked
   *   for dag_approve — the CALLER releases the resources outside the tx
   *   (source L255).
   */
  function parkOrResolveApproval(runId, spec, taskId, specTask, claim, slot, sessionKey) {
    const existing = store.findApprovalsByTask(runId, taskId)

    // Branch 1 — already approved: the gate is open (source L203-206).
    if (existing.some((a) => a.state === 'approved')) {
      commitTerminalAndRelease({
        store, admission, spec, clock: { now: clockNow() },
        runId, attempt: { taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
        slot, sessionKey, ownerToken: claim.ownerToken,
        failure: null, stopReason: 'completed', retryDecision: { retry: false, backoffMs: 0 },
      })
      return 'terminal'
    }

    // Branch 2 — already rejected: permanent policy denial (source
    // L208-215; approval retries are structurally impossible — the spec
    // matrix caps retry.maxAttempts at 1).
    if (existing.some((a) => a.state === 'rejected')) {
      const failure = {
        failureType: 'permanent',
        code: 'dag.approval_rejected',
        message: `approval rejected for task ${taskId}`,
      }
      commitTerminalAndRelease({
        store, admission, spec, clock: { now: clockNow() },
        runId, attempt: { taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
        slot, sessionKey, ownerToken: claim.ownerToken,
        failure, stopReason: 'refusal', retryDecision: { retry: false, backoffMs: 0 },
      })
      return 'terminal'
    }

    // Branch 3 — no decision: request + park (source L218-255).
    const pending = existing.find((a) => a.state === 'pending')
    const action = specTask.approval?.action ?? `approve_task_${taskId}`
    const prompt = specTask.approval?.prompt ?? null

    let approvalId = pending?.approval_id
    if (approvalId === undefined) {
      approvalId = randomUUID()
      store.tx(() => {
        store.insertApproval({
          approval_id: approvalId,
          run_id: runId,
          task_id: taskId,
          action,
          // The prompt rides the row (the approvals DDL has no prompt
          // column): dag_approve echoes it back as approval_prompt (§8.5).
          note: prompt,
          state: 'pending',
        })
        store.insertEvent(runId, {
          type: 'approval.requested',
          taskId,
          attemptId: claim.attemptId,
          payload: { approvalId, action, prompt },
          at: clockNow(),
        })
      })
    }

    // Park: attempt failed + task blocked(approval_pending), ONE tx, both
    // events paired (source L239-254 — commit.ok gates the attempt event,
    // cas.ok gates the task event; both inside the same tx).
    store.tx(() => {
      const nowTs = clockNow()
      const taskRow = store.findTasks(runId).find((t) => t.task_id === taskId)
      const commit = store.commitTerminal(claim.attemptId, claim.ownerToken, 'failed', {
        stop_reason: 'internal',
        failure_json: JSON.stringify({
          failureType: 'internal',
          code: 'dag.approval_pending',
          message: 'attempt parked awaiting an approval decision (dag_approve)',
        }),
        result_json: null,
      })
      if (commit.ok) {
        store.insertEvent(runId, {
          type: 'attempt.failed',
          taskId,
          attemptId: claim.attemptId,
          payload: { from: 'running', to: 'failed', reason: 'approval_pending', approvalId },
          at: nowTs,
        })
      }
      if (taskRow !== undefined && taskRow.state === 'running') {
        const cas = store.casTaskState(
          runId, taskId, taskRow.state, taskRow.version, 'blocked',
          { blocked_reason: JSON.stringify({ code: 'approval_pending', approvalId }) },
        )
        if (cas.ok) {
          store.insertEvent(runId, {
            type: 'task.blocked',
            taskId,
            payload: { from: taskRow.state, to: 'blocked', reason: { code: 'approval_pending', approvalId } },
            at: nowTs,
          })
        }
      }
    })

    return 'parked'
  }

  // ==========================================================================
  // oneRound step 5 — dispatchLoop (claimTask L1943-2056 narrowed)
  // ==========================================================================

  /**
   * Claim ONE task in a single transaction: re-read the row (TOCTOU), the
   * hasNonTerminalAttempt invariant check, ordinal = prior max + 1, the
   * ready→queued→running double CAS (two version bumps, BOTH events emitted —
   * the projection walks both hops in one txn per DESIGN §6.2's narrowing
   * note), the attempt row, and the attempt.claimed event.
   *
   * Returns the claimed handle or null (CAS loss / invariant guard).
   */
  function claimTask(runId, spec, taskId) {
    const attemptId = randomUUID()
    const ownerToken = randomUUID()
    let claimed = null

    store.tx(() => {
      const nowTs = clockNow()
      // Re-read the task INSIDE the transaction (H1 fix, source L1979-1986):
      // the hasNonTerminalAttempt check and the CAS must live in the same
      // serialized region, or two concurrent ticks could both insert attempts.
      const row = store.findTasks(runId).find((t) => t.task_id === taskId)
      if (!row) return
      if (row.state !== 'ready') return
      if (store.hasNonTerminalAttempt(runId, taskId)) return

      const specTask = spec.tasks.find((s) => s.id === taskId)
      const backend = specTask?.backend ?? 'spawn'

      // ordinal = prior max + 1, computed inside the txn (source L1988-1990).
      const prior = store.findAttempts(runId, taskId)
      const ordinal = prior.reduce((m, a) => Math.max(m, a.ordinal), 0) + 1

      // ready → queued (CAS) — source L2005-2016.
      const q = store.casTaskState(runId, taskId, 'ready', row.version, 'queued')
      if (!q.ok) return
      store.insertEvent(runId, {
        type: 'task.queued',
        taskId,
        payload: { from: 'ready', to: 'queued', reason: 'queued' },
        at: nowTs,
      })

      // queued → running (CAS) — source L2018-2030.
      const r = store.casTaskState(runId, taskId, 'queued', q.row.version, 'running')
      if (!r.ok) return
      store.insertEvent(runId, {
        type: 'task.running',
        taskId,
        payload: { from: 'queued', to: 'running', reason: 'running' },
        at: nowTs,
      })

      // Persist the Attempt (source L2032-2033); owner_token = the engine's
      // in-memory handle id (dag-store's sanctioned owner column).
      store.insertAttempt({
        attempt_id: attemptId,
        run_id: runId,
        task_id: taskId,
        ordinal,
        state: 'running',
        backend,
        child_session: null,
        owner_token: ownerToken,
        started_at: nowTs,
        updated_at: nowTs,
      })
      // attempt.claimed event alongside the insert (invariant #6; source
      // L2034-2043). The source's claimed→starting→running intermediate
      // states are narrowed away (DESIGN §6.2): the attempt row is born
      // running with this single event.
      store.insertEvent(runId, {
        type: 'attempt.claimed',
        taskId,
        attemptId,
        payload: { from: '', to: 'claimed', ordinal, backend },
        at: nowTs,
      })

      claimed = { attemptId, ownerToken, ordinal, backend, taskVersion: r.row.version }
    })

    return claimed
  }

  /**
   * Walk the drained queue entries: slot → sessionKey → claim → dispatch.
   * Slot-full breaks the loop (surplus stays ready for the next round — the
   * source's surplus semantics); a session-key conflict releases the slot and
   * leaves the task ready; a dispatch failure goes straight to
   * commitTerminalAndRelease (the attempt never took off).
   *
   * Kind routing: agent tasks flow to the subagent executor; approval (T12)
   * and merge (T17) route to their non-subagent executors BEFORE any
   * executor.dispatch — a prompt-less task must never reach a real
   * subagent. Any kind the validator admits without an executor keeps the
   * M5 fail-closed terminal (`dag.kind_not_implemented`).
   *
   * M2 throw guard: a THROWN dispatch error (e.g. the executor's
   * no-live-agent TypeError) used to reject the whole tick with the task
   * wedged running and the slot/sessionKey leaked. Now the throw is caught
   * and rolled forward exactly like a returned dispatch failure — internal
   * `dag.dispatch_threw`, resources released, loop continues. The merge
   * branch carries the same guard (`dag.merge_threw`).
   */
  async function dispatchLoop(runId, spec, run, queue, versionByTask, execAgent) {
    let dispatched = 0
    let terminal = 0
    const maxRunning = spec.limits?.maxRunningAgents ?? defaultMaxRunning

    for (const entry of queue.drain()) {
      const enqueuedVersion = versionByTask.get(entry.taskId)
      if (enqueuedVersion === undefined) continue

      const slot = admission.tryAcquireSlot(maxRunning, runId)
      if (slot === null) break // slots full — surplus stays ready (source semantics)

      const specTask = spec.tasks.find((t) => t.id === entry.taskId)
      const sessionKey = specTask?.concurrencyKey
      if (sessionKey !== undefined) {
        // Acquire BEFORE claiming (task brief decision): a conflicting key
        // must not consume a claim; release the slot and leave the task ready.
        const claimedKey = admission.tryAcquireSessionKey(sessionKey, 'pending')
        if (!claimedKey) {
          admission.releaseSlot(slot)
          continue
        }
        admission.releaseSessionKey(sessionKey, 'pending')
      }

      const claim = claimTask(runId, spec, entry.taskId)
      if (!claim) {
        admission.releaseSlot(slot)
        continue
      }

      // Bind the session key to the real attempt id now that the claim won.
      if (sessionKey !== undefined && !admission.tryAcquireSessionKey(sessionKey, claim.attemptId)) {
        // Extremely unlikely (nothing else runs between the probe and here in
        // a single-threaded process) — but fail closed: no dispatch without
        // the mutex. The claim is rolled forward as a terminal failure below.
        const failure = {
          failureType: 'transient',
          code: 'dag.session_key_conflict',
          message: `concurrency key ${JSON.stringify(sessionKey)} was acquired by another attempt`,
        }
        const retryDecision = shouldRetry(spec, runId, entry.taskId, claim, failure)
        commitTerminalAndRelease({
          store, admission, spec, clock: { now: clockNow() },
          runId, attempt: { taskId: entry.taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
          slot, sessionKey: undefined, ownerToken: claim.ownerToken,
          failure, stopReason: 'error', retryDecision,
        })
        attemptMeta.delete(claim.attemptId)
        terminal++
        dispatched++
        continue
      }
      if (sessionKey !== undefined) {
        attemptMeta.set(claim.attemptId, { runId, taskId: entry.taskId, ordinal: claim.ordinal, ownerToken: claim.ownerToken, slot, sessionKey })
      } else {
        attemptMeta.set(claim.attemptId, { runId, taskId: entry.taskId, ordinal: claim.ordinal, ownerToken: claim.ownerToken, slot })
      }

      // ---- kind routing (T12 approval / T17 merge) -------------------------
      // approval (T12): the runApprovalTask three-branch semantics (source
      // task-executors.ts L191-256, narrowed): already-approved → straight
      // success; already-rejected → permanent dag.approval_rejected; no
      // decision yet → insert the pending approval + PARK (single tx:
      // attempt failed(reason approval_pending) + task CAS blocked
      // {code:'approval_pending', approvalId} + paired events — source
      // L239-254 shape), slot/sessionKey released OUTSIDE the tx. Never any
      // subagent: approval IS the work.
      // merge (T17): runMergeTask replaces the M5 fail-closed gate — the
      // worktrees-queue executor (lib/executors/merge.js): enqueue every
      // worktree-declaring succeeded upstream (origin 'dag',
      // correlationId = attemptId) → drain → DrainOutcome five-state map
      // (succeeded/no_changes → success with integratedCommit output;
      // conflicted → the approval-shaped park blocked(merge_conflicted);
      // failed → transient retry path; queued → light-budget retry_wait
      // re-poll). Never any subagent: the merge queue IS the work.
      if (specTask !== undefined && specTask.kind === 'approval') {
        const outcome = parkOrResolveApproval(runId, spec, entry.taskId, specTask, claim, slot, sessionKey)
        attemptMeta.delete(claim.attemptId)
        dispatched++
        if (outcome === 'parked') {
          // Resources released OUTSIDE the parking tx (source L255:
          // admission.release after the tx — invariant #3).
          if (slot !== undefined) admission.releaseSlot(slot)
          if (sessionKey !== undefined) admission.releaseSessionKey(sessionKey, claim.attemptId)
        } else {
          terminal++
        }
        continue
      }
      if (specTask !== undefined && specTask.kind === 'merge') {
        let outcome
        try {
          outcome = await runMergeTask({
            store, spec, runId, taskId: entry.taskId, specTask,
            attempt: { taskId: entry.taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
            ownerToken: claim.ownerToken, slot, sessionKey,
            commitTerminalAndRelease, admission, executor,
            decideRetry: (failure) => shouldRetry(spec, runId, entry.taskId, claim, failure),
            // M-B (M3 review): the merge attempt terminal-commits INSIDE
            // dispatchLoop (never through harvestSettled), so the verify
            // gate rides with the executor — a merge task's verify
            // declaration is evaluated on its success path, not silently
            // skipped (red line 1).
            evaluateVerifyGate,
            now: clockNow, logger: log,
          })
        } catch (error) {
          // The M2 throw guard, merge-shaped: a THROWN executor (a
          // misbehaving worktrees queue face) rolls the attempt forward
          // instead of wedging the task or leaking the slot/sessionKey.
          log.error?.(`engine: merge executor threw for attempt ${claim.attemptId} (${entry.taskId}): ${String(error?.message ?? error)}`)
          commitTerminalAndRelease({
            store, admission, spec, clock: { now: clockNow() },
            runId, attempt: { taskId: entry.taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
            slot, sessionKey, ownerToken: claim.ownerToken,
            failure: {
              failureType: 'internal',
              code: 'dag.merge_threw',
              message: `merge executor threw after the claim committed: ${String(error?.message ?? error)}`,
            },
            stopReason: 'error', retryDecision: { retry: false, backoffMs: 0 },
          })
          attemptMeta.delete(claim.attemptId)
          dispatched++
          terminal++
          continue
        }
        attemptMeta.delete(claim.attemptId)
        dispatched++
        if (outcome === 'parked') {
          // Park: resources released OUTSIDE the tx (the approval-park
          // shape, invariant #3 — merge.js's parkMergeConflict never does).
          if (slot !== undefined) admission.releaseSlot(slot)
          if (sessionKey !== undefined) admission.releaseSessionKey(sessionKey, claim.attemptId)
        } else {
          terminal++
        }
        continue
      }
      if (specTask !== undefined && specTask.kind !== undefined && specTask.kind !== 'agent') {
        // Unreachable post-T17 (approval and merge both route above); kept
        // fail-closed for any FUTURE kind the validator admits before its
        // executor lands (the M5 gate's durable shape).
        const failure = {
          failureType: 'permanent',
          code: 'dag.kind_not_implemented',
          message: `task kind "${specTask.kind}" is not implemented (agent/approval/merge executors exist) — dispatch refused`,
        }
        const retryDecision = shouldRetry(spec, runId, entry.taskId, claim, failure)
        commitTerminalAndRelease({
          store, admission, spec, clock: { now: clockNow() },
          runId, attempt: { taskId: entry.taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
          slot, sessionKey, ownerToken: claim.ownerToken,
          failure, stopReason: 'error', retryDecision,
        })
        attemptMeta.delete(claim.attemptId)
        dispatched++
        terminal++
        continue
      }

      // ---- dispatch OUTSIDE the transaction (invariant #3) ----------------
      const inputs = []
      for (const ref of specTask?.inputs ?? []) {
        const match = /^task:\/\/([^/]+)\/([^/]+)$/.exec(ref)
        if (!match) continue
        const row = store.findOutput(runId, match[1], match[2])
        // Executor's prompt builder prefixes 'task://' itself (§7.4 header
        // format) — pass the bare producer/name ref.
        inputs.push({ ref: `${match[1]}/${match[2]}`, value: row === null ? null : JSON.parse(row.value_json) })
      }

      let result
      try {
        result = await executor.dispatch(
          specTask,
          { attemptId: claim.attemptId, ordinal: claim.ordinal, inputs },
          // baseCwd feeds the red-line-9 cwd gate (§4.6): the run row's
          // base_cwd is the containment root for every task.cwd of the run.
          // projectRoot (T16) feeds the worktree branch: a worktree-declaring
          // task resolves its repoRoot and cwd-gate base from the spec's
          // project.root, falling back to the run's base_cwd (DESIGN §4.6).
          {
            runName: run.name ?? runId,
            runId,
            baseCwd: run.base_cwd,
            projectRoot: spec.project?.root ?? run.base_cwd,
            // M3 review M-A — the worktree reuse OWNERSHIP gate: this
            // task's prior attempt ids (its worktree correlationId
            // history). An active worktree record is reusable only when
            // its correlationId is one of these (§11.3 scopes reuse to
            // the same task's re-dispatch); without the history the
            // executor conservatively creates fresh.
            taskAttemptIds: store.findAttempts(runId, entry.taskId).map((a) => a.attempt_id),
            execAgent,
            inputs,
          },
        )
      } catch (error) {
        // M2: the dispatch THREW (programming/infra error after the claim
        // committed) — roll the attempt forward as a terminal internal
        // failure instead of rejecting the tick and wedging the task.
        log.error?.(`engine: dispatch threw for attempt ${claim.attemptId} (${entry.taskId}): ${String(error?.message ?? error)}`)
        result = {
          ok: false,
          failure: {
            failureType: 'internal',
            code: 'dag.dispatch_threw',
            message: `executor.dispatch threw after the claim committed: ${String(error?.message ?? error)}`,
          },
        }
      }

      if (result.ok) {
        // Projection change + event in ONE tx (the child session is the
        // recovery-time locator — DESIGN §6.2 attempts.child_session).
        // T17: a worktree-declaring task's attempt.dispatched payload
        // additionally carries payload.worktreeId (the worktree record id
        // from the executor's create/reuse) — the merge executor reads it
        // from the upstream's LAST succeeded attempt's dispatched event
        // (event-sourced merge-source resolution, zero new tables).
        store.tx(() => {
          store.updateAttemptChildSession(claim.attemptId, result.childSession)
          store.insertEvent(runId, {
            type: 'attempt.dispatched',
            taskId: entry.taskId,
            attemptId: claim.attemptId,
            payload: {
              from: 'running',
              to: 'running',
              childSession: result.childSession,
              backend: claim.backend,
              ...(result.worktreeId !== undefined ? { worktreeId: result.worktreeId } : {}),
            },
            at: clockNow(),
          })
        })
        dispatched++
      } else {
        // The attempt never took off — classify straight through the terminal
        // commit (retry policy applies; resources released outside the tx).
        const retryDecision = shouldRetry(spec, runId, entry.taskId, claim, result.failure)
        commitTerminalAndRelease({
          store, admission, spec, clock: { now: clockNow() },
          runId, attempt: { taskId: entry.taskId, attemptId: claim.attemptId, ordinal: claim.ordinal },
          slot, sessionKey, ownerToken: claim.ownerToken,
          failure: result.failure, stopReason: 'error', retryDecision,
        })
        attemptMeta.delete(claim.attemptId)
        dispatched++
        terminal++
      }
    }

    return { dispatched, terminal }
  }

  // ==========================================================================
  // shouldRetry (scheduler-loop.ts L2295-2329 直搬)
  // ==========================================================================

  /**
   * Decide whether a failed attempt should retry, and the backoff delay.
   * failure null → no; no spec retry block → no; maxAttempts includes the
   * first attempt (executionAttemptNumber = prior retry_scheduled events +
   * 1); the retryOn filter maps through failureTypeToPolicyKey; exponential
   * backoff = min(base * 2^(n-1), maxBackoff) + jitter(ratio * exp * (rand*2-1)).
   *
   * Budget accounting (M1 review M4): a crash-recovery auto-retry's
   * attempt.retry_scheduled event carries payload.recovery === true (§12.1
   * — nothing ever ran, so the allowance is free); T17's merge queued_ahead
   * re-poll stamps payload.manual === true (a busy queue is not an
   * execution failure). Neither is counted here. Only real execution
   * retries consume the retryOn budget (manual dag_control retry_task
   * stays outside by event type — T11).
   */
  function shouldRetry(spec, runId, taskId, attempt, failure) {
    if (failure === null || failure === undefined) return { retry: false, backoffMs: 0 }
    const specTask = spec.tasks.find((t) => t.id === taskId)
    const retry = specTask?.retry
    if (!retry) return { retry: false, backoffMs: 0 }

    const maxAttempts = retry.maxAttempts ?? 1
    const priorExecutionRetries = store
      .findEvents(runId, {})
      .filter((event) => event.task_id === taskId && event.type === 'attempt.retry_scheduled')
      .filter((event) => {
        // recovery:true / manual:true events are budget-FREE retries: §12.1
        // machine retries of attempts that never ran, and T17's merge
        // queued_ahead re-polls (a busy queue is not an execution failure
        // the retryOn policy should bill).
        try {
          const payload = JSON.parse(event.payload_json)
          return payload.recovery !== true && payload.manual !== true
        } catch {
          return true
        }
      })
      .length
    const executionAttemptNumber = priorExecutionRetries + 1
    if (executionAttemptNumber >= maxAttempts) return { retry: false, backoffMs: 0 }

    // retryOn filter: only retry failure types the policy names.
    const retryOn = retry.retryOn ?? []
    const failureKey = failureTypeToPolicyKey(failure.failureType)
    if (retryOn.length > 0 && !retryOn.includes(failureKey)) {
      return { retry: false, backoffMs: 0 }
    }

    // Exponential backoff with jitter (injected random for determinism).
    const base = retry.backoffMs ?? 1000
    const maxBackoff = retry.maxBackoffMs ?? 60_000
    const jitterRatio = retry.jitterRatio ?? 0.25
    const exp = Math.min(base * 2 ** (executionAttemptNumber - 1), maxBackoff)
    const jitter = Math.round(exp * jitterRatio * (randomFn() * 2 - 1))
    const backoffMs = Math.max(0, exp + jitter)
    return { retry: true, backoffMs }
  }

  // ==========================================================================
  // oneRound step 6 — harvestSettled (D8; invariant #1)
  // ==========================================================================

  /**
   * Harvest every SETTLED in-flight attempt of this run, one at a time —
   * INVARIANT #1: every attempt commits its terminal in its OWN transaction;
   * there is never a batched sibling commit (the loop is sequential on
   * purpose; the source's Promise.allSettled was only a dispatch barrier).
   *
   * T18 verify gate: between harvest and the terminal commit, a task whose
   * spec declares `verify: {expectOutput, expectStatus}` must pass
   * evaluateVerifyGate (this attempt's structured output must carry
   * `status === expectStatus`) or the success collapses into a synthetic
   * permanent `dag.verify_gate_failed` (commitTerminalAndRelease failure
   * path — "process exit != Task success", DESIGN §7.3).
   */
  async function harvestSettled(runId, spec) {
    let terminal = 0
    for (const attemptId of executor.inFlightIds()) {
      const info = executor.inFlightInfo(attemptId)
      if (info === undefined || info.runId !== runId) continue
      const reflected = executor.reflectedOf(attemptId)
      if (reflected === undefined) continue
      if (promiseSettledSync(reflected) !== 'fulfilled') continue

      const outcome = await executor.harvest(attemptId)
      const meta = attemptMeta.get(attemptId)
      attemptMeta.delete(attemptId)
      const failure = outcome.failure ?? null
      const specTask = spec.tasks.find((s) => s.id === info.taskId)
      // T18 — the verify completion gate (verify-gate.ts L79-115 direct port;
      // terminal-commit.ts L100's placement: BEFORE shouldRetry, on the
      // SUCCESS path only). Receipt source swap (DESIGN §7.3): the outputs
      // table row this attempt is about to land — when the task declares
      // `verify`, the pending structured output IS the receipt, so the gate
      // reads it from the harvest outcome (terminal-commit upserts it in the
      // SAME tx only when the gate passes). effectiveFailure !== null → the
      // synthetic permanent dag.verify_gate_failed flows through the SAME
      // commitTerminalAndRelease retry/fail machinery below (no new terminal
      // state invented — 源哲学). A task WITHOUT a verify declaration is
      // untouched (evidence 'none_declared' only feeds the event stamp).
      const verifyGate = evaluateVerifyGate({
        specTask,
        runId,
        taskId: info.taskId,
        attemptId,
        failure,
        outputsReader: (rid, tid, name) => {
          // Only THIS attempt's pending structured output can be a receipt —
          // no store read: the outputs row does not exist yet at gate time.
          const declared = specTask?.verify
          if (declared === undefined || tid !== info.taskId || name !== declared.expectOutput) {
            return null
          }
          return outcome.structured === undefined
            ? null
            : { value_json: JSON.stringify(outcome.structured), produced_by_attempt: attemptId }
        },
      })
      const effectiveFailure = verifyGate.effectiveFailure
      // Control-plane abort (dag_control stop — the ONLY harvest-visible
      // producer of failureType 'aborted'; host teardown drops the entry
      // instead of leaving it here): cancelled semantics, never a retry.
      // forcedTaskTarget walks terminal-commit's cancelled branch —
      // attempt cancelled + task cancelled in the one-txn shape (DESIGN
      // §4.5 'aborted' row, §8.4 stop).
      const controlCancelled = failure !== null && failure.failureType === 'aborted'
      const retryDecision = controlCancelled
        ? { retry: false, backoffMs: 0 }
        : shouldRetry(spec, runId, info.taskId, { attemptId, runId, ordinal: meta?.ordinal ?? 1 }, effectiveFailure)
      commitTerminalAndRelease({
        store, admission, spec, clock: { now: clockNow() },
        runId,
        attempt: { taskId: info.taskId, attemptId, ordinal: meta?.ordinal ?? 1 },
        slot: meta?.slot, sessionKey: meta?.sessionKey, ownerToken: meta?.ownerToken ?? attemptId,
        failure: effectiveFailure, structured: outcome.structured, outputText: outcome.outputText,
        stopReason: outcome.stopReason, retryDecision,
        // attempt.succeeded payload stamp (terminal-commit.ts L200-207):
        // receipt present → verifyStatus 'pass' + evidence; none_declared →
        // evidence 'none_declared'.
        verifyStamp: verifyGate.receipt !== null
          ? { verifyStatus: 'pass', evidence: null }
          : { verifyStatus: null, evidence: verifyGate.evidence },
        ...(controlCancelled ? { forcedTaskTarget: 'cancelled' } : {}),
      })
      terminal++
    }
    return terminal
  }

  // ==========================================================================
  // oneRound step 7 — propagateDownstream (readiness.ts L165-203 直搬)
  // ==========================================================================

  function propagateDownstream(runId, spec) {
    const tasks = store.findTasks(runId)
    let blockedCount = 0

    for (const t of tasks) {
      if (t.state !== 'failed') continue
      const specTask = spec.tasks.find((s) => s.id === t.task_id)
      const policy = specTask?.failurePolicy ?? 'block_downstream'
      if (policy !== 'block_downstream') continue

      const downstreams = []
      for (const s of spec.tasks) {
        for (const dep of s.dependsOn ?? []) {
          if (dep.taskId === t.task_id) downstreams.push({ taskId: s.id, condition: dep.condition })
        }
      }
      if (downstreams.length === 0) continue

      // One transaction PER failed task (source shape: the tx opens inside
      // the failed-task loop).
      store.tx(() => {
        const nowTs = clockNow()
        const byId = new Map(store.findTasks(runId).map((row) => [row.task_id, row]))
        for (const edge of downstreams) {
          if (edge.condition !== 'succeeded') continue
          const ds = byId.get(edge.taskId)
          if (!ds) continue
          if (ds.state !== 'pending' && ds.state !== 'retry_wait') continue
          const reason = {
            code: READY_BLOCKED_CODES.upstreamFailed,
            details: { taskId: edge.taskId, upstreamTaskId: t.task_id, upstreamState: 'failed' },
          }
          const cas = store.casTaskState(
            runId, edge.taskId, ds.state, ds.version, 'blocked',
            { blocked_reason: JSON.stringify(reason) },
          )
          if (cas.ok) {
            blockedCount++
            store.insertEvent(runId, {
              type: 'task.blocked',
              taskId: edge.taskId,
              payload: { from: ds.state, to: 'blocked', reason },
              at: nowTs,
            })
          }
        }
      })
    }

    return blockedCount
  }

  // ==========================================================================
  // oneRound step 8 — finalizeRunIfDone (scheduler-driver.ts L401-489 直搬)
  // ==========================================================================

  /**
   * Soft blocks can clear WITHOUT a new Task claim (isSoftBlocked, source
   * L529-549 narrowed for this plugin's static-DAG reality): approval
   * parks (T12 — a human dag_approve clears them), merge-conflict parks
   * (T17 — a human worktree_queue resolve clears them), and a missing
   * declared output that a future upstream attempt may produce.
   *
   * `upstream_blocked` is NOT soft — it is HARD, a dead end (M1 review
   * B2): the source kept it soft because its promoteReady re-evaluates
   * blocked tasks each tick and an upstream state flip can promote them;
   * here the flip cannot happen — once an upstream settles terminal
   * (failed), a blocked(upstream_blocked) successor re-evaluates to
   * blocked(upstream_failed) (upstreamSatisfies' failed arm), never to
   * ready. Counting it as live work would leave a dead-end run `running`
   * forever while §8.3 tells the model to "tick again later" — an
   * instruction that can never make progress. finalizeRunIfDone therefore
   * derives Run failure on such graphs (source L420-444's onlyDeadBlocked
   * intent preserved).
   *
   * The pre-parse fallback (reasons stored as raw strings) mirrors the
   * codes above.
   */
  function isSoftBlocked(blockedReason) {
    if (blockedReason === undefined || blockedReason === null) return false
    const code = parseBlockedReasonCode(blockedReason)
    if (code !== undefined) {
      return (
        code === 'approval_pending'
        || code === 'merge_conflicted'
        || code === 'dag.output_missing'
      )
    }
    return (
      blockedReason.includes('approval_pending')
      || blockedReason.includes('merge_conflicted')
      || blockedReason.includes('dag.output_missing')
    )
  }

  function finalizeRunIfDone(runId) {
    const run = store.findRun(runId)
    if (!run) return false
    if (RUN_TERMINAL_STATES.has(run.state)) return true
    // Respect controlIntent (source L405-412): a pause intent owns its
    // terminal via drainToPaused — do not collapse a drained Run to
    // succeeded while it should stay resumable. stop is NOT blocked (its
    // cancelled terminal is correctly derived below).
    if (run.control_intent !== null && FINALIZE_BLOCKING_INTENTS[run.control_intent] === true) {
      return false
    }

    const tasks = store.findTasks(runId)
    if (tasks.length === 0) return false

    const allTerminal = tasks.every((t) => TASK_TERMINAL_STATES.has(t.state))
    // Dead-end: remaining tasks are only permanently blocked with no live
    // work — finalize as failed so the drive does not hang forever on
    // block_downstream graphs (source L420-444).
    const hasLiveWork = tasks.some(
      (t) =>
        t.state === 'pending'
        || t.state === 'ready'
        || t.state === 'queued'
        || t.state === 'running'
        || t.state === 'retry_wait'
        || (t.state === 'blocked' && isSoftBlocked(t.blocked_reason)),
    )
    const onlyDeadBlocked =
      !allTerminal
      && !hasLiveWork
      && tasks.every(
        (t) => TASK_TERMINAL_STATES.has(t.state) || (t.state === 'blocked' && !isSoftBlocked(t.blocked_reason)),
      )
    if (!allTerminal && !onlyDeadBlocked) return false

    // Derive the Run terminal from aggregate Task outcomes (H8 fix): a
    // CANCELLING run's terminal is `cancelled` outright (§8.4 stop — the
    // operator's explicit intent outranks the aggregate; partial success
    // under a stop must not read as `succeeded`). Otherwise any failed →
    // failed; all cancelled → cancelled; otherwise succeeded. Dead-end
    // blocked graphs count as failed.
    const cancelling = run.state === 'cancelling'
    const anyFailed = !cancelling && (tasks.some((t) => t.state === 'failed') || onlyDeadBlocked)
    const allCancelled = !cancelling && tasks.every((t) => t.state === 'cancelled')
    const target = cancelling ? 'cancelled' : anyFailed ? 'failed' : allCancelled ? 'cancelled' : 'succeeded'

    let landed = false
    store.tx(() => {
      const current = store.findRun(runId)
      if (!current || RUN_TERMINAL_STATES.has(current.state)) return
      const cas = store.casRunState(runId, current.state, current.version, target)
      if (!cas.ok) return
      store.insertEvent(runId, {
        type: `run.${target}`,
        payload: { from: current.state, to: target, reason: target },
        at: clockNow(),
      })
      landed = true
    })
    return landed
  }

  // ==========================================================================
  // oneRound step 9 — drainToPaused (scheduler-loop.ts L758-810 直搬)
  // ==========================================================================

  function drainToPaused(runId) {
    store.tx(() => {
      // Re-read INSIDE the txn (avoid a stale read losing the CAS to a
      // concurrent writer — source L760-762).
      const run = store.findRun(runId)
      if (!run) return
      // Only a pausing Run whose intent is still exactly `pause` drains.
      if (run.state !== 'pausing' || run.control_intent !== 'pause') return

      // Wait for in-flight attempts to terminate: if ANY task still has a
      // non-terminal attempt, leave the Run in `pausing` this tick.
      // Admission is already closed, so no NEW attempt can appear between
      // this read and the CAS — the non-terminal set can only shrink.
      const tasks = store.findTasks(runId)
      for (const t of tasks) {
        if (store.hasNonTerminalAttempt(runId, t.task_id)) return
      }

      const nowTs = clockNow()
      // CAS pausing→paused, KEEPING controlIntent=pause so admission stays
      // closed until an explicit resume clears it.
      const cas = store.casRunState(runId, 'pausing', run.version, 'paused')
      if (!cas.ok) return // lost the CAS race — retry on the next tick
      store.insertEvent(runId, {
        type: 'run.paused',
        payload: { from: 'pausing', to: 'paused', reason: 'pause-drain' },
        at: nowTs,
      })
    })
  }

  // ==========================================================================
  // oneRound — the ten-step pass
  // ==========================================================================

  /**
   * One single-round reconcile. Steps 1-10 per DESIGN §5.1. Every projection
   * change lands in its own transaction with its events (invariants #3/#6);
   * dispatch/harvest I/O happens between transactions (invariant #3).
   *
   * @param {string} runId
   * @param {{execAgent?: object}} [options]
   * @returns {Promise<{promoted: number, dispatched: number, terminal: number, propagated: number, finalized: boolean}>}
   */
  async function oneRound(runId, options = {}) {
    const run = requireRun(runId)

    // M3 idempotence: a TERMINAL run is done — re-ticking must be a no-op
    // that returns the zero-progress summary, not another pass that
    // re-writes blocked→blocked rows (duplicate task.blocked events,
    // version bumps, phantom `promoted` counts that defeat the quiescence
    // detection). Same-state rewrites are the crash/replay safety net
    // (DESIGN §6.2), never a per-tick routine write.
    if (RUN_TERMINAL_STATES.has(run.state)) {
      return { promoted: 0, dispatched: 0, terminal: 0, propagated: 0, finalized: true }
    }

    // R2 catch-up sweep (BEFORE the pass's own work): a control('stop')
    // that fired mid-dispatch leaves attempts the one-shot control-side
    // sweep never saw — re-affirm the abort for anything in flight under
    // this run's stop intent. Idempotent: already-aborted entries no-op.
    sweepStopAborts(run)

    const spec = specOfRun(run)

    // Step 1 — reconcileApprovals (T12): decided approvals promote their
    // parked tasks; the count feeds this round's progress accounting (a
    // promotion is progress — the multi-round loop re-promotes downstream
    // within the same tick).
    const reconciled = reconcileApprovals(runId)
    const approvalPromoted = reconciled.promoted

    // Step 2 — promoteReady.
    let promoted = promoteReady(runId, spec) + approvalPromoted

    // Steps 3-5 — admission gate → buildQueue → dispatchLoop. A closed gate
    // skips NEW dispatch only; harvesting/propagation/finalize continue.
    let dispatched = 0
    let terminal = 0
    if (!isAdmissionClosed(runId) && !RUN_TERMINAL_STATES.has(run.state)) {
      const { queue, versionByTask } = buildQueue(runId, spec)
      if (!queue.isEmpty) {
        const fresh = requireRun(runId)
        const outcome = await dispatchLoop(runId, spec, fresh, queue, versionByTask, options.execAgent)
        dispatched += outcome.dispatched
        terminal += outcome.terminal
      }
    }

    // Step 6 — harvestSettled (every settled in-flight attempt, one tx each).
    terminal += await harvestSettled(runId, spec)

    // Step 6b — re-run promoteReady after the harvest: newly-satisfied
    // downstream tasks become ready WITHIN the same round (task-weaver's
    // per-drive sequential promote→claim order collapsed into one pass; the
    // tick's multi-round loop otherwise needs one extra round per DAG level).
    const promotedAfterHarvest = promoteReady(runId, spec)
    promoted += promotedAfterHarvest
    if (promotedAfterHarvest > 0 && !isAdmissionClosed(runId)) {
      const { queue, versionByTask } = buildQueue(runId, spec)
      if (!queue.isEmpty) {
        const fresh = requireRun(runId)
        if (!RUN_TERMINAL_STATES.has(fresh.state)) {
          const outcome = await dispatchLoop(runId, spec, fresh, queue, versionByTask, options.execAgent)
          dispatched += outcome.dispatched
          terminal += outcome.terminal
          // A second harvest wave: attempts dispatched from already-resolved
          // fakes may settle before the round ends.
          terminal += await harvestSettled(runId, spec)
        }
      }
    }

    // Step 7 — propagateDownstream (re-read tasks: sees this round's commits).
    const propagated = propagateDownstream(runId, spec)

    // Step 8 — finalizeRunIfDone.
    const finalized = finalizeRunIfDone(runId)

    // Step 9 — drainToPaused.
    drainToPaused(runId)

    // R2 catch-up sweep (END of the pass): an attempt dispatched THIS round
    // (dispatchLoop's `await executor.dispatch` suspension window) can have
    // registered after the pass began — a stop that landed while it was
    // suspended never reached it. Sweep again so the intent catches up with
    // the late registration; the abort settles 'aborted', and the NEXT
    // pass's harvestSettled + finalizeRunIfDone walk it to cancelled.
    sweepStopAborts(store.findRun(runId))

    // Step 10 — summary.
    return { promoted, dispatched, terminal, propagated, finalized }
  }

  // ==========================================================================
  // tick — the bounded multi-round loop (§5.2, D10)
  // ==========================================================================

  /** In-flight attempt summaries for this run (§8.3 in_flight rows). */
  function inFlightForRun(runId) {
    const rows = []
    for (const attemptId of executor.inFlightIds()) {
      const info = executor.inFlightInfo(attemptId)
      if (info === undefined || info.runId !== runId) continue
      const meta = attemptMeta.get(attemptId)
      rows.push({
        taskId: info.taskId,
        attemptId,
        ordinal: meta?.ordinal ?? 1,
        startedAt: info.startedAt,
        elapsedMs: Math.max(0, clockNow() - info.startedAt),
      })
    }
    return rows
  }

  /**
   * Idempotent stop-intent abort sweep (M2 review R2). control('stop')
   * sweeps the in-flight map ONCE right after its tx — but an attempt whose
   * claim tx already committed while executor.dispatch was still suspended
   * at `await ctxSubagents.start` is in NEITHER set at that moment (the tx
   * skips running tasks; the map has no entry yet). When that dispatch
   * resumes it registers a LIVE attempt under a run already marked
   * cancelling, and nothing would ever abort it — the run sits cancelling
   * until timeoutMs and a retryable timeout can even re-arm the task
   * (retry_wait = live work), wedging the stop forever.
   *
   * This sweep closes the window from the reconcile side: whenever a pass
   * observes control_intent === 'stop' it re-affirms the abort for EVERY
   * in-flight attempt of the run. Re-aborting is a no-op (an AbortSignal
   * fires once; executor.abort is idempotent), so sweeping early, late, or
   * twice is always safe — the abort→harvest→cancelled path (harvestSettled
   * + finalizeRunIfDone) does the rest on the following pass.
   *
   * Called at the START of every oneRound + autoTick pass (catch-up for
   * anything registered since the last look) and at the END of every
   * oneRound (a dispatch that registered mid-round, the R2 window itself).
   *
   * @param {{run_id: string, control_intent: string | null} | null} run
   * @returns {number} in-flight entries swept (0 when no stop intent)
   */
  function sweepStopAborts(run) {
    if (run === null || run === undefined) return 0
    if (run.control_intent !== 'stop') return 0
    let swept = 0
    for (const row of inFlightForRun(run.run_id)) {
      executor.abort(row.attemptId)
      swept++
    }
    return swept
  }

  /**
   * waiting_on classification (§8.3): terminal → nothing; an
   * approval_pending park → approval; any in-flight attempt →
   * in_flight_attempts; a blocked upstream wait → external; else nothing.
   */
  function classifyWaitingOn(runId, inFlight) {
    const run = store.findRun(runId)
    if (run === null) return { waitingOn: 'nothing', hint: 'run not found' }
    if (RUN_TERMINAL_STATES.has(run.state)) {
      return { waitingOn: 'nothing', hint: `run is terminal (${run.state})` }
    }
    if (inFlight.length > 0) {
      return { waitingOn: 'in_flight_attempts', hint: 'call dag_tick again after in-flight work settles' }
    }
    const tasks = store.findTasks(runId)
    const approvalParked = tasks.some(
      (t) => t.state === 'blocked' && parseBlockedReasonCode(t.blocked_reason) === 'approval_pending',
    )
    if (approvalParked) {
      return { waitingOn: 'approval', hint: 'ask the user, then dag_approve' }
    }
    // T17: a merge-conflict park waits on an EXTERNAL human action (the
    // worktrees queue) — 'nothing/quiescent' would misread as done-ish on a
    // run that is still live.
    const mergeParked = tasks.some(
      (t) => t.state === 'blocked' && parseBlockedReasonCode(t.blocked_reason) === 'merge_conflicted',
    )
    if (mergeParked) {
      return { waitingOn: 'external', hint: 'a merge node is parked on a conflict — resolve the worktrees queue (worktree_queue resolve/retry), then dag_control retry_task' }
    }
    const blockedUpstream = tasks.some(
      (t) => t.state === 'blocked' && parseBlockedReasonCode(t.blocked_reason) === 'upstream_blocked',
    )
    if (blockedUpstream) {
      return { waitingOn: 'external', hint: 'blocked on upstream tasks; call dag_tick again later' }
    }
    return { waitingOn: 'nothing', hint: 'quiescent — nothing runnable right now' }
  }

  /**
   * Bounded multi-round reconcile (§5.2). Hard bound on the total blocking:
   * reconcile time + `settleMs` for the WHOLE call (the settle budget is
   * consumed across rounds and never reset — the 16×60s self-spin guard).
   *
   * @param {string} runId
   * @param {{maxRounds?: number, settleMs?: number, execAgent?: object}} [options]
   */
  async function tick(runId, options = {}) {
    const maxRounds = Math.min(
      MAX_ROUNDS_CEIL,
      Math.max(MAX_ROUNDS_FLOOR, Number.isSafeInteger(options.maxRounds) ? options.maxRounds : DEFAULT_MAX_ROUNDS),
    )
    const settleMs = Math.min(
      SETTLE_MS_CEIL,
      Math.max(SETTLE_MS_FLOOR, Number.isSafeInteger(options.settleMs) ? options.settleMs : DEFAULT_SETTLE_MS),
    )

    requireRun(runId)

    let settleBudget = settleMs
    let noSettleStreak = 0
    let rounds = 0
    const totals = { promoted: 0, dispatched: 0, terminal: 0, propagated: 0 }

    for (;;) {
      rounds++
      const progress = await oneRound(runId, { execAgent: options.execAgent })
      totals.promoted += progress.promoted
      totals.dispatched += progress.dispatched
      totals.terminal += progress.terminal
      totals.propagated += progress.propagated

      const runState = store.findRun(runId)?.state ?? 'failed'
      if (RUN_TERMINAL_STATES.has(runState)) break

      const madeProgress = progress.promoted + progress.dispatched + progress.terminal + progress.propagated > 0
      if (madeProgress) {
        noSettleStreak = 0
        if (rounds >= maxRounds) break
        continue
      }

      const pending = inFlightForRun(runId).map((row) => executor.reflectedOf(row.attemptId)).filter((p) => p !== undefined)
      if (pending.length === 0) break // quiescent stop: nothing in flight
      if (settleBudget <= 0 || noSettleStreak >= 2) break // budget / streak guard
      const waited = await boundedRace(pending, Math.min(settleMs, settleBudget))
      settleBudget -= waited.actualMs
      noSettleStreak = waited.anySettled ? 0 : noSettleStreak + 1
      if (rounds >= maxRounds) break
    }

    // tickSummary (§8.3 shape; json-safe — no undefined-valued keys).
    const run = store.findRun(runId)
    const runState = run?.state ?? 'failed'
    const inFlight = inFlightForRun(runId).map((row) => ({
      task_id: row.taskId,
      attempt: row.ordinal,
      started_at: row.startedAt,
      elapsed_ms: row.elapsedMs,
    }))
    const { waitingOn, hint } = classifyWaitingOn(runId, inFlight)
    return {
      kind: 'tick',
      run_id: runId,
      run_state: runState,
      rounds,
      promoted: totals.promoted,
      dispatched: totals.dispatched,
      terminal: totals.terminal,
      propagated: totals.propagated,
      in_flight: inFlight,
      waiting_on: waitingOn,
      next_hint: hint,
    }
  }

  // ==========================================================================
  // autoTick — the no-dispatch reconcile (T14; DESIGN §5.5 / TASKS.md T14)
  // ==========================================================================

  /**
   * The optional Timer pump (config.autoTickMs > 0 → apply() wires a
   * setInterval to this). HONEST BOUNDARY (DESIGN §5.5): a Timer context
   * has NO live exec.agent, so autoTick NEVER dispatches — admission gate /
   * buildQueue / dispatchLoop are all absent. Dispatch happens only inside
   * a tool exec (the pumping-Agent ownership rule, §4.4 / O1); caching an
   * Agent handle across the host lifetime to arm a dispatching timer would
   * violate the dies-with-host model. What a Timer CAN do honestly is the
   * projection-only subset of oneRound, per NON-TERMINAL run:
   *
   *   reconcileApprovals → harvestSettled → promoteReady →
   *   propagateDownstream → finalizeRunIfDone → drainToPaused
   *
   * (harvest needs no parent: the reflected result promise is the terminal
   * carrier, D8; the retry_wait it may arm on failure is also pure
   * projection — the next real tick's promoteReady picks it up.)
   *
   * promoteReady IS projection (CAS + events, no dispatch, no agent — the
   * same class of work as reconcileApprovals), and it is REQUIRED here,
   * not optional: without it, finalizeRunIfDone can DEAD-BLOCK a live
   * graph. Reproduced sequence — a tool-exec tick's step-6b promoteReady
   * can park a downstream task blocked(upstream_blocked) while its
   * upstream sits in an approval park; after dag_approve, a Timer pass
   * that promoted the gate but never re-evaluated the downstream would
   * see "only dead-blocked tasks" (upstream_blocked is HARD —
   * isSoftBlocked false) and finalize the HEALTHY run failed, permanently
   * (terminal runs no-op on tick; retry_task refuses
   * blocked(upstream_blocked)). Running promoteReady keeps every state it
   * touches truthful so finalize only ever fires on genuinely dead graphs;
   * a promoted-ready task simply waits for the next tool-exec tick to
   * dispatch it (the documented cost of the no-dispatch boundary).
   *
   * Fault isolation: ONE run's reconcile throwing is caught, warned, and
   * skipped — it never takes down the other runs in the loop. The
   * whole-call catch lives in the CALLER (apply's interval callback
   * logger.warns), so a throw outside the per-run region (e.g. the
   * non-terminal scan itself) surfaces there instead of rejecting inside
   * a timer context.
   *
   * @returns {Promise<{runs: number, terminal: number, approvals: number}>}
   *   runs = non-terminal runs visited; terminal = attempts harvested to a
   *   terminal state; approvals = decided approvals promoted.
   */
  async function autoTick() {
    let runs = 0
    let terminal = 0
    let approvals = 0
    for (const run of store.findNonTerminalRuns()) {
      runs += 1
      try {
        // R2 catch-up sweep: the no-dispatch pump must also honor a stop
        // intent's abort for anything in flight (a mid-dispatch stop's
        // straggler would otherwise sit until timeoutMs). Idempotent.
        sweepStopAborts(run)
        const spec = specOfRun(run)
        approvals += reconcileApprovals(run.run_id).promoted
        terminal += await harvestSettled(run.run_id, spec)
        promoteReady(run.run_id, spec)
        propagateDownstream(run.run_id, spec)
        finalizeRunIfDone(run.run_id)
        drainToPaused(run.run_id)
      } catch (error) {
        log.warn?.(
          `engine autoTick: run ${run.run_id} reconcile failed (skipped, other runs continue): ${String(error?.message ?? error)}`,
        )
      }
    }
    return { runs, terminal, approvals }
  }

  // ==========================================================================
  // planRun — validate + insert (the inline first tick belongs to dag_plan,
  // T08; this method only creates the run)
  // ==========================================================================

  /**
   * @param {unknown} spec raw WorkflowSpec document
   * @param {{baseCwd: string, parentSession?: string, execAgent?: object, runId?: string, now?: number}} [options]
   * @returns {{runId: string, specHash: string, taskCount: number}}
   */
  function planRun(spec, options = {}) {
    const validated = validateSpec(spec)
    if (!validated.ok) {
      const error = new Error(
        `engine.planRun: spec rejected — ${validated.errors.map((e) => `${e.code}${e.path ? ` (${e.path})` : ''}: ${e.message}`).join('; ')}`,
      )
      error.code = 'dag.schema_invalid'
      error.errors = validated.errors
      throw error
    }
    const value = validated.value
    const hash = specHash(value)
    const nowTs = options.now ?? clockNow()
    const runId = options.runId ?? newRunId(nowTs)

    store.tx(() => {
      store.insertRun({
        run_id: runId,
        name: value.name,
        spec_json: JSON.stringify(value),
        spec_hash: hash,
        state: 'running',
        control_intent: null,
        parent_session: options.parentSession ?? null,
        base_cwd: options.baseCwd,
        created_at: nowTs,
        updated_at: nowTs,
        version: 1,
      })
      store.insertTasks(runId, value.tasks.map((task) => ({ task_id: task.id, state: 'pending', version: 1 })))
      store.insertEvent(runId, {
        type: 'run.created',
        payload: { name: value.name, spec_hash: hash, task_count: value.tasks.length, from: '', to: 'running' },
        at: nowTs,
      })
    })

    knownRunIds.add(runId)
    return { runId, specHash: hash, taskCount: value.tasks.length }
  }

  // ==========================================================================
  // status — projection query (§8.2, json-safe)
  // ==========================================================================

  /** Aggregate task-state counts for one run (§8.2 summary rows). */
  function countsOf(tasks) {
    const counts = { pending: 0, ready: 0, running: 0, succeeded: 0, failed: 0, blocked: 0 }
    for (const t of tasks) {
      if (Object.hasOwn(counts, t.state)) counts[t.state]++
      else if (t.state === 'queued') counts.ready++ // queued is a sub-state of "runnable"
      else if (t.state === 'retry_wait') counts.pending++
      else if (t.state === 'cancelled') counts.failed++
    }
    return counts
  }

  /**
   * All known run ids. The engine tracks runs it created via planRun; runs
   * discovered in the store (crash recovery, T09) also surface. M1 keeps a
   * simple in-memory registry + the store's non-terminal scan for foreign
   * rows.
   */
  const knownRunIds = new Set()
  function allRunIds() {
    // T08: the store's findAllRuns (terminal included) is the complete
    // source when present; the non-terminal scan stays as the fallback so
    // older/fake stores without the projection keep working.
    if (typeof store.findAllRuns === 'function') {
      for (const run of store.findAllRuns()) knownRunIds.add(run.run_id)
      return [...knownRunIds]
    }
    const ids = new Set(knownRunIds)
    for (const run of store.findNonTerminalRuns()) ids.add(run.run_id)
    return [...ids]
  }

  /**
   * @param {string} runId omit for the all-runs summary
   * @param {{detail?: 'summary'|'tasks'|'attempts'|'events', taskId?: string, limit?: number, afterSeq?: number}} [options]
   */
  function status(runId, options = {}) {
    const detail = options.detail ?? 'tasks'
    const limit = options.limit ?? 50

    if (runId === undefined || runId === null || runId === '') {
      // summary rows: §8.2 lists ALL runs; terminal runs are the common case
      // ("dag_status → 终局核对"), so the store's non-terminal helper is not
      // the right source — findAllRuns (terminal included) drives the rows.
      const rows = []
      for (const candidate of allRunIds()) {
        const run = store.findRun(candidate)
        const tasks = store.findTasks(run.run_id)
        rows.push({
          run_id: run.run_id,
          name: run.name,
          state: run.state,
          counts: countsOf(tasks),
          created_at: run.created_at,
          updated_at: run.updated_at,
        })
      }
      rows.sort((a, b) => a.created_at - b.created_at || (a.run_id < b.run_id ? -1 : 1))
      return { kind: 'status', detail: 'summary', runs: rows }
    }

    const run = requireRun(runId)
    const tasks = store.findTasks(runId)
    const base = {
      kind: 'status',
      run_id: runId,
      name: run.name,
      state: run.state,
      counts: countsOf(tasks),
      created_at: run.created_at,
      updated_at: run.updated_at,
    }

    if (detail === 'summary') return base

    const taskRows = tasks.map((t) => {
      const attempts = store.findAttempts(runId, t.task_id)
      const last = attempts[attempts.length - 1]
      const row = {
        id: t.task_id,
        state: t.state,
        attempts: attempts.length,
        ordinal: last?.ordinal ?? 0,
      }
      if (t.blocked_reason !== null && t.blocked_reason !== undefined) row.blocked_reason = JSON.parse(t.blocked_reason)
      if (last?.stop_reason !== null && last?.stop_reason !== undefined) row.last_stop_reason = last.stop_reason
      if (t.retry_not_before !== null && t.retry_not_before !== undefined) row.retry_not_before = t.retry_not_before
      return row
    })
    const withTasks = { ...base, detail: 'tasks', tasks: taskRows }
    if (detail === 'tasks') return withTasks

    if (detail === 'attempts') {
      const filtered = options.taskId !== undefined ? taskRows.filter((row) => row.id === options.taskId) : taskRows
      const attempts = []
      for (const row of filtered) {
        for (const a of store.findAttempts(runId, row.id)) {
          const entry = {
            attempt_id: a.attempt_id,
            task_id: a.task_id,
            ordinal: a.ordinal,
            state: a.state,
            backend: a.backend,
            started_at: a.started_at,
          }
          if (a.child_session !== null && a.child_session !== undefined) entry.child_session = a.child_session
          if (a.stop_reason !== null && a.stop_reason !== undefined) entry.stop_reason = a.stop_reason
          if (a.failure_json !== null && a.failure_json !== undefined) entry.failure = JSON.parse(a.failure_json)
          attempts.push(entry)
        }
      }
      return { ...withTasks, detail: 'attempts', attempts }
    }

    // detail === 'events' — tail window from the chain
    const all = store.findEvents(runId, {})
    const events = (options.afterSeq !== undefined ? all.filter((e) => e.seq > options.afterSeq) : all)
      .filter((e) => options.taskId === undefined || e.task_id === options.taskId)
      .slice(-limit)
      .map((e) => {
        const row = { seq: e.seq, type: e.type, at: e.at }
        if (e.task_id !== null) row.task_id = e.task_id
        if (e.attempt_id !== null) row.attempt_id = e.attempt_id
        row.payload = JSON.parse(e.payload_json)
        return row
      })
    return { ...withTasks, detail: 'events', events }
  }

  // ==========================================================================
  // control — the §8.4 control plane (TASKS.md T11)
  // ==========================================================================

  /**
   * STATE MATRIX (loud `dag.invalid_run_state` / `dag.invalid_task_state`
   * outside the listed source states; a re-invocation that finds the run
   * already in the action's target state is an idempotent re-affirm, not an
   * error — operators double-send control commands):
   *
   *   pause       running → pausing (intent=pause, ONE tx: CAS + run.control
   *               event). drainToPaused (oneRound step 9) closes pausing →
   *               paused once every in-flight attempt has drained — pause
   *               never aborts anything, admission closing is the only gate.
   *               pausing/paused re-invoke idempotently.
   *   resume      paused → running + intent cleared (admission reopens).
   *               Everything else is loud.
   *   stop        running|pausing|paused → cancelling (intent=stop). The tx
   *               ALSO cancels every NON-running task (pending/ready/queued/
   *               retry_wait/blocked — the §6.2 task-state diagram's cancel
   *               edges); running tasks are aborted OUTSIDE the tx (one per
   *               attempt, invariant #3) and land attempt+task cancelled via
   *               the next tick's harvest. A tick is REQUIRED to finish a
   *               stop (harvest + finalizeRunIfDone aggregate cancelling →
   *               cancelled): control returns the intent-layer state only.
   *   retry_task  failed | blocked(upstream_failed) | blocked(
   *               merge_conflicted) → retry_wait with retry_not_before =
   *               now + `task.retry_requested` manual event. NOT billed
   *               against the retryOn budget: shouldRetry counts
   *               attempt.retry_scheduled events only, a different type —
   *               manual retry is an explicit human action (DESIGN §8.4
   *               "不与 retryOn 预算混账"). The merge_conflicted arm (T17)
   *               is the conflicted park's ONLY re-run edge: the human
   *               resolves the worktrees queue first (worktree_queue
   *               resolve/retry), then re-arms the merge node here. The
   *               next tick's promoteReady re-evaluates the task (retry_wait
   *               with an expired deadline is a ready source); if its
   *               upstream is STILL failed the evaluator bounces it back to
   *               blocked(upstream_failed) — retry the upstream, not just
   *               the victim.
   *   cancel_task pending | blocked (any reason, parks included) →
   *               cancelled + `task.cancelled` event. Running tasks are
   *               refused loud — cancelling a running task is the run-level
   *               stop (abort path).
   *
   * Task-level actions additionally validate the RUN state (M2 review R1,
   * validateRunForTaskControl): a failed run stays actionable (retry_task
   * revives it below; cancel_task is honest cleanup), while a cancelling/
   * cancelled/succeeded run refuses BOTH loud `dag.invalid_run_state`
   * BEFORE task validation — reviving a node inside a finalized run (or
   * under a draining stop) would desync the aggregated terminal.
   *
   * stop's abort sweep is idempotent and re-armed per pass: oneRound and
   * autoTick re-abort every in-flight attempt of a control_intent==='stop'
   * run (M2 review R2 — the claimed-but-not-yet-dispatched window).
   *
   * @param {string} runId
   * @param {'pause'|'resume'|'stop'|'retry_task'|'cancel_task'} action
   * @param {{taskId?: string, reason?: string, execAgent?: object}} [options]
   *   execAgent is accepted for signature symmetry with tick/planRun; the
   *   control plane never dispatches, so it has no consumer here.
   * @returns {{kind: 'control', run_id: string, action: string, run_state: string, effected: {task_id: string, from: string, to: string}[]}}
   */
  function control(runId, action, options = {}) {
    const ACTIONS = new Set(['pause', 'resume', 'stop', 'retry_task', 'cancel_task'])
    if (!ACTIONS.has(action)) {
      throw controlError('dag.invalid_action', `engine.control: unknown action ${JSON.stringify(action)}`)
    }
    const taskId = options.taskId
    const reason = options.reason

    // Run-level existence guard (the tx re-reads are the authoritative
    // check; this pre-check gives the crisp not-found error).
    const runLevel = action === 'pause' || action === 'resume' || action === 'stop'
    const preRun = requireRun(runId)
    if (runLevel && RUN_TERMINAL_STATES.has(preRun.state)) {
      throw controlError(
        'dag.invalid_run_state',
        `engine.control(${action}): run ${JSON.stringify(runId)} is terminal (${preRun.state}) — nothing to control`,
      )
    }
    // Task-level actions and the RUN state (M2 review R1 — ENFORCED by
    // validateRunForTaskControl, pre-tx AND inside the tx):
    //   * failed run — actionable by design: retry_task re-arms the task
    //     AND revives the run (failed→running, §8.6 ②'s recovery flow,
    //     inside the tx below); cancel_task is honest cleanup of a leftover
    //     blocked task (the aggregated terminal stays untouched — finalize
    //     never re-runs on a terminal run).
    //   * cancelling | cancelled — refused LOUD. A task that failed BEFORE
    //     the stop keeps state 'failed' (the stop tx skips terminal tasks),
    //     so without this gate retry_task "succeeded" silently and re-armed
    //     the task inside a terminal run where every later tick no-ops —
    //     the manual retry could never dispatch (the R1 defect). Stop is
    //     the operator's explicit terminal; plan a new run instead.
    //   * succeeded — refused loud for a uniform contract: the aggregation
    //     holds no failed/blocked(upstream_failed) task anyway, so the
    //     run-level refusal is the honest error, not a happenstance
    //     task-state code.

    /** @type {{task_id: string, from: string, to: string}[]} */
    const effected = []

    if (action === 'pause' || action === 'resume' || action === 'stop') {
      store.tx(() => {
        const run = store.findRun(runId)
        if (run === null) {
          throw controlError('dag.run_not_found', `engine.control: run ${JSON.stringify(runId)} not found`)
        }
        const nowTs = clockNow()

        if (action === 'pause') {
          if (RUN_TERMINAL_STATES.has(run.state)) {
            throw controlError('dag.invalid_run_state', `engine.control(pause): run is terminal (${run.state})`)
          }
          if (run.state === 'running') {
            // drainToPaused consumes state==='pausing' AND intent==='pause'
            // (oneRound step 9) — control must land BOTH in this one tx.
            const cas = store.casRunState(runId, 'running', run.version, 'pausing', { control_intent: 'pause' })
            if (!cas.ok) {
              throw controlError('dag.invalid_run_state', `engine.control(pause): lost the running→pausing CAS (${cas.reason})`)
            }
            store.insertEvent(runId, {
              type: 'run.control',
              payload: { action: 'pause', reason: reason ?? null, from: 'running', to: 'pausing' },
              at: nowTs,
            })
          } else if (run.state === 'pausing' || run.state === 'paused') {
            // Idempotent re-affirm. The intent write matters for a pausing
            // row whose intent was cleared (it could never drain otherwise);
            // projection change + event stay in the same tx (invariant #6).
            if (run.control_intent !== 'pause') store.setControlIntent(runId, 'pause')
            store.insertEvent(runId, {
              type: 'run.control',
              payload: { action: 'pause', reason: reason ?? null, from: run.state, to: run.state },
              at: nowTs,
            })
          } else {
            throw controlError(
              'dag.invalid_run_state',
              `engine.control(pause): cannot pause a ${run.state} run — stop owns that run's terminal`,
            )
          }
          return
        }

        if (action === 'resume') {
          if (run.state !== 'paused') {
            throw controlError(
              'dag.invalid_run_state',
              `engine.control(resume): only a paused run can resume (current ${run.state})`,
            )
          }
          // One CAS walks both hops of the resume: state paused→running AND
          // the intent cleared — admission reopens atomically.
          const cas = store.casRunState(runId, 'paused', run.version, 'running', { control_intent: null })
          if (!cas.ok) {
            throw controlError('dag.invalid_run_state', `engine.control(resume): lost the paused→running CAS (${cas.reason})`)
          }
          store.insertEvent(runId, {
            type: 'run.control',
            payload: { action: 'resume', reason: reason ?? null, from: 'paused', to: 'running' },
            at: nowTs,
          })
          return
        }

        // action === 'stop'
        if (RUN_TERMINAL_STATES.has(run.state)) {
          throw controlError('dag.invalid_run_state', `engine.control(stop): run is terminal (${run.state})`)
        }
        const from = run.state
        if (run.state === 'cancelling') {
          // Idempotent re-send while a previous stop is still draining.
          if (run.control_intent !== 'stop') store.setControlIntent(runId, 'stop')
        } else {
          const cas = store.casRunState(runId, run.state, run.version, 'cancelling', { control_intent: 'stop' })
          if (!cas.ok) {
            throw controlError('dag.invalid_run_state', `engine.control(stop): lost the ${run.state}→cancelling CAS (${cas.reason})`)
          }
        }
        // Deadlock guard (the non-obvious half of stop): finalizeRunIfDone
        // only lands on all-terminal / dead-blocked task sets, and with
        // admission closed a left-over pending/ready/queued/retry_wait task
        // could NEVER terminate — the run would sit in `cancelling`
        // forever. Cancel every NON-running task now, in the SAME tx as the
        // intent (each with its task.cancelled event — invariant #6; these
        // are the §6.2 task diagram's sanctioned cancel edges). Running
        // tasks are NOT touched here: they own the abort→harvest→cancelled
        // path below.
        for (const t of store.findTasks(runId)) {
          if (t.state === 'running' || TASK_TERMINAL_STATES.has(t.state)) continue
          const cas = store.casTaskState(
            runId, t.task_id, t.state, t.version, 'cancelled',
            { blocked_reason: null, retry_not_before: null },
          )
          if (!cas.ok) continue // concurrent writer won the row — its owner reports it
          store.insertEvent(runId, {
            type: 'task.cancelled',
            taskId: t.task_id,
            payload: { from: t.state, to: 'cancelled', reason: 'stop', manual: true, note: reason ?? null },
            at: nowTs,
          })
          effected.push({ task_id: t.task_id, from: t.state, to: 'cancelled' })
        }
        store.insertEvent(runId, {
          type: 'run.control',
          payload: { action: 'stop', reason: reason ?? null, from, to: 'cancelling' },
          at: nowTs,
        })
      })

      // OUTSIDE the tx (invariant #3 — subagent I/O never lives in a tx):
      // abort every in-flight attempt of this run. Their result promises
      // settle 'aborted' (the executor's abort-source discrimination keeps
      // the 'cancelled' classification even against a racing timer), the
      // NEXT tick's harvestSettled walks the cancelled semantics
      // (attempt cancelled + task cancelled, per-attempt independent tx),
      // and finalizeRunIfDone aggregates cancelling → cancelled.
      // STOP NEEDS A TICK — control returns the intent-layer state only.
      // This ONE-SHOT sweep is not the whole abort story (M2 review R2):
      // an attempt claimed while executor.dispatch was suspended at
      // `await ctxSubagents.start` registers AFTER this loop and sees
      // neither the tx (running tasks skipped) nor this map (no entry yet).
      // The reconcile-side sweepStopAborts (oneRound/autoTick, start AND
      // end of every pass) re-affirms the abort idempotently until the
      // run aggregates — a straggler can no longer outlive the stop.
      if (action === 'stop') {
        for (const row of inFlightForRun(runId)) executor.abort(row.attemptId)
      }
    } else {
      // ---- task-level actions: retry_task / cancel_task -------------------
      if (typeof taskId !== 'string' || taskId.length === 0) {
        throw controlError('dag.task_required', `engine.control(${action}): task_id is required for task-level actions`)
      }
      // Fast not-found + state validation outside the tx (crisp errors);
      // the tx below re-reads and re-validates — the authoritative check.
      validateRunForTaskControl(action, runId, requireRun(runId), requireTask(runId, taskId))
      let landed = null
      store.tx(() => {
        const row = store.findTasks(runId).find((t) => t.task_id === taskId)
        if (row === undefined) {
          throw controlError('dag.task_not_found', `engine.control: task ${JSON.stringify(taskId)} not found in run ${JSON.stringify(runId)}`)
        }
        validateRunForTaskControl(action, runId, store.findRun(runId), row)
        validateTaskForControl(action, row)
        const nowTs = clockNow()
        if (action === 'retry_task') {
          // Run revival (§8.6 ② crash-recovery flow): a permanent failure
          // finalized the run to `failed` (anyFailed aggregation); without
          // this the next tick would no-op on the terminal run and the
          // manual retry could never dispatch. failed → running + intent
          // cleared, in the SAME tx as the task re-arm (invariant #6: the
          // run.control event pairs the revival). Runs in any OTHER
          // terminal/draining state never reach here — validateRunFor-
          // TaskControl (M2 R1) refused them loud before the re-arm: a
          // cancelled run may still hold a 'failed' task (the stop tx
          // skips terminal tasks), and re-arming it would wedge a dead run.
          const currentRun = store.findRun(runId)
          if (currentRun !== null && currentRun.state === 'failed') {
            const cas = store.casRunState(runId, 'failed', currentRun.version, 'running', { control_intent: null })
            if (cas.ok) {
              store.insertEvent(runId, {
                type: 'run.control',
                payload: { action: 'retry_task', reason: reason ?? null, from: 'failed', to: 'running', taskId },
                at: nowTs,
              })
            }
            // A lost revival CAS is not fatal: the run may have re-finalized
            // between the read and the CAS; the task re-arm below still
            // holds, and the next tick re-derives the aggregate honestly.
          }
          const cas = store.casTaskState(
            runId, taskId, row.state, row.version, 'retry_wait',
            { blocked_reason: null, retry_not_before: nowTs },
          )
          if (!cas.ok) {
            throw controlError('dag.invalid_task_state', `engine.control(retry_task): lost the ${row.state}→retry_wait CAS (${cas.reason})`)
          }
          store.insertEvent(runId, {
            type: 'task.retry_requested',
            taskId,
            payload: { taskId, manual: true, reason: reason ?? null, from: row.state, to: 'retry_wait', retryNotBeforeMs: nowTs },
            at: nowTs,
          })
          landed = { task_id: taskId, from: row.state, to: 'retry_wait' }
        } else {
          const cas = store.casTaskState(
            runId, taskId, row.state, row.version, 'cancelled',
            { blocked_reason: null, retry_not_before: null },
          )
          if (!cas.ok) {
            throw controlError('dag.invalid_task_state', `engine.control(cancel_task): lost the ${row.state}→cancelled CAS (${cas.reason})`)
          }
          store.insertEvent(runId, {
            type: 'task.cancelled',
            taskId,
            payload: { from: row.state, to: 'cancelled', reason: 'cancel_task', manual: true, note: reason ?? null },
            at: nowTs,
          })
          landed = { task_id: taskId, from: row.state, to: 'cancelled' }
        }
      })
      effected.push(landed)
    }

    const after = store.findRun(runId)
    return {
      kind: 'control',
      run_id: runId,
      action,
      run_state: after === null ? 'unknown' : after.state,
      effected,
    }
  }

  /**
   * RUN-state validation for the task-level control actions (M2 review R1):
   * the action must not land inside a run whose terminal contradicts it.
   *
   *   * failed — ALLOWED (T11 / §8.6 ②): retry_task revives the run
   *     (failed→running revival CAS below); cancel_task is honest cleanup
   *     of a leftover non-terminal task.
   *   * cancelling — refused loud for BOTH actions: the stop intent owns
   *     this run's terminal; a node-level retry/cancel landing now would
   *     desync the pending cancelled aggregation (R1's silent re-arm).
   *   * cancelled | succeeded — refused loud: stop is the operator's
   *     explicit terminal (a failed task that survived the stop must not
   *     re-arm inside a dead run), and retrying a succeeded task is
   *     invalid on its face — the run-level refusal is the uniform
   *     contract (a succeeded run holds no retryable task anyway).
   *
   * Called BOTH pre-tx (crisp error) and inside the control tx (the
   * authoritative check — a run may have finalized between the two reads).
   *
   * @param {'retry_task'|'cancel_task'} action
   * @param {{state: string} | null} run the run row (null = vanished mid-flight)
   * @param {{task_id: string}} row the task row (error detail only)
   */
  function validateRunForTaskControl(action, runId, run, row) {
    if (run === null) {
      throw controlError('dag.run_not_found', `engine.control(${action}): run ${JSON.stringify(runId)} not found`)
    }
    if (run.state === 'failed') return // T11 revival / honest cleanup
    if (run.state === 'cancelling' || run.state === 'cancelled' || run.state === 'succeeded') {
      throw controlError(
        'dag.invalid_run_state',
        `engine.control(${action}) on task "${row.task_id}": run is ${run.state} — `
          + (run.state === 'cancelling'
            ? 'a stop intent owns this run\'s terminal; no task-level control until it aggregates (tick to finish the harvest)'
            : run.state === 'cancelled'
              ? 'stop is the operator\'s terminal — plan a new run instead of reviving a node inside a cancelled one'
              : 'the run already finalized succeeded — plan a new run instead'),
      )
    }
  }

  /**
   * Task-source validation for the task-level control actions (§8.4):
   *   retry_task  → failed (terminal) | blocked(upstream_failed) |
   *                 blocked(merge_conflicted) — the T17 park's ONLY re-run
   *                 edge: a human resolves the worktrees queue
   *                 (worktree_queue resolve/retry) then re-arms the merge
   *                 node here; isReadySource excludes the park from
   *                 automatic re-evaluation, so this is deliberately the
   *                 sole exit (approval_pending is NOT here: its exit is
   *                 dag_approve + reconcileApprovals, never retry_task).
   *   cancel_task → pending | ready | queued | blocked (any reason — parks
   *                 included; §6.2's task diagram draws all four cancel
   *                 edges, running's cancel being the run-level stop)
   * blocked(upstream_blocked) is refused for retry_task: the upstream has
   * not settled, re-arming the victim cannot clear it.
   */
  function validateTaskForControl(action, row) {
    if (action === 'retry_task') {
      const code = row.state === 'blocked' ? parseBlockedReasonCode(row.blocked_reason) : undefined
      const upstreamFailed = code === 'upstream_failed'
      const mergeConflicted = code === 'merge_conflicted'
      if (row.state !== 'failed' && !upstreamFailed && !mergeConflicted) {
        const detail = row.state === 'blocked' ? `blocked(${code ?? 'unspecified'})` : row.state
        throw controlError(
          'dag.invalid_task_state',
          `engine.control(retry_task): task "${row.task_id}" is ${detail} — retry_task accepts failed, blocked(upstream_failed), or blocked(merge_conflicted) (after worktree_queue resolve/retry)`,
        )
      }
      return
    }
    if (row.state !== 'pending' && row.state !== 'blocked' && row.state !== 'ready' && row.state !== 'queued') {
      throw controlError(
        'dag.invalid_task_state',
        `engine.control(cancel_task): task "${row.task_id}" is ${row.state} — cancel_task accepts pending/ready/queued/blocked (cancelling a running task is the run-level stop)`,
      )
    }
  }

  /** Loud `dag.task_not_found` for a task row lookup (control plane). */
  function requireTask(runId, taskId) {
    const row = store.findTasks(runId).find((t) => t.task_id === taskId)
    if (row === undefined) {
      throw controlError(
        'dag.task_not_found',
        `engine: task ${JSON.stringify(taskId)} not found in run ${JSON.stringify(runId)}`,
      )
    }
    return row
  }

  // ==========================================================================
  // disposeAll — teardown helper (host apply() effect / tests)
  // ==========================================================================

  function disposeAll() {
    for (const attemptId of executor.inFlightIds()) {
      executor.dispose(attemptId)
      attemptMeta.delete(attemptId)
    }
  }

  return {
    tick,
    oneRound,
    autoTick,
    reconcileApprovals,
    planRun,
    status,
    control,
    inFlightForRun,
    disposeAll,
  }
}
