/**
 * recovery — crash reconciliation (T09 + T13, DESIGN §12.1/§12.2).
 *
 * Runs at apply() time, BEFORE any tool registration (analysis §4-C4: the
 * first model-visible call must already see a truthful state). Ported from
 * task-weaver's recovery-service.ts decision tree with the DSH-narrowed
 * five-way classification (DESIGN §12.1):
 *
 *   * `controlled` — IMPOSSIBLE here: no reattach channel exists after a
 *     host exit (C4: the host's death kills every in-process subagent; a
 *     restarted host holds no handle, no result channel — not even an
 *     identity probe has an object to probe). No branch is implemented by
 *     design; this comment is the record.
 *
 *   * never-dispatched (the Window 1 / no-process arm): the attempt row
 *     was written but the dispatch never happened. Two row shapes reach
 *     this arm (DESIGN §6.2 attempt machine: claimed ─▶ running; the
 *     engine's claimTask persists the row born 'running' with
 *     child_session NULL — DESIGN §6.2's narrowing note — so BOTH count):
 *       - state 'claimed' (the spec's literal Window-1 shape), or
 *       - state 'running' AND child_session IS NULL (crash between the
 *         claim tx and the dispatch's updateAttemptChildSession — the
 *         production residue engine.claimTask leaves behind).
 *     Nothing ever ran → provably no side effects → the ONE machine-
 *     terminal action the bounded policy allows: fail the attempt
 *     (`recovery.no_dispatch`) and auto-retry the task IMMEDIATELY
 *     (retry_wait with retry_not_before = now — "nothing ever ran" is
 *     naturally idempotent; DESIGN §12.1's explicitly WIDER-than-source
 *     allowance, recorded in the event payload as recovery:true so
 *     shouldRetry does NOT bill it against the retryOn budget).
 *
 *   * `running` + child_session SET (the host-crash trace — Windows 3/4 +
 *     orphaned merged): the subagent took off and MAY have done partial
 *     work (dirty worktree, half-written files); success cannot be
 *     inferred and no automatic rollback may be invented for unknown
 *     non-idempotent side effects. → attempt `orphaned` + task `failed` +
 *     `recovery.action_requested` (child_session in the payload for manual
 *     DSH session-log inspection; the human reconciles and later
 *     dag_control retry_task re-runs it).
 *
 *   * Chain verification (the `inconsistent` arm's persistence-facing
 *     residue): every run's event hash chain is recomputed at load. A
 *     mismatch on a NON-terminal run → the run is parked failed +
 *     `recovery.chain_broken` (tools are then registered and the human
 *     disposes; other runs continue unaffected). A mismatch on an already
 *     TERMINAL run is a historical-audit finding only: logger.warn, state
 *     untouched — rewriting a terminal run's state would corrupt the audit
 *     trail that the tamper detection itself is meant to protect.
 *
 *   * Outputs orphan-row audit (the `inconsistent` arm's OTHER persistence
 *     residue, DESIGN §12.1): an outputs row whose produced_by_attempt is
 *     NOT a terminal attempt (still claimed/running after the pass above,
 *     or missing entirely) is collected into a logger.warn audit list —
 *     never a load blocker, never a projection write. The bounded policy
 *     forbids invented repairs; the rows are surfaced for human audit.
 *
 * Bounded policy (§12.2, carried over): the ONLY automatic action is the
 * retry of a never-dispatched attempt; everything that "ran but did not
 * reach a terminal state" routes to a human. No compensating commands are
 * invented, no "probably succeeded" is ever inferred.
 */

/**
 * One attempt's never-dispatched reconciliation, in ONE transaction
 * (invariants #3/#6 — the projection writes and the events land together):
 *
 *   1. commitTerminal(attemptId, attempt.owner_token, 'failed') — the owner
 *      CAS passes the row's PERSISTED owner_token, not a hardcoded null:
 *      engine.claimTask mints and persists the token at CLAIM time (inside
 *      the claim transaction), so real crash residue carries it SET. A null
 *      row token matches null the same way. The CAS still does real work:
 *      it refuses a row whose token was RE-minted between our read and
 *      this tx (a concurrent writer owns the row) — and at apply() time no
 *      in-flight handle exists yet (reconcile runs before any dispatch
 *      could register one), so a matching persisted token is by
 *      construction a dead process's.
 *   2. `recovery.no_dispatch` event (why the attempt died).
 *   3. `attempt.retry_scheduled` event — payload recovery:true marks the
 *      §12.1 wider-than-source allowance (no backoff: nothing ever ran)
 *      AND exempts it from shouldRetry's priorExecutionRetries count.
 *   4. CAS the task → retry_wait with retry_not_before = now (retry NOW —
 *      the ready evaluator promotes it on the next tick).
 *   5. `task.retry_wait` event.
 *
 * The event ordering mirrors terminal-commit.js's Issue-5 fix: the attempt
 * terminal commit lands FIRST; only a successful commit schedules the
 * retry (a failed commit must never leave the task in retry_wait with a
 * non-terminal attempt wedging hasNonTerminalAttempt forever).
 *
 * @param {object} store DagStore handle
 * @param {{run_id: string, task_id: string, attempt_id: string, ordinal: number, owner_token: string|null}} attempt
 * @param {{now: number}} clock
 * @param {object} log logger face ({info?, warn?, debug?})
 * @returns {boolean} true when the full recovery landed
 */
function recoverNeverDispatched(store, attempt, clock, log) {
  const { run_id: runId, task_id: taskId, attempt_id: attemptId } = attempt
  let landed = false
  store.tx(() => {
    // Re-read the attempt + task INSIDE the transaction for fresh rows (the
    // C1 stale-read fix — same rationale as terminal-commit.js). The
    // re-read attempt row also refreshes the owner token for the CAS below.
    const freshAttempt = store.findAttempt(attemptId)
    // Never-dispatched arm: claimed, OR running with no child session (the
    // engine's claim-task row shape — crash before updateAttemptChildSession).
    if (freshAttempt === null) return
    const neverDispatched = freshAttempt.state === 'claimed'
      || (freshAttempt.state === 'running' && freshAttempt.child_session === null)
    if (!neverDispatched) return // terminal now / a live-child orphan — not this arm
    const taskRow = store.findTasks(runId).find((t) => t.task_id === taskId)
    if (taskRow === undefined) return

    // 1. Attempt → failed. Owner CAS = the row's persisted token (minted at
    //    claim time by engine.claimTask; null rows match null). A lost CAS
    //    (already terminal / re-minted token) means a concurrent writer owns
    //    this row: leave everything untouched.
    const commit = store.commitTerminal(attemptId, freshAttempt.owner_token, 'failed', {
      stop_reason: 'internal',
      failure_json: JSON.stringify({
        failureType: 'internal',
        code: 'recovery.no_dispatch',
        message: 'host crashed before the attempt was dispatched; nothing ever ran',
      }),
      result_json: null,
    })
    if (!commit.ok) return

    // 2. Why the attempt died.
    store.insertEvent(runId, {
      type: 'recovery.no_dispatch',
      taskId,
      attemptId,
      payload: {
        attemptId,
        reason: 'claimed-but-never-dispatched (host crash between claim and dispatch)',
        attemptState: freshAttempt.state,
      },
      at: clock.now,
    })

    // 3. The §12.1 auto-retry allowance, recorded as such (recovery:true).
    store.insertEvent(runId, {
      type: 'attempt.retry_scheduled',
      taskId,
      attemptId,
      payload: {
        failedAttemptId: attemptId,
        nextAttemptNumber: attempt.ordinal + 1,
        retryNotBeforeMs: clock.now,
        backoffMs: 0,
        recovery: true,
      },
      at: clock.now,
    })

    // 4/5. Task → retry_wait ONLY after the attempt terminal commit
    //     succeeded (Issue-5 ordering).
    const cas = store.casTaskState(
      runId, taskId, taskRow.state, taskRow.version, 'retry_wait',
      { retry_not_before: clock.now },
    )
    if (!cas.ok) return
    store.insertEvent(runId, {
      type: 'task.retry_wait',
      taskId,
      payload: { from: taskRow.state, to: 'retry_wait', reason: 'retry_wait', retryNotBeforeMs: clock.now, attemptNumber: attempt.ordinal, recovery: true },
      at: clock.now,
    })
    landed = true
  })
  if (!landed) {
    log.debug?.(`recovery: never-dispatched attempt ${attemptId} of ${taskId}@${runId} lost its CAS race — left untouched`)
  }
  return landed
}

/**
 * One took-off-but-never-landed attempt's orphan reconciliation, in ONE
 * transaction (§12.2 step 2's running arm — T13):
 *
 *   1. commitTerminal(attemptId, persisted owner_token, 'orphaned') — the
 *      bounded policy's terminal for "ran but never reached a terminal";
 *      the owner CAS rationale is the same as recoverNeverDispatched's.
 *   2. `attempt.orphaned` event (failure_json carries the classification).
 *   3. CAS the task → failed + `task.failed` event (a dead-end: downstream
 *      propagates blocked(upstream_failed) on the next tick; nothing auto-
 *      retries — the human decides after inspecting the child session).
 *   4. `recovery.action_requested` event carrying child_session (the
 *      manual DSH session-log locator) — the §12.1 "routes to a human"
 *      marker.
 *
 * Ordering follows the same Issue-5 shape: attempt terminal first; only a
 * successful commit moves the task.
 *
 * @param {object} store DagStore handle
 * @param {{run_id: string, task_id: string, attempt_id: string, child_session: string|null}} attempt
 * @param {{now: number}} clock
 * @param {object} log logger face
 * @returns {boolean} true when the full orphan reconciliation landed
 */
function recoverOrphaned(store, attempt, clock, log) {
  const { run_id: runId, task_id: taskId, attempt_id: attemptId } = attempt
  let landed = false
  store.tx(() => {
    const freshAttempt = store.findAttempt(attemptId)
    if (freshAttempt === null || freshAttempt.state !== 'running') return
    const taskRow = store.findTasks(runId).find((t) => t.task_id === taskId)
    if (taskRow === undefined) return

    // 1. Attempt → orphaned (the bounded policy: no auto-success, no
    //    invented rollback; the row keeps its child_session locator).
    const commit = store.commitTerminal(attemptId, freshAttempt.owner_token, 'orphaned', {
      stop_reason: 'internal',
      failure_json: JSON.stringify({
        failureType: 'internal',
        code: 'recovery.orphaned',
        message: 'host crashed while the subagent was in flight; success cannot be inferred — manual reconciliation required',
      }),
      result_json: null,
    })
    if (!commit.ok) return

    // 2. The orphan event (failure classification on the record).
    store.insertEvent(runId, {
      type: 'attempt.orphaned',
      taskId,
      attemptId,
      payload: {
        from: 'running',
        to: 'orphaned',
        childSession: freshAttempt.child_session,
        code: 'recovery.orphaned',
      },
      at: clock.now,
    })

    // 3. Task → failed (dead-end; downstream propagation belongs to the
    //    engine's next tick — reconcile never invents run-level cascades).
    const cas = store.casTaskState(runId, taskId, taskRow.state, taskRow.version, 'failed')
    if (!cas.ok) return
    store.insertEvent(runId, {
      type: 'task.failed',
      taskId,
      payload: {
        from: taskRow.state,
        to: 'failed',
        reason: 'recovery.orphaned',
        failureType: 'internal',
        code: 'recovery.orphaned',
        attemptNumber: attempt.ordinal ?? freshAttempt.ordinal,
      },
      at: clock.now,
    })

    // 4. The human-routing marker (§12.1 "bounded policy": everything that
    //    ran-but-never-landed routes to a human, never auto-retried).
    store.insertEvent(runId, {
      type: 'recovery.action_requested',
      taskId,
      attemptId,
      payload: {
        attemptId,
        childSession: freshAttempt.child_session,
        action: 'retry_task',
        reason: 'in-flight attempt orphaned by a host crash — inspect the child session for partial work, then dag_control retry_task',
      },
      at: clock.now,
    })
    landed = true
  })
  if (!landed) {
    log.debug?.(`recovery: orphan attempt ${attemptId} of ${taskId}@${runId} lost its CAS race — left untouched`)
  }
  return landed
}

/**
 * The crash reconciliation (DESIGN §12.2 startup sequence).
 *
 * @param {object} store DagStore handle (freshly opened by apply())
 * @param {{logger?: object, now?: () => number}} [options]
 * @returns {Promise<{recoveredRuns: number, autoRetried: number, orphaned: number, chainBroken: number}>}
 *   recoveredRuns — non-terminal runs that received any recovery action
 *   autoRetried  — never-dispatched attempts retried (failed + retry_wait)
 *   orphaned     — in-flight attempts parked orphaned + action_requested
 *   chainBroken  — runs parked failed by chain verification
 */
export async function reconcile(store, { logger, now } = {}) {
  const log = logger ?? {}
  const clockNow = typeof now === 'function' ? now : Date.now

  const recoveredRuns = new Set()
  let autoRetried = 0
  let orphanedCount = 0
  let chainBroken = 0

  // findAllRuns (terminal included): the chain check must cover every run —
  // a tampered TERMINAL run is an audit finding even though its state stays
  // untouched, and marking it requires having seen it.
  const runs = store.findAllRuns()

  // ---- 1. chain verification (per run; a broken run never blocks the rest)
  for (const run of runs) {
    const verdict = store.verifyChain(run.run_id)
    if (verdict.ok) continue
    const terminal = ['succeeded', 'failed', 'cancelled'].includes(run.state)
    if (terminal) {
      // Historical audit finding only — rewriting a terminal run's state
      // would corrupt the very audit trail tamper detection protects.
      log.warn?.(
        `recovery: event chain of TERMINAL run ${run.run_id} is broken (first bad seq ${verdict.firstBadSeq}) — state left untouched, logged for audit`,
      )
      continue
    }
    let landed = false
    store.tx(() => {
      const fresh = store.findRun(run.run_id) // re-read: version may have moved
      if (fresh === null || ['succeeded', 'failed', 'cancelled'].includes(fresh.state)) return
      const cas = store.casRunState(run.run_id, fresh.state, fresh.version, 'failed')
      if (!cas.ok) return
      store.insertEvent(run.run_id, {
        type: 'recovery.chain_broken',
        payload: { firstBadSeq: verdict.firstBadSeq, from: fresh.state, to: 'failed' },
        at: clockNow(),
      })
      landed = true
    })
    if (landed) {
      chainBroken += 1
      recoveredRuns.add(run.run_id)
      log.warn?.(
        `recovery: event chain of run ${run.run_id} is broken (first bad seq ${verdict.firstBadSeq}) — run parked failed for manual disposal`,
      )
    } else {
      log.warn?.(
        `recovery: event chain of run ${run.run_id} is broken but the run-failed CAS lost its race — left for the next reconcile`,
      )
    }
  }

  // ---- 2. in-flight attempt classification (non-terminal runs only)
  // The run set is RE-TAKEN AFTER the chain pass: a run parked failed by
  // step 1 is now terminal and its attempts are deliberately left alone —
  // the chain is untrustworthy, so deriving ANY action from its rows would
  // be deriving truth from the very tamper we just quarantined.
  const nonTerminal = store.findAllRuns().filter((run) => !['succeeded', 'failed', 'cancelled'].includes(run.state))
  for (const run of nonTerminal) {
    for (const attempt of store.findNonTerminalAttempts(run.run_id)) {
      const neverDispatched = attempt.state === 'claimed'
        || (attempt.state === 'running' && attempt.child_session === null)
      if (neverDispatched) {
        // Never dispatched — the one auto-retry the bounded policy allows
        // (both row shapes: 'claimed' AND engine.claimTask's born-'running'
        // residue with no child session).
        if (recoverNeverDispatched(store, attempt, { now: clockNow() }, log)) {
          autoRetried += 1
          recoveredRuns.add(run.run_id)
        }
      } else if (attempt.state === 'running') {
        // Took off, never landed: partial work is possible, success is not
        // inferable, no automatic rollback may be invented. Park the
        // attempt orphaned, fail the task, request a human action.
        if (recoverOrphaned(store, attempt, { now: clockNow() }, log)) {
          orphanedCount += 1
          recoveredRuns.add(run.run_id)
        }
      }
    }
    // §12.2: a pausing run STAYS pausing (the next tick's drainToPaused
    // closes it); a running run is otherwise untouched — reconcile never
    // invents run-level transitions beyond the branches above.
  }

  // ---- 3. outputs orphan-row audit (§12.1 `inconsistent` residue)
  // An outputs row is only ever written by a SUCCESS terminal commit, so its
  // produced_by_attempt must be terminal (succeeded). A row pointing at a
  // non-terminal attempt (a crash window between the outputs upsert and the
  // attempt's terminal commit cannot exist — they share one tx — so a hit
  // here means foreign writes or manual db surgery) or at a MISSING attempt
  // is an audit finding, never a load blocker: the bounded policy forbids
  // invented repairs. Surfaced as logger.warn lines for the human.
  let orphanOutputs = 0
  if (typeof store.findAllOutputs === 'function') {
    for (const output of store.findAllOutputs()) {
      const attempt = store.findAttempt(output.produced_by_attempt)
      const terminal = attempt !== null
        && ['succeeded', 'failed', 'cancelled', 'orphaned'].includes(attempt.state)
      if (!terminal) {
        orphanOutputs += 1
        log.warn?.(
          `recovery: orphan output row — run ${output.run_id} task ${output.task_id} output "${output.name}" `
          + `cites attempt ${output.produced_by_attempt} which is ${attempt === null ? 'missing' : `still ${attempt.state}`} (non-terminal) `
          + '— surfaced for audit; not repaired, not blocking',
        )
      }
    }
  }

  // ---- 4. summary
  const summary = {
    recoveredRuns: recoveredRuns.size,
    autoRetried,
    orphaned: orphanedCount,
    chainBroken,
    orphanOutputs,
  }
  log.info?.(
    `recovery: ${summary.recoveredRuns} run(s) recovered `
      + `(${summary.autoRetried} never-dispatched attempt(s) auto-retried, `
      + `${summary.orphaned} in-flight attempt(s) parked orphaned for manual action, `
      + `${summary.chainBroken} chain-broken run(s) parked failed)`
      + (summary.orphanOutputs > 0 ? `; ${summary.orphanOutputs} orphan output row(s) warned for audit` : ''),
  )
  return summary
}
