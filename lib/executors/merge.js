/**
 * executors/merge — the T17 merge-kind executor (TASKS.md T17 / DESIGN
 * §11.1: the DrainOutcome five-state mapping over the dsh-worktrees merge
 * queue).
 *
 * Ported from task-weaver packages/scheduler/src/task-executors.ts
 * `runMergeTask` (L67-185 — enqueue 每源 → drain → 冲突/失败映射) and
 * re-targeted onto the worktrees §10 composition seam:
 *
 *   input mapping — the merge sources are the task's dependsOn(succeeded)
 *     upstreams that DECLARE a worktree (spec-validate enforces ≥1 at plan
 *     time, `dag.merge_source_missing`); each source's worktree id is read
 *     from the upstream's LAST succeeded attempt's `attempt.dispatched`
 *     event payload (event-sourced — zero new tables; the agent executor
 *     stamps payload.worktreeId at dispatch time, T16+T17).
 *   execution — enqueue every source onto the worktrees merge queue
 *     (origin 'dag', correlationId = THIS attempt), then
 *     drain(repoKey, integrationBranch) and map the DrainOutcome below.
 *
 * DrainOutcome five-state mapping (DESIGN §11.1):
 *
 *   succeeded   → terminal success; outputs[0] (when declared) persists
 *                 {integratedCommit, integrationBranch} in the SAME terminal
 *                 tx (commitTerminalAndRelease's outputs arm — the "outputs
 *                 记 integratedCommit" rule).
 *   no_changes  → terminal success too — an empty integration is legal.
 *   conflicted  → PARK, never terminal failed: ONE tx { commitTerminal(
 *                 attempt,'failed') + attempt.failed(reason merge_conflicted,
 *                 conflictFiles + retained paths) + task CAS running→blocked
 *                 {code:'merge_conflicted', conflictFiles,
 *                 retainedWorktrees} + task.blocked } — the exact
 *                 approval-park shape (parkOrResolveApproval, T12); the
 *                 slot is released OUTSIDE the tx by the caller. The park
 *                 is excluded from ready re-evaluation (T05's isReadySource
 *                 park set), so a human worktree_queue resolve followed by
 *                 dag_control retry_task is the ONLY re-run edge.
 *   failed      → commitTerminalAndRelease(transient `dag.merge_failed`) —
 *                 the retry policy decides (transient maps to the
 *                 transient_network retryOn key, failureTypeToPolicyKey).
 *   queued /    → THIS round does not terminal-succeed. task-weaver's
 *   queued_ahead  runMergeTask loops applyNext inside the executor; DSH has
 *                 no drive loop to hang the drain promise on, and the drain
 *                 has already returned — there is nothing in flight to
 *                 await. DECISION (task brief): commit the attempt as
 *                 transient `dag.merge_queued_ahead` + retry_wait(now +
 *                 5s), a LIGHT re-poll backoff. The retry_scheduled payload
 *                 carries manual:true — the recovery:true STYLE marker —
 *                 and shouldRetry skips manual:true events when billing the
 *                 retryOn budget, so a busy queue can never exhaust the
 *                 merge task's attempts; the next tick re-claims,
 *                 re-enqueues (the queue contract below is idempotent per
 *                 active job) and drains again until the queue reaches our
 *                 jobs.
 *
 * The DAG-side merge-queue contract (JSDoc anchor; also mirrored in
 * lib/worktrees-seam.js — dsh-worktrees implements to this):
 *
 *   enqueue({worktreeId, integrationBranch, origin:'dag', correlationId})
 *     → job record `{repoKey, …}` (jobId/id optional).
 *     The provider resolves repoKey/repoRoot/sourceBranch from the worktree
 *     record SERVER-SIDE (the DAG never learns git facts); the returned
 *     repoKey is REQUIRED — drain is keyed by it. Failure surfaces: THROWS,
 *     or returns the source's `{ok:false, error:{type}}` shape (both map to
 *     transient `dag.merge_enqueue_failed`, message naming the type — the
 *     active_job_exists family included). IDEMPOTENCE: while a job for the
 *     same (worktreeId, integrationBranch) is still active (queued/
 *     applying/conflicted), enqueue RETURNS THAT JOB instead of stacking a
 *     duplicate — a DAG retry re-polls, it does not re-stack.
 *   drain(repoKey, integrationBranch) → Promise<DrainOutcome>
 *     `{state:'succeeded', integratedCommit}` | `{state:'conflicted',
 *     conflictFiles: string[], integrationWorktree?}` | `{state:'failed',
 *     error}` | `{state:'no_changes'}` | `{state:'queued',
 *     queued_ahead}`. The `queued`/`queued_ahead` state payload's count key
 *     is `queued_ahead` ONLY (snake_case family — the camelCase
 *     `queuedAhead` variant is NOT a contract key; M3 m-5 unified here).
 *
 * Discipline (task brief): this module imports NOTHING — the store, the
 * admission handle, the executor (whose `.worktrees()` is the T15 use-time
 * seam probe), commitTerminalAndRelease and the retry decider are injected
 * by the engine's dispatchLoop; lib/worktrees-seam.js is never imported
 * here (constructor injection only, red line / T15 note).
 *
 * M-B (M3 review): the verify completion gate applies HERE TOO. The agent
 * path gates at harvestSettled (engine), but a merge attempt
 * terminal-commits INSIDE dispatchLoop through this executor — a merge
 * task declaring `verify` would have its block silently skipped (red line
 * 1's "admitted but silently not executed" class). The engine therefore
 * injects `evaluateVerifyGate` and this module runs it on the SUCCESS path
 * (succeeded | no_changes) BEFORE commitTerminalAndRelease(null): receipt
 * source = the outputs table row this attempt is about to land (same
 * DESIGN §7.3 substitution as the agent path — the pending structured
 * {integratedCommit, integrationBranch} IS the receipt; `expectStatus`
 * judges its `status` field). A miss collapses the success into the
 * synthetic permanent `dag.verify_gate_failed` through the SAME
 * commitTerminalAndRelease machinery — no new terminal state, and the
 * conflicted park / failed / queued paths are non-success by construction
 * and never pass the gate.
 */

/** queued_ahead re-poll backoff — light, budget-free (module header). */
const QUEUED_AHEAD_BACKOFF_MS = 5_000

/**
 * The worktree id of an upstream task's LAST succeeded attempt, read from
 * its `attempt.dispatched` event payload (event-sourced source resolution —
 * zero new tables). Returns null when the upstream has no succeeded
 * attempt, or when that attempt's dispatched event carries no usable
 * worktreeId (an attempt that pre-dates T16, or a lost event) — older
 * succeeded attempts are deliberately NOT consulted: the last one owns the
 * worktree state the merge should integrate.
 *
 * @param {object} store DagStore handle (findAttempts / findEvents).
 * @param {string} runId
 * @param {string} upstreamTaskId
 * @returns {string | null}
 */
function lastSucceededWorktreeId(store, runId, upstreamTaskId) {
  const attempts = store.findAttempts(runId, upstreamTaskId) // ordered by ordinal
  for (let i = attempts.length - 1; i >= 0; i -= 1) {
    if (attempts[i].state !== 'succeeded') continue
    const attemptId = attempts[i].attempt_id
    const dispatched = store
      .findEvents(runId, {})
      .filter((e) => e.type === 'attempt.dispatched' && e.task_id === upstreamTaskId && e.attempt_id === attemptId)
    for (let j = dispatched.length - 1; j >= 0; j -= 1) {
      try {
        const payload = JSON.parse(dispatched[j].payload_json)
        if (typeof payload.worktreeId === 'string' && payload.worktreeId.length > 0) {
          return payload.worktreeId
        }
      } catch {
        // unparsable payload — keep scanning older dispatched events
      }
    }
    return null // the last succeeded attempt dispatched WITHOUT a worktree id
  }
  return null // no succeeded attempt at all
}

/**
 * Park a conflicted merge attempt — the approval-park shape (T12's
 * parkOrResolveApproval branch 3, source task-executors.ts L239-254):
 * ONE transaction with commitTerminal(attempt,'failed') + attempt.failed
 * event (reason merge_conflicted + conflict scene) + task CAS
 * running→blocked {code:'merge_conflicted', …} + task.blocked event; the
 * commit.ok / cas.ok gates keep replay windows silent. Resource release
 * stays with the CALLER, outside the tx (invariant #3).
 *
 * @param {object} input
 */
function parkMergeConflict({ store, runId, taskId, attemptId, ownerToken, conflictFiles, retainedWorktrees, now }) {
  store.tx(() => {
    const nowTs = now()
    const taskRow = store.findTasks(runId).find((t) => t.task_id === taskId)
    const commit = store.commitTerminal(attemptId, ownerToken, 'failed', {
      stop_reason: 'internal',
      failure_json: JSON.stringify({
        failureType: 'internal',
        code: 'dag.merge_conflicted',
        message: 'merge conflict — integration branch retained for resolution (worktree_queue resolve/retry, then dag_control retry_task)',
      }),
      result_json: null,
    })
    if (commit.ok) {
      store.insertEvent(runId, {
        type: 'attempt.failed',
        taskId,
        attemptId,
        payload: { from: 'running', to: 'failed', reason: 'merge_conflicted', conflictFiles, retainedWorktrees },
        at: nowTs,
      })
    }
    if (taskRow !== undefined && taskRow.state === 'running') {
      const reason = { code: 'merge_conflicted', conflictFiles, retainedWorktrees }
      const cas = store.casTaskState(
        runId, taskId, taskRow.state, taskRow.version, 'blocked',
        { blocked_reason: JSON.stringify(reason) },
      )
      if (cas.ok) {
        store.insertEvent(runId, {
          type: 'task.blocked',
          taskId,
          payload: { from: taskRow.state, to: 'blocked', reason },
          at: nowTs,
        })
      }
    }
  })
}

/**
 * Execute one `kind: merge` attempt (the T17 replacement of the M5
 * fail-closed kind gate). Everything is injected by the engine's
 * dispatchLoop; see the module header for the full contract.
 *
 * @param {object} input
 * @param {object} input.store DagStore handle (the only persistence outlet).
 * @param {object} input.spec the run's validated spec.
 * @param {string} input.runId
 * @param {string} input.taskId the merge task id.
 * @param {object} input.specTask the merge task's spec node.
 * @param {{taskId: string, attemptId: string, ordinal: number}} input.attempt
 * @param {string} input.ownerToken the claim's owner token (commitTerminal CAS).
 * @param {object} [input.slot] admission slot handle (released by
 *   commitTerminalAndRelease, or by the CALLER on the park path).
 * @param {string} [input.sessionKey]
 * @param {Function} input.commitTerminalAndRelease terminal-commit fn.
 * @param {object} input.admission createAdmission() handle.
 * @param {object} input.executor createExecutor() handle — the worktrees
 *   face is reached ONLY through `executor.worktrees()` (the T15 use-time
 *   probe; this module never imports the seam).
 * @param {(failure: object) => {retry: boolean, backoffMs: number}} input.decideRetry
 *   engine.shouldRetry bound to (spec, runId, taskId, claim).
 * @param {Function} [input.evaluateVerifyGate]
 *   M-B (M3 review): the verify completion gate (lib/verify-gate.js),
 *   injected by the engine for the merge SUCCESS path. OPTIONAL and only
 *   meaningful together with a `verify` declaration on the merge task —
 *   absent gate fn behaves exactly as before T18+M-B (the block could not
 *   exist on merges pre-M3; a spec carrying one still validates, so the
 *   engine always passes the fn). Read-only pure function; injected to
 *   keep this module import-free.
 * @param {() => number} [input.now] injectable clock.
 * @param {object} [input.logger]
 * @returns {Promise<'parked' | 'terminal'>} 'parked' = conflicted park (the
 *   CALLER releases the slot/sessionKey outside the tx); 'terminal' = the
 *   attempt reached a terminal state through commitTerminalAndRelease
 *   (which already released the resources) — success, failure, or the
 *   queued_ahead retry_wait re-poll.
 */
export async function runMergeTask({
  store, spec, runId, taskId, specTask, attempt, ownerToken, slot, sessionKey,
  commitTerminalAndRelease, admission, executor, decideRetry, evaluateVerifyGate, now, logger,
}) {
  const log = logger ?? {}
  const nowFn = typeof now === 'function' ? now : Date.now

  /** One terminal commit through the injected commitTerminalAndRelease. */
  const commit = (failure, options = {}) => commitTerminalAndRelease({
    store, admission, spec, clock: { now: nowFn() }, runId,
    attempt: { taskId, attemptId: attempt.attemptId, ordinal: attempt.ordinal },
    slot, sessionKey, ownerToken,
    failure,
    stopReason: options.stopReason ?? 'internal',
    retryDecision: options.retryDecision ?? decideRetry(failure),
    ...(options.structured !== undefined ? { structured: options.structured } : {}),
    ...(options.retryStamp !== undefined ? { retryStamp: options.retryStamp } : {}),
    ...(options.verifyStamp !== undefined ? { verifyStamp: options.verifyStamp } : {}),
  })

  // ---- 1. seam absence is a configuration state (the M5 gate's honest
  // replacement — agent-only DAGs boot without dsh-worktrees; merge does not).
  const { worktrees } = executor.worktrees()
  const unavailable = () => commit({
    failureType: 'permanent',
    code: 'dag.worktrees_unavailable',
    message: 'merge task requires the dsh-worktrees engine service (ctx.get("worktreesEngine")) — load the dsh-worktrees plugin to run merge tasks',
  })
  if (worktrees === null) {
    unavailable()
    return 'terminal'
  }
  const queue = worktrees.getMergeQueue()
  if (queue === null || typeof queue !== 'object'
    || typeof queue.enqueue !== 'function' || typeof queue.drain !== 'function') {
    log.warn?.(
      'dag merge executor: getMergeQueue() returned a value without the required enqueue()/drain() functions — treated as unavailable',
    )
    unavailable()
    return 'terminal'
  }

  // ---- 2. source collection (event-sourced worktree ids).
  const sources = []
  for (const dep of specTask.dependsOn ?? []) {
    if (dep.condition !== 'succeeded') continue
    const upstream = spec.tasks.find((t) => t.id === dep.taskId)
    if (upstream === undefined || upstream.worktree === undefined) continue
    const worktreeId = lastSucceededWorktreeId(store, runId, dep.taskId)
    if (worktreeId === null) {
      commit({
        failureType: 'permanent',
        code: 'dag.merge_source_missing',
        message: `merge source unresolved: upstream task "${dep.taskId}" declares a worktree but its last succeeded attempt carries no attempt.dispatched worktreeId (the event was lost, or the attempt pre-dates T16) — re-run the upstream or repair the event chain`,
      })
      return 'terminal'
    }
    sources.push({ taskId: dep.taskId, worktreeId })
  }
  if (sources.length === 0) {
    // spec-validate enforces ≥1 worktree-declaring succeeded upstream at plan
    // time; reaching here means a hand-seeded store. Fail closed, loud.
    commit({
      failureType: 'permanent',
      code: 'dag.merge_source_missing',
      message: `merge task "${taskId}" has no worktree-declaring succeeded-condition upstream (spec-validate rejects this at plan time) — nothing to integrate`,
    })
    return 'terminal'
  }

  // ---- 3. enqueue every source (origin 'dag', correlationId = THIS attempt).
  const runName = store.findRun(runId)?.name ?? runId
  const integrationBranch = specTask.merge?.integrationBranch ?? `dag/${runName}/integration`
  const jobs = []
  for (const source of sources) {
    let job
    try {
      job = await queue.enqueue({
        worktreeId: source.worktreeId,
        integrationBranch,
        origin: 'dag',
        correlationId: attempt.attemptId,
      })
    } catch (error) {
      commit({
        failureType: 'transient',
        code: 'dag.merge_enqueue_failed',
        message: `merge enqueue failed for upstream "${source.taskId}" (worktree ${source.worktreeId}): ${String(error?.message ?? error)}`,
      })
      return 'terminal'
    }
    // The source's `{ok:false, error:{type}}` failure surface (r.error.type
    // family — active_job_exists included), mapped per the task brief.
    if (job !== null && typeof job === 'object' && job.ok === false) {
      const errorType = job.error?.type ?? 'unknown'
      commit({
        failureType: 'transient',
        code: 'dag.merge_enqueue_failed',
        message: `merge enqueue failed for upstream "${source.taskId}" (worktree ${source.worktreeId}): ${errorType}`,
      })
      return 'terminal'
    }
    if (job === null || typeof job !== 'object' || typeof job.repoKey !== 'string' || job.repoKey.length === 0) {
      commit({
        failureType: 'transient',
        code: 'dag.merge_enqueue_failed',
        message: `merge enqueue for upstream "${source.taskId}" returned a job record without a usable repoKey — the DAG cannot drain (provider contract: enqueue resolves repoKey from the worktree record)`,
      })
      return 'terminal'
    }
    jobs.push(job)
  }

  // ---- 4. drain per unique repoKey (one repo per run in practice); the
  // first bad outcome wins with precedence conflicted > failed > queued.
  let conflicted = null
  let failed = null
  let queued = null
  let integratedCommit = null
  for (const repoKey of [...new Set(jobs.map((j) => j.repoKey))]) {
    let outcome
    try {
      outcome = await queue.drain(repoKey, integrationBranch)
    } catch (error) {
      outcome = { state: 'failed', error: `drain threw: ${String(error?.message ?? error)}` }
    }
    if (outcome === null || typeof outcome !== 'object' || typeof outcome.state !== 'string') {
      outcome = { state: 'failed', error: `unrecognized DrainOutcome ${JSON.stringify(outcome)}` }
    }
    if (outcome.state === 'conflicted') {
      if (conflicted === null) conflicted = outcome
    } else if (outcome.state === 'failed') {
      if (failed === null) failed = outcome
    } else if (outcome.state === 'queued' || outcome.state === 'queued_ahead') {
      if (queued === null) queued = outcome
    } else if (outcome.state === 'succeeded') {
      if (typeof outcome.integratedCommit === 'string') integratedCommit = outcome.integratedCommit
    } else if (outcome.state !== 'no_changes') {
      if (failed === null) failed = { state: 'failed', error: `unrecognized DrainOutcome state ${JSON.stringify(outcome.state)}` }
    }
    log.debug?.(`dag merge executor: ${taskId}@${runId} drain(${repoKey}, ${integrationBranch}) → ${outcome.state}`)
  }

  // ---- 5. map the DrainOutcome (five states, DESIGN §11.1).
  if (conflicted !== null) {
    const conflictFiles = Array.isArray(conflicted.conflictFiles) ? [...conflicted.conflictFiles] : []
    const retainedWorktrees = typeof conflicted.integrationWorktree === 'string' && conflicted.integrationWorktree.length > 0
      ? [conflicted.integrationWorktree]
      : []
    parkMergeConflict({
      store, runId, taskId, attemptId: attempt.attemptId, ownerToken,
      conflictFiles, retainedWorktrees, now: nowFn,
    })
    return 'parked'
  }
  if (failed !== null) {
    commit({ failureType: 'transient', code: 'dag.merge_failed', message: failed.error ?? 'merge failed' })
    return 'terminal'
  }
  if (queued !== null) {
    // m-5: `queued_ahead` is THE contract key (snake_case family) — the
    // camelCase `queuedAhead` compatibility branch was removed in M3; the
    // provider must name its count field `queued_ahead`.
    const ahead = queued.queued_ahead
    commit(
      {
        failureType: 'transient',
        code: 'dag.merge_queued_ahead',
        message: `merge jobs still queued behind other work on ${integrationBranch}${ahead !== undefined ? ` (queued_ahead: ${JSON.stringify(ahead)})` : ''} — re-draining on a later tick`,
      },
      {
        stopReason: 'internal',
        // Deliberately NOT decideRetry: a busy queue is not an execution
        // failure the spec's retryOn policy should gate — always re-poll,
        // lightly, outside the retryOn budget (manual:true stamp below).
        retryDecision: { retry: true, backoffMs: QUEUED_AHEAD_BACKOFF_MS },
        retryStamp: { manual: true },
      },
    )
    return 'terminal'
  }
  // succeeded | no_changes → terminal success; an empty integration is legal.
  const structured = specTask.outputs?.[0] !== undefined
    ? { integratedCommit, integrationBranch }
    : undefined
  // ---- M-B (M3 review): the verify completion gate on the merge success
  // path — the SAME §7.3 receipt substitution the agent path uses in
  // harvestSettled (no store read: the outputs row does not exist yet at
  // gate time; the pending structured value IS the receipt,
  // attempt-bound). Receipt VIEW (documented substitution): the merge
  // output is {integratedCommit, integrationBranch} — it carries no
  // `status` field, and the gate judges `status`. The merge's
  // status-equivalent fact is the integratedCommit (the identity of the
  // integration result), so the receipt view exposes
  // status = integratedCommit; the PERSISTED outputs row keeps the raw
  // shape untouched. An empty integration (no_changes → null commit) has
  // no string status → no receipt → the gate fails 'missing' (a verify
  // block demanding a commit on an empty integration should fail). A miss
  // hands the synthetic permanent dag.verify_gate_failed to the SAME
  // commitTerminalAndRelease machinery (retry policy applies on top).
  // No verify declaration → evidence 'none_declared', success untouched.
  let verifyGate = null
  if (typeof evaluateVerifyGate === 'function') {
    verifyGate = evaluateVerifyGate({
      specTask,
      runId,
      taskId,
      attemptId: attempt.attemptId,
      failure: null,
      outputsReader: (rid, tid, name) => {
        const declared = specTask.verify
        if (declared === undefined || tid !== taskId || name !== declared.expectOutput) {
          return null
        }
        if (structured === undefined || typeof structured.integratedCommit !== 'string') {
          return null
        }
        return {
          value_json: JSON.stringify({ ...structured, status: structured.integratedCommit }),
          produced_by_attempt: attempt.attemptId,
        }
      },
    })
  }
  const effectiveFailure = verifyGate === null ? null : verifyGate.effectiveFailure
  // attempt.succeeded payload stamp (terminal-commit's verifyStamp), the
  // harvestSettled shape: receipt passed → verifyStatus 'pass'; declared
  // but missed → nothing stamped (the failure event carries the code); no
  // verify declaration → evidence 'none_declared'.
  const verifyStamp = verifyGate === null || (verifyGate.receipt === null && verifyGate.evidence === null)
    ? undefined
    : verifyGate.receipt !== null
      ? { verifyStatus: 'pass', evidence: null }
      : { verifyStatus: null, evidence: verifyGate.evidence }
  commit(
    effectiveFailure,
    {
      // Verify-gate failure keeps stopReason 'completed' (the merge RAN to
      // completion; the CONTRACT failed — the same shape the agent path
      // lands through harvest's stopReason 'completed' + effectiveFailure).
      stopReason: 'completed',
      ...(structured !== undefined && effectiveFailure === null ? { structured } : {}),
      ...(verifyStamp !== undefined ? { verifyStamp } : {}),
    },
  )
  return 'terminal'
}
