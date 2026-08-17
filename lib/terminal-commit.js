/**
 * terminal-commit — per-attempt independent terminal commit (DESIGN §5.3,
 * TASKS.md T07).
 *
 * Ported from task-weaver packages/scheduler/src/terminal-commit.ts
 * (commitTerminalAndReleaseStandalone, L80-261) — "语义直搬" per DESIGN §9.2:
 * every ordering decision and transaction boundary is preserved; the
 * persistent-lease layer is swapped for the in-memory admission controller
 * (D11) and the sqlite store.
 *
 * Shape (unchanged from source):
 *   * Commit the Attempt terminal + CAS the Task terminal + emit ALL terminal
 *     events in ONE transaction, then release the agent slot / session key
 *     OUTSIDE the transaction (source L257-261: "terminal CAS inside txn;
 *     resource release is a side-effect after commit" — invariant #3).
 *   * Retry path (source L106-176, the Issue 5 fix, comment carried over):
 *     commitTerminal(failed) FIRST; only when it succeeds do we emit
 *     attempt.retry_scheduled and CAS task running→retry_wait.
 *   * Terminal path (source L177-254): attempt.<target> + task.<target>
 *     events + task CAS in one txn; success lands outputs rows (upsertOutput)
 *     in the SAME txn.
 *   * Cancelled semantics (dag_control stop path): attempt cancelled +
 *     task cancelled, same one-txn shape.
 *
 * Narrowing manifest (source line refs; everything else verbatim):
 *  1. `evaluateVerifyGate` seam (source L100) MOVED to the CALLER at T18 —
 *     the engine's harvestSettled evaluates the gate (receipt source = the
 *     attempt's pending structured output, DESIGN §7.3) and hands the
 *     result in as `effectiveFailure` (= the gate-synthesized permanent
 *     `dag.verify_gate_failed` on a miss) plus `verifyStamp` for the
 *     attempt.succeeded payload. Everything downstream of source L104 is
 *     the module's own shape: `failure` here IS the source's
 *     `verifyGate.effectiveFailure`.
 *  2. workspace/change projection (source L222-236) CUT — no workspace
 *     lineage system migrates (DESIGN §9.2).
 *  3. `admission.release(slotLeaseId)` / extra lease ids (source L257-261)
 *     → `admission.releaseSlot(slot)` + `releaseSessionKey(key, attemptId)`
 *     (D11 memory semaphore; owner-checked session-key release).
 *  4. The ownerId lease validation is the store's `commitTerminal(attemptId,
 *     ownerToken, target)` state+owner double CAS (dag-store.js).
 *
 * INVARIANT #1 (DESIGN §2.1): this function processes EXACTLY ONE attempt
 * per call. The engine's harvestSettled loop calls it once per settled
 * attempt — never a batched Promise.all commit.
 */

/**
 * @typedef {object} HarvestOutcomeFailure
 * @property {string} failureType permanent | transient | timeout | aborted | internal
 * @property {string} code
 * @property {string} message
 */

/**
 * @typedef {object} TerminalCommitInput
 * @property {object} store DagStore handle (the ONLY persistence outlet).
 * @property {object} admission createAdmission() handle (slot/sessionKey release).
 * @property {object} spec validated spec (task retry policy + declared outputs).
 * @property {{now: number}} clock injected time source.
 * @property {{taskId: string, attemptId: string, ordinal: number}} attempt handle
 * @property {{slot: number}} [slot] agent slot handle from admission.tryAcquireSlot
 * @property {string} [sessionKey] spec concurrencyKey when the task declared one
 * @property {string} ownerToken engine's in-memory in-flight handle id (store
 *           commitTerminal's owner CAS input).
 * @property {HarvestOutcomeFailure | null} failure harvest outcome failure (null = success)
 * @property {unknown} [structured] structured output (success path)
 * @property {string} [outputText] text-block summary (result_json evidence)
 * @property {string} [stopReason] classified stop reason
 * @property {{retry: boolean, backoffMs: number}} retryDecision engine.shouldRetry result
 * @property {'cancelled'|'succeeded'|'failed'} [forcedTaskTarget] cancelled path override
 * @property {{verifyStatus: string | null, evidence: string | null}} [verifyStamp]
 *           T18 attempt.succeeded payload stamp (source L200-208): the
 *           engine's verify-gate result — {verifyStatus: 'pass'} when a
 *           receipt passed the gate, {evidence: 'none_declared'} when the task
 *           declares no verify block, both null otherwise (nothing stamped).
 * @property {{manual?: boolean}} [retryStamp]
 *           T17 attempt.retry_scheduled payload stamp: {manual: true} marks a
 *           NON-policy retry (the merge queued_ahead re-poll) — the same
 *           budget-free family as recovery:true (§12.1: shouldRetry skips
 *           manual:true events when billing the retryOn budget).
 */

/**
 * Commit ONE attempt's terminal state (+ the owning task's transition + all
 * terminal events) in ONE transaction, then release resources OUTSIDE the
 * transaction. Returns a disposition describing what landed.
 *
 * @param {TerminalCommitInput} input
 * @returns {{kind: 'retry'|'terminal'|'lost', taskFrom?: string, taskTo?: string}}
 */
export function commitTerminalAndRelease(input) {
  const {
    store, admission, spec, clock, attempt, slot, sessionKey, ownerToken,
    failure, structured, outputText, stopReason, retryDecision, forcedTaskTarget,
    verifyStamp, retryStamp,
  } = input;
  const { runId } = input;
  const now = clock.now;
  const specTask = spec.tasks.find((t) => t.id === attempt.taskId);

  // result_json: compact evidence of what the attempt produced (summary, not
  // full logs — DESIGN §4.5 output storage note). No undefined keys.
  const resultJson = JSON.stringify({
    ...(structured !== undefined ? { structured } : {}),
    ...(outputText !== undefined ? { outputText } : {}),
    ...(stopReason !== undefined ? { stopReason } : {}),
  });
  const attemptPatch = {
    stop_reason: stopReason ?? null,
    failure_json: failure === null ? null : JSON.stringify(failure),
    result_json: resultJson === '{}' ? null : resultJson,
  };

  let disposition;

  if (retryDecision.retry) {
    // Retry path — source L106-176. Ordering is the Issue 5 fix (comment
    // carried over verbatim in substance):
    //   The prior code CAS'd the task to retry_wait BEFORE commitTerminal.
    //   If commitTerminal failed (e.g. lease expired), the transaction body
    //   returned without emitting attempt.retry_scheduled, but the task CAS
    //   had already committed inside the same transaction — leaving the task
    //   in retry_wait with no terminal attempt event, so hasNonTerminalAttempt
    //   stayed true forever (the attempt never reached failed) and the task
    //   wedged. Mirroring the terminal branch (commit first, CAS only when
    //   commit.ok) keeps the attempt's terminal state and the task's
    //   retry_wait transition atomic and leak-free.
    const retryNotBeforeMs = now + retryDecision.backoffMs;
    let landed = false;
    store.tx(() => {
      // Re-read the task INSIDE the transaction to get the current version
      // (C1 fix: reading outside the txn produced a stale version → CAS
      // silently failed → task stuck in running forever).
      const taskRow = store.findTasks(runId).find((t) => t.task_id === attempt.taskId);
      if (taskRow === undefined) return;
      // Commit the Attempt terminal (failed) with owner validation. This MUST
      // precede the task CAS so a failed commit does not leave the task in
      // retry_wait with a non-terminal attempt.
      const commit = store.commitTerminal(attempt.attemptId, ownerToken, 'failed', attemptPatch);
      if (!commit.ok) return;
      store.insertEvent(runId, {
        type: 'attempt.failed',
        taskId: attempt.taskId,
        attemptId: attempt.attemptId,
        payload: {
          from: 'running',
          to: 'failed',
          failureType: failure?.failureType ?? null,
          code: failure?.code ?? null,
          message: failure?.message ?? null,
        },
        at: now,
      });
      store.insertEvent(runId, {
        type: 'attempt.retry_scheduled',
        taskId: attempt.taskId,
        attemptId: attempt.attemptId,
        payload: {
          failedAttemptId: attempt.attemptId,
          nextAttemptNumber: attempt.ordinal + 1,
          retryNotBeforeMs,
          backoffMs: retryDecision.backoffMs,
          failureType: failure?.failureType ?? null,
          code: failure?.code ?? null,
          // T17: {manual:true} = a non-policy retry (merge queued_ahead
          // re-poll) — budget-free, the recovery:true family.
          ...(retryStamp?.manual === true ? { manual: true } : {}),
        },
        at: now,
      });
      // CAS the task to retry_wait ONLY after the attempt terminal commit
      // succeeded — keeps the task transition consistent with the attempt.
      const cas = store.casTaskState(
        runId, attempt.taskId, taskRow.state, taskRow.version, 'retry_wait',
        { retry_not_before: retryNotBeforeMs },
      );
      if (!cas.ok) return;
      store.insertEvent(runId, {
        type: 'task.retry_wait',
        taskId: attempt.taskId,
        payload: { from: taskRow.state, to: 'retry_wait', reason: 'retry_wait', retryNotBeforeMs, attemptNumber: attempt.ordinal },
        at: now,
      });
      landed = true;
    });
    disposition = landed ? { kind: 'retry', taskTo: 'retry_wait' } : { kind: 'lost' };
  } else {
    // Terminal path — source L177-254. attemptTarget/taskTarget derived from
    // the effective failure (null → succeeded) or the forced cancel override.
    const attemptTarget = forcedTaskTarget === 'cancelled' ? 'cancelled'
      : failure === null ? 'succeeded' : 'failed';
    const taskTarget = forcedTaskTarget ?? (failure === null ? 'succeeded' : 'failed');
    const declared = specTask?.outputs?.[0];
    const persistOutput = attemptTarget === 'succeeded'
      && declared !== undefined && structured !== undefined;
    let landed = false;
    store.tx(() => {
      // Re-read the task INSIDE the transaction (C1 fix).
      const taskRow = store.findTasks(runId).find((t) => t.task_id === attempt.taskId);
      if (taskRow === undefined) return;
      // Commit the Attempt terminal (validates owner token + state CAS).
      const commit = store.commitTerminal(attempt.attemptId, ownerToken, attemptTarget, attemptPatch);
      if (!commit.ok) return;
      store.insertEvent(runId, {
        type: `attempt.${attemptTarget}`,
        taskId: attempt.taskId,
        attemptId: attempt.attemptId,
        payload: {
          from: 'running',
          to: attemptTarget,
          failureType: failure?.failureType ?? null,
          code: failure?.code ?? null,
          ...(attemptTarget === 'succeeded'
            ? {
              evidence: structured !== undefined ? 'structured' : 'output_text_or_none',
              // T18 verify stamp (source L200-208): verifyStatus 'pass' when
              // a verify receipt passed the gate; evidence 'none_declared'
              // when the task declares no verify block. Both may be absent —
              // the stamp keys land only when non-null (json-safe).
              ...(verifyStamp?.verifyStatus != null ? { verifyStatus: verifyStamp.verifyStatus } : {}),
              ...(verifyStamp?.evidence != null ? { verifyEvidence: verifyStamp.evidence } : {}),
            }
            : { message: failure?.message ?? null }),
        },
        at: now,
      });
      // Success + declared output + structured present → outputs row in the
      // SAME txn (upsert keeps the producer idempotent across retries).
      if (persistOutput) {
        store.upsertOutput(runId, attempt.taskId, declared.name, JSON.stringify(structured), attempt.attemptId);
      }
      // CAS the Task terminal.
      const cas = store.casTaskState(runId, attempt.taskId, taskRow.state, taskRow.version, taskTarget);
      if (!cas.ok) return;
      store.insertEvent(runId, {
        type: `task.${taskTarget}`,
        taskId: attempt.taskId,
        payload: {
          from: taskRow.state,
          to: taskTarget,
          reason: taskTarget,
          failureType: failure?.failureType ?? null,
          attemptNumber: attempt.ordinal,
        },
        at: now,
      });
      landed = true;
    });
    disposition = landed ? { kind: 'terminal', taskTo: taskTarget } : { kind: 'lost' };
  }

  // Release the agent slot (+ session key) OUTSIDE the terminal txn
  // (source L257-261; invariant #3: terminal CAS inside txn, resource
  // release is a post-commit side effect).
  if (slot !== undefined) admission.releaseSlot(slot);
  if (sessionKey !== undefined) admission.releaseSessionKey(sessionKey, attempt.attemptId);

  return disposition;
}
