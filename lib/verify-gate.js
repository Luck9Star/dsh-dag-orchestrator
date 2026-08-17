/**
 * verify-gate — the verify completion gate (DESIGN §7.3, TASKS.md T18).
 *
 * Direct port of task-weaver packages/scheduler/src/verify-gate.ts
 * (evaluateVerifyGate L79-115 + findVerifyReceipt L45-64) with the ONE
 * sanctioned substitution — the RECEIPT SOURCE (DESIGN §7.3 "receipt 源从
 * artifacts 换 outputs 表"): the source read `test-report-v1` artifacts from
 * the artifact store (written mid-attempt by its VerifyService); this plugin
 * has no artifact store and no verify executor, so the receipt is the
 * attempt's own structured output in the outputs table, checked against the
 * task's `verify: {expectOutput, expectStatus}` contract block. Both helpers
 * stay read-only pure queries over an injected `outputsReader` port — no
 * writes, no throws.
 *
 * domain-model invariant #3 ("process exit != Task success"), carried over
 * from the source L66-78: a task that declares `verify` may NOT reach
 * `succeeded` unless THIS attempt produced the expected output with
 * `value.status === expectStatus`. A miss synthesizes a PERMANENT failure so
 * the existing retry/fail machinery handles routing — no new terminal state
 * is invented (源哲学: verify 门合成失败进 retry/fail 机器).
 */

/**
 * Receipt lookup port: resolve one outputs-table row shape.
 * `(runId, taskId, name) → {value_json, produced_by_attempt} | null` —
 * engine-backed (the harvest's pending structured value; see the wiring
 * note in engine.js harvestSettled) or literal rows in tests.
 *
 * @typedef {(runId: string, taskId: string, name: string) => {value_json: string, produced_by_attempt: string} | null} OutputsReader
 */

/**
 * The verify receipt (source L28 shape, fields renamed to the outputs-table
 * reality: the source's artifactId/contentHash become the producing attempt
 * id — the row identity this plugin stamps).
 *
 * @typedef {object} VerifyReceipt
 * @property {string} status The parsed output value's `status` field.
 * @property {string} producedByAttempt The attempt that produced the row.
 */

/**
 * Result of the verify-gate evaluation (source VerifyGateResult L24-31).
 *
 * @typedef {object} VerifyGateResult
 * @property {{failureType: string, code: string, message: string} | null} effectiveFailure The effective failure (original or synthesized by the gate). Null = success.
 * @property {VerifyReceipt | null} receipt The verify receipt when a passing output was found.
 * @property {string | null} evidence Evidence annotation for the success path ('none_declared' when the task declares no verify block).
 */

/**
 * Read-only: find the `expectOutput` row for a specific attempt, parse its
 * value, and return the status + producing attempt. Returns `null` when no
 * row exists, the row belongs to ANOTHER attempt, or the value carries no
 * string `status` (source L45-64: filter → latest → parse; the outputs
 * table is keyed (run, task, name) so at most one row exists and the
 * source's createdAt sort collapses to the direct read).
 *
 * The attempt binding (source L52 `a.attemptId === attemptId`) is the core:
 * an output another attempt produced never satisfies THIS attempt's gate.
 *
 * @param {{outputsReader: OutputsReader, runId: string, taskId: string, attemptId: string, expectOutput: string}} input
 * @returns {VerifyReceipt | null}
 */
export function findVerifyReceipt({ outputsReader, runId, taskId, attemptId, expectOutput }) {
  if (typeof outputsReader !== 'function') return null;
  const row = outputsReader(runId, taskId, expectOutput);
  if (row === null || row === undefined) return null;
  // Attempt binding — source L52. A prior attempt's row (upserted under the
  // same (run, task, name) key) must not satisfy the current attempt's gate.
  if (row.produced_by_attempt !== attemptId) return null;
  let value;
  try {
    value = JSON.parse(row.value_json);
  } catch {
    return null; // source L58: unparsable payload → no receipt
  }
  if (value === null || typeof value !== 'object' || typeof value.status !== 'string') {
    return null;
  }
  return { status: value.status, producedByAttempt: row.produced_by_attempt };
}

/**
 * Evaluate the verify completion gate for an attempt about to reach terminal
 * (source L79-115, structure verbatim):
 *
 *   1. failure !== null      → passthrough (only the SUCCESS path is gated;
 *                              an existing failure already disqualifies —
 *                              source L86-89). This includes the executor's
 *                              harvest-level contract failures
 *                              (dag.missing_output / dag.output_schema_violated):
 *                              they arrive as failure and pass through
 *                              UNCHANGED, so the verify gate never
 *                              double-reports them.
 *   2. no verify declaration → success proceeds, evidence 'none_declared'
 *                              (source L93-95).
 *   3. receipt found with status === expectStatus → success with the
 *                              receipt carried for the event stamp
 *                              (source L97-100).
 *   4. otherwise             → synthesize PERMANENT `dag.verify_gate_failed`
 *                              (message names 'missing' or the actual
 *                              status — source L102-114).
 *
 * @param {{specTask?: {verify?: {expectOutput: string, expectStatus: string}}, outputsReader: OutputsReader, runId: string, taskId: string, attemptId: string, failure: {failureType: string, code: string, message: string} | null}} input
 * @returns {VerifyGateResult}
 */
export function evaluateVerifyGate({ specTask, outputsReader, runId, taskId, attemptId, failure }) {
  // Only gate on the success path — an existing failure already disqualifies
  // (source L86-89).
  if (failure !== null && failure !== undefined) {
    return { effectiveFailure: failure, receipt: null, evidence: null };
  }

  const verifyBlock = specTask?.verify;
  if (verifyBlock === undefined || verifyBlock === null) {
    return { effectiveFailure: null, receipt: null, evidence: 'none_declared' };
  }

  const receipt = findVerifyReceipt({
    outputsReader, runId, taskId, attemptId, expectOutput: verifyBlock.expectOutput,
  });
  if (receipt !== null && receipt.status === verifyBlock.expectStatus) {
    return { effectiveFailure: null, receipt, evidence: null };
  }

  const reason = receipt === null ? 'missing' : receipt.status; // source L102
  return {
    effectiveFailure: {
      failureType: 'permanent',
      code: 'dag.verify_gate_failed',
      message: `verify receipt ${reason}: task declares verify {expectOutput: "${verifyBlock.expectOutput}", expectStatus: "${verifyBlock.expectStatus}"} but no passing receipt is bound to this attempt (${reason === 'missing' ? 'no matching output produced by this attempt' : `actual status "${reason}"`})`,
    },
    receipt: null,
    evidence: null,
  };
}
