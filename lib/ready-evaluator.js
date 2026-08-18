/**
 * ready-evaluator — PURE `evaluateReady` over a TaskGraph snapshot.
 *
 * Ported from task-weaver packages/scheduler/src/ready-evaluator.ts (L1-444),
 * narrowed per DESIGN §9.2 row "ready-evaluator.ts → 直搬收窄" and TASKS.md
 * T05. The function is pure: it consumes a TaskGraphSnapshot + the validated
 * spec + an optional outputs resolver + an injectable `now`, and returns the
 * ready/blocked partition. It performs NO database I/O — the caller (the
 * engine's promoteReady, T07) persists promotions in ONE sqlite transaction
 * (invariant #3). This module does not import dag-store and uses no zod; the
 * spec is trusted to be T04-validated (JSDoc contract below).
 *
 * Ready source states (source module header, unchanged):
 *   - `pending` always eligible for evaluation
 *   - `retry_wait` only when `now >= task.retryNotBeforeMs` (backoff
 *     expired). If still waiting on backoff, the task is skipped (neither
 *     ready nor blocked — still in retry_wait until due).
 *   - other states are not re-evaluated here
 *
 * CORE INVARIANT (source Global Constraint #4, kept verbatim):
 *   `blocked` is NOT terminal and NOT `completed`. A `succeeded`/`completed`
 *   dependency condition does NOT treat an upstream `blocked` task as
 *   satisfied.
 *
 *     condition "succeeded" → upstream state MUST be "succeeded"
 *     condition "completed" → upstream state ∈ {succeeded, failed, cancelled}
 *
 * ─── Narrowing manifest (source line refs; everything else is verbatim) ───
 *  1. `evaluateGate` + `GateEvaluator` (source L81-122) REINSTATED at T18 —
 *     the five finite boolean operators and the `opts.gateEvaluator` seam
 *     are back (gate declarations reopened in spec-validate); the source's
 *     IGNORE-when-absent default is NARROWED to fail-closed here (source
 *     R-WP9 landed AFTER this file's M1 port as
 *     dependency_gate_evaluator_unavailable — the current source behavior).
 *  2. `ProfileResolver` + profile gate (source L124-131 / L339-354) CUT — no
 *     AgentProfile registry; delegation fields are flat spec fields (§7.1).
 *  3. `SandboxResolver` + strict-required gate (source L133-141 / L320-337)
 *     CUT — no SandboxProvider system (§7.1 "sandbox/substrate 砍").
 *  4. `classifyM1Scope` (source L169-198) CUT — this plugin's `kind` runtime
 *     gate lives in the engine/executor layer; the static kind matrix is
 *     spec-validate's job (T04). The evaluator is kind-blind by design.
 *  5. `isReadySource` park set narrowed to `approval_pending` (M2) +
 *     `merge_conflicted` (M3, T17); the source's `adapter_unavailable` /
 *     `review_pending` / `grant_revoked` codes have no counterparts in this
 *     plugin (no adapter / review / grant systems migrate — §9.2).
 *  6. `artifactResolver` → `opts.outputResolver` (task:// refs against the
 *     outputs table); blocked code `artifact_missing` renamed
 *     `dag.output_missing` (DESIGN §9.2 指定改名).
 *  7. The source's positional param tail (snapshot, spec, dead `_deps` map,
 *     artifactResolver, clock, profileResolver, gateEvaluator,
 *     sandboxResolver) collapsed into `(snapshot, spec, opts)`; the already
 *     unused `_deps` map is dropped and the injectable `Clock` becomes a
 *     plain `opts.now` epoch-ms number.
 */

/**
 * A single workflow-spec control dependency edge (source L43-53). The M3-WP5
 * optional `gate` field is REINSTATED at T18 (spec-validate reopens gate
 * declarations): `{ taskId, condition, gate? }` mirrors the spec's
 * validated dependsOn entry shape exactly.
 *
 * @typedef {object} DependencyEdge
 * @property {string} taskId
 * @property {'succeeded' | 'completed'} condition
 * @property {DependencyGate} [gate]
 */

/**
 * One dependency gate declaration (source DependencyGate, contracts
 * workflow-spec.ts L140-155): a FINITE boolean check over an upstream
 * output's content — five `expect` operators, no scripting surface.
 *
 * @typedef {object} DependencyGate
 * @property {string} artifact `task://<producer>/<name>` ref (the source's artifact:// DataRef renamed — §7.1).
 * @property {'exists' | 'not_exists' | 'contains' | 'not_contains' | 'equals'} expect
 * @property {string} [value] Required for contains/not_contains/equals; forbidden for exists/not_exists (spec-validate).
 */

/**
 * Optional callback that resolves a task-declared input ref
 * (`task://<taskId>/<name>`) to "present". Returns `true` when the referenced
 * output row exists in the outputs table, `false` when missing. The engine
 * wires this to the store's outputs lookup (T07; ref syntax per DESIGN
 * §7.2 — this plugin's replacement for the source's `artifact://` refs and
 * ArtifactService resolver). When omitted, tasks WITHOUT declared `inputs`
 * are not data-gated (tasks WITH inputs are treated as missing —
 * fail-closed).
 *
 * @typedef {(ref: string) => boolean} OutputResolver
 */

/**
 * T18: the gate-evaluator seam (source GateEvaluator L81). Resolves a
 * declared gate against the referenced output's CONTENT: `true` when the
 * gate passes (edge satisfied), `false` when not. The engine wires a
 * concrete evaluator that parses value_json from the outputs table; the
 * pure evaluator performs NO I/O itself (the evaluator is the ONLY seam
 * through which output content reaches it — source L79-80).
 *
 * Declared-but-unwired is FAIL-CLOSED (source R-WP9 / L387-399): the edge
 * blocks `dependency_gate_evaluator_unavailable` instead of silently
 * reducing to its base condition — a conditional dependency is never
 * admitted on an unevaluated gate.
 *
 * @typedef {(gate: DependencyGate) => boolean} GateEvaluator
 */

/**
 * Options bag — the source's positional parameter tail, collapsed.
 *
 * @typedef {object} EvaluateReadyOptions
 * @property {number} [now] Epoch-ms injected clock (source: injectable `Clock`; tests pass a fixed value). Defaults to `Date.now()`.
 * @property {OutputResolver} [outputResolver]
 * @property {GateEvaluator} [gateEvaluator]
 */

/**
 * Stable blocked-reason codes emitted by the evaluator (source L144-167,
 * narrowed). The two `dependency_gate_*` codes are LIVE since T18 (the
 * engine wires a concrete gateEvaluator over the outputs table); they were
 * exported as placeholders while the gate seam was trimmed in M1, which
 * kept the vocabulary stable across the activation.
 */
export const READY_BLOCKED_CODES = Object.freeze({
  upstreamBlocked: 'upstream_blocked',
  upstreamFailed: 'upstream_failed',
  upstreamCancelled: 'upstream_cancelled',
  upstreamNotSucceeded: 'upstream_not_succeeded',
  /**
   * Source `artifact_missing`, RENAMED `dag.output_missing` (DESIGN §9.2):
   * this plugin has no artifact store — inputs resolve against the outputs
   * table via `task://` refs.
   */
  outputMissing: 'dag.output_missing',
  /** M3 (T18): dependency gate did not pass (clear, non-terminal block). */
  dependencyGateNotMet: 'dependency_gate_not_met',
  /** M3 (T18): a `dependency.gate` is declared but no gateEvaluator is wired (fail-closed). */
  dependencyGateEvaluatorUnavailable: 'dependency_gate_evaluator_unavailable',
});

/**
 * Evaluate one dependency gate against the referenced output's content
 * (source L97-122 VERBATIM, artifact:// refs renamed task://). The five
 * `expect` operators over the JSON-stringified output value:
 *
 *   "exists"        → payload is not null
 *   "not_exists"    → payload is null
 *   "contains"      → JSON string contains `value` substring
 *   "not_contains"  → JSON string does NOT contain `value`
 *   "equals"        → JSON string === `value`
 *
 * This is a FINITE boolean check — there is NO scripting surface here
 * (source L95; the `expect` enum is closed in spec-validate). `readPayload`
 * is the injected read seam: `(ref) => unknown | null` returning the parsed
 * output value, or null when the row is missing/unparsable.
 *
 * @param {DependencyGate} gate
 * @param {(ref: string) => unknown | null} readPayload
 * @returns {boolean}
 */
export function evaluateGate(gate, readPayload) {
  const payload = readPayload(gate.artifact);
  switch (gate.expect) {
    case 'exists':
      return payload !== null;
    case 'not_exists':
      return payload === null;
    case 'contains': {
      if (payload === null) return false;
      return JSON.stringify(payload).includes(gate.value ?? '');
    }
    case 'not_contains': {
      if (payload === null) return true;
      return !JSON.stringify(payload).includes(gate.value ?? '');
    }
    case 'equals': {
      if (payload === null) return false;
      return JSON.stringify(payload) === (gate.value ?? '');
    }
    default:
      return false; // unknown operator: fail closed (spec-validate closes the enum)
  }
}

/**
 * Possible outcomes when checking one dependency edge against upstream state
 * (source L200-204).
 *
 * @typedef {{ outcome: 'succeeded' } | { outcome: 'waiting' } | { outcome: 'blocked', reason: string }} DepVerdict
 */

/**
 * Check one dependency edge against the upstream task's committed state
 * (source L206-225, VERBATIM — the in-progress/completed sets and the
 * three-state verdict table are unchanged). The CORE INVARIANT lives here:
 * `blocked` is NOT a completed state, so an upstream `blocked` task
 * satisfies neither condition and blocks the edge instead.
 *
 * @param {string} upstreamState
 * @param {'succeeded' | 'completed'} condition
 * @returns {DepVerdict}
 */
function upstreamSatisfies(upstreamState, condition) {
  const inProgress = new Set(['pending', 'ready', 'queued', 'running', 'retry_wait']);
  const completedSet = new Set(['succeeded', 'failed', 'cancelled']);

  if (condition === 'succeeded') {
    if (upstreamState === 'succeeded') return { outcome: 'satisfied' };
    if (inProgress.has(upstreamState)) return { outcome: 'waiting' };
    if (upstreamState === 'blocked') return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamBlocked };
    if (upstreamState === 'failed') return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamFailed };
    if (upstreamState === 'cancelled') return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamCancelled };
    return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamNotSucceeded };
  }
  if (completedSet.has(upstreamState)) return { outcome: 'satisfied' };
  if (inProgress.has(upstreamState)) return { outcome: 'waiting' };
  if (upstreamState === 'blocked') return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamBlocked };
  return { outcome: 'blocked', reason: READY_BLOCKED_CODES.upstreamNotSucceeded };
}

/**
 * Whether a task view is a ready-evaluation source at `now`
 * (source L227-263).
 *
 * - `pending` → always
 * - `retry_wait` → only when `retryNotBeforeMs` is set and `now >= retryNotBeforeMs`
 * - `blocked` → re-evaluate so upstream unblocks (e.g. upstream
 *   failed→succeeded after retry) can promote blocked→ready
 * - anything else → not re-evaluated here
 *
 * `retry_wait` with missing/future deadline is intentionally ignored (stays
 * in retry_wait; not listed as ready or blocked).
 *
 * @param {import('./types.js').TaskGraphTaskView} view
 * @param {number} now
 * @returns {boolean}
 */
function isReadySource(view, now) {
  if (view.state === 'pending') return true;
  // Re-evaluate blocked tasks so upstream unblocks can promote them — except
  // parks that must stay blocked until an EXTERNAL action releases them
  // (source comment semantics preserved, park set narrowed — see manifest
  // item #5):
  // - `approval_pending` (M2): human approval parks stay blocked until
  //   dag_approve + the engine's reconcileApprovals promote them;
  //   re-promoting an approval-parked task would immediately re-park it in
  //   a tight loop.
  // - `merge_conflicted` (M3, T17): a conflicted merge node stays blocked
  //   until a human resolves the worktree queue and dag_control retry_task
  //   re-runs it; re-promoting would re-enqueue the same conflict in a loop.
  // The source additionally excluded `adapter_unavailable` (circuit-breaker
  // park), `review_pending` (human review park) and `grant_revoked`
  // (execution-grant park) — none of those systems migrate to this plugin
  // (DESIGN §9.2), so the codes cannot occur and are trimmed from the
  // exclusion set. Mirrored in the engine tick's `waiting_on: 'approval'`
  // probe (T07/T12).
  if (view.state === 'blocked') {
    return (
      view.blockedReasonCode !== 'approval_pending' &&
      view.blockedReasonCode !== 'merge_conflicted'
    );
  }
  if (view.state === 'retry_wait') {
    const notBefore = view.retryNotBeforeMs;
    return notBefore !== undefined && now >= notBefore;
  }
  return false;
}

/**
 * Evaluate the ready/blocked partition of a TaskGraph snapshot.
 * PURE — no DB I/O. See module header for the full invariant rationale.
 *
 * Spec shape (JSDoc contract; T04's validated WorkflowSpec is a superset —
 * only these fields are read; `dependsOn`/`inputs` may be omitted):
 *   { tasks: [{ id, dependsOn?: [{ taskId, condition }], inputs?: string[] }] }
 *
 * @param {import('./types.js').TaskGraphSnapshot} snapshot
 * @param {{ tasks: ReadonlyArray<{ id: string, dependsOn?: ReadonlyArray<DependencyEdge>, inputs?: ReadonlyArray<string> }> }} spec
 * @param {EvaluateReadyOptions} [opts]
 * @returns {import('./types.js').ReadyEvaluation}
 */
export function evaluateReady(snapshot, spec, opts = {}) {
  const now = opts.now ?? Date.now();
  const outputResolver = opts.outputResolver;
  const gateEvaluator = opts.gateEvaluator;
  const viewById = new Map();
  const stateById = new Map();
  for (const t of snapshot.tasks) {
    viewById.set(t.taskId, t);
    stateById.set(t.taskId, t.state);
  }

  const readyTaskIds = [];
  const blocked = [];

  for (const task of spec.tasks) {
    const taskId = task.id;
    const view = viewById.get(task.id);
    if (view === undefined) continue;
    if (!isReadySource(view, now)) continue;

    // [M3 gate 回归 marker] The source ran three further gates at this call
    // site, all cut here (narrowing manifest #2-#4):
    //   - classifyM1Scope kind gate (source L314-318): this plugin's `kind`
    //     runtime gate lives in the engine/executor layer — the evaluator is
    //     kind-blind by design.
    //   - sandboxResolver strict-required gate (source L325-337,
    //     fail-closed): no SandboxProvider system migrates.
    //   - profileResolver agentProfile gate (source L343-354,
    //     `profile_not_found`): no profile registry.

    let controlBlocked = null;
    let controlWaiting = false;
    for (const dep of task.dependsOn ?? []) {
      const upstreamState = stateById.get(dep.taskId);
      if (upstreamState === undefined) {
        controlBlocked = {
          code: READY_BLOCKED_CODES.upstreamNotSucceeded,
          details: { taskId: task.id, upstreamTaskId: dep.taskId, reason: 'upstream task missing from snapshot' },
        };
        break;
      }
      const verdict = upstreamSatisfies(upstreamState, dep.condition);
      if (verdict.outcome === 'blocked') {
        controlBlocked = {
          code: verdict.reason,
          details: { taskId: task.id, upstreamTaskId: dep.taskId, upstreamState, condition: dep.condition },
        };
        break;
      }
      if (verdict.outcome === 'waiting') {
        controlWaiting = true;
        continue;
      }
      // verdict.outcome === 'satisfied' — base condition met. T18 (source
      // L391-411): if the dependency additionally declares a `gate`,
      // evaluate it NOW (only after the upstream reached the required
      // state). The gate is a finite boolean check over the upstream
      // output's content; a failed gate is a clear, NON-TERMINAL block —
      // the task stays blocked and is re-evaluated on subsequent ticks
      // (e.g. a new upstream attempt may produce different content).
      //
      // R-WP9 / fail-closed (source L387-399): a DECLARED gate with NO
      // gateEvaluator wired does NOT silently reduce to its base condition —
      // it blocks `dependency_gate_evaluator_unavailable` so a conditional
      // dependency is never admitted on an unevaluated gate.
      const gate = dep.gate;
      if (gate !== undefined && gate !== null) {
        if (gateEvaluator === undefined) {
          controlBlocked = {
            code: READY_BLOCKED_CODES.dependencyGateEvaluatorUnavailable,
            details: { taskId: task.id, upstreamTaskId: dep.taskId, gate },
          };
          break;
        }
        if (!gateEvaluator(gate)) {
          controlBlocked = {
            code: READY_BLOCKED_CODES.dependencyGateNotMet,
            details: { taskId: task.id, upstreamTaskId: dep.taskId, gate },
          };
          break;
        }
      }
    }
    if (controlBlocked !== null) {
      blocked.push({ taskId, reason: controlBlocked });
      continue;
    }
    if (controlWaiting) continue;

    const inputs = task.inputs ?? [];
    if (inputs.length > 0) {
      if (outputResolver === undefined) {
        // Fail-closed (source L419-429): a task declaring inputs with no
        // resolver wired is treated as missing — never admitted blind.
        blocked.push({
          taskId,
          reason: {
            code: READY_BLOCKED_CODES.outputMissing,
            details: { taskId: task.id, inputs: [...inputs], reason: 'no output resolver wired' },
          },
        });
        continue;
      }
      const missing = inputs.filter((ref) => !outputResolver(ref));
      if (missing.length > 0) {
        blocked.push({
          taskId,
          reason: { code: READY_BLOCKED_CODES.outputMissing, details: { taskId: task.id, missingInputs: missing } },
        });
        continue;
      }
    }

    readyTaskIds.push(taskId);
  }

  return { runId: snapshot.runId, readyTaskIds, blockedTasks: blocked };
}
