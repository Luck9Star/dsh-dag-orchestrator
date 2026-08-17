/**
 * T18 — verify contract gate (DESIGN §7.3 / TASKS.md T18 acceptance).
 *
 * Three layers:
 *   1. verify-gate pure semantics — the direct-port table (source
 *      verify-gate.ts L79-115): failure passthrough / none_declared /
 *      receipt pass / receipt missing / status mismatch / THIS-attempt
 *      binding (produced_by_attempt !== attemptId → missing).
 *   2. evaluateGate five operators (ready-evaluator.ts L97-122) — the
 *      full exists/not_exists/contains/not_contains/equals × payload
 *      null/matching/non-matching matrix; plus the evaluateReady seam:
 *      declared gate + no evaluator → fail-closed
 *      dependency_gate_evaluator_unavailable; evaluator wired + gate
 *      failing → dependency_gate_not_met; passing → edge satisfied.
 *   3. engine end-to-end (real sqlite + real executor on fake
 *      ctx.subagents): a verify-declared task succeeds only when its
 *      structured output carries status === expectStatus; a mismatch →
 *      permanent dag.verify_gate_failed, task failed, downstream blocked;
 *      the harvest-level missing_output stays the executor's own failure
 *      (verify gate passthrough — never double-reported); and spec-validate
 *      gate shape/structure negatives.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { evaluateVerifyGate, findVerifyReceipt } from '../lib/verify-gate.js'
import { evaluateGate, evaluateReady, READY_BLOCKED_CODES } from '../lib/ready-evaluator.js'
import { validateSpec, DAG_SPEC_ERROR_CODES } from '../lib/spec-validate.js'
import { createDagStore } from '../lib/dag-store.js'
import { createAdmission } from '../lib/admission.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

const RUN_ID = 'run_vg'
const TASK_ID = 'verify'
const ATTEMPT = 'att-1'
const OTHER_ATTEMPT = 'att-0'

/** Literal outputs table port: rows keyed `${taskId}/${name}`. */
const readerFrom = (rows) => (runId, taskId, name) => rows[`${runId}/${taskId}/${name}`] ?? null

const row = (value, attempt = ATTEMPT) => ({ value_json: JSON.stringify(value), produced_by_attempt: attempt })

const SPEC_TASK = { id: TASK_ID, kind: 'agent', verify: { expectOutput: 'report', expectStatus: 'passed' } }

// ---------------------------------------------------------------------------
// 1. evaluateVerifyGate — the direct-port semantics table
// ---------------------------------------------------------------------------

test('gate: failure passthrough — a non-null failure returns UNCHANGED (only the success path is gated)', () => {
  const original = { failureType: 'permanent', code: 'dag.output_schema_violated', message: 'boom' }
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => row({ status: 'passed' }),
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: original,
  })
  assert.equal(out.effectiveFailure, original)
  assert.equal(out.receipt, null)
  assert.equal(out.evidence, null)
})

test('gate: missing_output arrives as a failure and passes through UNCHANGED (executor layer wins; verify gate never double-reports)', () => {
  // The ORDER question from the task brief: the executor's harvest
  // missing_output IS a permanent failure BEFORE the gate ever runs —
  // evaluateVerifyGate receives failure !== null and returns it verbatim,
  // so the executor's own code (not dag.verify_gate_failed) lands.
  const missing = { failureType: 'permanent', code: 'dag.missing_output', message: 'declares output but no structured value' }
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => null,
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: missing,
  })
  assert.equal(out.effectiveFailure, missing)
  assert.equal(out.effectiveFailure.code, 'dag.missing_output')
})

test('gate: no verify declaration → success with evidence "none_declared"', () => {
  const out = evaluateVerifyGate({
    specTask: { id: TASK_ID, kind: 'agent' }, outputsReader: () => null,
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: null,
  })
  assert.equal(out.effectiveFailure, null)
  assert.equal(out.receipt, null)
  assert.equal(out.evidence, 'none_declared')
})

test('gate: declaration + receipt passing (status === expectStatus) → success with the receipt carried', () => {
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => row({ status: 'passed', failedFiles: [] }),
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: null,
  })
  assert.equal(out.effectiveFailure, null)
  assert.deepEqual(out.receipt, { status: 'passed', producedByAttempt: ATTEMPT })
  assert.equal(out.evidence, null)
})

test('gate: declaration + receipt MISSING (no row / outputs empty) → permanent dag.verify_gate_failed, message names "missing"', () => {
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => null,
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: null,
  })
  assert.deepEqual(
    { type: out.effectiveFailure.failureType, code: out.effectiveFailure.code },
    { type: 'permanent', code: 'dag.verify_gate_failed' },
  )
  assert.match(out.effectiveFailure.message, /missing/)
  assert.equal(out.receipt, null)
})

test('gate: declaration + receipt present but status ≠ expectStatus → failed, message carries the ACTUAL status', () => {
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => row({ status: 'failed' }),
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: null,
  })
  assert.equal(out.effectiveFailure.code, 'dag.verify_gate_failed')
  assert.equal(out.effectiveFailure.failureType, 'permanent')
  assert.match(out.effectiveFailure.message, /failed/)
  assert.doesNotMatch(out.effectiveFailure.message, /missing/)
})

test('gate: THIS-attempt binding — a row produced by ANOTHER attempt does not satisfy the gate (→ missing)', () => {
  // Source L52 `a.attemptId === attemptId`: the outputs table is keyed
  // (run, task, name) so a prior attempt's upserted row is what a naive
  // reader would return; the binding check must reject it.
  const out = evaluateVerifyGate({
    specTask: SPEC_TASK, outputsReader: () => row({ status: 'passed' }, OTHER_ATTEMPT),
    runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, failure: null,
  })
  assert.equal(out.effectiveFailure.code, 'dag.verify_gate_failed')
  assert.match(out.effectiveFailure.message, /missing/)
  assert.equal(out.receipt, null)
})

test('gate: unparsable value_json or a non-string status → no receipt (missing)', () => {
  for (const bad of [
    { value_json: 'not-json{', produced_by_attempt: ATTEMPT },
    row({ noStatus: true }),
    row({ status: 42 }),
    row(null),
  ]) {
    const receipt = findVerifyReceipt({
      outputsReader: () => bad, runId: RUN_ID, taskId: TASK_ID,
      attemptId: ATTEMPT, expectOutput: 'report',
    })
    assert.equal(receipt, null, `bad row ${JSON.stringify(bad)} → no receipt`)
  }
})

test('gate: findVerifyReceipt resolves by (runId, taskId, expectOutput) through the reader port', () => {
  const rows = readerFrom({ [`${RUN_ID}/${TASK_ID}/report`]: row({ status: 'passed' }) })
  assert.deepEqual(
    findVerifyReceipt({ outputsReader: rows, runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, expectOutput: 'report' }),
    { status: 'passed', producedByAttempt: ATTEMPT },
  )
  // Different name → null.
  assert.equal(
    findVerifyReceipt({ outputsReader: rows, runId: RUN_ID, taskId: TASK_ID, attemptId: ATTEMPT, expectOutput: 'other' }),
    null,
  )
})

// ---------------------------------------------------------------------------
// 2. evaluateGate — the five finite boolean operators (source L97-122)
// ---------------------------------------------------------------------------

const readFrom = (payload) => (_ref) => payload

test('evaluateGate: exists / not_exists × payload null/present', () => {
  assert.equal(evaluateGate({ artifact: 'task://p/o', expect: 'exists' }, readFrom(null)), false)
  assert.equal(evaluateGate({ artifact: 'task://p/o', expect: 'exists' }, readFrom({ a: 1 })), true)
  assert.equal(evaluateGate({ artifact: 'task://p/o', expect: 'not_exists' }, readFrom(null)), true)
  assert.equal(evaluateGate({ artifact: 'task://p/o', expect: 'not_exists' }, readFrom({ a: 1 })), false)
})

test('evaluateGate: contains — JSON string substring, null payload → false', () => {
  const gate = { artifact: 'task://p/o', expect: 'contains', value: 'passed' }
  assert.equal(evaluateGate(gate, readFrom({ status: 'passed' })), true)
  assert.equal(evaluateGate(gate, readFrom({ status: 'failed' })), false)
  assert.equal(evaluateGate(gate, readFrom(null)), false)
})

test('evaluateGate: not_contains — inverse, null payload → TRUE (nothing to contain)', () => {
  const gate = { artifact: 'task://p/o', expect: 'not_contains', value: 'passed' }
  assert.equal(evaluateGate(gate, readFrom({ status: 'failed' })), true)
  assert.equal(evaluateGate(gate, readFrom({ status: 'passed' })), false)
  assert.equal(evaluateGate(gate, readFrom(null)), true)
})

test('evaluateGate: equals — JSON string identity (compact, key order as serialized)', () => {
  const gate = { artifact: 'task://p/o', expect: 'equals', value: '{"status":"passed"}' }
  assert.equal(evaluateGate(gate, readFrom({ status: 'passed' })), true)
  assert.equal(evaluateGate(gate, readFrom({ status: 'failed' })), false)
  // Key order matters — JSON.stringify is the comparison basis (source L117).
  assert.equal(evaluateGate(gate, readFrom({ status: 'passed', extra: 1 })), false)
  assert.equal(evaluateGate(gate, readFrom(null)), false)
})

test('evaluateGate: unknown expect operator → false (fail closed; spec-validate closes the enum)', () => {
  assert.equal(evaluateGate({ artifact: 'task://p/o', expect: 'exec_js' }, readFrom({ a: 1 })), false)
})

// ---------------------------------------------------------------------------
// 2b. evaluateReady — the gateEvaluator seam (source L391-411 + R-WP9)
// ---------------------------------------------------------------------------

const gateSpec = (gate) => ({
  tasks: [
    { id: 'a', outputs: [{ name: 'o', schema: { type: 'object' } }] },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded', gate }] },
  ],
})
const gateSnap = { runId: RUN_ID, tasks: [
  { taskId: 'a', state: 'succeeded', version: 1 },
  { taskId: 'b', state: 'pending', version: 1 },
] }

test('evaluateReady: declared gate + gateEvaluator ABSENT → fail-closed dependency_gate_evaluator_unavailable', () => {
  const r = evaluateReady(gateSnap, gateSpec({ artifact: 'task://a/o', expect: 'exists' }))
  assert.equal(r.readyTaskIds.includes('b'), false)
  const blocked = r.blockedTasks.find((x) => x.taskId === 'b')
  assert.equal(blocked.reason.code, READY_BLOCKED_CODES.dependencyGateEvaluatorUnavailable)
  assert.deepEqual(blocked.reason.details, {
    taskId: 'b', upstreamTaskId: 'a', gate: { artifact: 'task://a/o', expect: 'exists' },
  })
})

test('evaluateReady: gateEvaluator wired + gate NOT met → dependency_gate_not_met (non-terminal block)', () => {
  const r = evaluateReady(gateSnap, gateSpec({ artifact: 'task://a/o', expect: 'exists' }), {
    gateEvaluator: () => false,
  })
  assert.equal(r.readyTaskIds.includes('b'), false)
  const blocked = r.blockedTasks.find((x) => x.taskId === 'b')
  assert.equal(blocked.reason.code, READY_BLOCKED_CODES.dependencyGateNotMet)
})

test('evaluateReady: gateEvaluator wired + gate met → edge satisfied, task ready', () => {
  const r = evaluateReady(gateSnap, gateSpec({ artifact: 'task://a/o', expect: 'equals', value: '{"ok":true}' }), {
    gateEvaluator: (gate) => evaluateGate(gate, readFrom({ ok: true })),
  })
  assert.deepEqual(r.readyTaskIds, ['b'])
  assert.deepEqual(r.blockedTasks, [])
})

test('evaluateReady: gate NOT evaluated while the base condition is unsatisfied (waiting → skipped, no block)', () => {
  // The gate runs ONLY on the satisfied branch (source L379-385): an
  // upstream still running must leave the task waiting, not gate-blocked.
  let evaluated = 0
  const r = evaluateReady(
    { runId: RUN_ID, tasks: [{ taskId: 'a', state: 'running', version: 1 }, { taskId: 'b', state: 'pending', version: 1 }] },
    gateSpec({ artifact: 'task://a/o', expect: 'exists' }),
    { gateEvaluator: () => { evaluated += 1; return true } },
  )
  assert.equal(r.readyTaskIds.includes('b'), false)
  assert.deepEqual(r.blockedTasks, [])
  assert.equal(evaluated, 0, 'the gate must not run before the base condition is satisfied')
})

test('evaluateReady: a gate on a completed-condition edge evaluates once the upstream completed', () => {
  const spec = { tasks: [
    { id: 'a', outputs: [{ name: 'o', schema: { type: 'object' } }] },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'completed', gate: { artifact: 'task://a/o', expect: 'not_contains', value: 'failed' } }] },
  ] }
  const r = evaluateReady(gateSnap, spec, {
    gateEvaluator: (gate) => evaluateGate(gate, readFrom({ status: 'passed' })),
  })
  assert.deepEqual(r.readyTaskIds, ['b'])
})

// ---------------------------------------------------------------------------
// 3. spec-validate — gate shape + structure (T18 reopened)
// ---------------------------------------------------------------------------

const VG = DAG_SPEC_ERROR_CODES
const PRODUCER = () => ({
  id: 'a', kind: 'agent', prompt: 'p',
  outputs: [{ name: 'report', schema: { type: 'object' } }],
})
const gateSpecJson = (gate) => ({
  version: 1, name: 'gate-spec',
  tasks: [PRODUCER(), { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'succeeded', gate }] }],
})

test('spec-validate: a well-formed gate passes and normalizes through', () => {
  for (const gate of [
    { artifact: 'task://a/report', expect: 'exists' },
    { artifact: 'task://a/report', expect: 'contains', value: 'passed' },
  ]) {
    const r = validateSpec(gateSpecJson(gate))
    assert.equal(r.ok, true, `${JSON.stringify(gate)}: ${JSON.stringify(r.errors)}`)
    assert.deepEqual(r.value.tasks[1].dependsOn[0].gate, gate)
  }
})

test('spec-validate: gate.artifact not a task:// URI → dag.schema_invalid', () => {
  const r = validateSpec(gateSpecJson({ artifact: 'artifact://a/report', expect: 'exists' }))
  assert.equal(r.ok, false)
  assert.ok(r.errors.some((e) => e.code === VG.schemaInvalid && e.path === 'tasks[1].dependsOn[0].gate.artifact'))
})

test('spec-validate: gate.expect outside the five operators → dag.schema_invalid naming the finite set', () => {
  const r = validateSpec(gateSpecJson({ artifact: 'task://a/report', expect: 'matches_regex' }))
  const hit = r.errors.find((e) => e.path === 'tasks[1].dependsOn[0].gate.expect')
  assert.equal(hit.code, VG.schemaInvalid)
  assert.match(hit.message, /exists.*not_exists.*contains.*not_contains.*equals/)
})

test('spec-validate: gate.value wrong type (number) → dag.schema_invalid', () => {
  const r = validateSpec(gateSpecJson({ artifact: 'task://a/report', expect: 'contains', value: 42 }))
  assert.ok(r.errors.some((e) => e.code === VG.schemaInvalid && e.path === 'tasks[1].dependsOn[0].gate.value'))
})

test('spec-validate: contains/not_contains/equals without value → dag.gate_value_required (source L541-564)', () => {
  for (const expect of ['contains', 'not_contains', 'equals']) {
    const r = validateSpec(gateSpecJson({ artifact: 'task://a/report', expect }))
    const hit = r.errors.find((e) => e.code === VG.gateValueRequired)
    assert.ok(hit !== undefined, `${expect} without value must be rejected`)
    assert.equal(hit.path, 'tasks[1].dependsOn[0].gate.value')
  }
})

test('spec-validate: exists/not_exists WITH value → dag.gate_value_forbidden (source L566-584)', () => {
  for (const expect of ['exists', 'not_exists']) {
    const r = validateSpec(gateSpecJson({ artifact: 'task://a/report', expect, value: 'x' }))
    const hit = r.errors.find((e) => e.code === VG.gateValueForbidden)
    assert.ok(hit !== undefined, `${expect} with value must be rejected`)
    assert.equal(hit.path, 'tasks[1].dependsOn[0].gate.value')
  }
})

test('spec-validate: gate.artifact producer NOT the edge upstream → dag.gate_artifact_not_reachable (inputs rule family)', () => {
  // Producer exists + declares the output, but the gate hangs on the a-edge
  // while reading a SIBLING's output — rejected.
  const spec = {
    version: 1, name: 'gate-sibling',
    tasks: [
      PRODUCER(),
      { id: 'c', kind: 'agent', prompt: 'p', outputs: [{ name: 'report', schema: { type: 'object' } }] },
      {
        id: 'b', kind: 'agent', prompt: 'p',
        dependsOn: [{ taskId: 'a', condition: 'succeeded', gate: { artifact: 'task://c/report', expect: 'exists' } }],
      },
    ],
  }
  const r = validateSpec(spec)
  const hit = r.errors.find((e) => e.code === VG.gateArtifactNotReachable)
  assert.ok(hit !== undefined)
  assert.equal(hit.path, 'tasks[2].dependsOn[0].gate.artifact')
  assert.match(hit.message, /must be the dependency's own upstream/)
})

test('spec-validate: gate.artifact unknown producer / undeclared output → rejected', () => {
  const unknownProducer = validateSpec(gateSpecJson({ artifact: 'task://ghost/report', expect: 'exists' }))
  assert.ok(unknownProducer.errors.some((e) => e.code === VG.gateArtifactNotReachable))
  const undeclared = validateSpec(gateSpecJson({ artifact: 'task://a/other', expect: 'exists' }))
  assert.ok(undeclared.errors.some((e) => e.code === VG.outputNotDeclared && e.path === 'tasks[1].dependsOn[0].gate.artifact'))
})

// ---------------------------------------------------------------------------
// 4. engine end-to-end (real store + real executor, fake ctx.subagents)
// ---------------------------------------------------------------------------

function fakeSubagents() {
  const script = []
  const calls = []
  let counter = 0
  return {
    script, calls,
    async start(name, request) {
      counter += 1
      calls.push({ name, request })
      const behavior = script.length > 0 ? script.shift() : { resolve: { output: [], stopReason: 'completed' } }
      return { id: `sess-${counter}`, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

async function makeHarness(spec) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'vg-')), 'dag.db') })
  const subagents = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subagents, execAgentProvider: () => ({ __live: 'agent' }) })
  const clock = { now: () => 1_000_000 }
  const engine = createEngine({
    store, executor, admission: createAdmission(), logger: {},
    now: clock.now, random: () => 0.5,
  })
  const runId = engine.planRun(spec, { baseCwd: '/tmp/repo', runId: 'run-vg' }).runId
  const states = () => Object.fromEntries(store.findTasks(runId).map((t) => [t.task_id, t.state]))
  const events = () => store.findEvents(runId).map((e) => ({ type: e.type, payload: JSON.parse(e.payload_json), taskId: e.task_id }))
  return { store, subagents, engine, runId, states, events, close() { engine.disposeAll(); store.close() } }
}

const REPORT_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    status: { type: 'string' },
    failedFiles: { type: 'array', items: { type: 'string' } },
  },
  required: ['status'],
}

test('engine e2e: verify-declared task produces status "passed" → succeeded, attempt.succeeded stamped verifyStatus pass', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'vg-pass',
    tasks: [{
      id: 'verify', kind: 'agent', prompt: 'run tests and report',
      outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
      verify: { expectOutput: 'report', expectStatus: 'passed' },
    }],
  })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'passed', failedFiles: [] }, stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.deepEqual(h.states(), { verify: 'succeeded' })

  // Output landed (gate passed → terminal-commit upserts in the same tx).
  const out = h.store.findOutput(h.runId, 'verify', 'report')
  assert.notEqual(out, null)
  assert.equal(JSON.parse(out.value_json).status, 'passed')

  // Event stamp: attempt.succeeded carries verifyStatus 'pass'.
  const attemptOk = h.events().find((e) => e.type === 'attempt.succeeded')
  assert.equal(attemptOk.payload.verifyStatus, 'pass')
  assert.equal(attemptOk.payload.verifyEvidence, undefined)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('engine e2e: verify-declared task produces status "failed" → permanent dag.verify_gate_failed, task failed, downstream blocked', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'vg-fail',
    tasks: [
      {
        id: 'verify', kind: 'agent', prompt: 'run tests and report',
        outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
        verify: { expectOutput: 'report', expectStatus: 'passed' },
      },
      { id: 'deploy', kind: 'agent', prompt: 'deploy', dependsOn: [{ taskId: 'verify', condition: 'succeeded' }] },
    ],
  })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'failed', failedFiles: ['a.test.js'] }, stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.deepEqual(h.states(), { verify: 'failed', deploy: 'blocked' })
  const deploy = h.store.findTasks(h.runId).find((r) => r.task_id === 'deploy')
  assert.equal(JSON.parse(deploy.blocked_reason).code, 'upstream_failed')

  // The gate's synthetic failure landed with the actual status in the message.
  const attemptFail = h.events().find((e) => e.type === 'attempt.failed')
  assert.equal(attemptFail.payload.code, 'dag.verify_gate_failed')
  assert.equal(attemptFail.payload.failureType, 'permanent')
  assert.match(attemptFail.payload.message, /failed/)
  // No outputs row — the success path never ran.
  assert.equal(h.store.findOutput(h.runId, 'verify', 'report'), null)
  // Downstream never dispatched.
  assert.equal(h.subagents.calls.length, 1)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('engine e2e: verify gate failure honors retry policy (synthetic failure feeds the retry machine)', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'vg-retry',
    tasks: [{
      id: 'verify', kind: 'agent', prompt: 'run tests and report',
      outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
      verify: { expectOutput: 'report', expectStatus: 'passed' },
      retry: { maxAttempts: 2, backoffMs: 0, jitterRatio: 0, retryOn: ['permanent'] },
    }],
  })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'failed' }, stopReason: 'completed' } })
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'passed' }, stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 8, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.deepEqual(h.states(), { verify: 'succeeded' })
  assert.equal(h.subagents.calls.length, 2, 'the verify_gate_failed (permanent, retryOn permanent) retried once')
  const retryEv = h.events().find((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(retryEv.payload.code, 'dag.verify_gate_failed')
  // Attempt 2's passing receipt satisfies the gate (binding: attempt 2's own row).
  const ok = h.events().filter((e) => e.type === 'attempt.succeeded')
  assert.equal(ok.length, 1)
  assert.equal(ok[0].payload.verifyStatus, 'pass')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('engine e2e: no structured output at all → executor dag.missing_output (pre-gate), NOT verify_gate_failed', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'vg-missing',
    tasks: [{
      id: 'verify', kind: 'agent', prompt: 'run tests and report',
      outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
      verify: { expectOutput: 'report', expectStatus: 'passed' },
    }],
  })
  t.after(() => h.close())
  // stopReason completed but structured undefined → harvest maps permanent
  // dag.missing_output; evaluateVerifyGate sees failure !== null and passes
  // it through UNCHANGED — the verify gate does not double-report.
  h.subagents.script.push({ resolve: { output: [{ type: 'text', text: 'no report' }], stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.deepEqual(h.states(), { verify: 'failed' })
  const attemptFail = h.events().find((e) => e.type === 'attempt.failed')
  assert.equal(attemptFail.payload.code, 'dag.missing_output')
  assert.equal(attemptFail.payload.message.includes('verify_gate'), false)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('engine e2e: task WITHOUT a verify declaration succeeds with evidence none_declared stamped', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'vg-none',
    tasks: [{
      id: 'plain', kind: 'agent', prompt: 'just work',
      outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
    }],
  })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'failed' }, stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  const attemptOk = h.events().find((e) => e.type === 'attempt.succeeded')
  assert.equal(attemptOk.payload.verifyEvidence, 'none_declared')
  assert.equal(attemptOk.payload.verifyStatus, undefined)
})

test('engine e2e: dependency gate on the edge — not_met blocks downstream, met lets it run; condition_not_met emitted', async (t) => {
  const h = await makeHarness({
    version: 1, name: 'gate-e2e',
    tasks: [
      {
        id: 'a', kind: 'agent', prompt: 'produce',
        outputs: [{ name: 'report', schema: REPORT_SCHEMA }],
      },
      {
        id: 'b', kind: 'agent', prompt: 'deploy',
        dependsOn: [{
          taskId: 'a', condition: 'succeeded',
          gate: { artifact: 'task://a/report', expect: 'equals', value: '{"status":"passed"}' },
        }],
      },
    ],
  })
  t.after(() => h.close())
  // a produces a FAILING report → gate not met → b blocked(dependency_gate_not_met).
  h.subagents.script.push({ resolve: { output: [], structured: { status: 'failed' }, stopReason: 'completed' } })
  const s1 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  // dependency_gate_not_met is NOT soft-blockable in this plugin: the
  // upstream is succeeded (its output row can only change via a NEW attempt,
  // and retry_task refuses succeeded tasks), so the graph is a dead end and
  // finalizeRunIfDone's onlyDeadBlocked arm derives run failed — the same
  // M1-review-B2 honesty as upstream_blocked (never leave a run `running`
  // on an instruction that cannot make progress).
  assert.equal(s1.run_state, 'failed')
  assert.deepEqual(h.states(), { a: 'succeeded', b: 'blocked' })
  const bBlocked = h.store.findTasks(h.runId).find((r) => r.task_id === 'b')
  assert.equal(JSON.parse(bBlocked.blocked_reason).code, 'dependency_gate_not_met')
  // Source readiness.ts L144-153: the paired task.condition_not_met event.
  assert.equal(h.events().some((e) => e.type === 'task.condition_not_met'), true)
  assert.equal(h.subagents.calls.length, 1, 'b never dispatched while its gate is unmet')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
  await h.close()

  // Same graph with a PASSING report → gate met → b runs.
  const h2 = await makeHarness({
    version: 1, name: 'gate-e2e-ok',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'produce', outputs: [{ name: 'report', schema: REPORT_SCHEMA }] },
      {
        id: 'b', kind: 'agent', prompt: 'deploy',
        dependsOn: [{
          taskId: 'a', condition: 'succeeded',
          gate: { artifact: 'task://a/report', expect: 'equals', value: '{"status":"passed"}' },
        }],
      },
    ],
  })
  t.after(() => h2.close())
  h2.subagents.script.push({ resolve: { output: [], structured: { status: 'passed' }, stopReason: 'completed' } })
  h2.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s2 = await h2.engine.tick(h2.runId, { maxRounds: 8, settleMs: 0 })
  assert.equal(s2.run_state, 'succeeded')
  assert.deepEqual(h2.states(), { a: 'succeeded', b: 'succeeded' })
  assert.equal(h2.subagents.calls.length, 2)
  assert.equal(h2.store.verifyChain(h2.runId).ok, true)
})
