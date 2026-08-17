/**
 * Approval end-to-end tests — TASKS.md T12 acceptance (DESIGN §8.5 / §5.1
 * step 2 / §8.6 sequence ③).
 *
 * Real sqlite store + real engine + real executor bound to a fake
 * ctx.subagents (the engine-suite harness shape). The full sequences:
 *
 *   * plan(含 gate) → tick → gate PARKS blocked(approval_pending) +
 *     waiting_on:'approval' + approval.requested event; NO subagent ever
 *     started for the approval task.
 *   * dag_approve(approve) → tick → gate succeeded (approvalId payload) +
 *     downstream dispatched in the SAME tick (reconcile runs first, then
 *     promoteReady).
 *   * reject path → task failed(dag.policy_denied) + downstream
 *     blocked(upstream_failed) + run failed.
 *   * Idempotence matrix: duplicate decision → dag.already_decided;
 *     approving a non-parked task → dag.invalid_task_state.
 *   * Crash injection: a throw mid-park-tx leaves attempt/approval/task
 *     events all absent (counts asserted).
 *   * Re-run semantics: re-ticking a parked task reuses the pending
 *     approval (no duplicate insertApproval; attempts count unchanged —
 *     isReadySource's park exclusion end-to-end).
 *   * Tool layer: §8.5 return shape, prompt echo, isConcurrencySafe false.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { validateArgs } from '@deepseek-ai/dsh-tools'

import { createDagStore } from '../lib/dag-store.js'
import { createAdmission } from '../lib/admission.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'
import { registerDagApprove } from '../lib/tools/dag-approve.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/** The engine-suite fake (a script queue of subagent behaviors). */
function fakeSubagents() {
  const script = []
  const calls = []
  const pending = []
  let counter = 0
  return {
    script,
    calls,
    pending,
    settle(index, result) {
      pending[index].resolve(result)
    },
    async start(name, request) {
      counter += 1
      const index = calls.length
      const behavior = script.length > 0 ? script.shift() : { resolve: { output: [], stopReason: 'completed' } }
      calls.push({ name, request, behavior, index })
      const id = `sess-${counter}`
      if (behavior.hang) {
        let resolveFn
        const promise = new Promise((res) => { resolveFn = res })
        pending[index] = { resolve: resolveFn }
        return { id, result: promise, dispose: async () => {} }
      }
      if (behavior.reject) {
        const p = Promise.reject(behavior.reject)
        p.catch(() => {})
        return { id, result: p, dispose: async () => {} }
      }
      return { id, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

/** store + admission + executor(fake) + engine, planRun'ed with the spec. */
async function makeHarness({ spec, config } = {}) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'apr-')), 'dag.db') })
  const subagents = fakeSubagents()
  const executor = createExecutor({
    ctxSubagents: subagents,
    execAgentProvider: () => ({ __live: 'agent' }),
    config,
  })
  const admission = createAdmission()
  const engine = createEngine({
    store, executor, admission, config, logger: {},
    now: () => 1_000_000, random: () => 0.5,
  })
  const runId = spec !== undefined ? engine.planRun(spec, { baseCwd: '/tmp/repo', runId: 'run-1' }).runId : 'run-1'
  return {
    store, subagents, executor, admission, engine, runId,
    async close() { engine.disposeAll(); store.close() },
  }
}

/** The §8.6 ③ shape: agent → approval gate → downstream agent. */
function gateSpec({ prompt = '两个实现分支已就绪，是否继续集成？', action = 'approve_integration' } = {}) {
  return {
    version: 1,
    name: 'gate-run',
    tasks: [
      { id: 'build', kind: 'agent', prompt: 'build it' },
      {
        id: 'gate', kind: 'approval', dependsOn: [{ taskId: 'build', condition: 'succeeded' }],
        approval: { action, ...(prompt !== null ? { prompt } : {}) },
      },
      { id: 'deploy', kind: 'agent', prompt: 'deploy it', dependsOn: [{ taskId: 'gate', condition: 'succeeded' }] },
    ],
  }
}

const states = (h) => Object.fromEntries(h.store.findTasks(h.runId).map((t) => [t.task_id, t.state]))
const types = (h) => h.store.findEvents(h.runId).map((e) => e.type)
const chainOk = (h) => h.store.verifyChain(h.runId).ok

/** Register dag_approve on a collector ctx; returns the definition. */
function registerApproveTool(deps) {
  const registered = []
  const ctx = { tools: { register(definition) { registered.push(definition) } } }
  registerDagApprove(ctx, deps)
  return registered[0]
}

// ---------------------------------------------------------------------------
// sequence ① — approve path (§8.6 ③)
// ---------------------------------------------------------------------------

test('approval: plan→tick parks the gate blocked(approval_pending); waiting_on approval; no subagent for the gate', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())

  // build runs + completes first.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s1 = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s1.run_state, 'running')

  // THE park assertions.
  assert.equal(states(h).gate, 'blocked')
  const gateRow = h.store.findTasks(h.runId).find((x) => x.task_id === 'gate')
  const reason = JSON.parse(gateRow.blocked_reason)
  assert.equal(reason.code, 'approval_pending')
  assert.equal(typeof reason.approvalId, 'string')
  assert.equal(s1.waiting_on, 'approval')
  assert.match(s1.next_hint, /dag_approve/)

  // attempt failed(reason approval_pending) + approval.requested event.
  const attempt = h.store.findAttempts(h.runId, 'gate')[0]
  assert.equal(attempt.state, 'failed')
  assert.equal(attempt.child_session, null)
  const events = h.store.findEvents(h.runId)
  const requested = events.find((e) => e.type === 'approval.requested')
  assert.ok(requested, 'approval.requested event missing')
  assert.equal(JSON.parse(requested.payload_json).action, 'approve_integration')
  assert.equal(JSON.parse(requested.payload_json).prompt, '两个实现分支已就绪，是否继续集成？')
  const attemptFailed = events.find((e) => e.type === 'attempt.failed' && e.task_id === 'gate')
  assert.equal(JSON.parse(attemptFailed.payload_json).reason, 'approval_pending')

  // The approval row exists, pending, action recorded.
  const approvals = h.store.findApprovalsByTask(h.runId, 'gate')
  assert.equal(approvals.length, 1)
  assert.equal(approvals[0].state, 'pending')
  assert.equal(approvals[0].action, 'approve_integration')

  // deploy still pending (gate not resolved); NO subagent dispatched for
  // the approval task itself (approval IS the work).
  assert.equal(states(h).deploy, 'pending')
  assert.equal(h.subagents.calls.length, 1, 'only the build task reached a subagent')
  assert.equal(chainOk(h), true)
})

test('approval: dag_approve(approve) → next tick promotes gate succeeded and dispatches downstream', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())
  const tool = registerApproveTool({ engine: h.engine, store: h.store })

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(states(h).gate, 'blocked')

  // §8.5 tool contract: decision only, task stays blocked, prompt echoed.
  const out = await tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve', note: 'user said go' }, {})
  assert.deepEqual(
    { ...out },
    {
      kind: 'approve',
      run_id: h.runId,
      task_id: 'gate',
      decision: 'approve',
      task_state: 'blocked',
      approval_prompt: '两个实现分支已就绪，是否继续集成？',
      next_hint: 'call dag_tick to promote the decided approval',
    },
  )
  // The tool did NOT touch the task (red line 2).
  assert.equal(states(h).gate, 'blocked')
  // approval.decided event in the chain.
  const decided = h.store.findEvents(h.runId).find((e) => e.type === 'approval.decided')
  assert.equal(JSON.parse(decided.payload_json).decision, 'approved')
  assert.equal(JSON.parse(decided.payload_json).note, 'user said go')

  // The promoting tick: reconcileApprovals (step 1) → gate succeeded →
  // promoteReady dispatches deploy within the SAME tick.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s2 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s2.run_state, 'succeeded')
  assert.deepEqual(states(h), { build: 'succeeded', gate: 'succeeded', deploy: 'succeeded' })
  // task.succeeded carries the approvalId.
  const promoted = h.store.findEvents(h.runId).find((e) => e.type === 'task.succeeded' && e.task_id === 'gate')
  assert.equal(JSON.parse(promoted.payload_json).reason, 'approval_approved')
  assert.equal(typeof JSON.parse(promoted.payload_json).approvalId, 'string')
  // deploy reached a real subagent exactly once.
  assert.deepEqual(h.subagents.calls.map((c) => c.request.label), ['gate-run/build#1', 'gate-run/deploy#1'])
  // NO new attempt for the gate (reconcile promotes the task, never re-runs).
  assert.equal(h.store.findAttempts(h.runId, 'gate').length, 1)
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// sequence ② — reject path
// ---------------------------------------------------------------------------

test('approval: dag_approve(reject) → tick → gate failed(dag.policy_denied) + downstream blocked(upstream_failed) + run failed', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())
  const tool = registerApproveTool({ engine: h.engine, store: h.store })

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  const out = await tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'reject', note: 'user said no' }, {})
  assert.equal(out.decision, 'reject')

  const s2 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(states(h).gate, 'failed')
  const failed = h.store.findEvents(h.runId).find((e) => e.type === 'task.failed' && e.task_id === 'gate')
  assert.equal(JSON.parse(failed.payload_json).code, 'dag.policy_denied')
  assert.equal(JSON.parse(failed.payload_json).failureType, 'permanent')
  // Downstream propagation: deploy blocked(upstream_failed); run failed.
  assert.equal(states(h).deploy, 'blocked')
  const deployRow = h.store.findTasks(h.runId).find((x) => x.task_id === 'deploy')
  assert.equal(JSON.parse(deployRow.blocked_reason).code, 'upstream_failed')
  assert.equal(s2.run_state, 'failed')
  // deploy never dispatched.
  assert.deepEqual(h.subagents.calls.map((c) => c.request.label), ['gate-run/build#1'])
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// idempotence matrix
// ---------------------------------------------------------------------------

test('approval: duplicate decision → dag.already_decided (loud)', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())
  const tool = registerApproveTool({ engine: h.engine, store: h.store })

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  await tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve' }, {})

  // Re-deciding (either direction) → loud already_decided.
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve' }, {}),
    (error) => error.code === 'dag.already_decided' && /already approved/.test(error.message),
  )
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'reject' }, {}),
    (error) => error.code === 'dag.already_decided',
  )
  // Exactly ONE approval.decided event — the rejected duplicate left no trace.
  assert.equal(h.store.findEvents(h.runId).filter((e) => e.type === 'approval.decided').length, 1)
  assert.equal(chainOk(h), true)
})

test('approval: approving a task that is not parked → dag.invalid_task_state', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())
  const tool = registerApproveTool({ engine: h.engine, store: h.store })

  // 'build' is a plain agent task, never parked.
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'build', decision: 'approve' }, {}),
    (error) => error.code === 'dag.invalid_task_state',
  )
  // Unknown task / unknown run.
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'ghost', decision: 'approve' }, {}),
    (error) => error.code === 'dag.task_not_found',
  )
  await assert.rejects(
    () => tool.execute({ run_id: 'nope', task_id: 'gate', decision: 'approve' }, {}),
    (error) => error.code === 'dag.run_not_found',
  )

  // After promotion (gate succeeded), approving again → invalid_task_state.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  await tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve' }, {})
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(states(h).gate, 'succeeded')
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve' }, {}),
    (error) => error.code === 'dag.invalid_task_state',
  )
})

// ---------------------------------------------------------------------------
// crash injection — park tx atomicity
// ---------------------------------------------------------------------------

test('approval: crash mid-park-tx rolls back attempt/task/events atomically; apply-time reconcile re-parks cleanly', async (t) => {
  const spec = {
    version: 1, name: 'crash-park',
    tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' } }],
  }
  const h = await makeHarness({ spec })
  // The crash is simulated in-process: the residue assertions read the same
  // store, then recovery.js's reconcile() (the apply-time entry) runs
  // against it directly.
  t.after(() => h.close())

  // Crash inside the PARK transaction (after the approval-request tx, at
  // the park tx's attempt.failed event write): the park tx rolls back.
  const originalInsertEvent = h.store.insertEvent.bind(h.store)
  let crashed = false
  h.store.insertEvent = (runId, event) => {
    if (!crashed && event.type === 'attempt.failed' && event.payload?.reason === 'approval_pending') {
      crashed = true
      throw new Error('INJECTED crash inside the park tx')
    }
    return originalInsertEvent(runId, event)
  }

  const tickPromise = h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  await assert.rejects(() => tickPromise, /INJECTED crash/)
  assert.equal(crashed, true)

  // The park tx rolled back COMPLETELY: no attempt.failed event, no
  // task.blocked event, the task never left running, the attempt never
  // reached a terminal state. (The approval.requested tx BEFORE it
  // committed — the source's request-first/park-second ordering; the
  // pending row is therefore legitimately present in the residue.)
  const events = h.store.findEvents(h.runId)
  assert.equal(events.filter((e) => e.type === 'attempt.failed').length, 0, 'no attempt.failed event')
  assert.equal(events.filter((e) => e.type === 'task.blocked').length, 0, 'no task.blocked event')
  assert.equal(events.filter((e) => e.type === 'approval.requested').length, 1, 'the request tx committed before the crash')
  const gateRow = h.store.findTasks(h.runId).find((x) => x.task_id === 'gate')
  assert.equal(gateRow.state, 'running', 'the task CAS rolled back with the tx')
  const attempts = h.store.findAttempts(h.runId, 'gate')
  assert.equal(attempts.length, 1)
  assert.equal(attempts[0].state, 'running', 'the attempt terminal commit rolled back')
  assert.equal(chainOk(h), true, 'the chain stays intact after the rollback')

  // The residue is EXACTLY §12.1's Window-1 shape (running task, running
  // attempt, child_session NULL — nothing ever dispatched): apply-time
  // recovery auto-retries it, and the next tick re-parks cleanly with the
  // SAME pending approval row (reused, never duplicated). The recovery
  // clock must match the engine's injected clock, or the written
  // retry_not_before (real Date.now) would sit forever past the frozen
  // test clock.
  const { reconcile } = await import('../lib/recovery.js')
  const summary = await reconcile(h.store, { logger: {}, now: () => 1_000_000 })
  assert.equal(summary.autoRetried, 1, 'the never-dispatched attempt was auto-retried')
  const approvalsBefore = h.store.findApprovalsByTask(h.runId, 'gate').length

  const s2 = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(states(h).gate, 'blocked')
  assert.equal(s2.waiting_on, 'approval')
  assert.equal(h.store.findApprovalsByTask(h.runId, 'gate').length, approvalsBefore, 'pending approval reused, not duplicated')
  assert.equal(JSON.parse(h.store.findTasks(h.runId).find((x) => x.task_id === 'gate').blocked_reason).code, 'approval_pending')
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// re-run semantics — park stability + isReadySource exclusion end-to-end
// ---------------------------------------------------------------------------

test('approval: repeated ticks while parked re-dispatch NOTHING (isReadySource park exclusion; attempts count frozen)', async (t) => {
  const h = await makeHarness({ spec: gateSpec() })
  t.after(() => h.close())

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(states(h).gate, 'blocked')

  // Settle the downstream projection first: with the gate parked, the
  // evaluator settles deploy pending→blocked(upstream_blocked) — the
  // standard blocked-upstream verdict (NOT park churn; it clears the moment
  // the gate promotes). One extra tick absorbs that one-time transition.
  const settleTick = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(settleTick.dispatched, 0)
  assert.equal(states(h).deploy, 'blocked')
  const deployRow = h.store.findTasks(h.runId).find((x) => x.task_id === 'deploy')
  assert.equal(JSON.parse(deployRow.blocked_reason).code, 'upstream_blocked')

  // NOW the freeze assertions: attempts/approvals/events counts frozen.
  const attemptsAtPark = h.store.findAttempts(h.runId, 'gate').length
  const approvalsAtPark = h.store.findApprovalsByTask(h.runId, 'gate').length
  const eventsAtPark = h.store.findEvents(h.runId).length

  // Hammer the tick: no re-claim, no duplicate approval row, no phantom
  // events — the park is stable until dag_approve decides.
  for (let i = 0; i < 4; i++) {
    const s = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
    assert.equal(states(h).gate, 'blocked')
    assert.equal(s.waiting_on, 'approval')
    assert.equal(s.dispatched, 0, 'a parked gate re-dispatches nothing')
  }
  assert.equal(h.store.findAttempts(h.runId, 'gate').length, attemptsAtPark)
  assert.equal(h.store.findApprovalsByTask(h.runId, 'gate').length, approvalsAtPark)
  assert.equal(h.store.findEvents(h.runId).length, eventsAtPark, 'no phantom events across re-ticks')
  assert.equal(h.subagents.calls.length, 1)
  assert.equal(chainOk(h), true)
})

test('approval: already-decided-before-dispatch — gate with a pre-approved row goes straight succeeded (no park)', async (t) => {
  // Source branch 1 (L203-206): an existing APPROVED decision resolves the
  // task at dispatch time — commitTerminalAndRelease success path.
  const spec = {
    version: 1, name: 'pre-approved',
    tasks: [
      { id: 'gate', kind: 'approval', approval: { action: 'go' } },
      { id: 'deploy', kind: 'agent', prompt: 'd', dependsOn: [{ taskId: 'gate', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // Seed a decided approval BEFORE the first tick.
  h.store.tx(() => {
    h.store.insertApproval({
      approval_id: 'apr-pre', run_id: h.runId, task_id: 'gate',
      action: 'go', state: 'approved', decided_at: 1,
    })
  })

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.deepEqual(states(h), { gate: 'succeeded', deploy: 'succeeded' })
  // The gate's single attempt SUCCEEDED (not failed/parked).
  const attempt = h.store.findAttempts(h.runId, 'gate')[0]
  assert.equal(attempt.state, 'succeeded')
  assert.equal(h.store.findAttempts(h.runId, 'gate').length, 1)
  assert.deepEqual(h.subagents.calls.map((c) => c.request.label), ['pre-approved/deploy#1'])
  assert.equal(chainOk(h), true)
})

test('approval: already-decided-before-dispatch — pre-REJECTED row → permanent dag.approval_rejected (no park)', async (t) => {
  // Source branch 2 (L208-215).
  const spec = {
    version: 1, name: 'pre-rejected',
    tasks: [
      { id: 'gate', kind: 'approval', approval: { action: 'go' } },
      { id: 'deploy', kind: 'agent', prompt: 'd', dependsOn: [{ taskId: 'gate', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  h.store.tx(() => {
    h.store.insertApproval({
      approval_id: 'apr-pre', run_id: h.runId, task_id: 'gate',
      action: 'go', state: 'rejected', decided_at: 1,
    })
  })

  const s = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(states(h).gate, 'failed')
  const attempt = h.store.findAttempts(h.runId, 'gate')[0]
  assert.equal(attempt.state, 'failed')
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.approval_rejected')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'permanent')
  assert.equal(states(h).deploy, 'blocked', 'downstream blocked(upstream_failed) via propagation')
  assert.equal(s.run_state, 'failed')
  assert.equal(h.subagents.calls.length, 0)
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// tool face
// ---------------------------------------------------------------------------

test('approval: dag_approve registration shape + isConcurrencySafe false + enforced argument face', () => {
  const store = {
    findRun: () => ({ run_id: 'r', spec_json: '{}' }),
    findTasks: () => [],
    findApprovalsByTask: () => [],
    decideApproval: () => ({ ok: true, row: {} }),
    tx: (fn) => fn(),
  }
  const tool = registerApproveTool({ store })

  assert.equal(tool.name, 'dag_approve')
  assert.equal(tool.isConcurrencySafe(), false)
  assert.ok(tool.description.includes('dag_tick'))

  // The enforced face, via the DECLARED spec shape (the tools.test.js form
  // — defineTool wraps tool.parameters with its own metadata, so the raw
  // map is reconstructed here): missing required args + bad enum value
  // reject; the full shape passes.
  const specMap = {
    run_id: { type: 'string', required: true },
    task_id: { type: 'string', required: true },
    decision: { type: 'string', required: true, enum: ['approve', 'reject'] },
    note: { type: 'string' },
  }
  assert.ok(validateArgs(specMap, {}).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(specMap, { run_id: 'r' }).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(specMap, { run_id: 'r', task_id: 't', decision: 'maybe' }).some((v) => v.includes('approve') || v.includes('reject')))
  assert.deepEqual(validateArgs(specMap, { run_id: 'r', task_id: 't', decision: 'approve' }), [])
  assert.deepEqual(validateArgs(specMap, { run_id: 'r', task_id: 't', decision: 'reject', note: 'x' }), [])
})

test('approval: dag_approve rejects when a parked task has no pending approval row', async (t) => {
  // Hand-park (the R5 shape) without inserting the approval row: the tool
  // must refuse loud rather than decide a phantom.
  const spec = { version: 1, name: 'hand', tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' } }] }
  const h = await makeHarness({ spec })
  t.after(() => h.close())
  const tool = registerApproveTool({ engine: h.engine, store: h.store })

  h.store.tx(() => {
    h.store.casTaskState(h.runId, 'gate', 'pending', 1, 'blocked', {
      blocked_reason: JSON.stringify({ code: 'approval_pending', approvalId: 'apr-ghost' }),
    })
  })
  await assert.rejects(
    () => tool.execute({ run_id: h.runId, task_id: 'gate', decision: 'approve' }, {}),
    (error) => error.code === 'dag.invalid_task_state' && /no approval row/.test(error.message),
  )
})

// ---------------------------------------------------------------------------
// M6 regression inside the approval flow — park releases the slot
// ---------------------------------------------------------------------------

test('approval: parking releases the run\'s admission slot (next dispatch is not starved)', async (t) => {
  const spec = {
    version: 1, name: 'slot-release',
    limits: { maxRunningAgents: 1 },
    tasks: [
      { id: 'gate', kind: 'approval', approval: { action: 'go' } },
      { id: 'worker', kind: 'agent', prompt: 'w' },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  // maxRunning=1: the gate parks (and must RELEASE its slot) inside the
  // first tick; the worker still dispatches in the SAME round's later pass.
  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(states(h).gate, 'blocked')
  assert.equal(states(h).worker, 'succeeded', 'the freed slot admitted the worker')
  assert.equal(s.run_state, 'running', 'parked approval keeps the run alive (soft block)')
  assert.equal(h.subagents.calls.length, 1)
  assert.equal(chainOk(h), true)
})
