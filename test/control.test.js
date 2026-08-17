/**
 * dag_control tests — TASKS.md T11 acceptance (DESIGN §8.4).
 *
 * Real store (sqlite tmpdir) + real engine + REAL executor bound to a
 * fake ctx.subagents. The fake extends the engine-suite fixture with
 * ABORT-AWARENESS: each hanging run listens to request.signal and resolves
 * {stopReason:'aborted'} when aborted — the contract the real subagent
 * runtime provides (§4.2: engine-held AbortController; §4.5 'aborted'
 * mapping). This is what makes the stop path observable end-to-end without
 * a real child.
 *
 * Coverage (the T11 acceptance matrix):
 *   * five-action state-transition matrix — every happy path + every
 *     illegal source state loud (dag.invalid_run_state /
 *     dag.invalid_task_state / dag.run_not_found / dag.task_not_found)
 *   * pause: hanging in-flight → pause → tick (no new dispatch, in-flight
 *     alive) → settle → tick → run paused (drainToPaused) → resume → tick
 *     re-opens dispatch
 *   * stop: two hanging in-flights → stop → aborts recorded (fake asserts
 *     the signal fired) → tick → attempts cancelled + tasks cancelled +
 *     run cancelled (cancelling→cancelled aggregation) + chain ok;
 *     non-running tasks (pending downstream) cancelled inside the control
 *     tx with paired events
 *   * retry_task: failed task (and blocked(upstream_failed)) → retry_wait
 *     now → tick re-dispatches a NEW attempt (ordinal+1) → manual event
 *     task.retry_requested → NOT billed to the retryOn budget (a further
 *     failure can still be retry_task'd after the policy is exhausted)
 *   * cancel_task: pending → cancelled + event; blocked(upstream_failed) →
 *     cancelled; running → loud; succeeded/cancelled → loud
 *   * pairing discipline: every projection change carries its event in the
 *     same tx (verified by chain integrity + event presence + count)
 *   * tool layer (fake engine): parameter passthrough, error codes,
 *     isConcurrencySafe false, json-safe returns
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
import { registerDagControl } from '../lib/tools/dag-control.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/**
 * Abort-aware fake ctx.subagents (the engine-suite fixture + signal
 * handling): a {hang:true} entry returns a run whose result promise only
 * settles when EITHER the test calls settle(index, result) OR the
 * executor's controller abort fires (→ {stopReason:'aborted'}, the real
 * runtime's contract). Every abort is recorded for assertions.
 */
function fakeSubagents() {
  const script = []
  const calls = []
  const pending = []
  const aborts = [] // {index, label} — signal-fired aborts, in order
  let counter = 0
  return {
    script,
    calls,
    pending,
    aborts,
    /** Settle the nth hang run (0-based dispatch index). */
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
        const promise = new Promise((res) => {
          resolveFn = res
          // The abort contract: an aborted signal settles 'aborted' (§4.2 —
          // the engine owns the controller; the runtime turns the signal
          // into the result's stopReason).
          request.signal?.addEventListener('abort', () => {
            aborts.push({ index, label: request.label })
            resolveFn({ output: [], stopReason: 'aborted' })
          })
        })
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

/** Deterministic manual clock. */
function fakeClock(start = 1_000_000) {
  let t = start
  return { now: () => t, advance: (ms) => { t += ms; return t } }
}

/** store + admission + executor(fake) + engine with an injected clock. */
async function makeHarness({ spec, clockStart = 1_000_000, config } = {}) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'ctl-')), 'dag.db') })
  const subagents = fakeSubagents()
  const executor = createExecutor({
    ctxSubagents: subagents,
    execAgentProvider: () => ({ __live: 'agent' }),
    config,
  })
  const admission = createAdmission()
  const clock = fakeClock(clockStart)
  const engine = createEngine({
    store, executor, admission, config, logger: {},
    now: clock.now, random: () => 0.5,
  })
  const runId = spec !== undefined ? engine.planRun(spec, { baseCwd: '/tmp/repo', runId: 'run-1' }).runId : 'run-1'
  return {
    store, subagents, executor, admission, clock, engine, runId,
    async close() { engine.disposeAll(); store.close() },
  }
}

const LINE = { version: 1, name: 'line', tasks: [
  { id: 'a', kind: 'agent', prompt: 'do a' },
  { id: 'b', kind: 'agent', prompt: 'do b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
] }

const states = (h) => Object.fromEntries(h.store.findTasks(h.runId).map((t) => [t.task_id, t.state]))
const types = (h) => h.store.findEvents(h.runId).map((e) => e.type)
const chainOk = (h) => h.store.verifyChain(h.runId).ok
const runState = (h) => h.store.findRun(h.runId).state
const taskRow = (h, taskId) => h.store.findTasks(h.runId).find((t) => t.task_id === taskId)
const eventsOf = (h, type) => h.store.findEvents(h.runId).filter((e) => e.type === type)
const flush = () => new Promise((res) => setImmediate(res))

/** Drive a task to terminal failed (permanent stopReason) via the engine. */
async function failTask(h, taskId) {
  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } }) // permanent
  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  await flush()
  assert.equal(taskRow(h, taskId).state, 'failed', 'fixture: task must be failed')
}

// ---------------------------------------------------------------------------
// pause / resume
// ---------------------------------------------------------------------------

test('control pause: hanging in-flight → pausing (no new dispatch, in-flight alive) → settle → paused → resume → dispatch reopens', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  // Round 1: a hangs in flight.
  h.subagents.script.push({ hang: true })
  const s1 = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s1.dispatched, 1)
  assert.equal(states(h).a, 'running')

  // pause: running → pausing in ONE tx with the intent + run.control event.
  const out = h.engine.control(h.runId, 'pause', { reason: 'operator break' })
  assert.deepEqual(
    { kind: out.kind, run_id: out.run_id, action: out.action, run_state: out.run_state, effected: out.effected },
    { kind: 'control', run_id: h.runId, action: 'pause', run_state: 'pausing', effected: [] },
  )
  assert.equal(runState(h), 'pausing')
  assert.deepEqual(JSON.parse(h.store.findRun(h.runId).control_intent === 'pause' ? '{"ok":true}' : 'null'), { ok: true })
  const ctrlEv = eventsOf(h, 'run.control')
  assert.equal(ctrlEv.length, 1)
  assert.deepEqual(JSON.parse(ctrlEv[0].payload_json), { action: 'pause', reason: 'operator break', from: 'running', to: 'pausing' })

  // Tick under pause: NO new dispatch (b stays unpromoted-to-run), the
  // in-flight a is NOT aborted (fake recorded no abort), run stays pausing.
  const s2 = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s2.dispatched, 0)
  assert.equal(h.subagents.calls.length, 1)
  assert.deepEqual(h.subagents.aborts, [])
  assert.equal(runState(h), 'pausing')

  // In-flight settles (allowed under pause) → next tick drains to paused.
  h.subagents.settle(0, { output: [], stopReason: 'completed' })
  await flush()
  const s3 = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s3.terminal, 1)
  assert.equal(runState(h), 'paused')
  assert.equal(types(h).includes('run.paused'), true)
  // b was never dispatched.
  assert.equal(h.subagents.calls.length, 1)
  assert.ok(['pending', 'ready'].includes(states(h).b))
  // The intent SURVIVES the drain (admission stays closed until resume).
  assert.equal(h.store.findRun(h.runId).control_intent, 'pause')

  // pause on an already-paused run: idempotent re-affirm (still one state,
  // a second run.control event, no error).
  const again = h.engine.control(h.runId, 'pause', {})
  assert.equal(again.run_state, 'paused')
  assert.equal(eventsOf(h, 'run.control').length, 2)

  // resume: paused → running + intent cleared, ONE tx + paired event.
  const res = h.engine.control(h.runId, 'resume', { reason: 'back to work' })
  assert.equal(res.run_state, 'running')
  assert.equal(runState(h), 'running')
  assert.equal(h.store.findRun(h.runId).control_intent, null)
  const resEv = eventsOf(h, 'run.control').find((e) => JSON.parse(e.payload_json).action === 'resume')
  assert.ok(resEv, 'resume run.control event present')
  assert.deepEqual(JSON.parse(resEv.payload_json), { action: 'resume', reason: 'back to work', from: 'paused', to: 'running' })

  // Dispatch reopens: b runs and completes → run succeeded.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s4 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(s4.dispatched, 1)
  assert.equal(s4.run_state, 'succeeded')
  assert.equal(chainOk(h), true)
})

test('control pause/resume illegal states: resume on running loud; pause on cancelling loud; run-level control on terminal run loud', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  // resume on a running run → loud.
  assert.throws(
    () => h.engine.control(h.runId, 'resume', {}),
    (error) => error.code === 'dag.invalid_run_state' && /paused/.test(error.message),
  )

  // cancelling refuses pause (stop owns that run's terminal).
  h.store.tx(() => {
    h.store.casRunState(h.runId, 'running', h.store.findRun(h.runId).version, 'cancelling', { control_intent: 'stop' })
  })
  assert.throws(
    () => h.engine.control(h.runId, 'pause', {}),
    (error) => error.code === 'dag.invalid_run_state' && /stop/.test(error.message),
  )
  // resume refuses a cancelling run too.
  assert.throws(
    () => h.engine.control(h.runId, 'resume', {}),
    (error) => error.code === 'dag.invalid_run_state',
  )

  // Terminal run: RUN-LEVEL actions all loud. Task-level actions are NOT
  // blanket-refused either, but the M2 review R1 fix narrows the fall-
  // through: a FAILED run stays actionable (§8.6 ② revival / honest
  // cleanup), while cancelling/cancelled/succeeded runs now refuse at the
  // RUN level (dag.invalid_run_state) BEFORE task validation — a task that
  // failed before a stop keeps state 'failed', and falling through would
  // silently re-arm it inside a dead run.
  h.store.tx(() => {
    h.store.casRunState(h.runId, 'cancelling', h.store.findRun(h.runId).version, 'cancelled')
  })
  for (const action of ['pause', 'resume', 'stop']) {
    assert.throws(
      () => h.engine.control(h.runId, action, {}),
      (error) => error.code === 'dag.invalid_run_state' && /terminal/.test(error.message),
      `${action} on a terminal run must be loud`,
    )
  }
  for (const taskAction of ['retry_task', 'cancel_task']) {
    assert.throws(
      () => h.engine.control(h.runId, taskAction, { taskId: 'a' }),
      (error) => error.code === 'dag.invalid_run_state' && /cancelled/.test(error.message),
      `${taskAction} inside a cancelled run must be refused loud at the run level (M2 R1)`,
    )
  }
})

test('control unknown run / unknown action loud', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())
  assert.throws(
    () => h.engine.control('nope', 'pause', {}),
    (error) => error.code === 'dag.run_not_found',
  )
  assert.throws(
    () => h.engine.control(h.runId, 'explode', {}),
    (error) => error.code === 'dag.invalid_action',
  )
})

// ---------------------------------------------------------------------------
// stop
// ---------------------------------------------------------------------------

test('control stop: two hanging in-flights aborted → tick lands attempts+tasks cancelled, pending downstream cancelled in-tx, run cancelling→cancelled, chain ok', async (t) => {
  const spec = {
    version: 1,
    name: 'stop-run',
    limits: { maxRunningAgents: 2 },
    tasks: [
      { id: 'x', kind: 'agent', prompt: 'x' },
      { id: 'y', kind: 'agent', prompt: 'y' },
      { id: 'z', kind: 'agent', prompt: 'z', dependsOn: [{ taskId: 'x', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // Two hanging dispatches (x, y); z is pending downstream.
  h.subagents.script.push({ hang: true }, { hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(h.subagents.calls.length, 2)
  assert.equal(states(h).z, 'pending')

  // stop: run → cancelling; z (pending) cancelled INSIDE the tx; x/y NOT
  // touched by the tx (they own the abort→harvest path).
  const out = h.engine.control(h.runId, 'stop', { reason: 'wrong branch' })
  assert.equal(out.run_state, 'cancelling')
  assert.equal(runState(h), 'cancelling')
  assert.equal(h.store.findRun(h.runId).control_intent, 'stop')
  // effected carries the in-tx task cancellations (z).
  assert.deepEqual(out.effected, [{ task_id: 'z', from: 'pending', to: 'cancelled' }])
  assert.equal(taskRow(h, 'z').state, 'cancelled')
  // The aborts fired immediately after the tx — the fake recorded both.
  assert.equal(h.subagents.aborts.length, 2)
  assert.deepEqual(h.subagents.aborts.map((a) => a.label).sort(), ['stop-run/x#1', 'stop-run/y#1'])
  // run.control event paired.
  const stopEv = eventsOf(h, 'run.control').find((e) => JSON.parse(e.payload_json).action === 'stop')
  assert.ok(stopEv, 'stop run.control event present')
  assert.deepEqual(JSON.parse(stopEv.payload_json), { action: 'stop', reason: 'wrong branch', from: 'running', to: 'cancelling' })
  // z's cancellation carries its task.cancelled event (same tx).
  assert.equal(eventsOf(h, 'task.cancelled').length, 1)

  // The aborted promises settle 'aborted' (the fake resolved them) — but
  // harvest needs a tick. Until then the run sits cancelling.
  await flush()
  assert.equal(runState(h), 'cancelling')

  // THE closing tick: harvest walks the cancelled semantics per attempt
  // (attempt cancelled + task cancelled), finalize aggregates cancelling →
  // cancelled.
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.terminal, 2)
  assert.equal(runState(h), 'cancelled')
  assert.deepEqual(states(h), { x: 'cancelled', y: 'cancelled', z: 'cancelled' })
  const attemptsX = h.store.findAttempts(h.runId, 'x')
  assert.equal(attemptsX[0].state, 'cancelled')
  assert.equal(attemptsX[0].stop_reason, 'aborted')
  const attemptsY = h.store.findAttempts(h.runId, 'y')
  assert.equal(attemptsY[0].state, 'cancelled')
  assert.equal(attemptsY[0].stop_reason, 'aborted')
  // No retries were scheduled for the aborted attempts (not retryable).
  assert.equal(eventsOf(h, 'attempt.retry_scheduled').length, 0)
  // Aggregate: run.cancelled event, chain intact, per-state-change events.
  assert.equal(types(h).includes('run.cancelled'), true)
  assert.equal(eventsOf(h, 'task.cancelled').length, 3) // z (control tx) + x,y (harvest)
  assert.equal(eventsOf(h, 'attempt.cancelled').length, 2)
  assert.equal(chainOk(h), true)
})

test('control stop: idempotent re-send while cancelling; stop from pausing/paused also works', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  h.subagents.script.push({ hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  h.engine.control(h.runId, 'pause', {})
  h.subagents.settle(0, { output: [], stopReason: 'completed' })
  await flush()
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(runState(h), 'paused')

  // stop from paused: b cancels in-tx, run → cancelling; nothing in flight
  // → the next tick finalizes straight to cancelled.
  const out = h.engine.control(h.runId, 'stop', {})
  assert.equal(out.run_state, 'cancelling')
  assert.equal(taskRow(h, 'b').state, 'cancelled')
  // Re-send: idempotent.
  const again = h.engine.control(h.runId, 'stop', {})
  assert.equal(again.run_state, 'cancelling')
  assert.equal(runState(h), 'cancelling')
  const s = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s.run_state, 'cancelled')
  assert.equal(chainOk(h), true)
})

test('control stop: cancelled-run aggregation outranks partial success (a succeeded before the stop)', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  // a completes normally — but the run does NOT finalize yet (b is still
  // live work), so the stop below is legal. b hangs in flight to prove the
  // abort path composes with the aggregate override.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } }) // a
  h.subagents.script.push({ hang: true }) // b
  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(taskRow(h, 'a').state, 'succeeded')
  assert.equal(taskRow(h, 'b').state, 'running')

  // Stop: the aggregate would read succeeded (a succeeded, not all
  // cancelled) — cancelling MUST outrank it.
  h.engine.control(h.runId, 'stop', {})
  assert.equal(h.subagents.aborts.length, 1)
  await flush()
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.run_state, 'cancelled', 'stop intent outranks the partial-success aggregate')
  assert.deepEqual(states(h), { a: 'succeeded', b: 'cancelled' })
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// retry_task
// ---------------------------------------------------------------------------

test('control retry_task: failed task → retry_wait now → tick re-dispatches ordinal+1 → manual event → budget NOT consumed', async (t) => {
  const spec = {
    version: 1,
    name: 'manual-retry',
    tasks: [{
      id: 'w', kind: 'agent', prompt: 'w',
      // Budget EXHAUSTED by policy: maxAttempts 1 means shouldRetry never
      // retries — manual retry_task must still work (explicit human action).
      retry: { maxAttempts: 1, backoffMs: 1000, jitterRatio: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  await failTask(h, 'w')
  assert.equal(eventsOf(h, 'attempt.retry_scheduled').length, 0) // policy: no auto retry

  // retry_task: failed → retry_wait with retry_not_before = now.
  const before = h.clock.now()
  const out = h.engine.control(h.runId, 'retry_task', { taskId: 'w', reason: 'flaky infra settled' })
  assert.deepEqual(out.effected, [{ task_id: 'w', from: 'failed', to: 'retry_wait' }])
  assert.equal(out.run_state, 'running')
  const row = taskRow(h, 'w')
  assert.equal(row.state, 'retry_wait')
  assert.equal(row.retry_not_before, before)
  // Manual event, distinct type from attempt.retry_scheduled.
  const req = eventsOf(h, 'task.retry_requested')
  assert.equal(req.length, 1)
  assert.equal(req[0].task_id, 'w')
  assert.deepEqual(JSON.parse(req[0].payload_json), {
    taskId: 'w', manual: true, reason: 'flaky infra settled',
    from: 'failed', to: 'retry_wait', retryNotBeforeMs: before,
  })

  // Tick re-dispatches a NEW attempt (ordinal 2) — the budget check lives
  // in shouldRetry (failure-time), claim never consults it.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.dispatched, 1)
  assert.equal(h.subagents.calls.at(-1).request.label, 'manual-retry/w#2')
  const attempts = h.store.findAttempts(h.runId, 'w')
  assert.equal(attempts.length, 2)
  assert.equal(attempts[1].ordinal, 2)
  assert.equal(s.run_state, 'succeeded')
  // shouldRetry never billed the manual retry: zero retry_scheduled events.
  assert.equal(eventsOf(h, 'attempt.retry_scheduled').length, 0)
  assert.equal(chainOk(h), true)
})

test('control retry_task: manual retry stays re-armed even after repeated failures (budget never consumed)', async (t) => {
  const spec = {
    version: 1,
    name: 'manual-again',
    tasks: [{
      id: 'w', kind: 'agent', prompt: 'w',
      retry: { maxAttempts: 1, backoffMs: 0, jitterRatio: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // Fail → manual retry → fail again → manual retry STILL accepted.
  await failTask(h, 'w')
  h.engine.control(h.runId, 'retry_task', { taskId: 'w' })
  await failTask(h, 'w')
  const out = h.engine.control(h.runId, 'retry_task', { taskId: 'w' })
  assert.deepEqual(out.effected, [{ task_id: 'w', from: 'failed', to: 'retry_wait' }])
  assert.equal(eventsOf(h, 'task.retry_requested').length, 2)
  // And the second re-arm really dispatches attempt 3.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(h.subagents.calls.at(-1).request.label, 'manual-again/w#3')
  assert.equal(s.run_state, 'succeeded')
  assert.equal(eventsOf(h, 'attempt.retry_scheduled').length, 0, 'manual retries never enter the budget')
  assert.equal(chainOk(h), true)
})

test('control retry_task on blocked(upstream_failed): accepted and re-armed', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  // a fails permanently → b lands blocked(upstream_failed).
  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } })
  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(taskRow(h, 'a').state, 'failed')
  assert.equal(states(h).b, 'blocked')
  assert.equal(JSON.parse(taskRow(h, 'b').blocked_reason).code, 'upstream_failed')

  const out = h.engine.control(h.runId, 'retry_task', { taskId: 'b' })
  assert.deepEqual(out.effected, [{ task_id: 'b', from: 'blocked', to: 'retry_wait' }])
  assert.equal(taskRow(h, 'b').state, 'retry_wait')

  // The next tick re-evaluates: upstream STILL failed → b bounces back to
  // blocked(upstream_failed) — retry the upstream, not just the victim.
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.dispatched, 0)
  assert.equal(states(h).b, 'blocked')
  assert.equal(JSON.parse(taskRow(h, 'b').blocked_reason).code, 'upstream_failed')
  assert.equal(chainOk(h), true)
})

test('control retry_task illegal task states: succeeded / running / pending / blocked(upstream_blocked) / blocked(approval_pending) all loud', async (t) => {
  const spec = {
    version: 1,
    name: 'illegal',
    limits: { maxRunningAgents: 2 },
    tasks: [
      { id: 'ok', kind: 'agent', prompt: 'ok' },
      { id: 'hang', kind: 'agent', prompt: 'h' },
      { id: 'apr', kind: 'approval', approval: { action: 'gate-it' } },
      { id: 'waiter', kind: 'agent', prompt: 'w', dependsOn: [{ taskId: 'hang', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // ok succeeds; hang stays running; apr parks blocked(approval_pending);
  // waiter stays pending (its upstream is running — a waiting verdict, not
  // blocked). Script entries are consumed in ALPHABETICAL dispatch order
  // (apr, hang, ok, waiter): apr parks WITHOUT a subagent, hang hangs, ok
  // completes.
  h.subagents.script.push({ hang: true }) // hang
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } }) // ok
  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(taskRow(h, 'ok').state, 'succeeded')
  assert.equal(taskRow(h, 'hang').state, 'running')
  assert.equal(taskRow(h, 'apr').state, 'blocked')
  assert.equal(JSON.parse(taskRow(h, 'apr').blocked_reason).code, 'approval_pending')
  assert.equal(taskRow(h, 'waiter').state, 'pending')
  assert.equal(taskRow(h, 'waiter').blocked_reason, null)

  for (const taskId of ['ok', 'hang', 'waiter', 'apr']) {
    assert.throws(
      () => h.engine.control(h.runId, 'retry_task', { taskId }),
      (error) => error.code === 'dag.invalid_task_state' && error.message.includes(taskId),
      `retry_task on ${taskId} must be loud`,
    )
  }
  // Unknown task loud.
  assert.throws(
    () => h.engine.control(h.runId, 'retry_task', { taskId: 'ghost' }),
    (error) => error.code === 'dag.task_not_found',
  )
  // Missing task_id loud.
  assert.throws(
    () => h.engine.control(h.runId, 'retry_task', {}),
    (error) => error.code === 'dag.task_required',
  )
  assert.equal(chainOk(h), true)
})

test('control retry_task on blocked(upstream_blocked): refused loud — the upstream has not settled', async (t) => {
  // Chain root→mid→leaf: root fails permanently → mid blocked(upstream_
  // failed) → leaf's dep (mid) is itself blocked → leaf blocked(upstream_
  // blocked). Re-arming leaf cannot clear an unsettled upstream.
  const spec = {
    version: 1,
    name: 'chain',
    tasks: [
      { id: 'root', kind: 'agent', prompt: 'r' },
      { id: 'mid', kind: 'agent', prompt: 'm', dependsOn: [{ taskId: 'root', condition: 'succeeded' }] },
      { id: 'leaf', kind: 'agent', prompt: 'l', dependsOn: [{ taskId: 'mid', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } }) // root permanent
  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.equal(taskRow(h, 'mid').state, 'blocked')
  assert.equal(JSON.parse(taskRow(h, 'mid').blocked_reason).code, 'upstream_failed')
  assert.equal(taskRow(h, 'leaf').state, 'blocked')
  assert.equal(JSON.parse(taskRow(h, 'leaf').blocked_reason).code, 'upstream_blocked')

  assert.throws(
    () => h.engine.control(h.runId, 'retry_task', { taskId: 'leaf' }),
    (error) => error.code === 'dag.invalid_task_state' && /upstream_blocked/.test(error.message),
  )
  // mid (upstream_failed) stays the legal target — retry the upstream path.
  const out = h.engine.control(h.runId, 'retry_task', { taskId: 'mid' })
  assert.deepEqual(out.effected, [{ task_id: 'mid', from: 'blocked', to: 'retry_wait' }])
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// cancel_task
// ---------------------------------------------------------------------------

test('control cancel_task: pending → cancelled + paired event; blocked(upstream_failed) → cancelled', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  // a hangs running; b is pending.
  h.subagents.script.push({ hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(states(h).b, 'pending')

  const out = h.engine.control(h.runId, 'cancel_task', { taskId: 'b', reason: 'not needed' })
  assert.deepEqual(out.effected, [{ task_id: 'b', from: 'pending', to: 'cancelled' }])
  assert.equal(taskRow(h, 'b').state, 'cancelled')
  const ev = eventsOf(h, 'task.cancelled')
  assert.equal(ev.length, 1)
  assert.equal(ev[0].task_id, 'b')
  assert.deepEqual(JSON.parse(ev[0].payload_json), {
    from: 'pending', to: 'cancelled', reason: 'cancel_task', manual: true, note: 'not needed',
  })

  // blocked(upstream_failed) target: a fails → b would be blocked, but b is
  // already cancelled — use a second run for the blocked case.
  const h2 = await makeHarness({ spec: LINE })
  t.after(() => h2.close())
  h2.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } })
  await h2.engine.tick(h2.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(JSON.parse(taskRow(h2, 'b').blocked_reason).code, 'upstream_failed')
  const out2 = h2.engine.control(h2.runId, 'cancel_task', { taskId: 'b' })
  assert.deepEqual(out2.effected, [{ task_id: 'b', from: 'blocked', to: 'cancelled' }])
  assert.equal(taskRow(h2, 'b').state, 'cancelled')
  assert.equal(taskRow(h2, 'b').blocked_reason, null, 'blocked_reason cleared on cancel')
  // Run aggregates failed (a failed, b cancelled).
  const s2 = await h2.engine.tick(h2.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s2.run_state, 'failed')
  assert.equal(chainOk(h), true)
  assert.equal(chainOk(h2), true)
})

test('control cancel_task illegal states: running / succeeded / cancelled all loud (running is stop territory)', async (t) => {
  const spec = {
    version: 1,
    name: 'cx',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'a' },
      { id: 'b', kind: 'agent', prompt: 'b', dependsOn: [{ taskId: 'a', condition: 'completed' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  h.subagents.script.push({ hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(taskRow(h, 'a').state, 'running')

  assert.throws(
    () => h.engine.control(h.runId, 'cancel_task', { taskId: 'a' }),
    (error) => error.code === 'dag.invalid_task_state' && /running.*stop/.test(error.message),
  )

  h.subagents.settle(0, { output: [], stopReason: 'completed' })
  await flush()
  await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(taskRow(h, 'a').state, 'succeeded')
  // b ran on the completed condition and succeeded too.
  assert.equal(taskRow(h, 'b').state, 'succeeded')
  // The run finalized succeeded — M2 R1: task-level actions on a terminal
  // run are refused at the RUN level (uniform contract; a succeeded task's
  // invalid_task_state case is covered above by the live-run fixture).
  assert.equal(runState(h), 'succeeded')
  for (const taskAction of ['retry_task', 'cancel_task']) {
    assert.throws(
      () => h.engine.control(h.runId, taskAction, { taskId: 'a' }),
      (error) => error.code === 'dag.invalid_run_state' && /succeeded/.test(error.message),
      `${taskAction} inside a succeeded run must be refused loud at the run level (M2 R1)`,
    )
  }

  // A cancelled task refuses further control (both task actions).
  const h2 = await makeHarness({ spec })
  t.after(() => h2.close())
  h2.engine.control(h2.runId, 'cancel_task', { taskId: 'b' })
  assert.throws(
    () => h2.engine.control(h2.runId, 'cancel_task', { taskId: 'b' }),
    (error) => error.code === 'dag.invalid_task_state' && /cancelled/.test(error.message),
  )
  assert.throws(
    () => h2.engine.control(h2.runId, 'retry_task', { taskId: 'b' }),
    (error) => error.code === 'dag.invalid_task_state' && /cancelled/.test(error.message),
  )
})

test('control cancel_task: a pause-promoted ready task is cancellable (§6.2 four cancel edges)', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  h.subagents.script.push({ hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  h.engine.control(h.runId, 'pause', {})
  h.subagents.settle(0, { output: [], stopReason: 'completed' })
  await flush()
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 }) // drain → paused; b promoted ready
  assert.equal(runState(h), 'paused')
  assert.equal(states(h).b, 'ready')

  const out = h.engine.control(h.runId, 'cancel_task', { taskId: 'b' })
  assert.deepEqual(out.effected, [{ task_id: 'b', from: 'ready', to: 'cancelled' }])
  assert.equal(taskRow(h, 'b').state, 'cancelled')
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// pairing discipline — every projection change carries its event
// ---------------------------------------------------------------------------

test('control events: each state transition emits exactly one paired event, and no transition lands without one', async (t) => {
  const h = await makeHarness({ spec: LINE })
  t.after(() => h.close())

  h.subagents.script.push({ hang: true })
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })

  h.engine.control(h.runId, 'pause', {})
  h.subagents.settle(0, { output: [], stopReason: 'completed' })
  await flush()
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 }) // drain to paused
  h.engine.control(h.runId, 'resume', {})
  h.engine.control(h.runId, 'cancel_task', { taskId: 'b' })

  // Exactly one run.control per control action (pause + resume — no stop
  // here), one run.paused from the drain, one task.cancelled — and the
  // chain (which hashes every event into the projection order) verifies.
  assert.equal(eventsOf(h, 'run.control').length, 2)
  assert.equal(eventsOf(h, 'run.paused').length, 1)
  assert.equal(eventsOf(h, 'task.cancelled').length, 1)
  assert.equal(runState(h), 'running')
  assert.equal(states(h).b, 'cancelled')
  assert.equal(chainOk(h), true)

  // Post-terminal quiescence: the next tick finalizes (a succeeded, b
  // cancelled → succeeded) with its ONE paired run.succeeded event, and
  // the tick after that emits NOTHING new (terminal runs are a no-op).
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(runState(h), 'succeeded')
  assert.equal(eventsOf(h, 'run.succeeded').length, 1)
  const before = types(h).length
  await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(types(h).length, before, 'a terminal-run tick emits nothing new')
  assert.equal(chainOk(h), true)
})

// ---------------------------------------------------------------------------
// tool layer (fake engine — contract, not behavior)
// ---------------------------------------------------------------------------

/** Fake engine — spy with a fixed control return. */
function fakeEngine({ controlResult } = {}) {
  const calls = { control: [] }
  return {
    calls,
    control(runId, action, options) {
      calls.control.push({ runId, action, options })
      if (controlResult instanceof Error) throw controlResult
      return controlResult ?? {
        kind: 'control', run_id: runId, action, run_state: 'pausing', effected: [],
      }
    },
  }
}

function fakeCtx() {
  const registered = []
  return {
    registered,
    tools: { register(definition) { registered.push(definition) } },
  }
}

function fakeStore({ runsById = new Map() } = {}) {
  return { findRun: (runId) => (runsById.has(runId) ? { ...runsById.get(runId) } : null) }
}

function execCtx() {
  return { agent: { __live: 'agent' } }
}

test('dag_control tool: parameter passthrough (task_id/reason/execAgent), summary verbatim, json-safe', async () => {
  const fixed = {
    kind: 'control', run_id: 'r1', action: 'retry_task', run_state: 'running',
    effected: [{ task_id: 'w', from: 'failed', to: 'retry_wait' }],
  }
  const engine = fakeEngine({ controlResult: fixed })
  const store = fakeStore({ runsById: new Map([['r1', { run_id: 'r1', state: 'running' }]]) })
  const ctx = fakeCtx()
  registerDagControl(ctx, { engine, store })

  const tool = ctx.registered[0]
  assert.equal(tool.name, 'dag_control')

  const exec = execCtx()
  const result = await tool.execute({ run_id: 'r1', action: 'retry_task', task_id: 'w', reason: 'redo' }, exec)
  assert.deepEqual(result, fixed)
  // JSON round-trip keeps every key.
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result)

  assert.equal(engine.calls.control.length, 1)
  assert.equal(engine.calls.control[0].runId, 'r1')
  assert.equal(engine.calls.control[0].action, 'retry_task')
  assert.equal(engine.calls.control[0].options.taskId, 'w')
  assert.equal(engine.calls.control[0].options.reason, 'redo')
  assert.equal(engine.calls.control[0].options.execAgent, exec.agent)
})

test('dag_control tool: parameters schema (run_id+action required, action enum) and enforced face', async () => {
  const ctx = fakeCtx()
  registerDagControl(ctx, { engine: fakeEngine(), store: fakeStore() })
  const tool = ctx.registered[0]

  assert.deepEqual(tool.parameters.required.sort(), ['action', 'run_id'])
  assert.deepEqual(
    Object.keys(tool.parameters.properties).sort(),
    ['action', 'reason', 'run_id', 'task_id'],
  )
  assert.deepEqual(tool.parameters.properties.action.enum, ['pause', 'resume', 'stop', 'retry_task', 'cancel_task'])
  assert.equal(tool.parameters.properties.run_id.type, 'string')
  assert.equal(tool.parameters.properties.task_id.type, 'string')
  assert.equal(tool.parameters.properties.reason.type, 'string')

  // Enforced argument face: missing-required + wrong-type via validateArgs,
  // and through the compiled definition (ToolArgsError before the body).
  const spec = {
    run_id: { type: 'string', required: true },
    action: { type: 'string', required: true, enum: ['pause', 'resume', 'stop', 'retry_task', 'cancel_task'] },
  }
  assert.ok(validateArgs(spec, {}).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(spec, { run_id: 'r1' }).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(spec, { run_id: 7, action: 'pause' }).some((v) => v.includes('string')))
  assert.ok(validateArgs(spec, { run_id: 'r1', action: 'explode' }).some((v) => v.includes('pause')))
  await assert.rejects(
    () => tool.execute({ run_id: 'r1' }, execCtx()),
    (error) => error.name === 'ToolArgsError',
  )
})

test('dag_control tool: unknown run throws dag.run_not_found before the engine; engine errors propagate', async () => {
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagControl(ctx, { engine, store: fakeStore() })
  await assert.rejects(
    () => ctx.registered[0].execute({ run_id: 'nope', action: 'pause' }, execCtx()),
    (error) => {
      assert.equal(error.message, 'dag_control: dag.run_not_found — nope')
      assert.equal(error.code, 'dag.run_not_found')
      return true
    },
  )
  assert.equal(engine.calls.control.length, 0)

  // Engine-coded errors surface verbatim (invalid_task_state etc.).
  const boom = new Error('engine.control(cancel_task): task "x" is running — cancel_task accepts pending or blocked (cancelling a running task is the run-level stop)')
  boom.code = 'dag.invalid_task_state'
  const ctx2 = fakeCtx()
  registerDagControl(ctx2, { engine: fakeEngine({ controlResult: boom }), store: fakeStore({ runsById: new Map([['r1', { run_id: 'r1' }]]) }) })
  await assert.rejects(
    () => ctx2.registered[0].execute({ run_id: 'r1', action: 'cancel_task', task_id: 'x' }, execCtx()),
    (error) => error.code === 'dag.invalid_task_state',
  )
})

test('dag_control tool: isConcurrencySafe false; description carries the five actions + the stop-tick note', () => {
  const ctx = fakeCtx()
  registerDagControl(ctx, { engine: fakeEngine(), store: fakeStore() })
  const tool = ctx.registered[0]

  assert.equal(tool.isConcurrencySafe({ run_id: 'r1', action: 'pause' }), false)
  for (const fragment of ['pause', 'resume', 'stop', 'retry_task', 'cancel_task', 'dag_tick']) {
    assert.ok(tool.description.includes(fragment), `description must mention ${fragment}`)
  }
})

test('dag_control tool: register throws loud on missing deps', () => {
  const ctx = fakeCtx()
  assert.throws(() => registerDagControl(ctx, {}), /dag_control: requires deps\.engine/)
  assert.throws(
    () => registerDagControl(ctx, { engine: fakeEngine() }),
    /dag_control: requires deps\.store/,
  )
})
