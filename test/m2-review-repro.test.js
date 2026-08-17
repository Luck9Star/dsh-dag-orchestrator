/**
 * M2 review reproduction tests — FIXED, kept as regressions.
 *
 * Written by the M2 milestone reviewer (read-only review; no business code
 * touched). Each contract test asserts the DESIGN-contract behavior; they
 * FAILED against the pre-fix code (that was the evidence) and now pin the
 * fixed behavior in place. The paired demonstration tests were flipped
 * together with the fixes — they now document the corrected semantics
 * (pre-fix they asserted the defect verbatim):
 *
 *   R1 (Major) — retry_task used to re-arm a task inside a CANCELLED
 *        (terminal) run silently: engine.control's own contract comment
 *        said a stopped run "is refused loud", but nothing validated the
 *        RUN state, and the revival CAS only fired for run.state ===
 *        'failed'. A task that failed BEFORE the stop keeps state 'failed'
 *        under stop (the stop tx skips terminal tasks), so retry_task
 *        "succeeded", the task landed retry_wait inside a terminal run,
 *        and every later tick no-op'd — the manual retry could never
 *        dispatch. FIX: validateRunForTaskControl refuses cancelling/
 *        cancelled/succeeded runs loud (dag.invalid_run_state) before the
 *        re-arm; a failed run stays actionable (§8.6 ② revival).
 *        DESIGN §8.4 (retry_task domain) + §8.6 ②.
 *
 *   R2 (Major) — stop's abort sweep missed attempts in the
 *        claimed-but-not-yet-dispatched window: control('stop') cancels
 *        non-running tasks in its tx and aborts executor in-flight handles
 *        right after, but an attempt whose claim tx already committed and
 *        whose executor.dispatch is suspended at `await ctxSubagents.start`
 *        is in NEITHER set, and the sweep ran only inside control(). FIX:
 *        sweepStopAborts re-affirms the abort idempotently at the START and
 *        END of every oneRound/autoTick pass while control_intent ===
 *        'stop' — a straggler can no longer outlive the stop to timeoutMs,
 *        and the abort→harvest→cancelled aggregation always completes.
 *        DESIGN §8.4 stop ("abort in-flight + cancelling 聚合").
 *
 * Zero network, zero CLI, zero models. Real sqlite + real engine + fake
 * ctx.subagents (control.test.js harness shapes).
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createDagStore } from '../lib/dag-store.js'
import { createAdmission } from '../lib/admission.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'

// ---------------------------------------------------------------------------
// fixtures (control.test.js shapes)
// ---------------------------------------------------------------------------

/** Script-queue fake with abort-aware hanging runs (control.test.js shape). */
function fakeSubagents() {
  const script = []
  const calls = []
  const pending = []
  const aborts = []
  let counter = 0
  return {
    script, calls, pending, aborts,
    settle(index, result) { pending[index].resolve(result) },
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
          request.signal?.addEventListener('abort', () => {
            aborts.push({ index, label: request.label })
            resolveFn({ output: [], stopReason: 'aborted' })
          })
        })
        pending[index] = { resolve: resolveFn }
        return { id, result: promise, dispose: async () => {} }
      }
      return { id, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

/**
 * Deferred-START fake: start() itself suspends until the test resolves it —
 * reproducing the claimed-but-not-dispatched window (executor.dispatch is
 * awaiting ctxSubagents.start when control('stop') fires).
 */
function deferredStartSubagents() {
  const calls = []
  const startGates = []
  const aborts = []
  const results = []
  return {
    calls, startGates, aborts, results,
    allowStart(index) { startGates[index].resolve() },
    settleResult(index, result) { results[index].resolve(result) },
    async start(name, request) {
      const index = calls.length
      calls.push({ name, request, index })
      let resolveStart
      const gate = new Promise((res) => { resolveStart = res })
      startGates.push({ resolve: resolveStart })
      await gate // << the window: the claim is committed, dispatch suspended
      let resolveResult
      const result = new Promise((res) => {
        resolveResult = res
        request.signal?.addEventListener('abort', () => {
          aborts.push({ index, label: request.label })
          resolveResult({ output: [], stopReason: 'aborted' })
        })
      })
      results.push({ resolve: resolveResult })
      return { id: `sess-${index + 1}`, result, dispose: async () => {} }
    },
  }
}

async function makeHarness({ spec, subagents }) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'm2rev-')), 'dag.db') })
  const executor = createExecutor({
    ctxSubagents: subagents,
    execAgentProvider: () => ({ __live: 'agent' }),
  })
  const admission = createAdmission()
  const engine = createEngine({
    store, executor, admission, logger: {},
    now: () => 1_000_000, random: () => 0.5,
  })
  const runId = engine.planRun(spec, { baseCwd: '/tmp/repo', runId: 'run-1' }).runId
  return {
    store, subagents, executor, admission, engine, runId,
    async close() { engine.disposeAll(); store.close() },
  }
}

const states = (h) => Object.fromEntries(h.store.findTasks(h.runId).map((t) => [t.task_id, t.state]))
const runState = (h) => h.store.findRun(h.runId).state
const taskRow = (h, taskId) => h.store.findTasks(h.runId).find((t) => t.task_id === taskId)
const flush = () => new Promise((res) => setImmediate(res))

// ---------------------------------------------------------------------------
// R1 — retry_task inside a CANCELLED run
// ---------------------------------------------------------------------------

test('R1 (Major): retry_task on a task that failed BEFORE a stop must be refused loud (cancelled run)', async (t) => {
  // h hangs in flight; w fails permanently first. Then stop aborts h, the
  // run aggregates to cancelled — and w keeps state 'failed' (the stop tx
  // skips terminal tasks). This exact shape is §8.6 ②-adjacent: the operator
  // retries w after reconsidering the stop.
  const spec = {
    version: 1, name: 'r1',
    tasks: [
      { id: 'h', kind: 'agent', prompt: 'h' },
      { id: 'w', kind: 'agent', prompt: 'w' },
    ],
  }
  const subagents = fakeSubagents()
  const h = await makeHarness({ spec, subagents })
  t.after(() => h.close())

  // Dispatch order is alphabetical among equal-priority roots: h first.
  subagents.script.push({ hang: true })                                  // h
  subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } }) // w (permanent)
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(taskRow(h, 'w').state, 'failed', 'fixture: w failed before the stop')
  assert.equal(taskRow(h, 'h').state, 'running')

  // stop: run cancelling; h aborted; w is terminal-failed → untouched by the tx.
  h.engine.control(h.runId, 'stop', { reason: 'wrong branch' })
  await flush()
  const s2 = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s2.run_state, 'cancelled')
  assert.equal(taskRow(h, 'w').state, 'failed', 'fixture: the failed task survives the stop')

  // ---- THE CONTRACT (engine.js's own doc): a CANCELLED run is refused loud.
  assert.throws(
    () => h.engine.control(h.runId, 'retry_task', { taskId: 'w' }),
    (error) => error.code === 'dag.invalid_run_state',
    'retry_task inside a cancelled run must fail loud — stop is the operator terminal',
  )
})

test('R1 (Major, demonstration): after the fix the loud refusal keeps the dead run untouched', async (t) => {
  // Same fixture as above; this test documents the FIXED behavior (flipped
  // together with the R1 fix — the pre-fix version asserted the silent
  // re-arm verbatim and passed only while the defect existed).
  const spec = {
    version: 1, name: 'r1b',
    tasks: [
      { id: 'h', kind: 'agent', prompt: 'h' },
      { id: 'w', kind: 'agent', prompt: 'w' },
    ],
  }
  const subagents = fakeSubagents()
  const h = await makeHarness({ spec, subagents })
  t.after(() => h.close())

  subagents.script.push({ hang: true })
  subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } })
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  h.engine.control(h.runId, 'stop', {})
  await flush()
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(runState(h), 'cancelled')

  // Fixed behavior: the refusal is LOUD (dag.invalid_run_state)…
  assert.throws(
    () => h.engine.control(h.runId, 'retry_task', { taskId: 'w' }),
    (error) => error.code === 'dag.invalid_run_state',
    'retry_task inside a cancelled run must fail loud — stop is the operator terminal',
  )
  // …the task keeps its honest pre-stop terminal…
  assert.equal(taskRow(h, 'w').state, 'failed')
  // …and the run stays cancelled with no dead re-arm lying around.
  const s3 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(s3.dispatched, 0)
  assert.equal(taskRow(h, 'w').state, 'failed')
  assert.equal(runState(h), 'cancelled')
})

// ---------------------------------------------------------------------------
// R2 — stop's abort sweep vs the claimed-but-not-dispatched window
// ---------------------------------------------------------------------------

test('R2 (Major): an attempt dispatched after stop must still be aborted (no orphan in a cancelling run)', async (t) => {
  const spec = { version: 1, name: 'r2', tasks: [{ id: 'a', kind: 'agent', prompt: 'a' }] }
  const subagents = deferredStartSubagents()
  const h = await makeHarness({ spec, subagents })
  t.after(() => h.close())

  // The tick claims task a and suspends INSIDE ctxSubagents.start (the
  // claimed-but-not-dispatched window — executor.dispatch's await).
  const tickPromise = h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  await flush(); await flush()
  assert.equal(subagents.calls.length, 1, 'fixture: the dispatch reached start()')
  assert.equal(taskRow(h, 'a').state, 'running', 'fixture: the claim tx committed')

  // stop fires WHILE the dispatch is suspended: the tx skips the running
  // task, and the abort sweep sees an EMPTY executor in-flight map.
  h.engine.control(h.runId, 'stop', { reason: 'operator stop' })
  assert.equal(runState(h), 'cancelling')

  // The dispatch now completes and registers the in-flight handle.
  subagents.allowStart(0)
  await tickPromise
  await flush()
  assert.equal(h.executor.inFlightIds().length, 1, 'fixture: the attempt is in flight')

  // ---- THE CONTRACT (§8.4 stop: abort in-flight + cancelling aggregation):
  // the stop intent must catch up with late-registered attempts. Currently
  // nothing ever aborts it — the sweep ran once, inside control().
  assert.equal(
    subagents.aborts.length, 1,
    'the stop must abort an attempt that registered after the sweep (current code: never aborted)',
  )
  await flush()
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.run_state, 'cancelled', 'the run finishes its stop instead of wedging in cancelling')
  assert.deepEqual(states(h), { a: 'cancelled' })
})

test('R2 (Major, demonstration): after the fix ticks sweep the stop intent idempotently (no manual re-send needed)', async (t) => {
  const spec = { version: 1, name: 'r2b', tasks: [{ id: 'a', kind: 'agent', prompt: 'a' }] }
  const subagents = deferredStartSubagents()
  const h = await makeHarness({ spec, subagents })
  t.after(() => h.close())

  const tickPromise = h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  await flush(); await flush()
  h.engine.control(h.runId, 'stop', {})
  subagents.allowStart(0)
  await tickPromise
  await flush()

  // FIXED: the pass-end sweep aborted the late-registered attempt — no
  // manual re-send of stop is needed, and no orphan runs to timeoutMs.
  assert.equal(subagents.aborts.length, 1, 'the pass-end sweep caught the late registration')
  assert.equal(runState(h), 'cancelling')
  assert.equal(h.executor.inFlightIds().length, 1)

  // Ticks keep sweeping (idempotent — re-aborts are signal-level no-ops)
  // until the harvest + finalize aggregate the run to cancelled.
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(runState(h), 'cancelled')
  assert.deepEqual(states(h), { a: 'cancelled' })
})
