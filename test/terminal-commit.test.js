/**
 * terminal-commit tests — DESIGN §5.3 / TASKS.md T07 (terminal-commit.ts
 * L80-261 semantics): success branch event order, the Issue 5 retry ordering
 * (commitTerminal FIRST, task CAS only on commit.ok), retry exhaustion,
 * deterministic backoff inputs, and slot/session-key release OUTSIDE the tx.
 *
 * Real sqlite store on a tmpdir file; the admission controller is wrapped in
 * a spy to observe release ordering vs transaction commits.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createDagStore } from '../lib/dag-store.js'
import { createAdmission } from '../lib/admission.js'
import { commitTerminalAndRelease } from '../lib/terminal-commit.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'

const SPEC = {
  version: 1,
  name: 'terminal-commit',
  tasks: [
    {
      id: 'worker',
      kind: 'agent',
      prompt: 'work',
      retry: { maxAttempts: 3, backoffMs: 1000, maxBackoffMs: 10_000, jitterRatio: 0.25, retryOn: ['transient_network'] },
    },
    { id: 'plain', kind: 'agent', prompt: 'plain' },
    { id: 'producer', kind: 'agent', prompt: 'produce', outputs: [{ name: 'result', schema: { type: 'object' } }] },
  ],
}

/** Wrap the admission controller to record the release call order. */
function spyAdmission(runId = 'run-1') {
  const inner = createAdmission()
  const events = []
  return {
    events,
    inner,
    tryAcquireSlot: (max) => {
      const slot = inner.tryAcquireSlot(max, runId)
      if (slot !== null) events.push({ op: 'acquire', slot: slot.slot })
      return slot
    },
    releaseSlot: (handle) => {
      const ok = inner.releaseSlot(handle)
      events.push({ op: 'releaseSlot', ok })
      return ok
    },
    tryAcquireSessionKey: (k, a) => inner.tryAcquireSessionKey(k, a),
    releaseSessionKey: (k, a) => {
      const ok = inner.releaseSessionKey(k, a)
      events.push({ op: 'releaseSessionKey', ok })
      return ok
    },
    heldCount: () => inner.heldCount(),
  }
}

async function fixture(spec = SPEC) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'tc-')), 'dag.db') })
  store.tx(() => {
    store.insertRun({
      run_id: 'run-1',
      name: spec.name,
      spec_json: JSON.stringify(spec),
      spec_hash: 'h'.repeat(64),
      state: 'running',
      base_cwd: '/tmp/repo',
      version: 1,
    })
    store.insertTasks('run-1', spec.tasks.map((t) => ({ task_id: t.id, state: 'pending', version: 1 })))
  })
  return store
}

/** Claim a task into running + a non-terminal attempt, task-weaver-style. */
function claim(store, { taskId = 'worker', attemptId = 'att-1', owner = 'owner-1', taskState = 'running' } = {}) {
  store.tx(() => {
    store.casTaskState('run-1', taskId, 'pending', 1, 'ready')
    store.casTaskState('run-1', taskId, 'ready', 2, 'queued')
    store.casTaskState('run-1', taskId, 'queued', 3, taskState)
    store.insertAttempt({
      attempt_id: attemptId, run_id: 'run-1', task_id: taskId, ordinal: 1,
      state: 'running', backend: 'spawn', owner_token: owner,
    })
    store.insertEvent('run-1', { type: 'attempt.claimed', taskId, attemptId, payload: {} })
  })
}

const types = (store) => store.findEvents('run-1').map((e) => e.type)

// ---------------------------------------------------------------------------
// success branch
// ---------------------------------------------------------------------------

test('terminal-commit: success branch commits attempt.succeeded → task.succeeded in ONE tx', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store, { taskId: 'plain', attemptId: 'att-ok' })
  const admission = spyAdmission()
  const slot = admission.tryAcquireSlot(4)

  const out = commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 1_000 },
    runId: 'run-1',
    attempt: { taskId: 'plain', attemptId: 'att-ok', ordinal: 1 },
    slot, ownerToken: 'owner-1',
    failure: null, stopReason: 'completed', retryDecision: { retry: false, backoffMs: 0 },
  })

  assert.deepEqual(out, { kind: 'terminal', taskTo: 'succeeded' })
  const seq = types(store).slice(-2)
  assert.deepEqual(seq, ['attempt.succeeded', 'task.succeeded'])
  assert.equal(store.findAttempt('att-ok').state, 'succeeded')
  assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'plain').state, 'succeeded')
  // Attempt evidence recorded.
  const result = JSON.parse(store.findAttempt('att-ok').result_json)
  assert.equal(result.stopReason, 'completed')
  // Slot released after the commit.
  assert.deepEqual(admission.events.filter((e) => e.op === 'releaseSlot'), [{ op: 'releaseSlot', ok: true }])
  assert.equal(admission.heldCount(), 0)
  assert.equal(store.verifyChain('run-1').ok, true)
})

test('terminal-commit: success with declared output lands the outputs row in the same tx', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store, { taskId: 'producer', attemptId: 'att-p' })
  const admission = spyAdmission()

  commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 1_000 },
    runId: 'run-1',
    attempt: { taskId: 'producer', attemptId: 'att-p', ordinal: 1 },
    ownerToken: 'owner-1',
    failure: null, structured: { answer: 42 }, stopReason: 'completed',
    retryDecision: { retry: false, backoffMs: 0 },
  })

  const row = store.findOutput('run-1', 'producer', 'result')
  assert.notEqual(row, null)
  assert.equal(row.produced_by_attempt, 'att-p')
  assert.deepEqual(JSON.parse(row.value_json), { answer: 42 })
  // The two terminal events + the persisted output share one commit (the
  // events table has no window between them — they are the last two rows).
  assert.deepEqual(types(store).slice(-2), ['attempt.succeeded', 'task.succeeded'])
  assert.equal(store.verifyChain('run-1').ok, true)
})

// ---------------------------------------------------------------------------
// retry branch — the Issue 5 ordering
// ---------------------------------------------------------------------------

test('terminal-commit: retry branch orders commitTerminal(failed) → attempt.retry_scheduled → task.retry_wait', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  const admission = spyAdmission()
  const slot = admission.tryAcquireSlot(4)

  const out = commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 5_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
    slot, ownerToken: 'owner-1',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'flaky' },
    stopReason: 'error',
    retryDecision: { retry: true, backoffMs: 750 },
  })

  assert.deepEqual(out, { kind: 'retry', taskTo: 'retry_wait' })
  assert.deepEqual(types(store).slice(-3), ['attempt.failed', 'attempt.retry_scheduled', 'task.retry_wait'])
  const task = store.findTasks('run-1').find((r) => r.task_id === 'worker')
  assert.equal(task.state, 'retry_wait')
  assert.equal(task.retry_not_before, 5_750)
  const ev = store.findEvents('run-1').find((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(ev.payload_json != null, true)
  const payload = JSON.parse(ev.payload_json)
  assert.equal(payload.backoffMs, 750)
  assert.equal(payload.nextAttemptNumber, 2)
  assert.equal(payload.retryNotBeforeMs, 5_750)
  assert.equal(payload.failureType, 'transient')
  assert.deepEqual(admission.events.filter((e) => e.op === 'releaseSlot'), [{ op: 'releaseSlot', ok: true }])
  assert.equal(store.verifyChain('run-1').ok, true)
})

test('terminal-commit: WRONG owner token → no retry_scheduled, task untouched (Issue 5 inverse)', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  const admission = spyAdmission()
  const slot = admission.tryAcquireSlot(4)
  const before = types(store)

  const out = commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 5_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
    slot, ownerToken: 'WRONG-OWNER',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'flaky' },
    stopReason: 'error',
    retryDecision: { retry: true, backoffMs: 750 },
  })

  assert.equal(out.kind, 'lost')
  // NOTHING landed: no attempt.failed, no retry_scheduled, task still running.
  assert.deepEqual(types(store), before)
  assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'worker').state, 'running')
  assert.equal(store.findAttempt('att-1').state, 'running')
  assert.equal(types(store).includes('attempt.retry_scheduled'), false)
  // Resources are STILL released outside the tx (a lost commit must not leak
  // the slot — the engine no longer owns the handle).
  assert.deepEqual(admission.events.filter((e) => e.op === 'releaseSlot'), [{ op: 'releaseSlot', ok: true }])
  assert.equal(store.verifyChain('run-1').ok, true)
})

test('terminal-commit: retry exhausted (prior retries ≥ max-1) routes terminal failed', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  // Simulate two prior retry_scheduled events for 'worker' (maxAttempts=3).
  store.tx(() => {
    store.insertEvent('run-1', { type: 'attempt.retry_scheduled', taskId: 'worker', attemptId: 'att-0', payload: {} })
    store.insertEvent('run-1', { type: 'attempt.retry_scheduled', taskId: 'worker', attemptId: 'att-0b', payload: {} })
  })

  const out = commitTerminalAndRelease({
    store, admission: spyAdmission(), spec: SPEC, clock: { now: 9_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 3 },
    ownerToken: 'owner-1',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'still flaky' },
    stopReason: 'error',
    // Engine's shouldRetry returned no-retry here (executionAttemptNumber 3 ≥ maxAttempts 3).
    retryDecision: { retry: false, backoffMs: 0 },
  })

  assert.equal(out.kind, 'terminal')
  assert.equal(out.taskTo, 'failed')
  assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'worker').state, 'failed')
  assert.equal(store.findAttempt('att-1').state, 'failed')
  assert.equal(types(store).includes('attempt.retry_scheduled'.concat('', '')), true) // the two priors
  const tail = types(store).slice(-2)
  assert.deepEqual(tail, ['attempt.failed', 'task.failed'])
  assert.equal(store.verifyChain('run-1').ok, true)
})

// ---------------------------------------------------------------------------
// cancelled + sessionKey release
// ---------------------------------------------------------------------------

test('terminal-commit: cancelled semantics — attempt cancelled + task cancelled in one tx', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store, { taskId: 'plain', attemptId: 'att-c' })
  const admission = spyAdmission()
  admission.tryAcquireSessionKey('shared-1', 'att-c')

  const out = commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 2_000 },
    runId: 'run-1',
    attempt: { taskId: 'plain', attemptId: 'att-c', ordinal: 1 },
    sessionKey: 'shared-1', ownerToken: 'owner-1',
    failure: { failureType: 'aborted', code: 'dag.cancelled', message: 'stop' },
    stopReason: 'aborted',
    retryDecision: { retry: false, backoffMs: 0 },
    forcedTaskTarget: 'cancelled',
  })

  assert.deepEqual(out, { kind: 'terminal', taskTo: 'cancelled' })
  assert.deepEqual(types(store).slice(-2), ['attempt.cancelled', 'task.cancelled'])
  assert.equal(store.findAttempt('att-c').state, 'cancelled')
  const release = admission.events.find((e) => e.op === 'releaseSessionKey')
  assert.deepEqual(release, { op: 'releaseSessionKey', ok: true })
  assert.equal(store.verifyChain('run-1').ok, true)
})

// ---------------------------------------------------------------------------
// release timing — outside the transaction
// ---------------------------------------------------------------------------

test('terminal-commit: slot/session release happen AFTER the transaction commit (tx boundary observed)', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store, { taskId: 'worker', attemptId: 'att-t' })
  const admission = spyAdmission()
  const slot = admission.tryAcquireSlot(4)
  admission.tryAcquireSessionKey('k1', 'att-t')

  const order = []
  // Patch findEvents to observe: if releaseSlot ran INSIDE the tx, the
  // event would not be committed-visible yet at release time.
  const realFindEvents = store.findEvents
  store.findEvents = (...args) => {
    if (admission.events.some((e) => e.op === 'releaseSlot')) {
      order.push('release-before-commit-observed')
    }
    return realFindEvents.apply(store, args)
  }

  commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 3_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-t', ordinal: 1 },
    slot, sessionKey: 'k1', ownerToken: 'owner-1',
    failure: null, stopReason: 'completed',
    retryDecision: { retry: false, backoffMs: 0 },
  })
  store.findEvents = realFindEvents

  // The terminal events WERE committed before any release was observed.
  assert.deepEqual(order, [])
  assert.deepEqual(admission.events.filter((e) => e.op.startsWith('release')), [
    { op: 'releaseSlot', ok: true },
    { op: 'releaseSessionKey', ok: true },
  ])
  // And the release did not roll anything back.
  assert.deepEqual(types(store).slice(-2), ['attempt.succeeded', 'task.succeeded'])
})

// ---------------------------------------------------------------------------
// deterministic backoff inputs (the engine computes; this pins the formula
// against a fake random via the ENGINE-level test in engine.test.js — here we
// pin that the commit records the GIVEN backoffMs verbatim)
// ---------------------------------------------------------------------------

test('terminal-commit: records the caller-computed backoff verbatim (fake clock arithmetic)', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  const admission = spyAdmission()

  commitTerminalAndRelease({
    store, admission, spec: SPEC, clock: { now: 10_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
    ownerToken: 'owner-1',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'x' },
    stopReason: 'error',
    retryDecision: { retry: true, backoffMs: 1_234 },
  })

  const task = store.findTasks('run-1').find((r) => r.task_id === 'worker')
  assert.equal(task.retry_not_before, 11_234)
  const payload = JSON.parse(store.findEvents('run-1').find((e) => e.type === 'attempt.retry_scheduled').payload_json)
  assert.equal(payload.backoffMs, 1_234)
})

// ---------------------------------------------------------------------------
// T10 §1 — commitTerminal failure inverses (Issue 5, both branches)
// ---------------------------------------------------------------------------

test('T10: ALREADY-TERMINAL attempt → no retry_scheduled, task untouched (Issue 5 inverse, terminal branch)', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  // A concurrent writer already terminally committed this attempt.
  store.tx(() => {
    store.commitTerminal('att-1', 'owner-1', 'failed', { stop_reason: 'error', failure_json: '{"x":1}' })
  })
  const before = types(store)
  const taskBefore = store.findTasks('run-1').find((r) => r.task_id === 'worker')

  const out = commitTerminalAndRelease({
    store, admission: spyAdmission(), spec: SPEC, clock: { now: 5_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
    ownerToken: 'owner-1',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'flaky' },
    stopReason: 'error',
    retryDecision: { retry: true, backoffMs: 750 },
  })

  assert.equal(out.kind, 'lost')
  // NOTHING landed: no new events at all (no attempt.failed, no
  // retry_scheduled, no task.retry_wait), and the task row is bit-identical.
  assert.deepEqual(types(store), before)
  const taskAfter = store.findTasks('run-1').find((r) => r.task_id === 'worker')
  assert.equal(taskAfter.state, taskBefore.state)
  assert.equal(taskAfter.version, taskBefore.version)
  assert.equal(taskAfter.retry_not_before, null)
  assert.equal(types(store).includes('attempt.retry_scheduled'), false)
  assert.equal(types(store).includes('task.retry_wait'), false)
  assert.equal(store.verifyChain('run-1').ok, true)
})

test('T10: wrong-owner on the TERMINAL branch → no attempt.succeeded/task.succeeded, task untouched', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store, { taskId: 'plain', attemptId: 'att-wt' })
  const before = types(store)

  const out = commitTerminalAndRelease({
    store, admission: spyAdmission(), spec: SPEC, clock: { now: 1_000 },
    runId: 'run-1',
    attempt: { taskId: 'plain', attemptId: 'att-wt', ordinal: 1 },
    ownerToken: 'STALE-OWNER',
    failure: null, stopReason: 'completed',
    retryDecision: { retry: false, backoffMs: 0 },
  })

  assert.equal(out.kind, 'lost')
  assert.deepEqual(types(store), before)
  assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'plain').state, 'running')
  assert.equal(store.findAttempt('att-wt').state, 'running')
  assert.equal(types(store).includes('task.succeeded'), false)
  assert.equal(store.verifyChain('run-1').ok, true)
})

// ---------------------------------------------------------------------------
// T10 §5 — crash injection inside the retry tx: all-or-nothing
// ---------------------------------------------------------------------------

test('T10 crash injection: a throw mid-retry-tx leaves attempt.failed/retry_scheduled/task.retry_wait ALL absent', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  const before = types(store)

  // Inject the crash: the FIRST event insert inside the retry tx throws (a
  // process death between the commitTerminal UPDATE and the event inserts
  // rolls the WHOLE tx back — node:sqlite tx(fn) is all-or-nothing).
  const realInsertEvent = store.insertEvent
  let calls = 0
  store.insertEvent = (...args) => {
    calls += 1
    if (calls === 1) {
      store.insertEvent = realInsertEvent // restore so ROLLBACK path is clean
      throw new Error('simulated crash mid-tx')
    }
    return realInsertEvent.apply(store, args)
  }

  let threw = null
  try {
    commitTerminalAndRelease({
      store, admission: spyAdmission(), spec: SPEC, clock: { now: 5_000 },
      runId: 'run-1',
      attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
      ownerToken: 'owner-1',
      failure: { failureType: 'transient', code: 'dag.agent_error', message: 'flaky' },
      stopReason: 'error',
      retryDecision: { retry: true, backoffMs: 750 },
    })
  } catch (error) {
    threw = error // the tx rethrow propagates — the honest crash surface
  }
  store.insertEvent = realInsertEvent
  assert.ok(threw !== null && /simulated crash mid-tx/.test(threw.message), 'the mid-tx throw propagates')

  // ALL THREE artifacts are absent — the tx rolled back atomically:
  // attempt still running, task still running, zero new events.
  assert.equal(store.findAttempt('att-1').state, 'running')
  const task = store.findTasks('run-1').find((r) => r.task_id === 'worker')
  assert.equal(task.state, 'running')
  assert.equal(task.retry_not_before, null)
  assert.deepEqual(types(store), before)
  assert.equal(types(store).includes('attempt.failed'), false)
  assert.equal(types(store).includes('attempt.retry_scheduled'), false)
  assert.equal(types(store).includes('task.retry_wait'), false)
  // And the chain is intact (no partial writes leaked).
  assert.equal(store.verifyChain('run-1').ok, true)
})

test('T10 crash injection: a throw AFTER attempt.failed + retry_scheduled but BEFORE the task CAS still rolls ALL back', async (t) => {
  const store = await fixture()
  t.after(() => store.close())
  claim(store)
  const before = types(store)

  // Crash one step deeper: the first TWO events land inside the tx, then
  // casTaskState throws — proving the event inserts cannot escape the tx.
  const realCas = store.casTaskState
  store.casTaskState = (...args) => {
    store.casTaskState = realCas
    throw new Error('simulated crash before task CAS')
  }

  assert.throws(() => commitTerminalAndRelease({
    store, admission: spyAdmission(), spec: SPEC, clock: { now: 5_000 },
    runId: 'run-1',
    attempt: { taskId: 'worker', attemptId: 'att-1', ordinal: 1 },
    ownerToken: 'owner-1',
    failure: { failureType: 'transient', code: 'dag.agent_error', message: 'flaky' },
    stopReason: 'error',
    retryDecision: { retry: true, backoffMs: 750 },
  }), /simulated crash before task CAS/)

  // Even the two events that "landed" inside the tx are gone — one atomic
  // unit, exactly the T10 acceptance ("三者全无").
  assert.equal(store.findAttempt('att-1').state, 'running')
  assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'worker').state, 'running')
  assert.deepEqual(types(store), before)
  assert.equal(store.verifyChain('run-1').ok, true)
})

// ---------------------------------------------------------------------------
// T10 §3 — backoff determinism: the full parameter table via the ENGINE's
// shouldRetry (fake clock + fake random); terminal-commit then records the
// computed backoffMs verbatim (already pinned above). The formula (source
// scheduler-loop.ts L2295-2329):
//     exp    = min(base * 2^(n-1), maxBackoff)
//     jitter = round(exp * ratio * (rand * 2 - 1))
//     backoff = max(0, exp + jitter)
// ---------------------------------------------------------------------------

/**
 * Drive the engine's REAL shouldRetry through its production path (claim →
 * dispatch → harvest → retry commit) and read the backoffMs the LAST retry
 * event recorded. shouldRetry is engine-internal; the harvest path is its
 * only production caller, so this observes the true computed value
 * end-to-end with a fake clock + fake random.
 *
 * priorRetries seeds REAL (recovery-free) attempt.retry_scheduled events,
 * positioning executionAttemptNumber = priorRetries + 1 for the fresh claim.
 */
async function observedBackoff({ maxAttempts = 10, backoffMs: base, maxBackoffMs, jitterRatio, rand, priorRetries = 0 }) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'tc-b-')), 'dag.db') })
  try {
    const spec = {
      version: 1, name: 'backoff-table',
      tasks: [{
        id: 'w', kind: 'agent', prompt: 'w',
        retry: { maxAttempts, backoffMs: base, maxBackoffMs, jitterRatio, retryOn: ['transient_network'] },
      }],
    }
    store.tx(() => {
      store.insertRun({
        run_id: 'run-1', name: spec.name, spec_json: JSON.stringify(spec), spec_hash: 'h'.repeat(64),
        state: 'running', base_cwd: '/tmp/repo', created_at: 1, updated_at: 1, version: 1,
      })
      store.insertTasks('run-1', [{ task_id: 'w', state: 'pending', version: 1 }])
      for (let i = 0; i < priorRetries; i++) {
        store.insertEvent('run-1', {
          type: 'attempt.retry_scheduled', taskId: 'w', attemptId: `att-p${i}`,
          payload: { failedAttemptId: `att-p${i}`, nextAttemptNumber: i + 2, retryNotBeforeMs: 0, backoffMs: 0 },
          at: 1,
        })
      }
    })
    const subagents = {
      calls: [],
      async start(name, request) {
        this.calls.push({ name, request })
        // Transient failure — retryOn matches transient_network.
        return { id: `s-${this.calls.length}`, result: Promise.resolve({ output: [], stopReason: 'error' }), dispose: async () => {} }
      },
    }
    const executor = createExecutor({ ctxSubagents: subagents, execAgentProvider: () => ({ __live: 'agent' }) })
    const engine = createEngine({
      store, executor, admission: createAdmission(), logger: {},
      now: () => 1_000_000, random: () => rand,
    })
    // Real claim path: leave the task pending — promoteReady → buildQueue →
    // claimTask mints the owner token the harvest CAS needs (seeding the
    // attempt outside claimTask loses the owner match and the commit is
    // silently lost). settleMs lets boundedRace observe the settlement.
    const s = await engine.tick('run-1', { maxRounds: 6, settleMs: 500 })
    void s
    const ev = store.findEvents('run-1', {}).filter((e) => e.type === 'attempt.retry_scheduled').pop()
    if (ev === undefined) return { retried: false }
    const payload = JSON.parse(ev.payload_json)
    return { retried: true, backoffMs: payload.backoffMs, retryNotBeforeMs: payload.retryNotBeforeMs }
  } finally {
    store.close()
  }
}

test('T10 backoff table: n=1/2/3 exponent ladder with jitterRatio 0 (pure exponential)', async () => {
  // base=1000, max=60000, ratio=0 → backoff = 1000, 2000, 4000.
  assert.deepEqual(await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0, rand: 0.5, priorRetries: 0 }), { retried: true, backoffMs: 1000, retryNotBeforeMs: 1_001_000 })
  assert.deepEqual(await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0, rand: 0.5, priorRetries: 1 }), { retried: true, backoffMs: 2000, retryNotBeforeMs: 1_002_000 })
  assert.deepEqual(await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0, rand: 0.5, priorRetries: 2 }), { retried: true, backoffMs: 4000, retryNotBeforeMs: 1_004_000 })
})

test('T10 backoff table: maxBackoffMs caps the exponent (truncation points)', async () => {
  // base=1000, max=3000: n=1 → 1000, n=2 → 2000, n=3 → min(4000,3000)=3000.
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 3000, jitterRatio: 0, rand: 0.5, priorRetries: 2 })).backoffMs, 3000)
  // base=1000, max=1500: n=1 → 1000, n=2 → min(2000,1500)=1500.
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 1500, jitterRatio: 0, rand: 0.5, priorRetries: 1 })).backoffMs, 1500)
  // Deep n saturates at max forever: n=8 → min(128000, 5000) = 5000.
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 5000, jitterRatio: 0, rand: 0.5, priorRetries: 7 })).backoffMs, 5000)
})

test('T10 backoff table: jitterRatio 0.25 — rand 0/0.5/1 boundaries', async () => {
  // n=1, base=1000, max=60000: exp=1000.
  //   rand=0.5 → jitter = round(1000*0.25*0) = 0 → 1000 (the midpoint).
  //   rand=1   → jitter = round(1000*0.25*1) = 250 → 1250 (max spread).
  //   rand=0   → jitter = round(1000*0.25*-1) = -250 → 750 (min spread).
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0.25, rand: 0.5, priorRetries: 0 })).backoffMs, 1000)
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0.25, rand: 1, priorRetries: 0 })).backoffMs, 1250)
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0.25, rand: 0, priorRetries: 0 })).backoffMs, 750)
})

test('T10 backoff table: jitter applies to the CAPPED exp, and max(0, exp+jitter) floors negatives', async () => {
  // base=1000, max=1500, n=2: exp=1500 (capped). rand=1, ratio=0.25 →
  // jitter = round(1500*0.25*1) = 375 → 1875 (jitter rides the cap, not
  // the raw exponent 2000).
  assert.equal((await observedBackoff({ backoffMs: 1000, maxBackoffMs: 1500, jitterRatio: 0.25, rand: 1, priorRetries: 1 })).backoffMs, 1875)
  // base=1, max=60000, n=1: exp=1. rand=0, ratio=1 → jitter = round(1*1*-1)
  // = -1 → max(0, 0) = 0 — the floor keeps backoff non-negative.
  assert.equal((await observedBackoff({ backoffMs: 1, maxBackoffMs: 60000, jitterRatio: 1, rand: 0, priorRetries: 0 })).backoffMs, 0)
})

// ---------------------------------------------------------------------------
// T10 §2/§4 — retryOn filter end-to-end + exhaustion with downstream
// propagation (engine-level integration)
// ---------------------------------------------------------------------------

test('T10 retryOn filter: permanent failures never enter the retry path (straight terminal, no retry_scheduled)', async () => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'tc-f-')), 'dag.db') })
  try {
    const spec = {
      version: 1, name: 'perm-no-retry',
      tasks: [{
        id: 'w', kind: 'agent', prompt: 'w',
        retry: { maxAttempts: 3, backoffMs: 1000, jitterRatio: 0, retryOn: ['transient_network'] },
      }],
    }
    store.tx(() => {
      store.insertRun({
        run_id: 'run-1', name: spec.name, spec_json: JSON.stringify(spec), spec_hash: 'h'.repeat(64),
        state: 'running', base_cwd: '/tmp/repo', created_at: 1, updated_at: 1, version: 1,
      })
      store.insertTasks('run-1', spec.tasks.map((task) => ({ task_id: task.id, state: 'pending', version: 1 })))
    })
    const subagents = {
      script: [{ resolve: { output: [], stopReason: 'refusal' } }], // PERMANENT
      calls: [],
      async start(name, request) {
        this.calls.push({ name, request })
        const behavior = this.script.shift() ?? { resolve: { output: [], stopReason: 'completed' } }
        return { id: `s-${this.calls.length}`, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
      },
    }
    const executor = createExecutor({ ctxSubagents: subagents, execAgentProvider: () => ({ __live: 'agent' }) })
    const engine = createEngine({ store, executor, admission: createAdmission(), logger: {}, now: () => 1_000_000, random: () => 0.5 })
    const s = await engine.tick('run-1', { maxRounds: 2, settleMs: 0 })
    assert.equal(s.run_state, 'failed')
    assert.equal(store.findTasks('run-1')[0].state, 'failed')
    assert.equal(store.findEvents('run-1', {}).some((e) => e.type === 'attempt.retry_scheduled'), false)
    assert.equal(subagents.calls.length, 1, 'no second dispatch for a filtered failure type')
    assert.equal(store.verifyChain('run-1').ok, true)
  } finally {
    store.close()
  }
})

test('T10 retryOn filter: timeout failures map transient_network and DO retry', async () => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'tc-t-')), 'dag.db') })
  try {
    const spec = {
      version: 1, name: 'timeout-retry',
      tasks: [{
        id: 'w', kind: 'agent', prompt: 'w', timeoutMs: 50,
        retry: { maxAttempts: 2, backoffMs: 0, jitterRatio: 0, retryOn: ['transient_network'] },
      }],
    }
    store.tx(() => {
      store.insertRun({
        run_id: 'run-1', name: spec.name, spec_json: JSON.stringify(spec), spec_hash: 'h'.repeat(64),
        state: 'running', base_cwd: '/tmp/repo', created_at: 1, updated_at: 1, version: 1,
      })
      store.insertTasks('run-1', spec.tasks.map((task) => ({ task_id: task.id, state: 'pending', version: 1 })))
    })
    // First start: the result only settles when the executor's 50ms timeout
    // timer aborts the signal (mirroring the real runtime: abort → an
    // 'aborted' result). Harvest then maps failureType 'timeout' → policyKey
    // 'transient_network' → retryOn match → retry. Second start: success.
    let started = 0
    const subagents = {
      calls: [],
      async start(name, request) {
        started += 1
        this.calls.push({ name, request })
        if (started === 1) {
          const result = new Promise((resolve) => {
            request.signal.addEventListener('abort', () => resolve({ output: [], stopReason: 'aborted' }))
          })
          return { id: `s-${started}`, result, dispose: async () => {} }
        }
        return { id: `s-${started}`, result: Promise.resolve({ output: [], stopReason: 'completed' }), dispose: async () => {} }
      },
    }
    const executor = createExecutor({ ctxSubagents: subagents, execAgentProvider: () => ({ __live: 'agent' }) })
    const engine = createEngine({ store, executor, admission: createAdmission(), logger: {}, now: () => 1_000_000, random: () => 0.5 })
    const s1 = await engine.tick('run-1', { maxRounds: 6, settleMs: 300 })
    // The timeout abort + retry wait (backoff 0, frozen clock) normally
    // re-dispatch within the same call; a second tick covers the boundary.
    if (subagents.calls.length < 2) {
      await new Promise((res) => setImmediate(res))
      await engine.tick('run-1', { maxRounds: 4, settleMs: 300 })
    }
    assert.equal(subagents.calls.length, 2, 'the timeout-mapped failure retried')
    const retryEv = store.findEvents('run-1', {}).find((e) => e.type === 'attempt.retry_scheduled')
    assert.ok(retryEv, 'timeout → transient_network mapped, retry scheduled')
    assert.equal(JSON.parse(retryEv.payload_json).failureType, 'timeout')
    const attempts = store.findAttempts('run-1', 'w')
    assert.equal(attempts.length, 2)
    assert.equal(attempts[0].stop_reason, 'timeout')
    assert.equal(store.findTasks('run-1')[0].state, 'succeeded', 'attempt 2 completed the task')
    assert.equal(store.verifyChain('run-1').ok, true)
    void s1
  } finally {
    store.close()
  }
})

test('T10 retry exhaustion: prior retries ≥ max-1 → terminal failed + downstream propagates blocked(upstream_failed) (engine integration)', async () => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'tc-x-')), 'dag.db') })
  try {
    const spec = {
      version: 1, name: 'exhaust-prop',
      tasks: [
        {
          id: 'flaky', kind: 'agent', prompt: 'f',
          retry: { maxAttempts: 2, backoffMs: 0, jitterRatio: 0, retryOn: ['transient_network'] },
        },
        { id: 'leaf', kind: 'agent', prompt: 'l', dependsOn: [{ taskId: 'flaky', condition: 'succeeded' }] },
      ],
    }
    store.tx(() => {
      store.insertRun({
        run_id: 'run-1', name: spec.name, spec_json: JSON.stringify(spec), spec_hash: 'h'.repeat(64),
        state: 'running', base_cwd: '/tmp/repo', created_at: 1, updated_at: 1, version: 1,
      })
      store.insertTasks('run-1', spec.tasks.map((task) => ({ task_id: task.id, state: 'pending', version: 1 })))
    })
    let started = 0
    const subagents = {
      calls: [],
      async start(name, request) {
        started += 1
        this.calls.push({ name, request })
        // Every attempt fails transiently — exhaustion is inevitable.
        return { id: `s-${started}`, result: Promise.resolve({ output: [], stopReason: 'error' }), dispose: async () => {} }
      },
    }
    const executor = createExecutor({ ctxSubagents: subagents, execAgentProvider: () => ({ __live: 'agent' }) })
    const engine = createEngine({ store, executor, admission: createAdmission(), logger: {}, now: () => 1_000_000, random: () => 0.5 })

    const s = await engine.tick('run-1', { maxRounds: 6, settleMs: 0 })

    // THE terminal-commit assertion: the exhausted attempt is task failed.
    assert.equal(store.findTasks('run-1').find((r) => r.task_id === 'flaky').state, 'failed')
    const attempts = store.findAttempts('run-1', 'flaky')
    assert.equal(attempts.length, 2, 'maxAttempts=2 → exactly two attempts')
    assert.deepEqual(attempts.map((a) => a.state), ['failed', 'failed'])
    // The terminal pair is the LAST pair (attempt.failed + task.failed).
    const typesNow = store.findEvents('run-1', {}).map((e) => e.type)
    const lastFailedIdx = typesNow.map((x, i) => [x, i]).filter(([x]) => x === 'attempt.failed').pop()[1]
    assert.deepEqual(typesNow.slice(lastFailedIdx, lastFailedIdx + 2), ['attempt.failed', 'task.failed'])

    // Downstream propagation (the ENGINE's job): leaf blocked upstream_failed.
    const leaf = store.findTasks('run-1').find((r) => r.task_id === 'leaf')
    assert.equal(leaf.state, 'blocked')
    assert.equal(JSON.parse(leaf.blocked_reason).code, 'upstream_failed')
    assert.equal(subagents.calls.length, 2, 'leaf never dispatched — the dead end propagated')
    // And the run derives failed (dead-end graph).
    assert.equal(s.run_state, 'failed')
    assert.equal(store.verifyChain('run-1').ok, true)
  } finally {
    store.close()
  }
})
