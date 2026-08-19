// dag-face tests — lib/dag-face.js + the apply() registration path.
//
// Focus: the read-only `dagOrchestrator` service face (UI / sibling-plugin
// consumption). Two layers, both fake-only (zero network, zero CLI, zero
// models — red line 8):
//
//   * PURE UNIT: a fake engine (canned status) + a fake store (canned
//     findRun/findTasks/findAttempts/findOutputsByTask rows) assert the
//     four methods' shapes — status passthrough with runId normalisation,
//     getSpec's parsed spec_json + spec_hash, listOutputs' parsed
//     value_json composition, attemptSummaries' conditional `summary`,
//     the dag.run_not_found error shape, and the frozen face.
//
//   * ASSEMBLY: the apply() path with the index.test.js fake ctx —
//     ctx.provide('dagOrchestrator', face) is called with a working face
//     over the REAL store/engine singletons (seeded + reconciled), the
//     graceful skip when ctx.provide is absent, and the warn-and-continue
//     when provide throws.
//
// The sqlite-outlet discipline governs lib/, not tests: the assembly arm
// seeds through the store's own API (createDagStore on a temp dir), never
// a raw handle.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { apply } from '../lib/index.js'
import { createDagFace } from '../lib/dag-face.js'
import { createDagStore } from '../lib/dag-store.js'

// ---------------------------------------------------------------------------
// fixtures — canned rows mirroring the store's REAL row shapes
// ---------------------------------------------------------------------------

/** Two-task spec with one dependsOn edge (the UI graph minimum). */
const SPEC = {
  version: 1,
  name: 'face-demo',
  tasks: [
    {
      id: 'alpha',
      kind: 'agent',
      prompt: 'produce the report',
      outputs: [{ name: 'report', schema: { type: 'object' } }],
    },
    {
      id: 'beta',
      kind: 'agent',
      prompt: 'consume the report',
      dependsOn: [{ taskId: 'alpha', condition: 'succeeded' }],
      inputs: ['task://alpha/report'],
    },
  ],
}
const SPEC_JSON = JSON.stringify(SPEC)
const SPEC_HASH = 'f'.repeat(64)
const RUN_ID = 'dag_20260818_face00001'

/** A full runs-table row (spec_json unparsed — parsing is the face's job). */
function runRow(overrides = {}) {
  return {
    run_id: RUN_ID,
    name: 'face-demo',
    spec_json: SPEC_JSON,
    spec_hash: SPEC_HASH,
    state: 'succeeded',
    control_intent: null,
    parent_session: null,
    base_cwd: '/tmp/repo',
    created_at: 1_700_000_000_000,
    updated_at: 1_700_000_000_500,
    version: 3,
    ...overrides,
  }
}

/** tasks-table rows for the two-task spec. */
function taskRows() {
  return [
    { run_id: RUN_ID, task_id: 'alpha', state: 'succeeded', version: 2, blocked_reason: null, retry_not_before: null, updated_at: 1_700_000_000_400 },
    { run_id: RUN_ID, task_id: 'beta', state: 'succeeded', version: 2, blocked_reason: null, retry_not_before: null, updated_at: 1_700_000_000_500 },
  ]
}

/**
 * attempts-table rows: att-1 carries result_json (summary present), att-2
 * does not (summary omitted) — the conditional-expansion pair.
 */
function attemptRows() {
  return [
    {
      attempt_id: 'att-1',
      run_id: RUN_ID,
      task_id: 'alpha',
      ordinal: 1,
      state: 'succeeded',
      backend: 'spawn',
      child_session: 'sess-alpha-1',
      owner_token: 'owner-1',
      started_at: 1_700_000_000_100,
      updated_at: 1_700_000_000_300,
      stop_reason: 'completed',
      failure_json: null,
      result_json: JSON.stringify({ structured: { rows: 2 }, outputText: 'report done', stopReason: 'completed' }),
    },
    {
      attempt_id: 'att-2',
      run_id: RUN_ID,
      task_id: 'beta',
      ordinal: 1,
      state: 'succeeded',
      backend: 'spawn',
      child_session: 'sess-beta-1',
      owner_token: 'owner-2',
      started_at: 1_700_000_000_350,
      updated_at: 1_700_000_000_450,
      stop_reason: 'completed',
      failure_json: null,
      result_json: null,
    },
  ]
}

/** outputs-table row(s) — value_json unparsed. */
function outputRows() {
  return [
    {
      run_id: RUN_ID,
      task_id: 'alpha',
      name: 'report',
      value_json: JSON.stringify({ rows: 2, verdict: 'ok' }),
      produced_by_attempt: 'att-1',
      created_at: 1_700_000_000_300,
    },
  ]
}

/** Fake store exposing exactly the reader surface the face consumes. */
function fakeStore({ run = runRow() } = {}) {
  const calls = { findRun: [], findTasks: [], findAttempts: [], findOutputsByTask: [] }
  return {
    calls,
    findRun(runId) {
      calls.findRun.push(runId)
      if (runId === run.run_id) return { ...run }
      return null
    },
    findTasks(runId) {
      calls.findTasks.push(runId)
      return taskRows().map((row) => ({ ...row }))
    },
    findAttempts(runId, taskId) {
      calls.findAttempts.push([runId, taskId])
      return attemptRows().filter((row) => row.task_id === taskId).map((row) => ({ ...row }))
    },
    findOutputsByTask(runId, taskId) {
      calls.findOutputsByTask.push([runId, taskId])
      return outputRows().filter((row) => row.task_id === taskId).map((row) => ({ ...row }))
    },
  }
}

/**
 * Fake store with the runs-by-session surface: two runs linked to the
 * planning session, one to another session, one unlinked (NULL).
 */
function fakeSessionStore() {
  const allRuns = [
    runRow({ run_id: 'dag_1', planner_session: 'gui-1', created_at: 1, updated_at: 2 }),
    runRow({ run_id: 'dag_2', planner_session: 'gui-1', created_at: 3, updated_at: 4 }),
    runRow({ run_id: 'dag_3', planner_session: 'gui-2', created_at: 5, updated_at: 6 }),
    runRow({ run_id: 'dag_4', planner_session: null, created_at: 7, updated_at: 8 }),
  ]
  const calls = { findRunsByPlannerSession: [], findTasks: [] }
  return {
    calls,
    allRuns,
    findRun(runId) {
      const found = allRuns.find((row) => row.run_id === runId)
      return found === undefined ? null : { ...found }
    },
    findRunsByPlannerSession(sessionId) {
      calls.findRunsByPlannerSession.push(sessionId)
      return allRuns.filter((row) => row.planner_session === sessionId).map((row) => ({ ...row }))
    },
    findTasks(runId) {
      calls.findTasks.push(runId)
      // One succeeded + one pending task per run — exercises countsOfTasks.
      return [
        { run_id: runId, task_id: 'alpha', state: 'succeeded', version: 2, blocked_reason: null, retry_not_before: null, updated_at: 2 },
        { run_id: runId, task_id: 'beta', state: 'pending', version: 1, blocked_reason: null, retry_not_before: null, updated_at: 2 },
      ]
    },
    findAttempts() {
      return []
    },
    findOutputsByTask() {
      return []
    },
  }
}

/** Fake engine: status records calls and returns a canned shape. */
function fakeEngine(canned = { kind: 'status', detail: 'summary', runs: [] }) {
  const calls = []
  return {
    calls,
    status(runId, options) {
      calls.push({ runId, options })
      return canned
    },
  }
}

// ---------------------------------------------------------------------------
// status passthrough
// ---------------------------------------------------------------------------

test('dag-face: status passthrough — undefined/null/\'\' normalise to all-runs, options by reference', () => {
  const canned = { kind: 'status', detail: 'summary', runs: [{ run_id: RUN_ID }] }
  const engine = fakeEngine(canned)
  const face = createDagFace({ engine, store: fakeStore(), logger: {} })

  for (const blank of [undefined, null, '']) {
    engine.calls.length = 0
    const options = { detail: 'tasks', limit: 7 }
    const out = face.status(blank, options)
    assert.equal(out, canned, `blank ${JSON.stringify(blank)} returns the engine value by identity`)
    assert.equal(engine.calls.length, 1)
    assert.equal(engine.calls[0].runId, undefined, 'blank reaches the engine as undefined (all-runs arm)')
    assert.equal(engine.calls[0].options, options, 'options pass through by reference')
  }

  // A concrete run id passes through verbatim.
  const options = { detail: 'attempts', taskId: 'alpha' }
  const out = face.status(RUN_ID, options)
  assert.equal(out, canned)
  assert.equal(engine.calls.at(-1).runId, RUN_ID)
  assert.equal(engine.calls.at(-1).options, options)
})

test('dag-face: status omits options entirely when not given (engine default {} arm)', () => {
  const engine = fakeEngine()
  const face = createDagFace({ engine, store: fakeStore() })
  face.status(RUN_ID)
  assert.equal(engine.calls.length, 1)
  assert.equal(engine.calls[0].runId, RUN_ID)
  assert.equal(engine.calls[0].options, undefined)
})

// ---------------------------------------------------------------------------
// getSpec
// ---------------------------------------------------------------------------

test('dag-face: getSpec parses spec_json and returns run_id/name/spec_hash/spec', () => {
  const face = createDagFace({ engine: fakeEngine(), store: fakeStore() })
  const out = face.getSpec(RUN_ID)

  assert.deepEqual(Object.keys(out).sort(), ['name', 'run_id', 'spec', 'spec_hash'])
  assert.equal(out.run_id, RUN_ID)
  assert.equal(out.name, 'face-demo')
  assert.equal(out.spec_hash, SPEC_HASH)
  // The parsed spec is the graph source: 2 tasks + the dependsOn edge.
  assert.equal(out.spec.tasks.length, 2)
  assert.deepEqual(out.spec.tasks[1].dependsOn, [{ taskId: 'alpha', condition: 'succeeded' }])
  assert.equal(out.spec.tasks[1].inputs[0], 'task://alpha/report')
})

// ---------------------------------------------------------------------------
// listOutputs
// ---------------------------------------------------------------------------

test('dag-face: listOutputs composes per-task outputs with parsed value_json', () => {
  const store = fakeStore()
  const face = createDagFace({ engine: fakeEngine(), store })
  const rows = face.listOutputs(RUN_ID)

  assert.equal(rows.length, 1)
  assert.deepEqual(rows[0], {
    task_id: 'alpha',
    name: 'report',
    value: { rows: 2, verdict: 'ok' },
    produced_by_attempt: 'att-1',
  })
  // Composed from findTasks order, one findOutputsByTask per task.
  assert.deepEqual(store.calls.findOutputsByTask, [[RUN_ID, 'alpha'], [RUN_ID, 'beta']])
  // json-safe: no undefined-valued keys anywhere.
  for (const row of rows) {
    for (const value of Object.values(row)) assert.notEqual(value, undefined)
  }
})

// ---------------------------------------------------------------------------
// attemptSummaries
// ---------------------------------------------------------------------------

test('dag-face: attemptSummaries — summary only when result_json present; no undefined-valued keys', () => {
  const face = createDagFace({ engine: fakeEngine(), store: fakeStore() })
  const rows = face.attemptSummaries(RUN_ID)

  assert.equal(rows.length, 2)
  const [alpha, beta] = rows

  // Core keys always present.
  assert.deepEqual(
    { attempt_id: alpha.attempt_id, task_id: alpha.task_id, ordinal: alpha.ordinal, state: alpha.state, backend: alpha.backend, started_at: alpha.started_at },
    { attempt_id: 'att-1', task_id: 'alpha', ordinal: 1, state: 'succeeded', backend: 'spawn', started_at: 1_700_000_000_100 },
  )
  // Optional keys expand conditionally.
  assert.equal(alpha.child_session, 'sess-alpha-1')
  assert.equal(alpha.stop_reason, 'completed')
  assert.deepEqual(alpha.summary, { structured: { rows: 2 }, outputText: 'report done', stopReason: 'completed' })

  // att-2 has NO result_json: `summary` must be OMITTED (not undefined).
  assert.equal('summary' in beta, false)
  assert.equal(beta.child_session, 'sess-beta-1')

  // No undefined-valued keys anywhere (the lossless-JSON gate).
  for (const row of rows) {
    for (const value of Object.values(row)) assert.notEqual(value, undefined)
  }
})

test('dag-face: attemptSummaries taskId filter narrows to one task; unknown task yields []', () => {
  const store = fakeStore()
  const face = createDagFace({ engine: fakeEngine(), store })

  const only = face.attemptSummaries(RUN_ID, 'beta')
  assert.equal(only.length, 1)
  assert.equal(only[0].task_id, 'beta')
  assert.equal('summary' in only[0], false)
  // The filtered arm must NOT walk every task.
  assert.deepEqual(store.calls.findAttempts, [[RUN_ID, 'beta']])

  assert.deepEqual(face.attemptSummaries(RUN_ID, 'no-such-task'), [])
})

// ---------------------------------------------------------------------------
// runsForSession
// ---------------------------------------------------------------------------

test('dag-face: runsForSession filters planner_session rows into summary-shaped rows', () => {
  const store = fakeSessionStore()
  const face = createDagFace({ engine: fakeEngine(), store })
  const out = face.runsForSession('gui-1')

  assert.deepEqual(store.calls.findRunsByPlannerSession, ['gui-1'])
  assert.equal(out.runs.length, 2)
  assert.deepEqual(out.runs.map((row) => row.run_id), ['dag_1', 'dag_2'])

  // Row shape matches the all-runs summary arm exactly (engine.status rows).
  const row = out.runs[0]
  assert.deepEqual(
    Object.keys(row).sort(),
    ['counts', 'created_at', 'name', 'run_id', 'state', 'updated_at'],
  )
  assert.equal(row.name, 'face-demo')
  assert.equal(row.state, 'succeeded')
  assert.deepEqual(row.counts, { pending: 1, ready: 0, running: 0, succeeded: 1, failed: 0, blocked: 0 })

  // The counts fold mirrors engine.status's countsOf: queued→ready,
  // retry_wait→pending, cancelled→failed.
  store.findTasks = () => [
    { task_id: 'a', state: 'queued' },
    { task_id: 'b', state: 'retry_wait' },
    { task_id: 'c', state: 'cancelled' },
  ]
  assert.deepEqual(face.runsForSession('gui-1').runs[0].counts, {
    pending: 1, ready: 1, running: 0, succeeded: 0, failed: 1, blocked: 0,
  })
})

test('dag-face: runsForSession — unknown session returns empty (never an error); blank ids fail loud', () => {
  const face = createDagFace({ engine: fakeEngine(), store: fakeSessionStore() })
  assert.deepEqual(face.runsForSession('no-such-session'), { runs: [] })
  assert.deepEqual(face.runsForSession('gui-2').runs.map((row) => row.run_id), ['dag_3'])

  // Non-string / empty session ids are treated as invalid, never guessed.
  for (const bad of [undefined, null, '', 42]) {
    assert.throws(
      () => face.runsForSession(bad),
      (error) => error instanceof Error && error.code === 'dag.run_not_found',
      `runsForSession(${JSON.stringify(bad)}) must fail loud`,
    )
  }
})

test('dag-face: runsForSession falls back to findAllRuns filtering when the store predates the helper', () => {
  const store = fakeSessionStore()
  // Simulate an older/fake store: the helper is absent, findAllRuns answers.
  delete store.findRunsByPlannerSession
  store.findAllRuns = () => store.allRuns.map((row) => ({ ...row }))
  const face = createDagFace({ engine: fakeEngine(), store })

  assert.deepEqual(face.runsForSession('gui-1').runs.map((row) => row.run_id), ['dag_1', 'dag_2'])
  assert.deepEqual(face.runsForSession('gui-9'), { runs: [] })
})

// ---------------------------------------------------------------------------
// dag.run_not_found + construction discipline
// ---------------------------------------------------------------------------

test('dag-face: unknown run → dag.run_not_found (message prefix + .code) for all three run-scoped methods', () => {
  const face = createDagFace({ engine: fakeEngine(), store: fakeStore() })
  for (const method of ['getSpec', 'listOutputs', 'attemptSummaries']) {
    assert.throws(
      () => face[method]('dag_20990101_nopeeeee'),
      (error) => error instanceof Error
        && error.message.startsWith('dag.run_not_found')
        && error.code === 'dag.run_not_found',
      `${method} must throw the dag.run_not_found shape`,
    )
  }
  // Non-string / empty run ids are treated as unknown, never forwarded.
  for (const bad of [undefined, null, '', 42]) {
    assert.throws(
      () => face.getSpec(bad),
      (error) => error.code === 'dag.run_not_found',
      `getSpec(${JSON.stringify(bad)}) must fail loud, not guess`,
    )
  }
})

test('dag-face: the face is frozen; malformed deps are loud constructor errors', () => {
  const face = createDagFace({ engine: fakeEngine(), store: fakeStore() })
  assert.equal(Object.isFrozen(face), true)
  assert.throws(() => {
    face.status = () => 'hijacked'
  }, TypeError)
  assert.equal(typeof face.status, 'function', 'the original method survives the freeze')

  // Deps validation mirrors the tool layer's shape checks.
  assert.throws(() => createDagFace({}), /engine/)
  assert.throws(() => createDagFace({ engine: {} }), /engine/)
  assert.throws(() => createDagFace({ engine: fakeEngine() }), /store/)
  assert.throws(() => createDagFace({ engine: fakeEngine(), store: { findRun() {} } }), /findOutputsByTask/)
})

// ---------------------------------------------------------------------------
// apply() registration path (fake ctx per test/index.test.js)
// ---------------------------------------------------------------------------

/** The index.test.js fake ctx, extended with an optional provide recorder. */
function fakeCtx({ provideImpl } = {}) {
  const registered = []
  const teardowns = []
  const lines = { info: [], warn: [], fatal: [] }
  const provided = {}
  const ctx = {
    registered,
    teardowns,
    lines,
    provided,
    tools: {
      register(definition) {
        registered.push(definition)
      },
    },
    subagents: {
      start: async () => {
        throw new Error('no dispatch in dag-face test')
      },
    },
    effect(body) {
      const disposer = body()
      teardowns.push(disposer)
      return () => disposer()
    },
    logger: {
      info: (m) => lines.info.push(m),
      warn: (m) => lines.warn.push(m),
      fatal: (m) => lines.fatal.push(m),
    },
  }
  if (provideImpl !== undefined) {
    ctx.provide = (name, value) => {
      provided[name] = value
      provideImpl(name, value)
    }
  }
  return ctx
}

/** Fresh temp db path per test. */
function tmpDbPath() {
  return join(mkdtempSync(join(tmpdir(), 'dag-face-')), 'dag.db')
}

/**
 * Seed one TERMINAL (succeeded) run — tasks, both attempts (one with
 * result_json, one without), the alpha output, the run.created event —
 * then close the store. A terminal run with an intact chain is exactly
 * the residue reconcile leaves untouched, so apply() observing it proves
 * the face is published over post-reconcile state (red line 4).
 */
async function seedTerminalRun({ path }) {
  const store = await createDagStore({ path })
  store.tx(() => {
    store.insertRun({
      run_id: RUN_ID,
      name: 'face-demo',
      spec_json: SPEC_JSON,
      spec_hash: SPEC_HASH,
      state: 'succeeded',
      control_intent: null,
      parent_session: null,
      planner_session: 'gui-sess-7',
      base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000,
      updated_at: 1_700_000_000_500,
      version: 3,
    })
    store.insertTasks(RUN_ID, [
      { task_id: 'alpha', state: 'succeeded', version: 2 },
      { task_id: 'beta', state: 'succeeded', version: 2 },
    ])
    for (const row of attemptRows()) {
      store.insertAttempt({
        attempt_id: row.attempt_id,
        run_id: row.run_id,
        task_id: row.task_id,
        ordinal: row.ordinal,
        state: row.state,
        backend: row.backend,
        child_session: row.child_session,
        owner_token: row.owner_token,
        started_at: row.started_at,
        updated_at: row.updated_at,
        stop_reason: row.stop_reason,
        failure_json: row.failure_json,
        result_json: row.result_json,
      })
    }
    store.upsertOutput(RUN_ID, 'alpha', 'report', JSON.stringify({ rows: 2, verdict: 'ok' }), 'att-1')
    store.insertEvent(RUN_ID, {
      type: 'run.created',
      payload: { name: 'face-demo', spec_hash: SPEC_HASH, task_count: 2, from: '', to: 'running' },
      at: 1_700_000_000_000,
    })
  })
  store.close()
}

test('apply: provides the frozen dagOrchestrator face over the reconciled db; all four methods work through it', async (t) => {
  const path = tmpDbPath()
  await seedTerminalRun({ path })
  const ctx = fakeCtx({ provideImpl: () => {} }) // record into ctx.provided
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  // Registered under the exact service name.
  assert.equal(typeof ctx.provided.dagOrchestrator, 'object')
  const face = ctx.provided.dagOrchestrator
  assert.equal(Object.isFrozen(face), true)

  // The five tools registered too — the face must not displace them.
  assert.deepEqual(
    ctx.registered.map((tool) => tool.name).sort(),
    ['dag_approve', 'dag_control', 'dag_plan', 'dag_status', 'dag_tick'],
  )

  // getSpec: parsed spec + hash straight off the reconciled rows.
  const spec = face.getSpec(RUN_ID)
  assert.equal(spec.spec_hash, SPEC_HASH)
  assert.equal(spec.spec.tasks.length, 2)
  assert.deepEqual(spec.spec.tasks[1].dependsOn, [{ taskId: 'alpha', condition: 'succeeded' }])

  // status: real engine passthrough (all-runs arm sees the terminal run).
  const all = face.status(undefined)
  assert.equal(all.kind, 'status')
  assert.equal(all.detail, 'summary')
  assert.equal(all.runs.length, 1)
  assert.equal(all.runs[0].run_id, RUN_ID)
  assert.equal(all.runs[0].state, 'succeeded')
  assert.equal(all.runs[0].counts.succeeded, 2)
  // ...and the single-run tasks arm.
  const tasks = face.status(RUN_ID, { detail: 'tasks' })
  assert.equal(tasks.detail, 'tasks')
  assert.deepEqual(tasks.tasks.map((row) => row.id), ['alpha', 'beta'])

  // listOutputs: the seeded alpha report, parsed.
  assert.deepEqual(face.listOutputs(RUN_ID), [
    { task_id: 'alpha', name: 'report', value: { rows: 2, verdict: 'ok' }, produced_by_attempt: 'att-1' },
  ])

  // attemptSummaries: summary present exactly for att-1.
  const attempts = face.attemptSummaries(RUN_ID)
  assert.equal(attempts.length, 2)
  assert.deepEqual(attempts.map((row) => row.attempt_id).sort(), ['att-1', 'att-2'])
  const withSummary = attempts.find((row) => row.attempt_id === 'att-1')
  const without = attempts.find((row) => row.attempt_id === 'att-2')
  assert.equal(withSummary.summary.stopReason, 'completed')
  assert.equal('summary' in without, false)

  // runsForSession: the seeded run planned by gui-sess-7; another session
  // (and the NULL default) yields empty through the SAME provided face.
  const mine = face.runsForSession('gui-sess-7')
  assert.equal(mine.runs.length, 1)
  assert.equal(mine.runs[0].run_id, RUN_ID)
  assert.equal(mine.runs[0].state, 'succeeded')
  assert.deepEqual(mine.runs[0].counts.succeeded, 2)
  assert.deepEqual(face.runsForSession('gui-sess-other'), { runs: [] })

  // Unknown run still fails loud through the provided face.
  assert.throws(() => face.getSpec('dag_20990101_nopeeeee'), (error) => error.code === 'dag.run_not_found')

  // No warn was needed on the happy path.
  assert.equal(ctx.lines.warn.filter((line) => line.includes('dagOrchestrator')).length, 0)
})

test('apply: ctx.provide absent → graceful skip, tools still register, no throw', async (t) => {
  const path = tmpDbPath()
  const ctx = fakeCtx() // no provide key at all
  const returned = await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  assert.equal(returned, undefined, 'apply still returns undefined (Cordis contract)')
  assert.deepEqual(
    ctx.registered.map((tool) => tool.name).sort(),
    ['dag_approve', 'dag_control', 'dag_plan', 'dag_status', 'dag_tick'],
  )
  assert.equal(ctx.provided.dagOrchestrator, undefined)
  assert.equal(ctx.lines.warn.filter((line) => line.includes('dagOrchestrator')).length, 0)
})

test('apply: ctx.provide throwing → warned, apply completes, tools still register', async (t) => {
  const path = tmpDbPath()
  const ctx = fakeCtx({
    provideImpl: () => {
      throw new Error('host refused the service name')
    },
  })
  const returned = await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  assert.equal(returned, undefined)
  assert.deepEqual(
    ctx.registered.map((tool) => tool.name).sort(),
    ['dag_approve', 'dag_control', 'dag_plan', 'dag_status', 'dag_tick'],
  )
  const faceWarns = ctx.lines.warn.filter((line) => line.includes('dagOrchestrator'))
  assert.equal(faceWarns.length, 1)
  assert.match(faceWarns[0], /host refused the service name/)
})
