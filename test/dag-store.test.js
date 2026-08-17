// T03 DagStore tests — DESIGN §6 (schema, hash chain, tx atomicity, ownership).
//
// Zero network, zero CLI, zero models: only node:test + node:sqlite on temp
// dirs. Tamper/foreign-db cases open raw `DatabaseSync` connections on the
// test's own temp files — the "sqlite only in lib/dag-store.js" discipline
// governs lib/ modules, not tests that deliberately play the attacker.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { canonicalJson, createDagStore } from '../lib/dag-store.js'

const GENESIS = '0'.repeat(64)

/** Fresh temp db path per test — isolation without shared fixtures. */
function tmpDbPath() {
  return join(mkdtempSync(join(tmpdir(), 'dag-store-')), 'dag.db')
}

async function openStore() {
  const store = await createDagStore({ path: tmpDbPath() })
  return store
}

/** Minimal seeded run: 1 run (running v1) + 2 pending tasks. */
function seedRun(store, { runId = 'run-1', state = 'running' } = {}) {
  const run = store.insertRun({
    run_id: runId,
    name: 'refactor-auth',
    spec_json: JSON.stringify({ version: 1, name: 'refactor-auth', tasks: [] }),
    spec_hash: 'a'.repeat(64),
    state,
    control_intent: null,
    parent_session: 'sess-42',
    base_cwd: '/tmp/repo',
    created_at: 1_700_000_000_000,
    updated_at: 1_700_000_000_000,
    version: 3,
  })
  store.insertTasks(runId, [
    { task_id: 'analyze', state: 'pending', version: 1 },
    { task_id: 'impl-core', state: 'pending', version: 1 },
  ])
  return run
}

test('createDagStore: creates a 0600 WAL database with the ownership magic', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  t.after(() => store.close())

  const mode = statSync(path).mode & 0o777
  assert.equal(mode, 0o600, `db file mode must be 0600, got ${mode.toString(8)}`)

  const raw = new DatabaseSync(path)
  t.after(() => raw.close())
  assert.equal(raw.prepare('PRAGMA journal_mode').get().journal_mode, 'wal')
  assert.equal(raw.prepare('PRAGMA application_id').get().application_id, 0x44147d20)
  assert.equal(raw.prepare('PRAGMA user_version').get().user_version, 1)
  const tables = raw
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT GLOB 'sqlite_*' ORDER BY name")
    .all()
    .map((row) => row.name)
  assert.deepEqual(tables, ['approvals', 'attempts', 'events', 'outputs', 'runs', 'tasks'])
})

test('createDagStore: rejects bad arguments loud', async () => {
  await assert.rejects(() => createDagStore(), TypeError)
  await assert.rejects(() => createDagStore({ path: '' }), TypeError)
  await assert.rejects(() => createDagStore({ path: 42 }), TypeError)
})

test('createDagStore: refuses a foreign sqlite database without the magic (application_id mismatch)', async () => {
  const path = tmpDbPath()
  const foreign = new DatabaseSync(path)
  foreign.exec('CREATE TABLE their_stuff (id INTEGER PRIMARY KEY)')
  foreign.prepare('INSERT INTO their_stuff (id) VALUES (?)').run(1)
  foreign.close()

  await assert.rejects(
    () => createDagStore({ path }),
    (error) => {
      assert.equal(error.code, 'dag.store_ownership')
      assert.match(error.message, /another application|not empty/)
      return true
    },
  )
})

test('createDagStore: refuses a non-sqlite file', async () => {
  const path = tmpDbPath()
  writeFileSync(path, 'definitely not a sqlite database, not even close')
  await assert.rejects(
    () => createDagStore({ path }),
    (error) => error.code === 'dag.store_ownership',
  )
})

test('round-trip: run, tasks, attempt, approval and output survive a close/reopen intact', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  const insertedRun = seedRun(store)
  const attempt = store.tx(() => {
    const row = store.insertAttempt({
      attempt_id: 'att-1',
      run_id: 'run-1',
      task_id: 'analyze',
      ordinal: 1,
      state: 'claimed',
      backend: 'spawn',
      child_session: 'child-7',
      owner_token: 'handle-1',
      started_at: 1_700_000_000_123,
      updated_at: 1_700_000_000_123,
    })
    store.insertEvent('run-1', { type: 'attempt.claimed', taskId: 'analyze', attemptId: 'att-1', payload: { backend: 'spawn' }, at: 1_700_000_000_200 })
    store.insertApproval({ approval_id: 'apr-1', run_id: 'run-1', task_id: 'impl-core', action: 'approve_integration', state: 'pending', note: 'ship it?', created_at: 1_700_000_001_000 })
    store.upsertOutput('run-1', 'analyze', 'analysis', '{"summary":"ok"}', 'att-1')
    return row
  })
  store.close()
  t.after(() => {})

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())

  assert.deepEqual(reopened.findRun('run-1'), insertedRun)
  assert.deepEqual(reopened.findAttempt('att-1'), attempt)
  assert.equal(reopened.findTasks('run-1').length, 2)
  assert.deepEqual(reopened.findTasks('run-1').map((row) => row.task_id), ['analyze', 'impl-core'])
  assert.deepEqual(reopened.findAttempts('run-1', 'analyze'), [attempt])
  const events = reopened.findEvents('run-1')
  assert.equal(events.length, 1)
  assert.equal(events[0].type, 'attempt.claimed')
  assert.equal(events[0].task_id, 'analyze')
  assert.equal(events[0].attempt_id, 'att-1')
  assert.equal(events[0].payload_json, '{"backend":"spawn"}')
  assert.equal(events[0].prev_hash, GENESIS)
  assert.equal(reopened.findPendingApprovals('run-1').length, 1)
  const output = reopened.findOutput('run-1', 'analyze', 'analysis')
  assert.equal(output.value_json, '{"summary":"ok"}')
  assert.equal(output.produced_by_attempt, 'att-1')
  assert.deepEqual(reopened.verifyChain('run-1'), { ok: true })
  // WAL journal mode persists across connections.
  const raw = new DatabaseSync(path)
  t.after(() => raw.close())
  assert.equal(raw.prepare('PRAGMA journal_mode').get().journal_mode, 'wal')
})

test('runs: findNonTerminalRuns excludes terminal states; casRunState patches control_intent', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store, { runId: 'r-live', state: 'running' })
  seedRun(store, { runId: 'r-paused', state: 'paused' })
  seedRun(store, { runId: 'r-done', state: 'succeeded' })

  assert.deepEqual(
    store.findNonTerminalRuns().map((row) => row.run_id).sort(),
    ['r-live', 'r-paused'],
  )

  // Successful CAS with a patch column.
  const ok = store.casRunState('r-live', 'running', 3, 'pausing', { control_intent: 'pause' })
  assert.equal(ok.ok, true)
  assert.equal(ok.row.state, 'pausing')
  assert.equal(ok.row.control_intent, 'pause')
  assert.equal(ok.row.version, 4)

  // Lost race on state → not-ok, no throw, current shape returned.
  const stale = store.casRunState('r-live', 'running', 4, 'paused')
  assert.equal(stale.ok, false)
  assert.equal(stale.reason, 'cas_mismatch')
  assert.deepEqual(stale.current, { state: 'pausing', version: 4 })

  // Lost race on version.
  const staleVersion = store.casRunState('r-live', 'pausing', 3, 'paused')
  assert.equal(staleVersion.ok, false)
  assert.equal(staleVersion.reason, 'cas_mismatch')

  // Unknown run.
  assert.deepEqual(store.casRunState('nope', 'running', 1, 'paused'), { ok: false, reason: 'not_found' })

  // setControlIntent set/clear/missing.
  assert.equal(store.setControlIntent('r-live', 'stop').row.control_intent, 'stop')
  assert.equal(store.setControlIntent('r-live', null).row.control_intent, null)
  assert.deepEqual(store.setControlIntent('nope', 'stop'), { ok: false, reason: 'not_found' })

  // Unknown patch key is a loud programming error.
  assert.throws(() => store.casRunState('r-live', 'pausing', 4, 'paused', { bogus: 1 }), /unknown patch column/)
})

test('runs: findAllRuns returns terminal and non-terminal runs (T08 dag_status source)', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store, { runId: 'r-live', state: 'running' })
  seedRun(store, { runId: 'r-done', state: 'failed' })

  // seedRun pins created_at, so the documented ORDER BY created_at, run_id
  // tie-breaks alphabetically here — the assertion checks BOTH halves of
  // that contract.
  const all = store.findAllRuns()
  assert.deepEqual(all.map((row) => row.run_id), ['r-done', 'r-live'])
  assert.deepEqual(all.map((row) => row.state), ['failed', 'running'])
})

test('tasks: casTaskState CAS semantics and whitelisted patches', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)

  const ok = store.casTaskState('run-1', 'analyze', 'pending', 1, 'blocked', { blocked_reason: 'approval_pending', retry_not_before: 123 })
  assert.equal(ok.ok, true)
  assert.equal(ok.row.state, 'blocked')
  assert.equal(ok.row.blocked_reason, 'approval_pending')
  assert.equal(ok.row.retry_not_before, 123)
  assert.equal(ok.row.version, 2)

  const stale = store.casTaskState('run-1', 'analyze', 'pending', 2, 'ready')
  assert.equal(stale.ok, false)
  assert.equal(stale.reason, 'cas_mismatch')
  assert.deepEqual(stale.current, { state: 'blocked', version: 2 })

  assert.deepEqual(store.casTaskState('run-1', 'ghost', 'pending', 1, 'ready'), { ok: false, reason: 'not_found' })
  assert.throws(() => store.casTaskState('run-1', 'analyze', 'blocked', 2, 'ready', { nope: 1 }), /unknown patch column/)
})

test('attempts: commitTerminal enforces state CAS + owner token, never throws on a lost race', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  store.insertAttempt({ attempt_id: 'a1', run_id: 'run-1', task_id: 'analyze', ordinal: 1, state: 'running', backend: 'spawn', owner_token: 'handle-1' })
  store.insertAttempt({ attempt_id: 'a2', run_id: 'run-1', task_id: 'impl-core', ordinal: 1, state: 'claimed', backend: 'spawn' })

  // Happy path: matching owner + in-flight state → terminal, patch recorded.
  const ok = store.commitTerminal('a1', 'handle-1', 'succeeded', { stop_reason: 'completed', result_json: '{"x":1}' })
  assert.equal(ok.ok, true)
  assert.equal(ok.row.state, 'succeeded')
  assert.equal(ok.row.stop_reason, 'completed')
  assert.equal(ok.row.result_json, '{"x":1}')

  // Same attempt again: already terminal — no regression, not-ok.
  const redo = store.commitTerminal('a1', 'handle-1', 'failed')
  assert.equal(redo.ok, false)
  assert.equal(redo.reason, 'not_in_flight')
  assert.equal(redo.attempt.state, 'succeeded')

  // Wrong owner on an in-flight attempt.
  const wrongOwner = store.commitTerminal('a2', 'handle-9', 'failed')
  assert.equal(wrongOwner.ok, false)
  assert.equal(wrongOwner.reason, 'owner_mismatch')
  assert.equal(wrongOwner.attempt.state, 'claimed')

  // NULL owner_token matches only a null ownerId.
  const nullOwner = store.commitTerminal('a2', null, 'orphaned')
  assert.equal(nullOwner.ok, true)
  assert.equal(nullOwner.row.state, 'orphaned')

  // Unknown attempt.
  assert.deepEqual(store.commitTerminal('ghost', null, 'failed'), { ok: false, reason: 'not_found' })

  // Illegal target is a loud programming error.
  assert.throws(() => store.commitTerminal('a1', 'handle-1', 'running'), /target must be one of/)

  // Non-terminal bookkeeping.
  assert.equal(store.hasNonTerminalAttempt('run-1', 'analyze'), false)
  assert.deepEqual(store.findNonTerminalAttempts('run-1'), [])
})

test('attempts: updateAttemptChildSession records the dispatched session on in-flight attempts only', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  store.insertAttempt({ attempt_id: 'a1', run_id: 'run-1', task_id: 'analyze', ordinal: 1, state: 'running', backend: 'spawn', owner_token: 'h1' })

  const ok = store.updateAttemptChildSession('a1', 'child-session-9')
  assert.equal(ok.ok, true)
  assert.equal(ok.row.child_session, 'child-session-9')

  // A terminal attempt refuses the update.
  store.commitTerminal('a1', 'h1', 'succeeded')
  const refused = store.updateAttemptChildSession('a1', 'child-session-10')
  assert.equal(refused.ok, false)
  assert.equal(refused.reason, 'not_in_flight')

  // Unknown attempt.
  assert.deepEqual(store.updateAttemptChildSession('ghost', 'x'), { ok: false, reason: 'not_found' })
  assert.throws(() => store.updateAttemptChildSession('a1', ''), /childSession/)
})

test('tasks: findReadyTasks returns only state=ready rows', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  store.tx(() => {
    store.casTaskState('run-1', 'analyze', 'pending', 1, 'ready')
    store.casTaskState('run-1', 'impl-core', 'pending', 1, 'blocked', { blocked_reason: JSON.stringify({ code: 'upstream_blocked' }) })
  })
  const ready = store.findReadyTasks('run-1')
  assert.deepEqual(ready.map((row) => row.task_id), ['analyze'])
})

test('tx: projection writes and events roll back together (invariants #3/#6)', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)

  const eventsBefore = store.findEvents('run-1').length
  const taskBefore = store.findTasks('run-1').find((row) => row.task_id === 'analyze')
  assert.equal(eventsBefore, 0)
  let casInsideTx
  assert.throws(
    () =>
      store.tx(() => {
        casInsideTx = store.casTaskState('run-1', 'analyze', 'pending', 1, 'running', { blocked_reason: null })
        store.insertAttempt({ attempt_id: 'att-x', run_id: 'run-1', task_id: 'analyze', ordinal: 1, state: 'claimed', backend: 'spawn' })
        store.insertEvent('run-1', { type: 'task.running', taskId: 'analyze', payload: { v: 1 } })
        throw new Error('simulated crash mid-tx')
      }),
    /simulated crash mid-tx/,
  )
  assert.equal(casInsideTx.ok, true, 'CAS must have succeeded inside the tx before the crash')

  // BOTH the projection and the event chain are unchanged.
  const taskAfter = store.findTasks('run-1').find((row) => row.task_id === 'analyze')
  assert.deepEqual(taskAfter, taskBefore)
  assert.equal(store.findEvents('run-1').length, 0)
  assert.equal(store.findAttempts('run-1', 'analyze').length, 0)
  assert.deepEqual(store.verifyChain('run-1'), { ok: true })

  // The store stays fully usable after a rollback.
  store.tx(() => store.insertEvent('run-1', { type: 'run.resumed', payload: {} }))
  assert.equal(store.findEvents('run-1').length, 1)
})

test('tx: nested transactions are rejected loud', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)

  assert.throws(
    () =>
      store.tx(() => {
        store.insertEvent('run-1', { type: 'outer.before', payload: {} })
        try {
          store.tx(() => {})
        } catch (error) {
          assert.equal(error.code, 'dag.nested_tx')
          throw error
        }
      }),
    (error) => error.code === 'dag.nested_tx',
  )
  // The rejected nesting rolled the outer tx back and left the store usable.
  assert.equal(store.findEvents('run-1').length, 0)
  store.tx(() => store.insertEvent('run-1', { type: 'after', payload: {} }))
  assert.equal(store.findEvents('run-1').length, 1)
})

test('tx: async callbacks are rejected loud (synchronous transactions only)', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  // tx() throws synchronously (it is a sync function); wrap so assert.rejects sees it.
  await assert.rejects(
    async () => store.tx(async () => {}),
    (error) => error.code === 'dag.async_tx',
  )
  assert.equal(store.findEvents('run-1').length, 0)
})

test('events: insertEvent outside tx is rejected loud', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  assert.throws(
    () => store.insertEvent('run-1', { type: 'task.ready', payload: {} }),
    (error) => error.code === 'dag.tx_required',
  )
})

test('events: seq allocation, genesis prev_hash and linkage', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  const rows = [1, 2, 3].map((n) =>
    store.tx(() => store.insertEvent('run-1', { type: `e${n}`, payload: { n }, at: 1000 + n })),
  )
  assert.deepEqual(rows.map((row) => row.seq), [1, 2, 3])
  assert.equal(rows[0].prev_hash, GENESIS)
  assert.equal(rows[1].prev_hash, rows[0].hash)
  assert.equal(rows[2].prev_hash, rows[1].hash)
  assert.match(rows[0].hash, /^[0-9a-f]{64}$/)

  // findEvents window options.
  assert.equal(store.findEvents('run-1').length, 3)
  assert.deepEqual(store.findEvents('run-1', { afterSeq: 1 }).map((row) => row.seq), [2, 3])
  assert.deepEqual(store.findEvents('run-1', { afterSeq: 0, limit: 2 }).map((row) => row.seq), [1, 2])

  // Two events in ONE tx get consecutive seqs (same-transaction chaining).
  const pair = store.tx(() => [
    store.insertEvent('run-1', { type: 'p1', payload: {}, at: 2000 }),
    store.insertEvent('run-1', { type: 'p2', payload: {}, at: 2001 }),
  ])
  assert.deepEqual(pair.map((row) => row.seq), [4, 5])
  assert.equal(pair[1].prev_hash, pair[0].hash)
  assert.deepEqual(store.verifyChain('run-1'), { ok: true })
})

test('events: chains are per-run (independent genesis and seq)', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store, { runId: 'run-a' })
  seedRun(store, { runId: 'run-b' })
  const a = store.tx(() => store.insertEvent('run-a', { type: 'x', payload: {}, at: 1 }))
  const b = store.tx(() => store.insertEvent('run-b', { type: 'y', payload: {}, at: 1 }))
  assert.equal(a.seq, 1)
  assert.equal(b.seq, 1)
  assert.equal(a.prev_hash, GENESIS)
  assert.equal(b.prev_hash, GENESIS)
  assert.deepEqual(store.verifyChain('run-a'), { ok: true })
  assert.deepEqual(store.verifyChain('run-b'), { ok: true })
})

test('chain: verifyChain detects a tampered payload', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store)
  for (let n = 1; n <= 5; n += 1) {
    store.tx(() => store.insertEvent('run-1', { type: `e${n}`, payload: { n }, at: 1000 + n }))
  }
  store.close()

  const raw = new DatabaseSync(path)
  raw.prepare('UPDATE events SET payload_json = ? WHERE run_id = ? AND seq = ?').run('{"tampered":true}', 'run-1', 3)
  raw.close()

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())
  const verdict = reopened.verifyChain('run-1')
  assert.equal(verdict.ok, false)
  assert.equal(verdict.firstBadSeq, 3, 'firstBadSeq must point at the tampered event')
})

test('chain: verifyChain detects corrupted payload_json that is not valid JSON', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store)
  store.tx(() => store.insertEvent('run-1', { type: 'e1', payload: { a: 1 }, at: 1 }))
  store.tx(() => store.insertEvent('run-1', { type: 'e2', payload: { b: 2 }, at: 2 }))
  store.close()

  const raw = new DatabaseSync(path)
  raw.prepare('UPDATE events SET payload_json = ? WHERE run_id = ? AND seq = ?').run('<<<not json>>>', 'run-1', 2)
  raw.close()

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())
  assert.deepEqual(reopened.verifyChain('run-1'), { ok: false, firstBadSeq: 2 })
})

test('chain: verifyChain detects a deleted middle event (gap in seq + broken linkage)', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store)
  for (let n = 1; n <= 4; n += 1) {
    store.tx(() => store.insertEvent('run-1', { type: `e${n}`, payload: { n }, at: 1000 + n }))
  }
  store.close()

  const raw = new DatabaseSync(path)
  raw.prepare('DELETE FROM events WHERE run_id = ? AND seq = ?').run('run-1', 2)
  raw.close()

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())
  const verdict = reopened.verifyChain('run-1')
  assert.equal(verdict.ok, false)
  assert.equal(verdict.firstBadSeq, 3, 'the event after the gap is the first bad one')
})

test('chain: verifyChain detects reordered seq numbers', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store)
  for (let n = 1; n <= 3; n += 1) {
    store.tx(() => store.insertEvent('run-1', { type: `e${n}`, payload: { n }, at: 1000 + n }))
  }
  store.close()

  // Swap seq 2 and 3 (via a spare value to dodge the UNIQUE constraint).
  const raw = new DatabaseSync(path)
  raw.prepare('UPDATE events SET seq = ? WHERE run_id = ? AND seq = ?').run(99, 'run-1', 2)
  raw.prepare('UPDATE events SET seq = ? WHERE run_id = ? AND seq = ?').run(2, 'run-1', 3)
  raw.prepare('UPDATE events SET seq = ? WHERE run_id = ? AND seq = ?').run(3, 'run-1', 99)
  raw.close()

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())
  const verdict = reopened.verifyChain('run-1')
  assert.equal(verdict.ok, false)
  assert.ok(verdict.firstBadSeq === 2 || verdict.firstBadSeq === 3, `unexpected firstBadSeq ${verdict.firstBadSeq}`)
})

test('chain: verifyChain detects a forged hash cell', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store)
  for (let n = 1; n <= 3; n += 1) {
    store.tx(() => store.insertEvent('run-1', { type: `e${n}`, payload: { n }, at: 1000 + n }))
  }
  store.close()

  const raw = new DatabaseSync(path)
  raw.prepare('UPDATE events SET hash = ? WHERE run_id = ? AND seq = ?').run('f'.repeat(64), 'run-1', 2)
  raw.close()

  const reopened = await createDagStore({ path })
  t.after(() => reopened.close())
  assert.deepEqual(reopened.verifyChain('run-1'), { ok: false, firstBadSeq: 2 })
})

test('canonical JSON: key order is irrelevant, arrays keep order, undefined keys drop', () => {
  assert.equal(canonicalJson({ a: 1, b: 2 }), canonicalJson({ b: 2, a: 1 }))
  assert.equal(canonicalJson({ a: 1, b: 2 }), '{"a":1,"b":2}')
  assert.equal(canonicalJson({ b: { d: 4, c: 3 }, a: [3, 1, 2] }), '{"a":[3,1,2],"b":{"c":3,"d":4}}')
  assert.equal(canonicalJson({ a: undefined, b: null }), '{"b":null}')
  assert.equal(canonicalJson('é'), '"é"')
  assert.equal(canonicalJson([]), '[]')
  assert.equal(canonicalJson({}), '{}')
  assert.throws(() => canonicalJson(undefined), TypeError)
  assert.throws(() => canonicalJson(10n), TypeError)
})

test('hash stability: same event content with different insertion key order → identical hash', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  seedRun(store, { runId: 'run-1' })
  seedRun(store, { runId: 'run-2' })
  const first = store.tx(() =>
    store.insertEvent('run-1', { type: 'task.ready', taskId: 't1', payload: { a: 1, nested: { y: 2, x: [3, 1] } }, at: 42 }),
  )
  const second = store.tx(() =>
    store.insertEvent('run-2', { type: 'task.ready', taskId: 't1', payload: { nested: { x: [3, 1], y: 2 }, a: 1 }, at: 42 }),
  )
  assert.equal(first.hash, second.hash)
  assert.equal(first.payload_json, second.payload_json)
  t.after(() => store.close())
})

test('approvals: pending lookup, decision CAS and idempotence', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  store.insertApproval({ approval_id: 'apr-1', run_id: 'run-1', task_id: 'gate', action: 'approve_integration', note: 'first note' })
  store.insertApproval({ approval_id: 'apr-2', run_id: 'run-1', task_id: 'gate2', action: 'deploy', state: 'approved', decided_at: 1 })

  const pending = store.findPendingApprovals('run-1')
  assert.deepEqual(pending.map((row) => row.approval_id), ['apr-1'])

  const decided = store.decideApproval('apr-1', 'approved', 'user said go')
  assert.equal(decided.ok, true)
  assert.equal(decided.row.state, 'approved')
  assert.equal(decided.row.note, 'user said go')
  assert.ok(decided.row.decided_at > 0)

  // Second decision on the same approval loses the CAS — not-ok, no throw.
  const again = store.decideApproval('apr-1', 'rejected', 'changed mind')
  assert.equal(again.ok, false)
  assert.equal(again.reason, 'already_decided')
  assert.equal(again.approval.state, 'approved')

  assert.deepEqual(store.decideApproval('ghost', 'approved'), { ok: false, reason: 'not_found' })
  assert.deepEqual(store.findPendingApprovals('run-1'), [])
  assert.throws(() => store.decideApproval('apr-2', 'maybe'), /approved\|rejected/)
})

test('outputs: upsert overwrites on conflict, lookups by task', async (t) => {
  const store = await openStore()
  t.after(() => store.close())
  seedRun(store)
  store.insertAttempt({ attempt_id: 'a1', run_id: 'run-1', task_id: 'analyze', ordinal: 1, state: 'running', backend: 'spawn' })
  store.insertAttempt({ attempt_id: 'a2', run_id: 'run-1', task_id: 'analyze', ordinal: 2, state: 'claimed', backend: 'spawn' })

  const first = store.upsertOutput('run-1', 'analyze', 'analysis', '{"v":1}', 'a1')
  assert.equal(first.value_json, '{"v":1}')
  const second = store.upsertOutput('run-1', 'analyze', 'analysis', '{"v":2}', 'a2')
  assert.equal(second.value_json, '{"v":2}')
  assert.equal(second.produced_by_attempt, 'a2')
  assert.equal(store.findOutput('run-1', 'analyze', 'analysis').produced_by_attempt, 'a2')

  store.upsertOutput('run-1', 'analyze', 'extra', 'null', 'a2')
  assert.deepEqual(store.findOutputsByTask('run-1', 'analyze').map((row) => row.name), ['analysis', 'extra'])
  assert.equal(store.findOutput('run-1', 'analyze', 'missing'), null)
  assert.throws(() => store.upsertOutput('run-1', 'analyze', 'bad', 42), /valueJson must be a string/)
})

test('close: idempotent; store methods fail loud afterwards', async (t) => {
  const store = await openStore()
  seedRun(store)
  store.close()
  store.close() // second close is a no-op
  assert.throws(() => store.findRun('run-1'), (error) => error.code === 'dag.store_closed')
  assert.throws(() => store.tx(() => {}), (error) => error.code === 'dag.store_closed')
  assert.throws(() => store.insertEvent('run-1', { type: 'x', payload: {} }), (error) => error.code === 'dag.store_closed')
})

test('in-memory store works for tests and tooling', async (t) => {
  const store = await createDagStore({ path: ':memory:' })
  t.after(() => store.close())
  seedRun(store)
  store.tx(() => store.insertEvent('run-1', { type: 'e', payload: { ok: true } }))
  assert.deepEqual(store.verifyChain('run-1'), { ok: true })
})

test('reopening an owned store at a different user_version is refused loud', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  store.close()
  const raw = new DatabaseSync(path)
  raw.exec('PRAGMA user_version = 99')
  raw.close()
  await assert.rejects(() => createDagStore({ path }), (error) => error.code === 'dag.store_version')
})
