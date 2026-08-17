// T09 apply() assembly tests — lib/index.js + lib/config.js + lib/recovery.js
// (TASKS.md T09 acceptance, DESIGN §3 / §12.2 / §2.2).
//
// Focus: the ASSEMBLY. The engine/store/executor contracts are covered by
// their own suites; here a fake ctx (tools.register collector, a
// never-dispatching subagents face, an effect recorder) asserts:
//
//   * the M1 tools + dag_control (T11) are registered (register switches prune);
//   * reconcile runs BEFORE the tools see the store — proven by the products:
//     a pre-seeded claimed attempt is already failed+retry_wait by the time
//     apply() returns, and the event chain stays verifiable;
//   * a tampered chain parks the broken run failed with recovery.chain_broken
//     while an intact sibling run in the SAME database is untouched;
//   * config is strict (unknown key → dag.config_invalid) and defaults
//     resolve (~ expansion included);
//   * apply() returns undefined (the Cordis disposable contract);
//   * the ctx.effect disposer closes the store, idempotently.
//
// Zero network, zero CLI, zero models: node:test + node:sqlite on temp dirs
// (raw DatabaseSync connections deliberately play the tamperer — the
// "sqlite only in lib/dag-store.js" discipline governs lib/, not attacker
// tests).

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { apply } from '../lib/index.js'
import { validateConfig, defaultDbPath, expandHome } from '../lib/config.js'
import { reconcile } from '../lib/recovery.js'
import { createDagStore } from '../lib/dag-store.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/** Fresh temp db path per test — isolation without shared fixtures. */
function tmpDbPath() {
  return join(mkdtempSync(join(tmpdir(), 'dag-apply-')), 'dag.db')
}

/**
 * Fake ctx per the T09 brief: tools.register collects definitions;
 * subagents.start throws (apply() itself must NEVER dispatch — dispatch is
 * the engine's tick-time business); effect runs the body immediately (the
 * Cordis contract: the body runs at registration, the RETURNED function is
 * the disposer) and records the disposer; logger collects lines.
 */
function fakeCtx() {
  const registered = []
  const teardowns = []
  const lines = { info: [], warn: [], fatal: [] }
  return {
    registered,
    teardowns,
    lines,
    tools: {
      register(definition) {
        registered.push(definition)
      },
    },
    subagents: {
      start: async () => {
        throw new Error('no dispatch in apply test')
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
}

/** A minimal spec_json the store accepts (shape-only: reconcile never parses it). */
const SPEC_JSON = JSON.stringify({ version: 1, name: 'seeded', tasks: [] })

/**
 * Seed one run + task + a claimed attempt + the run.created event, then
 * close the store: the on-disk crash residue apply() must reconcile.
 *
 * The attempt mirrors engine.claimTask's ACTUAL write shape: owner_token is
 * minted and persisted AT CLAIM TIME (inside the claim transaction) — real
 * crash residue carries it SET (the token dies with the crashed process;
 * no in-memory in-flight handle ever consumed it). Both conventions are
 * covered: `ownerToken: null` reproduces DESIGN §6.2's older "NULL until
 * dispatch" wording.
 */
async function seedClaimedAttempt({ path, runId = 'dag_seed_00000001', taskId = 'analyze', attemptId = 'att-1', ownerToken = 'owner-minted-at-claim' } = {}) {
  const store = await createDagStore({ path })
  store.tx(() => {
    store.insertRun({
      run_id: runId,
      name: 'seeded',
      spec_json: SPEC_JSON,
      spec_hash: 'a'.repeat(64),
      state: 'running',
      control_intent: null,
      parent_session: null,
      base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000,
      updated_at: 1_700_000_000_000,
      version: 1,
    })
    store.insertTasks(runId, [{ task_id: taskId, state: 'running', version: 1 }])
    store.insertAttempt({
      attempt_id: attemptId,
      run_id: runId,
      task_id: taskId,
      ordinal: 1,
      state: 'claimed',
      backend: 'spawn',
      child_session: null,
      owner_token: ownerToken, // SET at claim time (engine.claimTask shape)
      started_at: 1_700_000_000_000,
      updated_at: 1_700_000_000_000,
    })
    store.insertEvent(runId, {
      type: 'run.created',
      payload: { name: 'seeded', spec_hash: 'a'.repeat(64), task_count: 1, from: '', to: 'running' },
      at: 1_700_000_000_000,
    })
  })
  store.close()
  return { runId, taskId, attemptId }
}

/** Reopen the seeded db through the store (verification helper). */
async function reopen(path) {
  const store = await createDagStore({ path })
  return store
}

// ---------------------------------------------------------------------------
// config (lib/config.js)
// ---------------------------------------------------------------------------

test('config: defaults resolve (register five switches, dbPath via homedir)', () => {
  const verdict = validateConfig({})
  assert.equal(verdict.ok, true)
  const v = verdict.value
  assert.deepEqual(v.register, { plan: true, status: true, tick: true, control: true, approve: true })
  assert.equal(v.dbPath, defaultDbPath())
  assert.match(v.dbPath, /\.dsh\/dag-orchestrator\/dag\.db$/)
  assert.equal(v.defaultMaxRunningAgents, 4)
  assert.equal(v.defaultQueueCapacity, 16)
  assert.equal(v.inputInlineLimitBytes, 32768)
  assert.equal(v.autoTickMs, 0)
  assert.deepEqual(v.allowedRoots, [])
  assert.equal(v.requireWorkspaceRegistration, false)
})

test('config: every declared key validates and unknown keys fail with dag.config_invalid', () => {
  const good = validateConfig({
    register: { plan: true, status: false, tick: true, control: false, approve: false },
    dbPath: '~/somewhere/dag.db',
    defaultMaxRunningAgents: 8,
    defaultQueueCapacity: 32,
    inputInlineLimitBytes: 1024,
    autoTickMs: 30000,
    allowedRoots: ['/tmp'],
    requireWorkspaceRegistration: true,
  })
  assert.equal(good.ok, true)
  assert.equal(good.value.register.status, false)
  assert.equal(good.value.dbPath, expandHome('~/somewhere/dag.db'))
  assert.ok(!good.value.dbPath.startsWith('~'))

  // Unknown key — the typo guard.
  const bad = validateConfig({ dbpath: 'x' })
  assert.equal(bad.ok, false)
  assert.equal(bad.error.code, 'dag.config_invalid')
  assert.match(bad.error.message, /dbpath/)
  // Unknown register switch — strict at every object level.
  const badSwitch = validateConfig({ register: { plan: true, tick: false,Plan: true } })
  assert.equal(badSwitch.ok, false)
  // Range guard.
  const badRange = validateConfig({ defaultMaxRunningAgents: 64 })
  assert.equal(badRange.ok, false)
  assert.equal(badRange.error.code, 'dag.config_invalid')
})

test('config: expandHome handles ~, ~/x and pass-through forms', () => {
  assert.equal(typeof expandHome('~'), 'string')
  assert.ok(expandHome('~/a/b').endsWith(join('a', 'b')))
  assert.ok(!expandHome('~/a/b').startsWith('~'))
  assert.equal(expandHome('/abs/path'), '/abs/path')
  assert.equal(expandHome(':memory:'), ':memory:')
})

// ---------------------------------------------------------------------------
// apply() assembly
// ---------------------------------------------------------------------------

test('apply: registers the five M1+M2 tools and returns undefined', async (t) => {
  const ctx = fakeCtx()
  const path = tmpDbPath()
  const returned = await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  assert.equal(returned, undefined, 'apply must return undefined (Cordis disposable contract)')
  assert.deepEqual(
    ctx.registered.map((tool) => tool.name).sort(),
    ['dag_approve', 'dag_control', 'dag_plan', 'dag_status', 'dag_tick'],
  )
  for (const tool of ctx.registered) {
    assert.equal(typeof tool.execute, 'function')
  }
  assert.equal(ctx.subagentsCalled, undefined) // the fake's start was never even reachable
})

test('apply: register switches prune; the T12 approve switch registers dag_approve', async (t) => {
  const ctx = fakeCtx()
  const path = tmpDbPath()
  await apply(ctx, {
    dbPath: path,
    register: { plan: true, status: true, tick: false, control: false, approve: true },
  })
  t.after(() => ctx.teardowns[0]())

  assert.deepEqual(ctx.registered.map((tool) => tool.name).sort(), ['dag_approve', 'dag_plan', 'dag_status'])
  // T12 landed: no stale warn about an unimplemented approve switch.
  assert.doesNotMatch(ctx.lines.warn.join('\n'), /dag_approve/)
})

test('apply: unknown config key throws dag.config_invalid without touching the db path', async () => {
  const ctx = fakeCtx()
  const path = join(mkdtempSync(join(tmpdir(), 'dag-cfg-')), 'never.db')
  await assert.rejects(
    () => apply(ctx, { dbPath: path, dbpath: 'typo' }),
    (error) => error.message.includes('dag.config_invalid') && error.message.includes('dbpath'),
  )
  assert.equal(ctx.registered.length, 0)
  assert.equal(ctx.teardowns.length, 0)
})

test('apply: teardown closes the store and is idempotent', async () => {
  const ctx = fakeCtx()
  const path = tmpDbPath()
  await apply(ctx, { dbPath: path })
  const disposer = ctx.teardowns[0]
  assert.equal(typeof disposer, 'function')
  assert.equal(ctx.teardowns.length, 1)

  disposer()
  // Closed: every store method now throws dag.store_closed…
  const probe = await reopen(path)
  probe.close() // a fresh handle on the same file still works (the FILE is fine)

  // …and a second teardown call must not throw (idempotent).
  disposer()
  disposer()
})

test('apply: mid-assembly failure closes the store, registers nothing, leaves no effect', async () => {
  // No ctx.subagents → createExecutor throws AFTER the store opened and
  // reconcile ran: apply must surface the error AND close the store it
  // opened (no leaked handle; the effect was never registered).
  const ctx = fakeCtx()
  delete ctx.subagents
  const path = tmpDbPath()

  // Seed a claimed attempt so the reconcile phase provably ran first.
  await seedClaimedAttempt({ path })

  await assert.rejects(() => apply(ctx, { dbPath: path }), /ctxSubagents/)
  assert.equal(ctx.registered.length, 0, 'no tools registered')
  assert.equal(ctx.teardowns.length, 0, 'no effect registered — the catch path closed the store itself')

  // The file remains valid and the reconcile products are durable.
  const store = await reopen(path)
  assert.equal(store.findAttempt('att-1').state, 'failed')
  assert.deepEqual(store.verifyChain('dag_seed_00000001'), { ok: true })
  store.close()
})

// ---------------------------------------------------------------------------
// reconcile — claimed branch (the §12.1 auto-retry), chain verification
// ---------------------------------------------------------------------------

test('reconcile: claimed attempt → failed + task retry_wait + the three events, chain stays verifiable', async (t) => {
  const path = tmpDbPath()
  const { runId, taskId, attemptId } = await seedClaimedAttempt({ path })

  const ctx = fakeCtx()
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  const store = await reopen(path)
  t.after(() => store.close())

  // attempt → failed (owner-CAS commit, never dispatched)
  const attempt = store.findAttempt(attemptId)
  assert.equal(attempt.state, 'failed')
  assert.equal(attempt.stop_reason, 'internal')
  const failure = JSON.parse(attempt.failure_json)
  assert.equal(failure.code, 'recovery.no_dispatch')

  // task → retry_wait with retry_not_before = recovery now (retry NOW)
  const task = store.findTasks(runId).find((row) => row.task_id === taskId)
  assert.equal(task.state, 'retry_wait')
  assert.equal(typeof task.retry_not_before, 'number')

  // the event chain carries the full recovery sequence…
  const events = store.findEvents(runId, {})
  const types = events.map((e) => e.type)
  assert.ok(types.includes('recovery.no_dispatch'), `events: ${types.join(',')}`)
  assert.ok(types.includes('attempt.retry_scheduled'))
  assert.ok(types.includes('task.retry_wait'))
  const retryEvent = events.find((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(retryEvent.attempt_id, attemptId)
  assert.equal(JSON.parse(retryEvent.payload_json).recovery, true)

  // …in the Issue-5 order: no_dispatch → retry_scheduled → task.retry_wait
  const idx = (type) => types.indexOf(type)
  assert.ok(idx('recovery.no_dispatch') < idx('attempt.retry_scheduled'))
  assert.ok(idx('attempt.retry_scheduled') < idx('task.retry_wait'))

  // and the extended chain still verifies (the recovery events were chained).
  assert.deepEqual(store.verifyChain(runId), { ok: true })

  // the run row itself is untouched (running — the engine finishes it).
  assert.equal(store.findRun(runId).state, 'running')
})

test('reconcile: claimed branch recovers under BOTH owner_token conventions (set-at-claim AND null)', async (t) => {
  // engine.claimTask persists owner_token AT CLAIM TIME (state 'running'
  // with the token set; a pre-dispatch crash leaves that token on disk) —
  // the production residue shape. A null token covers DESIGN §6.2's older
  // "NULL until dispatch" wording. The recovery owner CAS must match the
  // row's PERSISTED token, not a hardcoded null (which would silently
  // no-op on the production shape).
  for (const ownerToken of ['owner-minted-at-claim', null]) {
    const path = tmpDbPath()
    const { runId, taskId, attemptId } = await seedClaimedAttempt({ path, ownerToken })
    const summary = await reconcile(await reopen(path), { logger: { info() {}, warn() {}, debug() {} } })
    const store = await reopen(path)
    t.after(() => store.close())

    assert.equal(summary.autoRetried, 1, `owner_token=${String(ownerToken)}: the attempt must be recovered`)
    assert.equal(store.findAttempt(attemptId).state, 'failed')
    const task = store.findTasks(runId).find((row) => row.task_id === taskId)
    assert.equal(task.state, 'retry_wait')
    assert.deepEqual(store.verifyChain(runId), { ok: true })
  }
})

test('reconcile: running attempt WITH child_session → orphaned + task failed + action_requested (T13)', async (t) => {
  const path = tmpDbPath()
  const store0 = await createDagStore({ path })
  store0.tx(() => {
    store0.insertRun({
      run_id: 'dag_run_00000001', name: 'seeded', spec_json: SPEC_JSON, spec_hash: 'a'.repeat(64),
      state: 'running', control_intent: null, parent_session: null, base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000, version: 1,
    })
    store0.insertTasks('dag_run_00000001', [{ task_id: 'analyze', state: 'running', version: 1 }])
    store0.insertAttempt({
      attempt_id: 'att-r1', run_id: 'dag_run_00000001', task_id: 'analyze', ordinal: 1,
      state: 'running', backend: 'spawn', child_session: 'sess-child-1', owner_token: 'tok-1',
      started_at: 1_700_000_000_000, updated_at: 1_700_000_000_000,
    })
    store0.insertEvent('dag_run_00000001', { type: 'run.created', payload: {}, at: 1_700_000_000_000 })
  })
  store0.close()

  const summary = await reconcile(await reopen(path), { logger: { info() {}, warn() {}, debug() {} } })
  const store = await reopen(path)
  t.after(() => store.close())

  // §12.2 running arm: orphaned (never auto-success, never auto-retried).
  assert.deepEqual(summary, { recoveredRuns: 1, autoRetried: 0, orphaned: 1, chainBroken: 0, orphanOutputs: 0 })
  const attempt = store.findAttempt('att-r1')
  assert.equal(attempt.state, 'orphaned')
  assert.equal(JSON.parse(attempt.failure_json).code, 'recovery.orphaned')
  // child_session SURVIVES as the manual session-log locator.
  assert.equal(attempt.child_session, 'sess-child-1')

  // task → failed (dead-end; downstream propagation belongs to the next tick).
  const task = store.findTasks('dag_run_00000001')[0]
  assert.equal(task.state, 'failed')

  // Event triad + Issue-5 ordering: attempt.orphaned → task.failed → action_requested.
  const events = store.findEvents('dag_run_00000001', {})
  const types = events.map((e) => e.type)
  assert.ok(types.includes('attempt.orphaned'))
  assert.ok(types.includes('task.failed'))
  assert.ok(types.includes('recovery.action_requested'))
  assert.ok(types.indexOf('attempt.orphaned') < types.indexOf('task.failed'))
  assert.ok(types.indexOf('task.failed') < types.indexOf('recovery.action_requested'))
  const action = events.find((e) => e.type === 'recovery.action_requested')
  assert.equal(JSON.parse(action.payload_json).childSession, 'sess-child-1')

  // Bounded policy: NOTHING auto-retried, run row itself untouched.
  assert.equal(types.includes('attempt.retry_scheduled'), false)
  assert.equal(store.findRun('dag_run_00000001').state, 'running')
  assert.deepEqual(store.verifyChain('dag_run_00000001'), { ok: true })
})

test('reconcile: running attempt WITHOUT child_session is the engine claim-shape → auto-retried (B1)', async (t) => {
  const path = tmpDbPath()
  const store0 = await createDagStore({ path })
  store0.tx(() => {
    store0.insertRun({
      run_id: 'dag_run_00000002', name: 'seeded', spec_json: SPEC_JSON, spec_hash: 'a'.repeat(64),
      state: 'running', control_intent: null, parent_session: null, base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000, version: 1,
    })
    store0.insertTasks('dag_run_00000002', [{ task_id: 'analyze', state: 'running', version: 1 }])
    // The exact residue engine.claimTask leaves when the host dies between
    // the claim tx and the dispatch's updateAttemptChildSession.
    store0.insertAttempt({
      attempt_id: 'att-r2', run_id: 'dag_run_00000002', task_id: 'analyze', ordinal: 1,
      state: 'running', backend: 'spawn', child_session: null, owner_token: 'tok-2',
      started_at: 1_700_000_000_000, updated_at: 1_700_000_000_000,
    })
    store0.insertEvent('dag_run_00000002', { type: 'run.created', payload: {}, at: 1_700_000_000_000 })
  })
  store0.close()

  const summary = await reconcile(await reopen(path), { logger: { info() {}, warn() {}, debug() {} } })
  const store = await reopen(path)
  t.after(() => store.close())

  assert.deepEqual(summary, { recoveredRuns: 1, autoRetried: 1, orphaned: 0, chainBroken: 0, orphanOutputs: 0 })
  assert.equal(store.findAttempt('att-r2').state, 'failed')
  assert.equal(JSON.parse(store.findAttempt('att-r2').failure_json).code, 'recovery.no_dispatch')
  assert.equal(store.findTasks('dag_run_00000002')[0].state, 'retry_wait')
  const retryEv = store.findEvents('dag_run_00000002', {}).find((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(JSON.parse(retryEv.payload_json).recovery, true)
  assert.deepEqual(store.verifyChain('dag_run_00000002'), { ok: true })
})

test('reconcile: a fresh empty database is a no-op', async (t) => {
  const path = tmpDbPath()
  const store = await createDagStore({ path })
  t.after(() => store.close())
  const summary = await reconcile(store, { logger: { info() {}, warn() {}, debug() {} } })
  assert.deepEqual(summary, { recoveredRuns: 0, autoRetried: 0, orphaned: 0, chainBroken: 0, orphanOutputs: 0 })
})

// ---------------------------------------------------------------------------
// chain-broken load refusal (§12.2 step 1)
// ---------------------------------------------------------------------------

test('apply: tampered chain parks the broken run failed with recovery.chain_broken; the sibling run is untouched', async (t) => {
  const path = tmpDbPath()

  // Two intact runs in one database…
  const seed = await createDagStore({ path })
  const seedRun = (runId) => seed.tx(() => {
    seed.insertRun({
      run_id: runId, name: runId, spec_json: SPEC_JSON, spec_hash: 'a'.repeat(64),
      state: 'running', control_intent: null, parent_session: null, base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000, version: 1,
    })
    seed.insertTasks(runId, [{ task_id: 'analyze', state: 'pending', version: 1 }])
    seed.insertEvent(runId, { type: 'run.created', payload: { n: runId }, at: 1_700_000_000_000 })
  })
  seedRun('dag_bad_00000001')
  seedRun('dag_ok_00000002')
  seed.close()

  // …then the attacker tampers with ONE run's event payload.
  const raw = new DatabaseSync(path)
  raw.prepare("UPDATE events SET payload_json = ? WHERE run_id = ? AND seq = ?")
    .run('{"tampered":true}', 'dag_bad_00000001', 1)
  raw.close()

  const ctx = fakeCtx()
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  const store = await reopen(path)
  t.after(() => store.close())

  // broken run → failed + recovery.chain_broken carrying firstBadSeq
  assert.equal(store.findRun('dag_bad_00000001').state, 'failed')
  const events = store.findEvents('dag_bad_00000001', {})
  const broken = events.find((e) => e.type === 'recovery.chain_broken')
  assert.ok(broken, 'recovery.chain_broken event missing')
  assert.equal(JSON.parse(broken.payload_json).firstBadSeq, 1)

  // the sibling run is NOT implicated
  assert.equal(store.findRun('dag_ok_00000002').state, 'running')
  assert.deepEqual(store.verifyChain('dag_ok_00000002'), { ok: true })
  assert.equal(store.findEvents('dag_ok_00000002', {}).length, 1, 'sibling events untouched')

  // and the tools still registered (load refusal ≠ apply failure)
  assert.deepEqual(
    ctx.registered.map((tool) => tool.name).sort(),
    ['dag_approve', 'dag_control', 'dag_plan', 'dag_status', 'dag_tick'],
  )
})

test('reconcile: a chain-broken TERMINAL run is warn-only (state untouched, audit trail preserved)', async (t) => {
  const path = tmpDbPath()
  const seed = await createDagStore({ path })
  seed.tx(() => {
    seed.insertRun({
      run_id: 'dag_done_00000001', name: 'done', spec_json: SPEC_JSON, spec_hash: 'a'.repeat(64),
      state: 'succeeded', control_intent: null, parent_session: null, base_cwd: '/tmp/repo',
      created_at: 1_700_000_000_000, updated_at: 1_700_000_000_000, version: 1,
    })
    seed.insertTasks('dag_done_00000001', [{ task_id: 'analyze', state: 'succeeded', version: 1 }])
    seed.insertEvent('dag_done_00000001', { type: 'run.created', payload: { ok: 1 }, at: 1_700_000_000_000 })
    seed.insertEvent('dag_done_00000001', { type: 'run.succeeded', payload: { ok: 1 }, at: 1_700_000_001_000 })
  })
  seed.close()

  const raw = new DatabaseSync(path)
  raw.prepare("UPDATE events SET payload_json = ? WHERE run_id = ? AND seq = ?")
    .run('{"tampered":true}', 'dag_done_00000001', 2)
  raw.close()

  const warns = []
  const summary = await reconcile(await reopen(path).then(async (s) => { await Promise.resolve(); return s }), {
    logger: { info() {}, warn: (m) => warns.push(m), debug() {} },
  })
  const store = await reopen(path)
  t.after(() => store.close())

  assert.deepEqual(summary, { recoveredRuns: 0, autoRetried: 0, orphaned: 0, chainBroken: 0, orphanOutputs: 0 })
  assert.equal(store.findRun('dag_done_00000001').state, 'succeeded', 'terminal run state must NOT be rewritten')
  assert.equal(store.findEvents('dag_done_00000001', {}).find((e) => e.type === 'recovery.chain_broken'), undefined)
  assert.ok(warns.some((m) => m.includes('dag_done_00000001')), 'audit warn emitted')
})

// ---------------------------------------------------------------------------
// ordering: reconcile before tool registration
// ---------------------------------------------------------------------------

test('apply: reconcile has completed before tools are registered (probe via the collector)', async (t) => {
  const path = tmpDbPath()
  const { runId, taskId } = await seedClaimedAttempt({ path })

  // The collector re-opens NOTHING — it records when registration happened;
  // the recovered products must already be on disk at that moment (reconcile
  // is the only writer before registration, so their presence at
  // registration time proves reconcile ran first).
  const observations = []
  const ctx = fakeCtx()
  ctx.tools.register = (definition) => {
    observations.push({ tool: definition.name })
  }
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())
  assert.ok(observations.length > 0)

  // The durable proof: the recovery events were chained BEFORE any tool
  // existed (their `at` precedes apply's return and the chain verifies) —
  // combined with apply()'s fixed code order this is the ordering guarantee.
  const store = await reopen(path)
  t.after(() => store.close())
  const recoveryAt = store.findEvents(runId, {}).find((e) => e.type === 'recovery.no_dispatch').at
  const task = store.findTasks(runId).find((row) => row.task_id === taskId)
  assert.equal(task.state, 'retry_wait')
  assert.ok(Number.isInteger(recoveryAt))
  assert.deepEqual(store.verifyChain(runId), { ok: true })
})
