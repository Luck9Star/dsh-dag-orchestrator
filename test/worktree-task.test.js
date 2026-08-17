// worktree-task tests — TASKS.md T16 (DESIGN §4.6 M3 row / §11.3: worktree-
// declaring tasks — create before dispatch, reuse on retry, zero lifecycle
// takeover).
//
// dsh-worktrees does not yet provide the service face (see the T15 note) —
// these tests define the ACCEPTANCE SHAPE for the provider task via fakes:
// a fake engine face over a fake worktree service injected through the REAL
// seam (createWorktreesSeam over a fake ctx.get) into the REAL executor, so
// the consumer-side contract (create params, reuse probe, cwd hand-off,
// failure mapping) is pinned exactly as lib/worktrees-seam.js's JSDoc states.
//
// Covered, per the T16 brief:
//
//   * create 入参断言 — task slug / repoRoot = spec project.root /
//     baseRef / origin:'dag' / correlationId = attemptId;
//   * repoRoot falls back to the run base_cwd when the spec has no
//     project.root;
//   * cwd 透传 — request.cwd === the worktree path (fake ctx.subagents
//     captures the request);
//   * 缺席引擎 → permanent dag.worktrees_unavailable, no subagent started
//     (engine integration: task failed, code in the event chain);
//   * create 抛 → transient dag.worktree_create_failed (engine integration:
//     retryOn ['transient_network'] matches → attempt 2 scheduled, create
//     retried);
//   * a create returning no usable path → transient dag.worktree_create_failed;
//   * the cwd gate still applies to the ENGINE-provided path — a worktree
//     outside the repoRoot subtree → transient dag.worktree_create_failed
//     (engine misbehavior family, NOT dag.cwd_denied);
//   * 终态后无 cleanup — the fake service exposes a cleanup spy that must
//     stay at zero calls through harvest + terminal commit (lifecycle stays
//     with worktrees, DESIGN §11.3);
//   * 重试复用 — attempt 1 creates wt-A, fails transient → retry_task-style
//     re-dispatch finds findActiveByTask → create stays at ONE call and
//     attempt 2's request.cwd === wt-A.path;
//   * findActiveByTask 不在场 (older service) → honest degradation: every
//     attempt creates fresh (count 2);
//   * spec: worktree + cwd 并存 → dag.worktree_cwd_conflict;
//   * e2e: worktree-declaring task over the real engine + real store, fake
//     service returning a REAL tmpdir path (the cwd gate passes) → run
//     succeeded with request.cwd === the worktree path.
//
// Zero network, zero CLI, zero models, no dsh-worktrees import.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createWorktreesSeam } from '../lib/worktrees-seam.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'
import { createAdmission } from '../lib/admission.js'
import { createDagStore } from '../lib/dag-store.js'
import { validateSpec, DAG_SPEC_ERROR_CODES } from '../lib/spec-validate.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/**
 * A fake dsh-worktrees worktree service matching the seam's JSDoc contract:
 * create() records every call and answers with a configurable record;
 * findActiveByTask(repoRoot, taskSlug) probes the created records (the
 * reuse lookup family — M-A: the returned record carries its
 * correlationId, the create-time attemptId, so the executor's reuse
 * OWNERSHIP gate can decide); cleanup is a SPY that must never be called
 * (the DAG takes zero lifecycle ownership — DESIGN §11.3).
 */
function fakeWorktreeService({ createImpl, records } = {}) {
  const service = {
    createCalls: [],
    findCalls: [],
    cleanupCalls: 0,
    /** Records the DAG created (also the reuse pool). */
    pool: records ?? [],
    async create(params) {
      service.createCalls.push(params)
      if (createImpl !== undefined) return createImpl(params, service)
      const record = { id: `wt-${service.createCalls.length}`, path: params.__path, task: params.task }
      service.pool.push(record)
      return record
    },
    async findActiveByTask(repoRoot, taskSlug) {
      service.findCalls.push({ repoRoot, taskSlug })
      const hit = service.pool.find((r) => r.task === taskSlug) ?? null
      return hit
    },
    async cleanup() {
      service.cleanupCalls += 1
      throw new Error('cleanup must NEVER be called by the DAG (lifecycle belongs to worktrees)')
    },
  }
  return service
}

/** A fake engine face (the seam's admit shape) over a worktree service. */
function fakeEngineFace(service) {
  return {
    getMergeQueue: () => ({ __queue: true }),
    getWorktreeService: () => service,
    available: true,
  }
}

/** Seam over a controllable engine value (undefined = the absent engine). */
function seamOver(engineValue) {
  let current = engineValue
  const ctx = { get: (name) => (name === 'worktreesEngine' ? current : undefined) }
  return { seam: createWorktreesSeam(ctx, {}), setEngine: (v) => { current = v } }
}

/** Fake ctx.subagents capturing requests; script behaviors pop per start. */
function fakeSubagents() {
  const script = []
  const calls = []
  let counter = 0
  return {
    script,
    calls,
    async start(name, request) {
      counter += 1
      calls.push({ name, request })
      const behavior = script.length > 0 ? script.shift() : { resolve: { output: [], stopReason: 'completed' } }
      if (behavior.reject !== undefined) {
        const p = Promise.reject(behavior.reject)
        p.catch(() => {})
        return { id: `sess-${counter}`, result: p, dispose: async () => {} }
      }
      return { id: `sess-${counter}`, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

/** Real-dir fixture: a project root containing a worktree dir. */
function projectFixture() {
  const root = mkdtempSync(join(tmpdir(), 'dag-wt-root-'))
  const worktree = join(root, '.worktrees', 'wt-a')
  mkdirSync(worktree, { recursive: true })
  return { root, worktree }
}

// ---------------------------------------------------------------------------
// executor units — create params, cwd hand-off, failure mapping
// ---------------------------------------------------------------------------

test('worktree task: create 入参 — slug/repoRoot=project.root/baseRef/origin/correlationId=attemptId', async (t) => {
  const { root, worktree } = projectFixture()
  t.after(() => { /* tmpdir cleanup is the OS's */ })
  const service = fakeWorktreeService()
  // The fake's create answers with the prepared REAL path (cwd gate needs
  // an existing directory); pool starts EMPTY so the dispatch takes the
  // create arm, not the reuse arm.
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-a', path: worktree, task: params.task }
    service.pool.push(record)
    return record
  }
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const ctxInfo = {
    runName: 'wt-run', runId: 'run-1', baseCwd: root, projectRoot: root,
    execAgent: { __live: 'agent' },
  }

  const res = await executor.dispatch(
    {
      id: 'impl', kind: 'agent', prompt: 'do it',
      worktree: { task: 'auth-refactor', baseRef: 'main' },
    },
    { attemptId: 'att-create-1', ordinal: 1 },
    ctxInfo,
  )
  assert.equal(res.ok, true, JSON.stringify(res))
  assert.equal(service.createCalls.length, 1)
  assert.deepEqual(service.createCalls[0], {
    task: 'auth-refactor',
    repoRoot: root,
    baseRef: 'main',
    origin: 'dag',
    correlationId: 'att-create-1',
  })
  // cwd hand-off for this arm too.
  assert.equal(subs.calls[0].request.cwd, worktree)
  executor.dispose('att-create-1')
})

test('worktree task: create WITHOUT baseRef omits the key (undefined not serialized)', async () => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug-x' } },
    { attemptId: 'att-nb', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.deepEqual(service.createCalls[0], {
    task: 'slug-x', repoRoot: root, baseRef: undefined, origin: 'dag', correlationId: 'att-nb',
  })
  executor.dispose('att-nb')
})

test('worktree task: repoRoot falls back to run base_cwd when project.root absent', async () => {
  const { root } = projectFixture()
  const service = fakeWorktreeService()
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  // No projectRoot in ctxInfo → repoRoot = baseCwd (the engine's
  // spec.project?.root ?? run.base_cwd default reaches the executor here).
  await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug-y' } },
    { attemptId: 'att-fb', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, execAgent: {} },
  )
  assert.equal(service.createCalls[0].repoRoot, root)
  executor.dispose('att-fb')
})

test('worktree task: cwd 透传 — request.cwd === the created worktree path', async () => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  // Pre-active record WITH its correlationId — but a correlationId that is
  // NOT this dispatch's attempt and NOT in the task's attempt history (no
  // taskAttemptIds in a direct executor call): the ownership gate must
  // REFUSE the reuse and create instead (M-A). The created record points
  // at the same prepared dir, so the cwd assertion keeps its meaning.
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-a', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  service.pool.push({ id: 'wt-a', path: worktree, task: 'auth-refactor', correlationId: 'att-someone-else' })
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const res = await executor.dispatch(
    {
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'auth-refactor' },
    },
    { attemptId: 'att-cwd', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.equal(res.ok, true)
  assert.equal(subs.calls.length, 1)
  assert.equal(subs.calls[0].request.cwd, worktree, 'the worktree path IS the request cwd')
  assert.equal(service.createCalls.length, 1, 'M-A: the foreign-correlationId record was NOT reused — create ran')
  executor.dispose('att-cwd')
})

test('worktree task: 缺席引擎 → permanent dag.worktrees_unavailable, no subagent started', async () => {
  const { root } = projectFixture()
  const { seam } = seamOver(undefined) // ctx.get returns undefined — agent-only deployment
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } },
    { attemptId: 'att-absent', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.equal(res.ok, false)
  assert.equal(res.failure.failureType, 'permanent', 'absence is a configuration state, not retryable')
  assert.equal(res.failure.code, 'dag.worktrees_unavailable')
  assert.match(res.failure.message, /ctx\.get\("worktreesEngine"\)/)
  assert.equal(subs.calls.length, 0, 'no subagent started')
})

test('worktree task: executor built WITHOUT a seam → same loud unavailable', async () => {
  const { root } = projectFixture()
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs }) // direct construction, no seam
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } },
    { attemptId: 'att-noseam', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.equal(res.ok, false)
  assert.equal(res.failure.code, 'dag.worktrees_unavailable')
  assert.equal(subs.calls.length, 0)
})

test('worktree task: create 抛 → transient dag.worktree_create_failed', async () => {
  const { root } = projectFixture()
  const service = fakeWorktreeService({
    createImpl: async () => { throw new Error('index.lock: lock exists') },
  })
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } },
    { attemptId: 'att-throw', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.equal(res.ok, false)
  assert.equal(res.failure.failureType, 'transient', 'creation races (locks/staging) are retryable')
  assert.equal(res.failure.code, 'dag.worktree_create_failed')
  assert.match(res.failure.message, /index\.lock/)
  assert.equal(subs.calls.length, 0)
})

test('worktree task: create returns no usable path → transient dag.worktree_create_failed', async () => {
  const { root } = projectFixture()
  const service = fakeWorktreeService({ createImpl: async () => ({ id: 'wt-x' }) }) // no .path
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } },
    { attemptId: 'att-nopath', ordinal: 1 },
    { runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {} },
  )
  assert.equal(res.ok, false)
  assert.equal(res.failure.failureType, 'transient')
  assert.equal(res.failure.code, 'dag.worktree_create_failed')
  assert.equal(subs.calls.length, 0)
})

test('worktree task: engine-provided path OUTSIDE the repoRoot subtree → transient dag.worktree_create_failed (gate discipline)', async () => {
  const { root } = projectFixture()
  const outside = mkdtempSync(join(tmpdir(), 'dag-wt-out-')) // real dir, but a sibling of root
  const service = fakeWorktreeService()
  // Active record pointing OUTSIDE the root, owned by THIS task's history
  // (so the reuse arm engages and its gate rejection is the thing under
  // test — M-A moved the gate INSIDE the ownership branch).
  service.pool.push({ id: 'wt-bad', path: outside, task: 'rogue-slug', correlationId: 'att-outside' })
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam })
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'rogue-slug' } },
    { attemptId: 'att-outside', ordinal: 1 },
    {
      runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {},
      taskAttemptIds: ['att-outside'],
    },
  )
  assert.equal(res.ok, false)
  // The worktree path is engine-provided (trusted product) but STILL passes
  // the red-line-9 gate; a rejection means engine misbehavior → classified
  // with the create-failure family, NOT dag.cwd_denied.
  assert.equal(res.failure.failureType, 'transient')
  assert.equal(res.failure.code, 'dag.worktree_create_failed')
  assert.match(res.failure.message, /cwd gate/)
  assert.equal(subs.calls.length, 0)
})

test('worktree task: config.allowedRoots admits an engine worktree outside repoRoot', async () => {
  const { root } = projectFixture()
  const foreign = mkdtempSync(join(tmpdir(), 'dag-wt-allow-'))
  const service = fakeWorktreeService()
  // Owned by this attempt's history (M-A ownership gate engages) and
  // admitted by allowedRoots — the reuse path exercises the gate union.
  service.pool.push({ id: 'wt-foreign', path: foreign, task: 'slug', correlationId: 'att-allowed' })
  const { seam } = seamOver(fakeEngineFace(service))
  const subs = fakeSubagents()
  const executor = createExecutor({ ctxSubagents: subs, worktreesSeam: seam, config: { allowedRoots: [foreign] } })
  const res = await executor.dispatch(
    { id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } },
    { attemptId: 'att-allowed', ordinal: 1 },
    {
      runName: 'r', runId: 'run-1', baseCwd: root, projectRoot: root, execAgent: {},
      taskAttemptIds: ['att-allowed'],
    },
  )
  assert.equal(res.ok, true, JSON.stringify(res))
  assert.equal(subs.calls[0].request.cwd, foreign)
  executor.dispose('att-allowed')
})

// ---------------------------------------------------------------------------
// engine integration — failure mapping × retry policy, no-cleanup, reuse
// ---------------------------------------------------------------------------

/** Engine harness over the real store + real executor + seam-injected fake.
 * engineValue === null means "deliberately absent engine" (ctx.get → undefined). */
async function makeHarness({ spec, service, engineValue, baseCwd }) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'wt-')), 'dag.db') })
  const subagents = fakeSubagents()
  const face = engineValue === null ? undefined : engineValue !== undefined ? engineValue : fakeEngineFace(service)
  const { seam } = seamOver(face)
  const executor = createExecutor({
    ctxSubagents: subagents,
    worktreesSeam: seam,
    execAgentProvider: () => ({ __live: 'agent' }),
  })
  const engine = createEngine({
    store, executor, admission: createAdmission(), logger: {},
    now: () => 1_000_000, random: () => 0.5,
  })
  const runId = engine.planRun(spec, { baseCwd, runId: 'run-wt' }).runId
  const states = () => Object.fromEntries(store.findTasks(runId).map((t) => [t.task_id, t.state]))
  return { store, subagents, service, engine, runId, states, close() { engine.disposeAll(); store.close() } }
}

test('engine: 缺席引擎 → task failed dag.worktrees_unavailable, run failed, nothing started', async (t) => {
  const { root } = projectFixture()
  const spec = {
    version: 1, name: 'wt-absent',
    project: { root },
    tasks: [{ id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug' } }],
  }
  const h = await makeHarness({ spec, engineValue: null, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.equal(h.states().impl, 'failed')
  const failEvents = h.store.findEvents(h.runId).filter((e) => e.type === 'attempt.failed')
  assert.equal(failEvents.length, 1, 'exactly one attempt — permanent failures never retry')
  const payload = JSON.parse(failEvents[0].payload_json)
  assert.equal(payload.failureType, 'permanent')
  assert.equal(payload.code, 'dag.worktrees_unavailable')
  assert.equal(h.subagents.calls.length, 0, 'no subagent was ever started')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('engine: create 抛 → transient dag.worktree_create_failed, retryOn transient_network 匹配可重试', async (t) => {
  const { root } = projectFixture()
  let calls = 0
  const service = fakeWorktreeService({
    createImpl: async () => {
      calls += 1
      throw new Error(`boom #${calls}`)
    },
  })
  const spec = {
    version: 1, name: 'wt-create-fail',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug' },
      retry: { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  // Both attempts failed on create → the task (and run) fail terminally,
  // but the retry WAS scheduled (transient mapped to transient_network).
  assert.equal(s.run_state, 'failed')
  assert.equal(h.states().impl, 'failed')
  const retryEvents = h.store.findEvents(h.runId).filter((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(retryEvents.length, 1, 'the transient failure matched the retryOn policy')
  assert.equal(calls, 2, 'create retried on attempt 2')
  const codes = h.store.findEvents(h.runId)
    .filter((e) => e.type === 'attempt.failed')
    .map((e) => JSON.parse(e.payload_json).code)
  assert.deepEqual(codes, ['dag.worktree_create_failed', 'dag.worktree_create_failed'])
  assert.equal(h.subagents.calls.length, 0, 'no subagent started on either attempt')
})

test('engine: 终态后无 cleanup 调用 — create 只调一次, cleanup spy 保持 0', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  // Attempt 1 CREATES wt-A (recording correlationId) and fails
  // transiently; the retryTask re-dispatch then reuses it through the
  // OWNERSHIP gate (M-A: the record's correlationId is inside this
  // task's attempt history) — create stays at ONE call and no lifecycle
  // method is touched at either terminal.
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-a', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-no-cleanup',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug-done' },
      retry: { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  // Attempt 1 runs inside the created worktree and errors (transient);
  // attempt 2 reuses it and succeeds.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.equal(service.createCalls.length, 1, 'created once; the retry re-dispatch REUSED the owned active worktree')
  assert.equal(h.subagents.calls.length, 2)
  assert.equal(h.subagents.calls[1].request.cwd, worktree)
  assert.equal(service.cleanupCalls, 0, 'lifecycle belongs to the worktrees plugin (DESIGN §11.3)')
  const ok = h.store.verifyChain(h.runId)
  assert.equal(ok.ok, true)
})

test('engine: create-once path also leaves cleanup untouched through harvest + terminal', async (t) => {
  const { root } = projectFixture()
  const service = fakeWorktreeService()
  const spec = {
    version: 1, name: 'wt-create-once',
    project: { root },
    tasks: [{ id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug-once' } }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  // findActiveByTask present but EMPTY pool → dispatch creates (the fake
  // create returns a record whose path must exist for the gate: patch it).
  service.create = async (params) => {
    service.createCalls.push(params)
    const path = join(root, 'wt-created')
    mkdirSync(path, { recursive: true })
    const record = { id: 'wt-created', path, task: params.task }
    service.pool.push(record)
    return record
  }

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.equal(service.createCalls.length, 1, 'created exactly once for the one attempt')
  assert.equal(service.cleanupCalls, 0, 'no cleanup at the terminal — zero lifecycle takeover')
  assert.equal(h.subagents.calls[0].request.cwd, join(root, 'wt-created'))
})

test('engine: 重试复用 — attempt1 建 wt-A 失败 transient, attempt2 findActiveByTask → create 仍 1 次, cwd === wt-A.path', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-A', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-retry-reuse',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-reuse' },
      retry: { maxAttempts: 3, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  // Attempt 1: worktree created, subagent errors (transient dag.agent_error).
  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', 'attempt 2 reused the worktree and succeeded')

  assert.equal(service.createCalls.length, 1, 'retry re-dispatch REUSED the active worktree — no second create')
  assert.equal(h.subagents.calls.length, 2, 'two subagent dispatches (attempt 1 + 2)')
  // The reuse probe consulted the task slug (stable across attempts), with
  // the spec project.root as repoRoot.
  assert.deepEqual(
    service.findCalls.map((c) => ({ repoRoot: c.repoRoot, taskSlug: c.taskSlug })),
    [{ repoRoot: root, taskSlug: 'slug-reuse' }, { repoRoot: root, taskSlug: 'slug-reuse' }],
  )
  // Both attempts ran INSIDE the same worktree path.
  assert.equal(h.subagents.calls[0].request.cwd, worktree)
  assert.equal(h.subagents.calls[1].request.cwd, worktree, 'attempt 2 cwd === the reused wt-A path')
  assert.equal(service.cleanupCalls, 0)
})

test('engine: findActiveByTask 不在场（旧版服务）→ 每次新建（降级诚实: create 2 次）', async (t) => {
  const { root } = projectFixture()
  const service = fakeWorktreeService()
  service.create = async (params) => {
    service.createCalls.push(params)
    const path = join(root, `wt-${service.createCalls.length}`)
    mkdirSync(path, { recursive: true })
    const record = { id: `wt-${service.createCalls.length}`, path, task: params.task }
    service.pool.push(record)
    return record
  }
  // Old service shape: NO findActiveByTask member at all.
  delete service.findActiveByTask

  const spec = {
    version: 1, name: 'wt-no-reuse-probe',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-legacy' },
      retry: { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  // Honest degradation: without the optional probe, every attempt creates.
  assert.equal(service.createCalls.length, 2)
  assert.equal(h.subagents.calls.length, 2)
  assert.notEqual(h.subagents.calls[0].request.cwd, h.subagents.calls[1].request.cwd,
    'two DISTINCT worktrees — the old service cannot express reuse')
})

// ---------------------------------------------------------------------------
// M3 review M-A — the reuse OWNERSHIP gate (§11.3 scope made mechanical)
// ---------------------------------------------------------------------------

test('M-A: reuse granted ONLY when correlationId ∈ 本 task 历史 attempts（他 task 的 active worktree 不复用）', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  const foreignPath = join(root, '.worktrees', 'wt-foreign')
  mkdirSync(foreignPath, { recursive: true })
  // The pool holds ANOTHER task's active record under a DIFFERENT slug —
  // not hit by the slug probe (control case: slug mismatch never reuses).
  service.pool.push({ id: 'wt-foreign', path: foreignPath, task: 'other-slug', correlationId: 'att-other-task-1' })
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-own', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-ma-foreign',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-ma' },
      retry: { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  // Attempt 1 creates wt-own (correlationId = attempt 1) and fails
  // transient; attempt 2's slug probe finds wt-own — correlationId is in
  // THIS task's history (store.findAttempts) → REUSE, create stays 1.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } })
  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.equal(service.createCalls.length, 1, 'OWNED record reused — exactly one create')
  assert.equal(h.subagents.calls[1].request.cwd, worktree, 'attempt 2 ran inside the reused worktree')

  // And the foreign record was never handed out: its path never appears
  // as any dispatch cwd of this run's task.
  assert.ok(
    h.subagents.calls.every((c) => c.request.cwd !== foreignPath),
    'another task\'s active worktree path is never reused',
  )
})

test('M-A: same-slug active record owned by ANOTHER task → 不复用, create 走 slug 占用大声失败（transient worktree_create_failed）', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  // A foreign task's record sits ACTIVE under THIS task's slug — the
  // pre-plan-time-guard runtime world (hand-seeded store / provider
  // drift). Its correlationId is NOT in this task's history.
  service.pool.push({ id: 'wt-foreign', path: worktree, task: 'slug-ma2', correlationId: 'att-foreign-1' })
  // create answers the occupied slug loud (branch_exists family — the
  // branch derives from the slug, exactly as the review describes).
  service.create = async (params) => {
    service.createCalls.push(params)
    throw new Error('branch_exists: dag/slug-ma2 already checked out')
  }
  const spec = {
    version: 1, name: 'wt-ma2-foreign',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-ma2' },
      retry: { maxAttempts: 1, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  // The honest exposure: NO silent sharing — create ran, hit the occupied
  // slug, and the transient failure is on the record.
  assert.equal(s.run_state, 'failed')
  assert.equal(h.subagents.calls.length, 0, 'no subagent ever ran in the foreign worktree')
  const attempt = h.store.findAttempts(h.runId, 'impl').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.worktree_create_failed')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'transient')
  assert.match(JSON.parse(attempt.failure_json).message, /branch_exists/)
})

test('M-A: active record WITHOUT correlationId → 保守不复用（每次新建, 诚实降级）', async (t) => {
  const { root } = projectFixture()
  const service = fakeWorktreeService()
  // An active record under this task's slug whose shape cannot prove
  // ownership (older service / stale record): the gate must CONSERVATIVELY
  // refuse reuse and create fresh — never hand out an unowned path.
  service.pool.push({ id: 'wt-legacy', path: join(root, '.worktrees', 'wt-legacy'), task: 'slug-ma3' })
  service.create = async (params) => {
    service.createCalls.push(params)
    const path = join(root, `wt-ma3-${service.createCalls.length}`)
    mkdirSync(path, { recursive: true })
    const record = { id: `wt-ma3-${service.createCalls.length}`, path, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-ma3-legacy',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-ma3' },
      retry: { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.equal(h.subagents.calls.length, 1, 'attempt 1 did NOT reuse the unprovable record — it created')
  assert.equal(h.subagents.calls[0].request.cwd, join(root, 'wt-ma3-1'))
  assert.equal(join(root, '.worktrees', 'wt-legacy') !== h.subagents.calls[0].request.cwd, true)
})

test('engine: retry_task 手动重派发也复用 active worktree（新 attemptId, 同 slug 查询）', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  // Attempt 1 creates wt-A (correlationId = attempt 1's id) and refuses;
  // the retry_task re-dispatch (a NEW attemptId) finds wt-A still ACTIVE
  // with a correlationId inside THIS task's attempt history → reuse.
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-A', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-manual-retry',
    project: { root },
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      worktree: { task: 'slug-manual' },
      // NO retry block: attempt 1 fails permanently (refusal) → the run
      // fails → dag_control retry_task re-arms it.
    }],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } })
  let s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.equal(service.createCalls.length, 1, 'attempt 1 created wt-A (correlationId = attempt 1)')

  await h.engine.control(h.runId, 'retry_task', { taskId: 'impl' })
  s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', 'the manually re-armed attempt reused wt-A and finished')
  assert.equal(service.createCalls.length, 1, 'retry_task re-dispatch REUSED the owned record — never a duplicate create')
  assert.equal(h.subagents.calls.length, 2)
  assert.equal(h.subagents.calls[1].request.cwd, worktree)
  assert.equal(service.cleanupCalls, 0)
})

test('engine e2e: worktree 声明任务全链 — fake service path=tmpdir 真目录 → succeeded', async (t) => {
  const { root, worktree } = projectFixture()
  const service = fakeWorktreeService()
  // No pre-active record: the impl attempt CREATES the worktree (with its
  // correlationId) and runs inside it — the plain e2e create path.
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: 'wt-e2e', path: worktree, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }
  const spec = {
    version: 1, name: 'wt-e2e',
    project: { root },
    tasks: [
      {
        id: 'impl', kind: 'agent', prompt: 'refactor in the worktree',
        worktree: { task: 'e2e-slug', baseRef: 'main' },
        outputs: [{ name: 'report', schema: { type: 'object', required: ['files'] } }],
      },
      {
        id: 'check', kind: 'agent', prompt: 'read the report',
        dependsOn: [{ taskId: 'impl', condition: 'succeeded' }],
        inputs: ['task://impl/report'],
      },
    ],
  }
  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  h.subagents.script.push({
    resolve: { output: [], structured: { files: ['a.js'] }, stopReason: 'completed' },
  })
  const s = await h.engine.tick(h.runId, { maxRounds: 8, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.deepEqual(h.states(), { impl: 'succeeded', check: 'succeeded' })

  // The worktree task dispatched with the worktree path as cwd; the plain
  // downstream task carried NO cwd (inherits the parent's).
  assert.equal(h.subagents.calls[0].request.cwd, worktree)
  assert.equal('cwd' in h.subagents.calls[1].request, false)
  // Output flowed impl → check through the outputs table.
  const out = h.store.findOutput(h.runId, 'impl', 'report')
  assert.notEqual(out, null)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
  assert.equal(service.cleanupCalls, 0, 'terminal leaves the worktree to the worktrees plugin')
})

// ---------------------------------------------------------------------------
// spec-validate — the worktree/cwd conflict
// ---------------------------------------------------------------------------

test('spec: worktree + cwd 并存 → dag.worktree_cwd_conflict（专用码, agent 任务）', () => {
  const spec = {
    version: 1, name: 'wt-conflict',
    tasks: [{
      id: 'impl', kind: 'agent', prompt: 'p',
      cwd: '/abs/somewhere',
      worktree: { task: 'slug' },
    }],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, false)
  const hit = r.errors.find((e) => e.code === DAG_SPEC_ERROR_CODES.worktreeCwdConflict)
  assert.ok(hit !== undefined, `expected dag.worktree_cwd_conflict, got ${JSON.stringify(r.errors)}`)
  assert.equal(hit.path, 'tasks[0].cwd')
  assert.match(hit.message, /drop the 'cwd'/)
})

test('spec: worktree WITHOUT cwd still validates (and approval keeps its own kind-matrix rejection)', () => {
  const ok = validateSpec({
    version: 1, name: 'wt-ok',
    tasks: [{ id: 'impl', kind: 'agent', prompt: 'p', worktree: { task: 'slug', baseRef: 'main' } }],
  })
  assert.equal(ok.ok, true, JSON.stringify(ok.errors))

  // approval + worktree remains the kind-matrix rule (approval tasks are
  // not subagent delegations), NOT the cwd-conflict code.
  const approval = validateSpec({
    version: 1, name: 'wt-approval',
    tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' }, worktree: { task: 'slug' } }],
  })
  assert.equal(approval.ok, false)
  assert.ok(approval.errors.some((e) => e.code === DAG_SPEC_ERROR_CODES.kindFieldMismatch && e.path === 'tasks[0].worktree'))
  assert.ok(!approval.errors.some((e) => e.code === DAG_SPEC_ERROR_CODES.worktreeCwdConflict))
})
