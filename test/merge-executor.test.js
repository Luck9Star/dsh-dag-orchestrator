// merge-executor tests — TASKS.md T17 (DESIGN §11.1: DrainOutcome five-state
// mapping over the dsh-worktrees merge queue).
//
// dsh-worktrees does not yet expose the engine service face (the T15 note) —
// these tests pin the ACCEPTANCE SHAPE through a fake merge queue + fake
// worktree service injected through the REAL seam into the REAL executor +
// REAL engine + REAL store (the worktree-task.test.js harness shape), so the
// consumer-side contract (enqueue/drain call shapes, the five-state mapping,
// the conflicted park, the queued_ahead re-poll) is pinned exactly as
// lib/executors/merge.js's JSDoc states.
//
// Covered, per the T17 brief:
//
//   * 五态各一用例 + 事件序列断言 —
//       succeeded: attempt.succeeded + task.succeeded + outputs
//         integratedCommit + run.succeeded;
//       conflicted: the park shape (attempt failed reason merge_conflicted +
//         task blocked {code, conflictFiles, retainedWorktrees});
//       failed: transient dag.merge_failed → retryOn path (retry_scheduled,
//         attempt 2 lands terminal failed when the queue keeps failing);
//       no_changes: succeeded (empty integration legal);
//       queued_ahead: transient dag.merge_queued_ahead → retry_wait light
//         backoff + manual:true (budget-free) → next tick re-claims,
//         re-enqueues, second drain returns succeeded → 全链绿.
//   * enqueue/drain 调用形状逐字段 — worktreeId / integrationBranch /
//     origin:'dag' / correlationId === attemptId.
//   * conflicted → blocked 后 tick 不死循环 (attempts count frozen over 3
//     ticks) + retry_task 是唯一出口 (retry_task → re-claim → re-enqueue).
//   * 源缺失 (upstream's dispatched event carries no worktreeId) →
//     permanent dag.merge_source_missing.
//   * 引擎缺席 → dag.worktrees_unavailable.
//   * spec-validate: merge 无 worktree 上游 → 拒 (see also
//     test/spec-validate.test.js's dedicated cases).
//
// Zero network, zero git, zero real dsh-worktrees import.

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

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/**
 * A fake worktree service (the seam's JSDoc contract). M-A: the reuse
 * OWNERSHIP gate means a pre-seeded "active" record without a
 * correlationId is no longer blindly reusable — the harness therefore
 * serves the PREPARED records as create templates: `records` map task
 * slug → {id, path} (real dirs), `create(params)` answers the template's
 * path/id stamped with the dispatch correlationId, and the pool starts
 * EMPTY (a record only becomes active once THIS run's dispatch created
 * it, mirroring the real provider).
 */
function fakeWorktreeService({ records } = {}) {
  const prepared = new Map((records ?? []).map((r) => [r.task, r]))
  return {
    pool: [],
    async create(params) {
      const template = prepared.get(params.task) ?? {}
      const record = {
        id: template.id,
        path: template.path,
        task: params.task,
        correlationId: params.correlationId,
      }
      this.pool.push(record)
      return record
    },
    async findActiveByTask(repoRoot, taskSlug) {
      return this.pool.find((r) => r.task === taskSlug) ?? null
    },
  }
}

/**
 * A fake worktrees merge queue (the lib/executors/merge.js JSDoc contract):
 *
 *   enqueue({worktreeId, integrationBranch, origin, correlationId}) —
 *     records the call, returns the job (repoKey 'repo-main'); scriptable
 *     failure modes: throw / {ok:false, error:{type}} / no-repoKey.
 *   drain(repoKey, integrationBranch) — records the call, pops the next
 *     scripted DrainOutcome (default {state:'succeeded',
 *     integratedCommit:'deadbeef'}).
 *
 * The queue is per-INSTANCE, exactly like the singleton seam would serve it.
 */
function fakeMergeQueue() {
  const queue = {
    enqueueCalls: [],
    drainCalls: [],
    enqueueScript: [],
    drainScript: [],
    enqueue(params) {
      queue.enqueueCalls.push(params)
      const behavior = queue.enqueueScript.length > 0 ? queue.enqueueScript.shift() : undefined
      if (behavior !== undefined) {
        if (behavior.throw !== undefined) throw behavior.throw
        if (behavior.okFalse !== undefined) return { ok: false, error: { type: behavior.okFalse } }
        if (behavior.noRepoKey === true) return { id: 'mgj_x', state: 'queued' }
      }
      // Idempotence arm (the contract): an active job for the same
      // (worktreeId, integrationBranch) is RETURNED, not stacked.
      const active = queue.enqueueCalls.find((c) =>
        c.worktreeId === params.worktreeId && c.integrationBranch === params.integrationBranch)
      if (active !== undefined && active.__job !== undefined) return active.__job
      const job = { id: `mgj_${queue.enqueueCalls.length}`, repoKey: 'repo-main', state: 'queued' }
      params.__job = job
      return job
    },
    async drain(repoKey, integrationBranch) {
      queue.drainCalls.push({ repoKey, integrationBranch })
      if (queue.drainScript.length > 0) {
        const outcome = queue.drainScript.shift()
        return typeof outcome === 'function' ? outcome() : outcome
      }
      return { state: 'succeeded', integratedCommit: 'deadbeef' }
    },
  }
  return queue
}

/** A fake engine face (the seam's admit shape) over service + queue. */
function fakeEngineFace(service, queue) {
  return {
    getMergeQueue: () => queue,
    getWorktreeService: () => service,
    available: true,
  }
}

/** Seam over a controllable engine value. */
function seamOver(engineValue) {
  let current = engineValue
  const ctx = { get: (name) => (name === 'worktreesEngine' ? current : undefined) }
  return { seam: createWorktreesSeam(ctx, {}), setEngine: (v) => { current = v } }
}

/** Fake ctx.subagents (script queue, T16 harness shape). */
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
      return { id: `sess-${counter}`, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

/** A real project fixture: root + one worktree dir (the cwd gate needs real dirs). */
function projectFixture() {
  const root = mkdtempSync(join(tmpdir(), 'dag-mrg-'))
  const worktreeA = join(root, '.worktrees', 'wt-src-a')
  const worktreeB = join(root, '.worktrees', 'wt-src-b')
  mkdirSync(worktreeA, { recursive: true })
  mkdirSync(worktreeB, { recursive: true })
  return { root, worktreeA, worktreeB }
}

/**
 * The merge harness: real store + real executor (seam-injected fake engine
 * face) + real engine, planRun'ed with the two-source merge spec.
 *
 * clock: fixed now=1_000_000 (the queued_ahead backoff lands at 1_005_000;
 * advanceClock lets the retry_wait deadline pass for the next tick).
 */
async function makeHarness({ spec, service, queue, engineValue, baseCwd, nowImpl } = {}) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'mrg-')), 'dag.db') })
  const subagents = fakeSubagents()
  const face = engineValue === null ? undefined : engineValue !== undefined ? engineValue : fakeEngineFace(service, queue)
  const { seam, setEngine } = seamOver(face)
  const executor = createExecutor({
    ctxSubagents: subagents,
    worktreesSeam: seam,
    execAgentProvider: () => ({ __live: 'agent' }),
  })
  let clock = 1_000_000
  const now = nowImpl ?? (() => clock)
  const engine = createEngine({
    store, executor, admission: createAdmission(), logger: {},
    now, random: () => 0.5,
  })
  const runId = engine.planRun(spec, { baseCwd, runId: 'run-mrg' }).runId
  return {
    store, subagents, service, queue, engine, runId, setEngine,
    advanceClock(ms) { clock += ms },
    states() { return Object.fromEntries(store.findTasks(runId).map((t) => [t.task_id, t.state])) },
    attempts(taskId) { return store.findAttempts(runId, taskId) },
    eventsOfType(type) { return store.findEvents(runId).filter((e) => e.type === type) },
    close() { engine.disposeAll(); store.close() },
  }
}

/** The canonical T17 spec: two worktree-declaring sources → one merge. */
function mergeSpec({ integrationBranch, sources = ['src-a', 'src-b'], mergeId = 'integrate' } = {}) {
  const { root, worktreeA, worktreeB } = projectFixture()
  const paths = { 'src-a': worktreeA, 'src-b': worktreeB }
  // Every source task always has an ACTIVE worktree record (id + real path)
  // — the T16 dispatch's findActiveByTask reuses them, so the source agents
  // dispatch with worktree cwds and their dispatched events carry ids.
  const records = sources.map((id) => ({ id: `wt-${id}`, path: paths[id], task: `slug-${id}` }))
  const spec = {
    version: 1,
    name: 'merge-run',
    project: { root },
    tasks: sources.map((id) => ({
      id, kind: 'agent', prompt: `work in ${id}`,
      worktree: { task: `slug-${id}` },
    })),
  }
  if (mergeId !== null) {
    spec.tasks.push({
      id: mergeId, kind: 'merge',
      dependsOn: sources.map((id) => ({ taskId: id, condition: 'succeeded' })),
      ...(integrationBranch !== undefined ? { merge: { integrationBranch } } : {}),
      outputs: [{ name: 'integration', schema: { type: 'object', required: ['integratedCommit'] } }],
    })
  }
  return { spec, root, paths, records }
}

// ---------------------------------------------------------------------------
// source collection + enqueue/drain call shapes
// ---------------------------------------------------------------------------

test('merge: enqueue/drain 调用形状逐字段 — worktreeId/integrationBranch/origin dag/correlationId=attemptId', async (t) => {
  const { spec, root, paths, records } = mergeSpec({ integrationBranch: 'dsh-wt/integration/x' })
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))

  // The sources' dispatched events carried their worktree ids (T16+T17).
  for (const src of ['src-a', 'src-b']) {
    const dispatched = h.eventsOfType('attempt.dispatched').filter((e) => e.task_id === src)
    assert.equal(dispatched.length, 1)
    assert.equal(JSON.parse(dispatched[0].payload_json).worktreeId, `wt-${src}`)
  }

  // THE shape assertions: one enqueue per source, exact fields.
  assert.equal(queue.enqueueCalls.length, 2)
  const mergeAttempt = h.attempts('integrate').find((a) => a.state === 'succeeded')
  assert.ok(mergeAttempt !== undefined)
  for (const call of queue.enqueueCalls) {
    assert.equal(call.integrationBranch, 'dsh-wt/integration/x')
    assert.equal(call.origin, 'dag')
    assert.equal(call.correlationId, mergeAttempt.attempt_id)
  }
  assert.deepEqual(
    queue.enqueueCalls.map((c) => c.worktreeId).sort(),
    ['wt-src-a', 'wt-src-b'],
  )
  assert.deepEqual(queue.drainCalls, [{ repoKey: 'repo-main', integrationBranch: 'dsh-wt/integration/x' }],
    'drain once per unique repoKey with the integration branch')
  assert.equal(h.subagents.calls.length, 2, 'only the two source agents ran — the merge never reached a subagent')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge: integrationBranch 缺省 = dag/<run.name>/integration', async (t) => {
  const { spec, root, paths, records } = mergeSpec() // no merge block
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(h.states().integrate, 'succeeded')
  assert.ok(queue.enqueueCalls.length >= 1)
  assert.equal(queue.enqueueCalls[0].integrationBranch, 'dag/merge-run/integration')
  assert.equal(queue.drainCalls[0].integrationBranch, 'dag/merge-run/integration')
})

// ---------------------------------------------------------------------------
// state 1 — succeeded (with the integratedCommit output)
// ---------------------------------------------------------------------------

test('merge: DrainOutcome succeeded → task succeeded, outputs integratedCommit, run succeeded', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'abc1234' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.deepEqual(h.states(), { 'src-a': 'succeeded', 'src-b': 'succeeded', integrate: 'succeeded' })

  // Event sequence: claim → terminal, with the structured output landing.
  const attemptOk = h.eventsOfType('attempt.succeeded').filter((e) => e.task_id === 'integrate')
  assert.equal(attemptOk.length, 1)
  assert.equal(h.eventsOfType('task.succeeded').some((e) => e.task_id === 'integrate'), true)
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.notEqual(out, null, 'outputs row persisted in the terminal tx')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'abc1234', integrationBranch: 'dag/merge-run/integration' })
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(attempt.state, 'succeeded')
  assert.equal(attempt.child_session, null, 'a merge attempt never starts a subagent')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

// ---------------------------------------------------------------------------
// state 2 — conflicted → PARK (blocked, not terminal failed)
// ---------------------------------------------------------------------------

test('merge: DrainOutcome conflicted → park blocked(merge_conflicted) — 不终态 failed; conflictFiles+retained 全记录', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({
    state: 'conflicted',
    conflictFiles: ['src/auth.ts', 'src/session.ts'],
    integrationWorktree: '/tmp/.integration/mgj_1',
  })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'running', 'merge_conflicted is SOFT-blocked — the run stays live for the human resolve')
  assert.equal(s.waiting_on, 'external', 'the park waits on the worktrees queue, not on an internal drain')
  assert.match(s.next_hint, /worktree_queue/)

  // The park shape: attempt failed(reason merge_conflicted) + task blocked
  // {code, conflictFiles, retainedWorktrees} — the approval-park form.
  const task = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(task.state, 'blocked')
  assert.deepEqual(JSON.parse(task.blocked_reason), {
    code: 'merge_conflicted',
    conflictFiles: ['src/auth.ts', 'src/session.ts'],
    retainedWorktrees: ['/tmp/.integration/mgj_1'],
  })
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(attempt.state, 'failed')
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.merge_conflicted')
  const attemptFailed = h.eventsOfType('attempt.failed').filter((e) => e.task_id === 'integrate').at(-1)
  const failedPayload = JSON.parse(attemptFailed.payload_json)
  assert.equal(failedPayload.reason, 'merge_conflicted')
  assert.deepEqual(failedPayload.conflictFiles, ['src/auth.ts', 'src/session.ts'])
  assert.deepEqual(failedPayload.retainedWorktrees, ['/tmp/.integration/mgj_1'])
  assert.equal(h.eventsOfType('task.blocked').some((e) => e.task_id === 'integrate'
    && JSON.parse(e.payload_json).reason.code === 'merge_conflicted'), true)
  // The park emitted NO task.failed / task.succeeded.
  assert.equal(h.eventsOfType('task.failed').some((e) => e.task_id === 'integrate'), false)
  assert.equal(h.eventsOfType('task.succeeded').some((e) => e.task_id === 'integrate'), false)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
  // The slot was released outside the park tx: a second source can dispatch.
  // (implicitly proven by run_state running; make it explicit —)
  assert.equal(h.engine.status(h.runId).counts.running, 0)
})

test('merge: conflicted → blocked 后 tick 不死循环 — 3 次 tick attempts 计数不变', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'conflicted', conflictFiles: ['f.ts'], integrationWorktree: '/tmp/x' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  const attemptsAfterPark = h.attempts('integrate').length
  assert.equal(attemptsAfterPark, 1)
  const enqueuesAfterPark = queue.enqueueCalls.length

  for (let i = 0; i < 3; i += 1) {
    await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  }
  assert.equal(h.attempts('integrate').length, attemptsAfterPark, 'parked: no new attempts across 3 ticks')
  assert.equal(queue.enqueueCalls.length, enqueuesAfterPark, 'parked: no re-enqueue')
  assert.equal(h.states().integrate, 'blocked')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge: retry_task 是唯一出口 — resolve 后 retry_task → 重派 → 第二次 drain succeeded → run succeeded', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'conflicted', conflictFiles: ['f.ts'], integrationWorktree: '/tmp/x' })
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'resolved99' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(h.states().integrate, 'blocked')

  // The human edge: (out-of-band worktree_queue resolve) + dag_control retry_task.
  h.engine.control(h.runId, 'retry_task', { taskId: 'integrate', reason: 'conflict resolved via worktree_queue' })
  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.equal(h.attempts('integrate').length, 2, 'attempt 2 = the manual re-run')
  assert.equal(queue.drainCalls.length, 2, 'the retry drained again')
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'resolved99', integrationBranch: 'dag/merge-run/integration' })
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

// ---------------------------------------------------------------------------
// state 3 — failed → retry policy path
// ---------------------------------------------------------------------------

test('merge: DrainOutcome failed → transient dag.merge_failed 走 retry 策略 — attempt2 耗尽后终态 failed', async (t) => {
  const { spec: baseSpec, root, paths, records } = mergeSpec()
  // Retry policy on the merge task: 2 attempts, transient matches.
  const spec = structuredClone(baseSpec)
  spec.tasks[spec.tasks.length - 1].retry = { maxAttempts: 2, backoffMs: 0, retryOn: ['transient_network'] }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'failed', error: 'git_operation_failed: index corrupt' })
  queue.drainScript.push({ state: 'failed', error: 'git_operation_failed: index corrupt again' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'failed', JSON.stringify(h.states()))
  assert.equal(h.states().integrate, 'failed')

  const attempts = h.attempts('integrate')
  assert.equal(attempts.length, 2, 'the retryOn policy scheduled attempt 2 (transient → transient_network)')
  for (const a of attempts) {
    assert.equal(a.state, 'failed')
    assert.equal(JSON.parse(a.failure_json).code, 'dag.merge_failed')
    assert.equal(JSON.parse(a.failure_json).failureType, 'transient')
  }
  assert.equal(h.eventsOfType('attempt.retry_scheduled').length, 1)
  // No outputs row on the failure path.
  assert.equal(h.store.findOutput(h.runId, 'integrate', 'integration'), null)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

// ---------------------------------------------------------------------------
// state 4 — no_changes → succeeded (empty integration legal)
// ---------------------------------------------------------------------------

test('merge: DrainOutcome no_changes → succeeded（空集成合法）', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'no_changes' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  assert.equal(h.states().integrate, 'succeeded')
  // The declared output still lands (integratedCommit null — legal).
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: null, integrationBranch: 'dag/merge-run/integration' })
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

// ---------------------------------------------------------------------------
// state 5 — queued / queued_ahead → retry_wait light backoff, next tick re-drains
// ---------------------------------------------------------------------------

test('merge: DrainOutcome queued_ahead → retry_wait 轻退避 + manual:true（不入预算）→ 下轮重派再 drain → 全链绿', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'queued', queued_ahead: 2 })
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'finally77' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  // Round 1: sources succeed, merge drains → queued_ahead → retry_wait (now + 5s).
  const s1 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s1.run_state, 'running', JSON.stringify(h.states()))
  const mergeTask = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(mergeTask.state, 'retry_wait', 'queued_ahead parks the attempt in retry_wait — 本轮不终态')

  const attempts1 = h.attempts('integrate')
  assert.equal(attempts1.length, 1)
  assert.equal(attempts1[0].state, 'failed')
  assert.equal(JSON.parse(attempts1[0].failure_json).code, 'dag.merge_queued_ahead')

  // The retry_scheduled stamp: manual:true — the recovery:true STYLE budget
  // marker (shouldRetry skips manual:true when billing the retryOn budget).
  const scheduled = h.eventsOfType('attempt.retry_scheduled').filter((e) => e.task_id === 'integrate')
  assert.equal(scheduled.length, 1)
  assert.equal(JSON.parse(scheduled[0].payload_json).manual, true)
  assert.equal(JSON.parse(scheduled[0].payload_json).backoffMs, 5000)

  // Before the deadline: another tick does NOT re-claim (backoff not due).
  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(h.attempts('integrate').length, 1, 'not yet due — no premature re-claim')

  // Deadline passes; the next tick re-claims, re-enqueues, drains → succeeded.
  h.advanceClock(6000)
  const s2 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s2.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.equal(h.attempts('integrate').length, 2, 'the re-poll claimed attempt 2')
  assert.equal(queue.drainCalls.length, 2, 'drain ran again on the retry round')
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'finally77', integrationBranch: 'dag/merge-run/integration' })
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge: queued_ahead 重评不耗 retryOn 预算 — maxAttempts 1 下仍能重派（manual:true 不计）', async (t) => {
  const { spec: baseSpec, root, paths, records } = mergeSpec()
  const spec = structuredClone(baseSpec)
  spec.tasks[spec.tasks.length - 1].retry = { maxAttempts: 1, retryOn: ['transient_network'] }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'queued', queued_ahead: 1 })
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'budget-free' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(h.states().integrate, 'retry_wait')
  h.advanceClock(6000)
  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', 'a busy queue never exhausts the merge budget — the re-poll is manual')
  assert.equal(h.attempts('integrate').length, 2)
})

// ---------------------------------------------------------------------------
// enqueue failure modes
// ---------------------------------------------------------------------------

test('merge: enqueue 抛 → transient dag.merge_enqueue_failed', async (t) => {
  const { spec, root, paths, records } = mergeSpec({ sources: ['src-a'], mergeId: null })
  // Build a dedicated single-source spec WITH the merge node.
  const singleSpec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'integrate', kind: 'merge', dependsOn: [{ taskId: 'src-a', condition: 'succeeded' }] },
    ],
  }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.enqueueScript.push({ throw: new Error('active_job_exists: mgj_9 holds branch') })
  const h = await makeHarness({ spec: singleSpec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.merge_enqueue_failed')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'transient')
  assert.match(JSON.parse(attempt.failure_json).message, /active_job_exists/)
  assert.equal(queue.drainCalls.length, 0, 'no drain after an enqueue failure')
})

test('merge: enqueue 源形态 {ok:false, error:{type}} → transient dag.merge_enqueue_failed（照 r.error.type 分）', async (t) => {
  const { root, paths, records } = mergeSpec({ sources: ['src-a'] })
  const singleSpec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'integrate', kind: 'merge', dependsOn: [{ taskId: 'src-a', condition: 'succeeded' }] },
    ],
  }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.enqueueScript.push({ okFalse: 'worktree_not_active' })
  const h = await makeHarness({ spec: singleSpec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.merge_enqueue_failed')
  assert.match(JSON.parse(attempt.failure_json).message, /worktree_not_active/)
})

test('merge: enqueue 返回无 repoKey 的记录 → transient dag.merge_enqueue_failed（provider 契约）', async (t) => {
  const { root, paths, records } = mergeSpec({ sources: ['src-a'] })
  const singleSpec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'integrate', kind: 'merge', dependsOn: [{ taskId: 'src-a', condition: 'succeeded' }] },
    ],
  }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.enqueueScript.push({ noRepoKey: true })
  const h = await makeHarness({ spec: singleSpec, service, queue, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.merge_enqueue_failed')
  assert.match(JSON.parse(attempt.failure_json).message, /repoKey/)
})

// ---------------------------------------------------------------------------
// source missing + engine unavailable
// ---------------------------------------------------------------------------

test('merge: 源缺失 — 上游 succeeded 但 dispatched 事件无 worktreeId → permanent dag.merge_source_missing', async (t) => {
  const { root, paths, records } = mergeSpec({ sources: ['src-a'] })
  const singleSpec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'integrate', kind: 'merge', dependsOn: [{ taskId: 'src-a', condition: 'succeeded' }] },
    ],
  }
  // The service's record has NO id — the dispatched event carries no
  // worktreeId (the pre-T16 record shape / lost event).
  const service = fakeWorktreeService({
    records: [{ path: paths['src-a'], task: 'slug-src-a' }],
  })
  const queue = fakeMergeQueue()
  const h = await makeHarness({ spec: singleSpec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.merge_source_missing')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'permanent')
  assert.match(JSON.parse(attempt.failure_json).message, /src-a/)
  assert.equal(queue.enqueueCalls.length, 0, 'no enqueue without a resolved source')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge: 引擎缺席 — merge-only 分支直接 → permanent dag.worktrees_unavailable', async (t) => {
  const { root } = mergeSpec({ sources: ['src-a'] })
  // A hand-shaped store: src-a ALREADY succeeded with a dispatched
  // worktreeId (the event-sourced source exists), leaving ONLY the merge
  // runnable — then the engine face is ABSENT at the merge's use time.
  const singleSpec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'integrate', kind: 'merge', dependsOn: [{ taskId: 'src-a', condition: 'succeeded' }] },
    ],
  }
  const queue = fakeMergeQueue()
  const h = await makeHarness({ spec: singleSpec, service: fakeWorktreeService(), queue, baseCwd: root })
  t.after(() => h.close())

  // Seed: src-a succeeded (attempt + dispatched worktreeId + task row).
  h.store.tx(() => {
    const cas = h.store.casTaskState(h.runId, 'src-a', 'pending', 1, 'succeeded')
    assert.equal(cas.ok, true)
    h.store.insertAttempt({
      attempt_id: 'att-seed', run_id: h.runId, task_id: 'src-a', ordinal: 1,
      state: 'succeeded', backend: 'spawn', child_session: 'sess-seed',
      owner_token: 'seed', started_at: 1, updated_at: 1,
    })
    h.store.insertEvent(h.runId, {
      type: 'attempt.dispatched', taskId: 'src-a', attemptId: 'att-seed',
      payload: { from: 'running', to: 'running', childSession: 'sess-seed', backend: 'spawn', worktreeId: 'wt-src-a' },
      at: 1,
    })
    h.store.insertEvent(h.runId, {
      type: 'task.succeeded', taskId: 'src-a',
      payload: { from: 'pending', to: 'succeeded', reason: 'succeeded', attemptNumber: 1 },
      at: 1,
    })
  })

  // Remove the engine face BEFORE the merge's dispatch — the use-time
  // re-probe (T15 cadence) reads absence at the merge executor.
  h.setEngine(undefined)

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.worktrees_unavailable')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'permanent')
  assert.equal(queue.enqueueCalls.length, 0)
  assert.equal(h.subagents.calls.length, 0)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

// ---------------------------------------------------------------------------
// multi-source ordering: conflicted wins over failed/queued (precedence)
// ---------------------------------------------------------------------------

test('merge: 多 repoKey drain 结果合并 — conflicted 优先于 failed/queued', async (t) => {
  const { root, paths } = mergeSpec({ sources: ['src-a'] })
  // Two sources on DIFFERENT repoKeys → two drains, one conflicted.
  const spec = {
    version: 1, name: 'merge-run', project: { root },
    tasks: [
      { id: 'src-a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-a' } },
      { id: 'src-b', kind: 'agent', prompt: 'p', worktree: { task: 'slug-src-b' } },
      {
        id: 'integrate', kind: 'merge',
        dependsOn: [
          { taskId: 'src-a', condition: 'succeeded' },
          { taskId: 'src-b', condition: 'succeeded' },
        ],
      },
    ],
  }
  const service = fakeWorktreeService({
    records: [
      { id: 'wt-src-a', path: paths['src-a'], task: 'slug-src-a' },
      { id: 'wt-src-b', path: paths['src-b'], task: 'slug-src-b' },
    ],
  })
  const queue = fakeMergeQueue()
  // First enqueue → repo-main; second enqueue → repo-other (scripted via a
  // stateful queue variant).
  let repoFlip = 0
  const originalEnqueue = queue.enqueue.bind(queue)
  queue.enqueue = (params) => {
    repoFlip += 1
    const job = originalEnqueue(params)
    return { ...job, repoKey: repoFlip === 1 ? 'repo-main' : 'repo-other' }
  }
  queue.drainScript.push({ state: 'failed', error: 'boom on repo-main' })
  queue.drainScript.push({ state: 'conflicted', conflictFiles: ['c.ts'], integrationWorktree: '/tmp/scene' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  assert.equal(s.run_state, 'running', 'conflicted parks (soft) even though another repo failed')
  const task = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(task.state, 'blocked')
  assert.equal(JSON.parse(task.blocked_reason).code, 'merge_conflicted')
  assert.equal(queue.drainCalls.length, 2, 'one drain per unique repoKey')
})

// ---------------------------------------------------------------------------
// spec-validate — the merge-source rule (companion to spec-validate.test.js)
// ---------------------------------------------------------------------------

import { validateSpec, DAG_SPEC_ERROR_CODES } from '../lib/spec-validate.js'

test('merge spec: 无 worktree 上游 → dag.merge_source_missing（plan 期防呆）', () => {
  const spec = {
    version: 1, name: 'bad-merge',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'm', kind: 'merge', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, false)
  const hit = r.errors.find((e) => e.code === DAG_SPEC_ERROR_CODES.mergeSourceMissing)
  assert.ok(hit !== undefined, JSON.stringify(r.errors))
  assert.equal(hit.path, 'tasks[1].dependsOn')
})

// ---------------------------------------------------------------------------
// M3 review M-B — the verify completion gate on the merge success path.
// A merge attempt terminal-commits INSIDE dispatchLoop (runMergeTask →
// commitTerminalAndRelease), never through harvestSettled where the agent
// path gates; pre-fix, a merge task's verify declaration validated clean
// and was then silently skipped (red line 1's class). The gate now rides
// with the merge executor: receipt = the pending integratedCommit output
// (status view = integratedCommit), a miss synthesizes permanent
// dag.verify_gate_failed through the same terminal machinery.
// ---------------------------------------------------------------------------

test('merge + verify: expectStatus === integratedCommit → 过门 succeeded, attempt.succeeded 盖 verifyStatus pass', async (t) => {
  const { spec: baseSpec, root, paths, records } = mergeSpec()
  const spec = structuredClone(baseSpec)
  const mergeTask = spec.tasks[spec.tasks.length - 1]
  mergeTask.outputs = [{ name: 'integration', schema: { type: 'object' }, required: false }]
  mergeTask.verify = { expectOutput: 'integration', expectStatus: 'deadbeef' }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'deadbeef' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  // Plan time: merge + verify coexist legally (the capability is kept).
  const validated = validateSpec(spec)
  assert.equal(validated.ok, true, JSON.stringify(validated.errors))

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded', JSON.stringify(h.states()))
  assert.equal(h.states().integrate, 'succeeded', 'the verify gate PASSED — the receipt (integratedCommit deadbeef) matched expectStatus')

  const attemptOk = h.eventsOfType('attempt.succeeded').filter((e) => e.task_id === 'integrate').at(-1)
  assert.equal(JSON.parse(attemptOk.payload_json).verifyStatus, 'pass', 'the gate ran and stamped pass')
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'deadbeef', integrationBranch: 'dag/merge-run/integration' })
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge + verify: expectStatus 不匹配 integratedCommit → permanent dag.verify_gate_failed, 任务 failed（不再静默跳过）', async (t) => {
  const { spec: baseSpec, root, paths, records } = mergeSpec()
  const spec = structuredClone(baseSpec)
  const mergeTask = spec.tasks[spec.tasks.length - 1]
  mergeTask.outputs = [{ name: 'integration', schema: { type: 'object' }, required: false }]
  // A contract the merge's {integratedCommit…} shape can never satisfy —
  // pre-fix this spec SUCCEEDED with the block silently ignored (the R2
  // evidence); post-fix it fails loud.
  mergeTask.verify = { expectOutput: 'integration', expectStatus: 'passed' }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'abc123' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  assert.equal(h.states().integrate, 'failed', 'the verify contract failed → the task CANNOT land succeeded')
  const attempt = h.attempts('integrate').at(-1)
  assert.equal(attempt.state, 'failed')
  const failure = JSON.parse(attempt.failure_json)
  assert.equal(failure.code, 'dag.verify_gate_failed')
  assert.equal(failure.failureType, 'permanent')
  assert.match(failure.message, /actual status "abc123"/)
  // No verify pass stamp, and NO outputs row — the contract failed before
  // the success landing.
  const attemptEvents = h.eventsOfType('attempt.failed').filter((e) => e.task_id === 'integrate')
  assert.equal(attemptEvents.length, 1)
  assert.equal(h.store.findOutput(h.runId, 'integrate', 'integration'), null)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('merge + verify: no_changes → 无 commit 可作 receipt → verify 门按 missing 失败（空集成不能伪装过门）', async (t) => {
  const { spec: baseSpec, root, paths, records } = mergeSpec()
  const spec = structuredClone(baseSpec)
  const mergeTask = spec.tasks[spec.tasks.length - 1]
  mergeTask.outputs = [{ name: 'integration', schema: { type: 'object' }, required: false }]
  mergeTask.verify = { expectOutput: 'integration', expectStatus: 'deadbeef' }
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'no_changes' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'failed')
  const attempt = h.attempts('integrate').at(-1)
  const failure = JSON.parse(attempt.failure_json)
  assert.equal(failure.code, 'dag.verify_gate_failed')
  assert.match(failure.message, /missing/)
})

test('merge WITHOUT verify: the gate stays skipped — evidence none_declared, no regression to the plain five-state mapping', async (t) => {
  const { spec, root, paths, records } = mergeSpec()
  const service = fakeWorktreeService({ records })
  const queue = fakeMergeQueue()
  queue.drainScript.push({ state: 'succeeded', integratedCommit: 'deadbeef' })
  const h = await makeHarness({ spec, service, queue, baseCwd: root })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s.run_state, 'succeeded')
  const attemptOk = h.eventsOfType('attempt.succeeded').filter((e) => e.task_id === 'integrate').at(-1)
  const okPayload = JSON.parse(attemptOk.payload_json)
  assert.equal(okPayload.verifyEvidence, 'none_declared', 'no verify declaration → evidence none_declared (the harvestSettled shape)')
  assert.equal(okPayload.verifyStatus, undefined)
  const out = h.store.findOutput(h.runId, 'integrate', 'integration')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'deadbeef', integrationBranch: 'dag/merge-run/integration' })
})
