/**
 * M3 milestone review — REVIEW ASSET TESTS (post-fix regression suite).
 *
 * These tests were added by the final (M3) review as DEFECT
 * REPRODUCTIONS (asserting the then-broken behavior to prove two Major
 * findings). The fixing batch rewrote each into the DESIGN-conformant
 * assertion; the R# ids and scenario shapes are kept as review assets —
 * each block names the finding it guards against (same convention as
 * m1/m2-review-repro.test.js).
 *
 *   R1 (Major M-A) — worktree slug collision: two CONCURRENT tasks
 *        declaring the same `worktree.task` slug used to silently share
 *        ONE worktree — the T16 reuse probe `findActiveByTask(repoRoot,
 *        taskSlug)` matched by slug alone (lib/executor.js
 *        resolveWorktreeCwd), so task b's dispatch found task a's
 *        still-ACTIVE worktree and reused its path: two parallel
 *        subagents writing into the same checkout, defeating the
 *        isolation the worktree declaration exists to provide, with
 *        prompt-pollution risk on top. DESIGN §11.3 scopes reuse to
 *        "retry_task 重派发时" (the SAME task's re-dispatch); the
 *        implementation had widened it to any same-slug task.
 *        FIX (two layers):
 *          1. spec-validate — `worktree.task` slugs are UNIQUE across
 *             the whole spec (`dag.worktree_slug_conflict`); the hazard
 *             is structurally unreachable at plan time.
 *          2. executor reuse ownership — the active record is reused
 *             ONLY when its `correlationId` (the create-time attemptId)
 *             belongs to THIS task's attempt history (engine dispatch
 *             ctxInfo.taskAttemptIds); a foreign or
 *             ownership-unprovable record falls through to create, which
 *             fails loud on the occupied slug (transient
 *             `dag.worktree_create_failed`).
 *
 *   R2 (Major M-B) — `verify` on a merge task was silently ignored: the
 *        spec kind matrix rejects `verify`/`worktree` on approval tasks
 *        (APPROVAL_FORBIDDEN_FIELDS) but has NO rule against merge, and
 *        the verify gate was only ever evaluated in harvestSettled
 *        (agent attempts). A merge attempt terminal-commits inside
 *        dispatchLoop via runMergeTask, so a spec like
 *        `{kind:'merge', outputs:[report], verify:{expectOutput:'report',
 *        expectStatus:'passed'}}` validated clean and succeeded while the
 *        verify block was dead weight — red line #1's class of defect
 *        (a spec field admitted in a position where it is silently not
 *        executed). FIX (option a — capability kept): the merge
 *        executor's SUCCESS path (succeeded | no_changes) evaluates the
 *        SAME evaluateVerifyGate before commitTerminalAndRelease(null),
 *        with the §7.3 receipt substitution (receipt = the pending
 *        integratedCommit output; status view = integratedCommit). A
 *        miss synthesizes permanent `dag.verify_gate_failed` through the
 *        same terminal machinery; the conflicted park / failed / queued
 *        paths are non-success by construction and never pass the gate.
 *
 * Zero network, zero git, zero real dsh-worktrees import (fakes only).
 */

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
// fixtures (the merge-executor.test.js harness shape, minimal)
// ---------------------------------------------------------------------------

/**
 * Fake worktree service matching the M-A contract: create() stamps the
 * dispatch correlationId onto the record; findActiveByTask answers the
 * pool (records carry {path, correlationId} — the ownership evidence).
 */
function fakeWorktreeService() {
  const service = {
    createCalls: [],
    pool: [],
    async create(params) {
      service.createCalls.push(params)
      const record = { id: `wt-${service.createCalls.length}`, path: params.__path, task: params.task, correlationId: params.correlationId }
      service.pool.push(record)
      return record
    },
    async findActiveByTask(repoRoot, taskSlug) {
      return service.pool.find((r) => r.task === taskSlug) ?? null
    },
  }
  return service
}

function fakeEngineFace(service, queue) {
  return {
    getMergeQueue: () => queue ?? {},
    getWorktreeService: () => service,
    available: true,
  }
}

function fakeSubagents() {
  const calls = []
  const script = []
  let counter = 0
  return {
    calls,
    script,
    async start(name, request) {
      counter += 1
      calls.push({ name, request })
      // Scripted behaviors pop per start; the default HANGS (children stay
      // "running" until harvested — the R1 CONCURRENT dispatch window).
      const behavior = script.length > 0 ? script.shift() : { hang: true }
      if (behavior.hang) {
        return {
          id: `sess-${counter}`,
          result: new Promise(() => {}),
          dispose: async () => {},
        }
      }
      return {
        id: `sess-${counter}`,
        result: Promise.resolve(behavior.resolve ?? { output: [], stopReason: 'completed' }),
        dispose: async () => {},
      }
    },
  }
}

async function makeHarness({ spec, service, baseCwd, queue }) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'm3rev-')), 'dag.db') })
  const subagents = fakeSubagents()
  const ctx = { get: (name) => (name === 'worktreesEngine' ? fakeEngineFace(service, queue) : undefined) }
  const executor = createExecutor({
    ctxSubagents: subagents,
    worktreesSeam: createWorktreesSeam(ctx, {}),
    execAgentProvider: () => ({ __live: 'agent' }),
  })
  const engine = createEngine({ store, executor, admission: createAdmission(), logger: {} })
  const runId = engine.planRun(spec, { baseCwd, runId: 'run-m3rev' }).runId
  return {
    store, subagents, engine, runId,
    close() { engine.disposeAll(); store.close() },
  }
}
// ---------------------------------------------------------------------------
// R1 — same-slug CONCURRENT tasks: plan-time rejection + runtime ownership
// ---------------------------------------------------------------------------

test('M3 review R1 (fixed): two CONCURRENT tasks with the same worktree slug are rejected at plan time — dag.worktree_slug_conflict', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'm3rev-root-'))
  const pathA = join(root, '.worktrees', 'shared')
  mkdirSync(pathA, { recursive: true })

  const service = fakeWorktreeService()

  const spec = {
    version: 1, name: 'slug-collision',
    project: { root },
    tasks: [
      { id: 'impl-a', kind: 'agent', prompt: 'write a', worktree: { task: 'same-slug' } },
      { id: 'impl-b', kind: 'agent', prompt: 'write b', worktree: { task: 'same-slug' } },
    ],
  }

  // ---- LAYER 1 (plan time): the duplicate slug is a loud rejection —
  // the hazard is structurally unreachable from a copy-paste spec error.
  const validated = validateSpec(spec)
  assert.equal(validated.ok, false, 'the duplicate worktree slug no longer validates')
  const hit = validated.errors.find((e) => e.code === DAG_SPEC_ERROR_CODES.worktreeSlugConflict)
  assert.ok(hit !== undefined, `expected dag.worktree_slug_conflict, got ${JSON.stringify(validated.errors)}`)
  assert.equal(hit.path, 'tasks[1].worktree.task')
  assert.match(hit.message, /first declared by task 'impl-a'/)

  // And planRun refuses it too (the engine's validation entry).
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'm3rev-')), 'dag.db') })
  t.after(() => store.close())
  const subagents = fakeSubagents()
  const ctx = { get: () => fakeEngineFace(service) }
  const executor = createExecutor({
    ctxSubagents: subagents,
    worktreesSeam: createWorktreesSeam(ctx, {}),
    execAgentProvider: () => ({ __live: 'agent' }),
  })
  const engine = createEngine({ store, executor, admission: createAdmission(), logger: {} })
  assert.throws(
    () => engine.planRun(spec, { baseCwd: root, runId: 'run-m3rev-plan' }),
    (error) => error.errors?.some((e) => e.code === DAG_SPEC_ERROR_CODES.worktreeSlugConflict) === true,
    'planRun surfaces the stable code through its aggregated error',
  )
})

test('M3 review R1 (fixed): a foreign same-slug ACTIVE record is NOT reused — create exposes the occupied slug loud (runtime ownership gate)', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'm3rev-root-'))
  const pathA = join(root, '.worktrees', 'shared')
  mkdirSync(pathA, { recursive: true })

  const service = fakeWorktreeService()
  // The runtime world plan time cannot see (hand-seeded store / provider
  // drift): ANOTHER task's record sits ACTIVE under this task's slug.
  // create answers the occupied slug loud (branch_exists — the branch
  // derives from the slug).
  service.pool.push({ id: 'wt-foreign', path: pathA, task: 'same-slug', correlationId: 'att-foreign-task' })
  service.create = async (params) => {
    service.createCalls.push(params)
    throw new Error('branch_exists: dag/same-slug already checked out')
  }

  // Distinct slugs keep the spec plan-clean — the isolation question is
  // purely the runtime probe's now.
  const spec = {
    version: 1, name: 'slug-runtime',
    project: { root },
    tasks: [{ id: 'impl-b', kind: 'agent', prompt: 'write b', worktree: { task: 'same-slug' } }],
  }
  assert.equal(validateSpec(spec).ok, true)

  const h = await makeHarness({ spec, service, baseCwd: root })
  t.after(() => h.close())

  await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })

  // FIXED behavior: NO silent sharing. The reuse arm REFUSED the foreign
  // record (its correlationId is not in this task's attempt history),
  // create ran and failed loud on the occupied slug, and no subagent ever
  // dispatched into another task's checkout.
  assert.equal(h.subagents.calls.length, 0, 'no subagent dispatched into the foreign worktree')
  assert.equal(service.createCalls.length, 1, 'the reuse arm did NOT serve the foreign record — create ran')
  const attempt = h.store.findAttempts(h.runId, 'impl-b').at(-1)
  assert.equal(attempt.state, 'failed')
  const failure = JSON.parse(attempt.failure_json)
  assert.equal(failure.code, 'dag.worktree_create_failed')
  assert.equal(failure.failureType, 'transient', 'honest exposure, retry policy applies')
  assert.match(failure.message, /branch_exists/)
})

// ---------------------------------------------------------------------------
// R2 — verify on a merge task: the gate now rides with the merge executor
// ---------------------------------------------------------------------------

test('M3 review R2 (fixed): a merge task declaring verify is GATED on its success path — a contract the output can never satisfy fails loud', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'm3rev-root-'))
  const srcPath = join(root, '.worktrees', 'wt-src')
  mkdirSync(srcPath, { recursive: true })

  const service = fakeWorktreeService()
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: `wt-${service.createCalls.length}`, path: srcPath, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }

  const spec = {
    version: 1, name: 'merge-verify-dead',
    project: { root },
    tasks: [
      { id: 'src', kind: 'agent', prompt: 'work', worktree: { task: 'slug-src' } },
      {
        id: 'integrate', kind: 'merge',
        dependsOn: [{ taskId: 'src', condition: 'succeeded' }],
        outputs: [{ name: 'report', schema: { type: 'object' } }],
        // A verify block whose expectStatus can NEVER be satisfied by the
        // merge executor's integratedCommit output — pre-fix this task
        // landed succeeded with the block silently ignored.
        verify: { expectOutput: 'report', expectStatus: 'passed' },
      },
    ],
  }
  const validated = validateSpec(spec)
  assert.equal(validated.ok, true, 'the kind matrix keeps merge+verify legal (the capability is kept)')

  const queue = {
    async enqueue() { return { id: 'mgj_1', repoKey: 'repo-main', state: 'queued' } },
    async drain() { return { state: 'succeeded', integratedCommit: 'abc123' } },
  }
  const h = await makeHarness({ spec, service, baseCwd: root, queue })
  t.after(() => h.close())

  // The src agent completes immediately (no outputs declared), so the
  // merge's succeeded-condition upstream is satisfied within the tick.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })

  // FIXED behavior: the gate RAN for the merge attempt. The contract
  // (expectStatus 'passed' vs the actual integratedCommit 'abc123') fails
  // → permanent dag.verify_gate_failed, the task lands failed, the run
  // fails — never a silent success.
  const integrate = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(s.run_state, 'failed')
  assert.equal(integrate.state, 'failed', 'the verify contract is enforced for merge tasks — no silent skip')
  const attempt = h.store.findAttempts(h.runId, 'integrate').at(-1)
  const failure = JSON.parse(attempt.failure_json)
  assert.equal(failure.code, 'dag.verify_gate_failed')
  assert.equal(failure.failureType, 'permanent')
  assert.match(failure.message, /actual status "abc123"/)
  // No outputs row landed — the contract failed before the success landing.
  assert.equal(h.store.findOutput(h.runId, 'integrate', 'report'), null)
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})

test('M3 review R2 (fixed, companion): a SATISFIABLE merge verify contract passes and stamps the gate on the attempt event', async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'm3rev-root-'))
  const srcPath = join(root, '.worktrees', 'wt-src')
  mkdirSync(srcPath, { recursive: true })

  const service = fakeWorktreeService()
  service.create = async (params) => {
    service.createCalls.push(params)
    const record = { id: `wt-${service.createCalls.length}`, path: srcPath, task: params.task, correlationId: params.correlationId }
    service.pool.push(record)
    return record
  }

  const spec = {
    version: 1, name: 'merge-verify-pass',
    project: { root },
    tasks: [
      { id: 'src', kind: 'agent', prompt: 'work', worktree: { task: 'slug-src' } },
      {
        id: 'integrate', kind: 'merge',
        dependsOn: [{ taskId: 'src', condition: 'succeeded' }],
        outputs: [{ name: 'report', schema: { type: 'object' } }],
        // The merge's status-equivalent fact is the integratedCommit — a
        // contract pinned to the expected commit passes the gate.
        verify: { expectOutput: 'report', expectStatus: 'abc123' },
      },
    ],
  }

  const queue = {
    async enqueue() { return { id: 'mgj_1', repoKey: 'repo-main', state: 'queued' } },
    async drain() { return { state: 'succeeded', integratedCommit: 'abc123' } },
  }
  const h = await makeHarness({ spec, service, baseCwd: root, queue })
  t.after(() => h.close())

  // src completes immediately — the merge runs and its gate passes.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })

  const s = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })

  assert.equal(s.run_state, 'succeeded')
  const integrate = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(integrate.state, 'succeeded', 'the satisfiable contract passes — the capability is real, not just policed')
  const out = h.store.findOutput(h.runId, 'integrate', 'report')
  assert.deepEqual(JSON.parse(out.value_json), { integratedCommit: 'abc123', integrationBranch: 'dag/merge-verify-pass/integration' })
  // The gate's verdict is stamped on the attempt.succeeded event — the
  // reviewer's "no verifyStatus stamp" observation is closed.
  const succeededEvent = h.store.findEvents(h.runId).find((e) => e.type === 'attempt.succeeded' && e.task_id === 'integrate')
  const payload = JSON.parse(succeededEvent.payload_json)
  assert.equal(payload.verifyStatus, 'pass')
  assert.equal(h.store.verifyChain(h.runId).ok, true)
})
