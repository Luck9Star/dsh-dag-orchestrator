/**
 * M1 milestone review — REVIEW ASSET TESTS (post-fix regression suite).
 *
 * These tests were added by the design-contract review as DEFECT
 * REPRODUCTIONS (asserting the then-broken behavior to prove four
 * findings). The fixing batch rewrote each into the DESIGN-conformant
 * assertion; the R# ids and scenario shapes are kept as review assets —
 * each block names the finding it guards against.
 *
 *   R1 (Blocker B1): the §12.1 Window-1 auto-retry must fire on the
 *       residue engine.claimTask ACTUALLY produces — state 'running' with
 *       child_session NULL (born-running narrowing, DESIGN §6.2) — not
 *       only on the hand-seedable 'claimed' shape.
 *
 *   R2 (Major M2): a THROWN executor.dispatch (e.g. the no-live-agent
 *       TypeError) must roll the claimed attempt forward as an internal
 *       terminal failure (`dag.dispatch_threw`) — the tick resolves, the
 *       task is not wedged, the slot/sessionKey do not leak.
 *
 *   R3 (Major M3): re-ticking a TERMINAL run is an idempotent no-op —
 *       zero progress, no duplicate task.blocked events, no phantom
 *       `promoted` counts.
 *
 *   R4 (Major M4): a crash-recovery auto-retry (payload recovery:true)
 *       does NOT consume the task's retryOn budget — only real execution
 *       retries are billed (DESIGN §12.1 "nothing ever ran").
 *
 *   R5 (Blocker B2): a transitive dead-end (A failed → B blocked
 *       upstream_failed → C blocked upstream_blocked) MUST finalize the
 *       Run failed — upstream_blocked is a HARD block (upstreamSatisfies
 *       re-evaluates it to upstream_failed, never to ready), so
 *       finalizeRunIfDone derives Run failure (§5.1 step 9) instead of
 *       keeping the run alive forever on a can-never-progress graph.
 *
 *   R6 (Major M1): reconcile step 2's run set is re-taken AFTER the chain
 *       pass — a run parked failed by step 1 is terminal; deriving retry
 *       actions from its rows would be deriving truth from the tamper we
 *       just quarantined.
 *
 *   R7 (Blocker B3): the red-line-9 cwd gate — resolveTaskCwd's full
 *       decision table (subtree containment, allowlist, symlink realpath
 *       hardening, relative/nonexistent denial, fail-closed workspace
 *       registration, baseCwd default) plus the executor's
 *       no-subagent-on-denial wiring.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, mkdirSync, symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createDagStore } from '../lib/dag-store.js'
import { createAdmission } from '../lib/admission.js'
import { createExecutor } from '../lib/executor.js'
import { createEngine } from '../lib/engine.js'
import { reconcile } from '../lib/recovery.js'
import { resolveTaskCwd } from '../lib/cwd-gate.js'

function fakeSubagents() {
  const script = []
  const calls = []
  return {
    script,
    calls,
    async start(name, request) {
      calls.push({ name, request })
      const behavior = script.length > 0 ? script.shift() : { resolve: { output: [], stopReason: 'completed' } }
      if (behavior.hang) {
        return { id: `s-${calls.length}`, result: new Promise(() => {}), dispose: async () => {} }
      }
      return { id: `s-${calls.length}`, result: Promise.resolve(behavior.resolve), dispose: async () => {} }
    },
  }
}

async function makeHarness({ spec, withProvider = true, config } = {}) {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'rev-')), 'dag.db') })
  const subagents = fakeSubagents()
  const executor = createExecutor({
    ctxSubagents: subagents,
    ...(withProvider ? { execAgentProvider: () => ({ __live: 'agent' }) } : {}),
    ...(config !== undefined ? { config } : {}),
  })
  const admission = createAdmission()
  const engine = createEngine({
    store, executor, admission, logger: {}, config,
    now: () => 1_000_000, random: () => 0.5,
  })
  const runId = spec !== undefined ? engine.planRun(spec, { baseCwd: '/tmp/repo', runId: 'run-1' }).runId : 'run-1'
  return { store, subagents, executor, admission, engine, runId, async close() { store.close() } }
}

// ---------------------------------------------------------------------------
// R1 — the §12.1 Window-1 auto-retry must fire on ENGINE-SHAPED crash
// residue (running + child_session NULL), not just the 'claimed' shape.
// ---------------------------------------------------------------------------

/** What a crash between the claim tx and subagents.start ACTUALLY leaves. */
function seedEngineShapedCrash(store, { attemptState, runId = 'run-c', taskId = 'analyze', attemptId = 'att-c' } = {}) {
  store.tx(() => {
    store.insertRun({
      run_id: runId, name: 'crashed', spec_json: JSON.stringify({ version: 1, name: 'crashed', tasks: [] }),
      spec_hash: 'a'.repeat(64), state: 'running', control_intent: null, parent_session: null,
      base_cwd: '/tmp/repo', created_at: 1, updated_at: 1, version: 1,
    })
    store.insertTasks(runId, [{ task_id: taskId, state: 'running', version: 1 }])
    store.insertAttempt({
      attempt_id: attemptId, run_id: runId, task_id: taskId, ordinal: 1,
      state: attemptState, backend: 'spawn', child_session: null,
      owner_token: 'engine-minted-at-claim', // engine.js claim-time mint
      started_at: 1, updated_at: 1,
    })
    store.insertEvent(runId, { type: 'run.created', payload: {}, at: 1 })
  })
}

test('R1: engine-shaped crash residue (running, pre-dispatch) IS auto-retried', async (t) => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'rev-')), 'dag.db') })
  t.after(() => store.close())
  // The residue of a crash between the claim tx and subagents.start:
  // state 'running' + child_session null + owner_token set (claimTask shape).
  seedEngineShapedCrash(store, { attemptState: 'running' })

  const summary = await reconcile(store, { logger: {} })

  // DESIGN §12.1 expectation: failed(recovery.no_dispatch) + task retry_wait.
  assert.equal(summary.autoRetried, 1)
  const attempt = store.findAttempt('att-c')
  assert.equal(attempt.state, 'failed')
  assert.equal(JSON.parse(attempt.failure_json).code, 'recovery.no_dispatch')
  assert.equal(store.findTasks('run-c')[0].state, 'retry_wait')
  const retryEv = store.findEvents('run-c').find((e) => e.type === 'attempt.retry_scheduled')
  assert.equal(JSON.parse(retryEv.payload_json).recovery, true)
  assert.deepEqual(store.verifyChain('run-c'), { ok: true })
})

test('R1 (control): the claimed row shape recovers identically', async (t) => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'rev-')), 'dag.db') })
  t.after(() => store.close())
  seedEngineShapedCrash(store, { attemptState: 'claimed' })
  const summary = await reconcile(store, { logger: {} })
  assert.equal(summary.autoRetried, 1)
  assert.equal(store.findAttempt('att-c').state, 'failed')
  assert.equal(store.findTasks('run-c')[0].state, 'retry_wait')
})

test('R1b: running WITH child_session is the T13 orphan arm — never auto-retried', async (t) => {
  const store = await createDagStore({ path: join(mkdtempSync(join(tmpdir(), 'rev-')), 'dag.db') })
  t.after(() => store.close())
  seedEngineShapedCrash(store, { attemptState: 'running' })
  // The subagent took off: the dispatch recorded its child session.
  store.tx(() => store.updateAttemptChildSession('att-c', 'sess-took-off'))

  const summary = await reconcile(store, { logger: {} })

  assert.equal(summary.autoRetried, 0, 'partial work possible — the bounded policy forbids auto-retry')
  assert.equal(summary.orphaned, 1)
  const attempt = store.findAttempt('att-c')
  assert.equal(attempt.state, 'orphaned')
  assert.equal(attempt.child_session, 'sess-took-off', 'the manual session-log locator survives')
  assert.equal(store.findTasks('run-c')[0].state, 'failed')
  const types = store.findEvents('run-c').map((e) => e.type)
  assert.ok(types.includes('recovery.action_requested'))
  assert.ok(types.includes('attempt.orphaned'))
  assert.deepEqual(store.verifyChain('run-c'), { ok: true })
})

// ---------------------------------------------------------------------------
// R2 — a dispatch THROW after a successful claim rolls forward terminally.
// ---------------------------------------------------------------------------

test('R2: executor.dispatch THROWS after claim → terminal dag.dispatch_threw, tick resolves, nothing wedged', async (t) => {
  const spec = { version: 1, name: 'wedge', tasks: [{ id: 'a', kind: 'agent', prompt: 'a' }] }
  // No execAgentProvider + tick called without execAgent → executor.dispatch
  // throws its TypeError (no live exec agent) AFTER claimTask committed.
  const h = await makeHarness({ spec, withProvider: false })
  t.after(() => h.close())

  // The tick RESOLVES (the throw is contained), and the attempt lands
  // terminal with the internal classification.
  const s1 = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(s1.dispatched, 1)
  assert.equal(s1.terminal, 1)
  assert.equal(s1.run_state, 'failed')

  const task = h.store.findTasks(h.runId)[0]
  const attempt = h.store.findAttempts(h.runId, 'a')[0]
  assert.equal(task.state, 'failed')
  assert.equal(attempt.state, 'failed')
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.dispatch_threw')
  assert.equal(JSON.parse(attempt.failure_json).failureType, 'internal')
  // No in-flight handle exists and no admission resources leaked.
  assert.deepEqual(h.executor.inFlightIds(), [])
  assert.equal(h.admission.heldCount(), 0)

  // Idempotent: a second tick on the terminal run is a no-op.
  const s2 = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s2.run_state, 'failed')
  assert.equal(s2.dispatched, 0)
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

// ---------------------------------------------------------------------------
// R3 — re-ticking a terminal run is an idempotent no-op.
// ---------------------------------------------------------------------------

test('R3: re-ticking a terminal run emits no duplicate events and reports zero progress', async (t) => {
  const spec = {
    version: 1, name: 'dead',
    tasks: [
      { id: 'root', kind: 'agent', prompt: 'r' },
      { id: 'leaf', kind: 'agent', prompt: 'l', dependsOn: [{ taskId: 'root', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } }) // permanent

  const s1 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(s1.run_state, 'failed')
  const eventsAfter1 = h.store.findEvents(h.runId)
  const blockedAfter1 = eventsAfter1.filter((e) => e.type === 'task.blocked').length
  assert.equal(blockedAfter1, 1)
  const leafAfter1 = h.store.findTasks(h.runId).find((r) => r.task_id === 'leaf')

  // Second tick on the SAME terminal state: no new events, no version bump,
  // no phantom progress (blocked→blocked same-reason rewrites are only the
  // crash/replay safety net — DESIGN §6.2 — never a per-tick routine write).
  const s2 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  assert.equal(h.store.findEvents(h.runId).length, eventsAfter1.length, 'no new events')
  assert.equal(
    h.store.findEvents(h.runId).filter((e) => e.type === 'task.blocked').length,
    blockedAfter1,
    'no duplicate task.blocked',
  )
  assert.equal(s2.promoted, 0, 'no phantom progress')
  assert.equal(s2.dispatched, 0)
  assert.equal(s2.terminal, 0)
  assert.equal(s2.propagated, 0)
  assert.equal(s2.run_state, 'failed')
  const leafAfter2 = h.store.findTasks(h.runId).find((r) => r.task_id === 'leaf')
  assert.equal(leafAfter2.version, leafAfter1.version, 'no version bump for nothing')
  assert.equal(leafAfter2.state, 'blocked')
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

// ---------------------------------------------------------------------------
// R4 — a crash-recovery retry does NOT burn the task's retryOn budget.
// ---------------------------------------------------------------------------

test('R4: a crash-recovery retry_scheduled event (recovery:true) does not consume a retryOn budget slot', async (t) => {
  const spec = {
    version: 1, name: 'budget-burn',
    tasks: [{
      id: 'flaky', kind: 'agent', prompt: 'f',
      retry: { maxAttempts: 2, backoffMs: 0, jitterRatio: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // The durable residue of one crash-recovery auto-retry: exactly what
  // recovery.js appends (payload recovery:true — §12.1's free allowance).
  h.store.tx(() => {
    h.store.insertEvent(h.runId, {
      type: 'attempt.retry_scheduled', taskId: 'flaky',
      payload: { failedAttemptId: 'att-old', nextAttemptNumber: 2, retryNotBeforeMs: 1, backoffMs: 0, recovery: true },
      at: 1,
    })
  })

  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } }) // transient
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })

  // The budget is intact: the first REAL failure still gets its designed
  // second attempt. backoffMs=0 + the frozen clock make the retry eligible
  // within the same tick — attempt 2 is claimed and dispatched (calls=2).
  assert.equal(h.subagents.calls.length, 2, 'the task got its designed second real attempt')
  assert.equal(h.subagents.calls[1].request.label, 'budget-burn/flaky#2')
  assert.notEqual(s.run_state, 'failed', 'the run is not prematurely failed')

  // The retry then completes on the next pump.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'completed' } })
  const s2 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s2.run_state, 'succeeded')
  assert.equal(h.subagents.calls.length, 2)
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

test('R4 (control): a REAL retry_scheduled event (no recovery flag) still bills the budget', async (t) => {
  const spec = {
    version: 1, name: 'budget-real',
    tasks: [{
      id: 'flaky', kind: 'agent', prompt: 'f',
      retry: { maxAttempts: 2, backoffMs: 0, jitterRatio: 0, retryOn: ['transient_network'] },
    }],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } }) // real retry #1

  const s1 = await h.engine.tick(h.runId, { maxRounds: 1, settleMs: 0 })
  assert.equal(h.subagents.calls.length, 1)
  // backoffMs=0 + the frozen clock: the real retry is already eligible, so
  // the task may be retry_wait (single round) or re-dispatched (running).
  const stateAfterFirst = h.store.findTasks(h.runId)[0].state
  assert.ok(['retry_wait', 'running'].includes(stateAfterFirst), `state ${stateAfterFirst}`)

  // Budget now genuinely exhausted (one real retry consumed the second
  // slot) → the next real failure goes straight terminal.
  h.subagents.script.push({ resolve: { output: [], stopReason: 'error' } })
  const s2 = await h.engine.tick(h.runId, { maxRounds: 4, settleMs: 0 })
  assert.equal(s2.run_state, 'failed')
  assert.equal(h.subagents.calls.length, 2, 'no third attempt — the budget did its job')
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

// ---------------------------------------------------------------------------
// R5 — a transitive dead-end finalizes the Run (upstream_blocked is HARD).
// ---------------------------------------------------------------------------

test('R5: A(failed)→B→C chain — transitive upstream_blocked is a dead end; the Run finalizes failed', async (t) => {
  const spec = {
    version: 1, name: 'chain-dead',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'a' },
      { id: 'b', kind: 'agent', prompt: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
      { id: 'c', kind: 'agent', prompt: 'c', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())
  h.subagents.script.push({ resolve: { output: [], stopReason: 'refusal' } }) // a: permanent

  const s = await h.engine.tick(h.runId, { maxRounds: 6, settleMs: 0 })
  const states = Object.fromEntries(h.store.findTasks(h.runId).map((r) => [r.task_id, r.state]))

  assert.deepEqual(states, { a: 'failed', b: 'blocked', c: 'blocked' })

  // DESIGN §5.1 step 9: onlyDeadBlocked → anyFailed → run failed — the
  // drive must NOT hang forever telling the model to tick a graph that
  // can never progress (b/c are dead-ended through a's permanent failure;
  // re-evaluating c flips the code to upstream_failed, never to ready).
  assert.equal(s.run_state, 'failed', 'the dead-end graph derives Run failure')
  assert.equal(h.store.findRun(h.runId).state, 'failed')
  assert.ok(h.store.findEvents(h.runId).some((e) => e.type === 'run.failed'))
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

test('R5 (control): approval_pending stays SOFT — a parked approval keeps the Run alive', async (t) => {
  // The narrowing must not over-reach: a blocked(approval_pending) task is
  // clearable by a human dag_approve, so it counts as live work and the
  // Run must NOT finalize while one is parked.
  const spec = { version: 1, name: 'parked', tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' } }] }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  // Hand-park the gate task exactly as T12's executor will (M1: no
  // approval executor yet — the M5 kind gate fails it closed instead, so
  // seed the park directly to probe isSoftBlocked's decision).
  h.store.tx(() => {
    h.store.casTaskState(h.runId, 'gate', 'pending', 1, 'blocked', {
      blocked_reason: JSON.stringify({ code: 'approval_pending', details: { approvalId: 'ap-1' } }),
    })
  })
  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.run_state, 'running', 'approval_pending is soft — the Run waits for dag_approve')
  assert.equal(h.store.findRun(h.runId).state, 'running')
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})

// ---------------------------------------------------------------------------
// R6 — reconcile step 2 uses the POST-chain run set.
// ---------------------------------------------------------------------------

test('R6: chain-broken run with a claimed attempt is NOT auto-retried (post-chain snapshot)', async (t) => {
  const { DatabaseSync } = await import('node:sqlite')
  const p = join(mkdtempSync(join(tmpdir(), 'rev-')), 'dag.db')

  // Seed a run whose ONLY event is then tampered → verifyChain fails.
  const seedStore = await createDagStore({ path: p })
  seedEngineShapedCrash(seedStore, { attemptState: 'claimed', runId: 'run-t', taskId: 'analyze', attemptId: 'att-t' })
  seedStore.close()
  const rawDb = new DatabaseSync(p)
  rawDb.prepare("UPDATE events SET payload_json = ? WHERE run_id = ? AND seq = ?").run('{"tampered":true}', 'run-t', 1)
  rawDb.close()

  const reopened = await createDagStore({ path: p })
  t.after(() => reopened.close())
  const summary = await reconcile(reopened, { logger: {} })

  // The run is quarantined (chainBroken) and its attempts are LEFT ALONE —
  // deriving recovery actions from a quarantined run's rows would derive
  // truth from the very tamper step 1 isolated (recovery.js's own contract).
  assert.equal(summary.chainBroken, 1)
  assert.equal(summary.autoRetried, 0, 'no recovery actions from a quarantined run\'s rows')
  assert.equal(reopened.findAttempt('att-t').state, 'claimed', 'attempt rows untouched')
  assert.equal(reopened.findTasks('run-t')[0].state, 'running', 'task rows untouched')
  assert.equal(reopened.findRun('run-t').state, 'failed', 'the run itself is parked failed for manual disposal')
})

// ---------------------------------------------------------------------------
// R7 — red-line-9 cwd gate (B3): the resolveTaskCwd decision table.
// ---------------------------------------------------------------------------

test('R7: resolveTaskCwd — subtree containment allows, outside denies, allowlist admits', () => {
  const base = mkdtempSync(join(tmpdir(), 'dag-r7-'))
  mkdirSync(join(base, 'sub'), { recursive: true })
  const sibling = mkdtempSync(join(tmpdir(), 'dag-r7-out-'))
  const allowed = mkdtempSync(join(tmpdir(), 'dag-r7-ok-'))

  // Inside the base subtree (the base itself and a deeper path).
  assert.deepEqual(resolveTaskCwd({ taskCwd: base, baseCwd: base }), { ok: true, cwd: base })
  assert.deepEqual(resolveTaskCwd({ taskCwd: join(base, 'sub'), baseCwd: base }), { ok: true, cwd: join(base, 'sub') })

  // Outside the base, no allowlist → deny.
  const denied = resolveTaskCwd({ taskCwd: sibling, baseCwd: base })
  assert.equal(denied.ok, false)
  assert.equal(denied.code, 'dag.cwd_denied')

  // Outside the base, inside an allowedRoot → admit.
  assert.deepEqual(
    resolveTaskCwd({ taskCwd: sibling, baseCwd: base, allowedRoots: [allowed, sibling] }),
    { ok: true, cwd: sibling },
  )
})

test('R7: resolveTaskCwd — relative and nonexistent paths deny; absent taskCwd falls back to baseCwd', () => {
  const base = mkdtempSync(join(tmpdir(), 'dag-r7b-'))

  const relative = resolveTaskCwd({ taskCwd: 'relative/dir', baseCwd: base })
  assert.equal(relative.ok, false)
  assert.equal(relative.code, 'dag.cwd_denied')
  assert.match(relative.message, /absolute/)

  const missing = resolveTaskCwd({ taskCwd: join(base, 'no-such-dir'), baseCwd: base })
  assert.equal(missing.ok, false)
  assert.equal(missing.code, 'dag.cwd_denied')
  assert.match(missing.message, /does not exist/)

  // Absent taskCwd → the run's base (the default; no gate decision).
  assert.deepEqual(resolveTaskCwd({ taskCwd: undefined, baseCwd: base }), { ok: true, cwd: base })
})

test('R7: resolveTaskCwd — symlink escape is judged on REALPATH (both ends)', () => {
  const base = mkdtempSync(join(tmpdir(), 'dag-r7c-'))
  const outside = mkdtempSync(join(tmpdir(), 'dag-r7c-out-'))
  // A symlink under the base pointing OUTSIDE must not smuggle the target.
  const link = join(base, 'escape')
  symlinkSync(outside, link)

  const denied = resolveTaskCwd({ taskCwd: link, baseCwd: base })
  assert.equal(denied.ok, false)
  assert.equal(denied.code, 'dag.cwd_denied')
})

test('R7: resolveTaskCwd — requireWorkspaceRegistration=true denies every explicit taskCwd (fail-closed M1)', () => {
  const base = mkdtempSync(join(tmpdir(), 'dag-r7d-'))
  // Even a path that would pass containment: the switch demands a registry
  // channel M1 does not have — refuse rather than silently bypass.
  const denied = resolveTaskCwd({
    taskCwd: base, baseCwd: base, allowedRoots: [], requireWorkspaceRegistration: true,
  })
  assert.equal(denied.ok, false)
  assert.equal(denied.code, 'dag.cwd_denied')
  assert.match(denied.message, /workspace registration/)

  // But an absent taskCwd still falls back to the base (no explicit cwd,
  // no registry demand to satisfy).
  const fallback = resolveTaskCwd({ taskCwd: undefined, baseCwd: base, requireWorkspaceRegistration: true })
  assert.deepEqual(fallback, { ok: true, cwd: base })
})

// ---------------------------------------------------------------------------
// M5 — the kind gate: no real subagent for non-agent kinds. T12 took over
// the approval branch (park semantics); T17 took over merge (queue executor).
// The full merge coverage lives in test/merge-executor.test.js.
// ---------------------------------------------------------------------------

test('M5: approval kind parks blocked(approval_pending) (T12 branch); non-agent kinds never reach a subagent', async (t) => {
  // T17 note: merge specs must declare a worktree-declaring succeeded
  // upstream (spec-validate's merge-source rule), and this harness has NO
  // worktrees engine — the merge executor therefore lands its loud
  // permanent dag.worktrees_unavailable arm. That arm IS this review
  // asset's post-T17 truth: a prompt-less kind still never reaches a real
  // subagent.
  const spec = {
    version: 1, name: 'kinds',
    tasks: [
      { id: 'gate', kind: 'approval', approval: { action: 'approve_integration', prompt: 'go?' } },
      { id: 'src', kind: 'agent', prompt: 'change the code', worktree: { task: 'm5-src' } },
      {
        id: 'integrate', kind: 'merge',
        dependsOn: [
          { taskId: 'src', condition: 'succeeded' },
          { taskId: 'gate', condition: 'completed' },
        ],
        merge: { integrationBranch: 'b' },
      },
    ],
  }
  const h = await makeHarness({ spec })
  t.after(() => h.close())

  const s = await h.engine.tick(h.runId, { maxRounds: 2, settleMs: 0 })
  assert.equal(s.dispatched, 2) // gate parks + src fails loud (no worktrees engine)
  // THE assertion: prompt undefined never reached a real subagent.
  assert.equal(h.subagents.calls.length, 0)

  // T12 replaced the M5 gate for approval: the task parks (not fails).
  const tasks = Object.fromEntries(h.store.findTasks(h.runId).map((x) => [x.task_id, x]))
  assert.equal(tasks.gate.state, 'blocked')
  assert.equal(JSON.parse(tasks.gate.blocked_reason).code, 'approval_pending')
  const attempt = h.store.findAttempts(h.runId, 'gate')[0]
  assert.equal(attempt.state, 'failed')
  assert.equal(JSON.parse(attempt.failure_json).code, 'dag.approval_pending')
  assert.equal(s.waiting_on, 'approval')

  // T17's territory: the merge kind routes to the merge executor the
  // moment readiness reaches it. With NO worktrees engine the executor's
  // fail-loud arm fires — still NO subagent. Approve the gate, tick: gate
  // promotes succeeded; merge (src completed + gate succeeded… but src
  // FAILED on the unavailable seam) is blocked upstream — the kind
  // contract holds either way.
  const [approval] = h.store.findApprovalsByTask(h.runId, 'gate')
  h.store.tx(() => { h.store.decideApproval(approval.approval_id, 'approved', 'go') })
  const s2 = await h.engine.tick(h.runId, { maxRounds: 3, settleMs: 0 })
  const mergeTask = h.store.findTasks(h.runId).find((x) => x.task_id === 'integrate')
  assert.equal(mergeTask.state, 'blocked', 'src failed → merge blocked(upstream_failed), not dispatched')
  assert.equal(JSON.parse(mergeTask.blocked_reason).code, 'upstream_failed')
  assert.equal(h.store.findAttempts(h.runId, 'integrate').length, 0, 'the merge executor never even claimed the task')
  assert.equal(s2.run_state, 'failed', 'dead-blocked graph finalizes failed (B2 semantics)')
  assert.equal(h.subagents.calls.length, 0, 'no subagent for any non-agent kind, through every branch')
  assert.deepEqual(h.store.verifyChain(h.runId), { ok: true })
})
