import { test } from 'node:test'
import assert from 'node:assert/strict'

import { evaluateReady, READY_BLOCKED_CODES } from '../lib/ready-evaluator.js'

// Ported from task-weaver packages/scheduler/src/__tests__/ready-evaluator.test.ts
// (L1-592, bun:test → node:test) NARROWED like the module itself: gate /
// sandbox / profile cases dropped with their seams (M3 regression lands with
// T18); park set gains merge_conflicted (T17); artifact refs become task://
// and artifact_missing becomes dag.output_missing (DESIGN §9.2). Migrated
// cases keep the source expectations verbatim — no weakening.

const RUN_ID = 'run_01J'

/**
 * Minimal spec shell — only the fields evaluateReady reads (JSDoc contract;
 * the real T04-validated spec is a superset).
 *
 * @param {ReadonlyArray<{ id: string, kind?: string, dependsOn?: Array<{ taskId: string, condition: 'succeeded' | 'completed' }>, inputs?: string[] }>} tasks
 */
function specShell(tasks) {
  return { version: 1, name: 'test-workflow', tasks: tasks.map((t) => ({ kind: 'agent', ...t })) }
}

/**
 * @param {ReadonlyArray<{ taskId: string, state: string, version?: number, retryNotBeforeMs?: number, blockedReasonCode?: string }>} tasks
 */
function snapshot(tasks) {
  return {
    runId: RUN_ID,
    tasks: tasks.map((t) => ({
      taskId: t.taskId,
      state: t.state,
      version: t.version ?? 1,
      ...(t.retryNotBeforeMs !== undefined ? { retryNotBeforeMs: t.retryNotBeforeMs } : {}),
      ...(t.blockedReasonCode !== undefined ? { blockedReasonCode: t.blockedReasonCode } : {}),
    })),
  }
}

const readyIds = (r) => [...r.readyTaskIds]
const blockedIds = (r) => r.blockedTasks.map((b) => b.taskId)
const blockedCode = (r, taskId) => r.blockedTasks.find((b) => b.taskId === taskId)?.reason.code
const blockedReason = (r, taskId) => r.blockedTasks.find((b) => b.taskId === taskId)?.reason

// --- basic graph a→b→c + independent d (source "basic graph" describe) -----

test('basic graph: returns {b,d} ready when a is succeeded and c still pending', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    { id: 'c', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
    { id: 'd' },
  ])
  const snap = snapshot([
    { taskId: 'a', state: 'succeeded' },
    { taskId: 'b', state: 'pending' },
    { taskId: 'c', state: 'pending' },
    { taskId: 'd', state: 'pending' },
  ])
  const r = evaluateReady(snap, spec)
  assert.deepEqual(readyIds(r).sort(), ['b', 'd'])
  assert.deepEqual(blockedIds(r), [])
})

test('basic graph: returns {a,d} ready when nothing has run yet', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    { id: 'c', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
    { id: 'd' },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'pending' },
    { taskId: 'b', state: 'pending' },
    { taskId: 'c', state: 'pending' },
    { taskId: 'd', state: 'pending' },
  ]), spec)
  assert.deepEqual(readyIds(r).sort(), ['a', 'd'])
})

// --- CORE INVARIANT table (source "CORE INVARIANT" describe) ---------------
// `blocked` is NOT terminal and NOT `completed`: an upstream blocked task
// satisfies NEITHER condition and blocks the edge instead.

test('core invariant table: upstream state × condition → verdict (blocked does NOT count as completed)', () => {
  const cases = [
    // [upstreamState, condition, expected ready, expected blocked code]
    { upstream: 'blocked', condition: 'succeeded', ready: false, code: 'upstream_blocked', note: 'KEY case: blocked ≠ completed' },
    { upstream: 'blocked', condition: 'completed', ready: false, code: 'upstream_blocked', note: 'KEY case: blocked ∉ {succeeded,failed,cancelled}' },
    { upstream: 'failed', condition: 'succeeded', ready: false, code: 'upstream_failed' },
    { upstream: 'failed', condition: 'completed', ready: true },
    { upstream: 'cancelled', condition: 'succeeded', ready: false, code: 'upstream_cancelled' },
    { upstream: 'cancelled', condition: 'completed', ready: true },
    { upstream: 'succeeded', condition: 'succeeded', ready: true },
    { upstream: 'succeeded', condition: 'completed', ready: true },
  ]
  for (const c of cases) {
    const spec = specShell([
      { id: 'a' },
      { id: 'b', dependsOn: [{ taskId: 'a', condition: c.condition }] },
    ])
    const r = evaluateReady(snapshot([
      { taskId: 'a', state: c.upstream },
      { taskId: 'b', state: 'pending' },
    ]), spec)
    if (c.ready) {
      assert.ok(readyIds(r).includes('b'), `${c.upstream}+${c.condition} → b ready (${c.note ?? ''})`)
      assert.ok(!blockedIds(r).includes('b'), `${c.upstream}+${c.condition} → b not blocked`)
    } else {
      assert.ok(!readyIds(r).includes('b'), `${c.upstream}+${c.condition} → b NOT ready (${c.note ?? ''})`)
      assert.equal(blockedCode(r, 'b'), c.code, `${c.upstream}+${c.condition} → ${c.code}`)
    }
  }
})

test('unknown upstream state → blocked upstream_not_succeeded for both conditions', () => {
  for (const condition of ['succeeded', 'completed']) {
    const spec = specShell([
      { id: 'a' },
      { id: 'b', dependsOn: [{ taskId: 'a', condition }] },
    ])
    const r = evaluateReady(snapshot([
      { taskId: 'a', state: 'garbage_state' },
      { taskId: 'b', state: 'pending' },
    ]), spec)
    assert.equal(blockedCode(r, 'b'), 'upstream_not_succeeded')
    assert.ok(!readyIds(r).includes('b'))
  }
})

test('blocked reason details carry taskId/upstreamTaskId/upstreamState/condition', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'completed' }] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'blocked' },
    { taskId: 'b', state: 'pending' },
  ]), spec)
  assert.deepEqual(blockedReason(r, 'b').details, {
    taskId: 'b',
    upstreamTaskId: 'a',
    upstreamState: 'blocked',
    condition: 'completed',
  })
})

// --- waiting: upstream in-progress → neither ready nor blocked -------------

test('waiting table: upstream pending/ready/queued/running/retry_wait → task skipped (not ready, not blocked)', () => {
  for (const upstream of ['pending', 'ready', 'queued', 'running', 'retry_wait']) {
    for (const condition of ['succeeded', 'completed']) {
      const spec = specShell([
        { id: 'a' },
        { id: 'b', dependsOn: [{ taskId: 'a', condition }] },
      ])
      const r = evaluateReady(snapshot([
        { taskId: 'a', state: upstream },
        { taskId: 'b', state: 'pending' },
      ]), spec)
      assert.ok(!readyIds(r).includes('b'), `${upstream}+${condition}: b not ready`)
      assert.ok(!blockedIds(r).includes('b'), `${upstream}+${condition}: b not blocked`)
    }
  }
})

// --- transitive blocking (single pass reflects committed snapshot) ----------

test('transitive: first tick — a blocked re-eval→ready; b blocked on a; c stays pending (waiting)', () => {
  // Source case verbatim: the snapshot still has a=blocked; re-eval promotes
  // a (no deps) to ready. b sees upstream a as blocked in the snapshot →
  // blocked. c sees upstream b pending → waiting → skipped (single pass
  // reflects the committed snapshot, not in-pass mutations).
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    { id: 'c', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'blocked' },
    { taskId: 'b', state: 'pending' },
    { taskId: 'c', state: 'pending' },
  ]), spec)
  assert.deepEqual(readyIds(r), ['a'])
  assert.deepEqual(blockedIds(r), ['b'])
})

test('transitive: second tick — after b persisted blocked → c now blocked (blocked tasks re-evaluated)', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    { id: 'c', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'blocked' },
    { taskId: 'b', state: 'blocked' },
    { taskId: 'c', state: 'pending' },
  ]), spec)
  // a has no deps → re-eval of blocked promotes it to ready when eligible.
  // b stays blocked (upstream a still blocked in the snapshot).
  // c is pending and blocked by b.
  assert.deepEqual(readyIds(r), ['a'])
  assert.ok(blockedIds(r).includes('b'))
  assert.ok(blockedIds(r).includes('c'))
})

test('blocked(upstream_blocked) task re-evaluated after upstream succeeded → ready', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'succeeded' },
    { taskId: 'b', state: 'blocked', blockedReasonCode: 'upstream_blocked' },
  ]), spec)
  assert.deepEqual(readyIds(r), ['b'])
  assert.deepEqual(blockedIds(r), [])
})

// --- parks: excluded from re-evaluation entirely ----------------------------

test('park table: approval_pending / merge_conflicted blocked tasks are absent from the evaluation output entirely', () => {
  for (const code of ['approval_pending', 'merge_conflicted']) {
    const spec = specShell([
      { id: 'a' },
      { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ])
    const r = evaluateReady(snapshot([
      { taskId: 'a', state: 'succeeded' },
      { taskId: 'b', state: 'blocked', blockedReasonCode: code },
    ]), spec)
    assert.ok(!readyIds(r).includes('b'), `${code}: not promoted to ready`)
    assert.ok(!blockedIds(r).includes('b'), `${code}: not re-blocked (completely absent)`)
  }
})

test('park rationale: re-promoting an approval park would re-park in a tight loop — evaluator never sees it', () => {
  // Even a fully-unblocked context cannot re-promote an approval park;
  // promotion is dag_approve + reconcileApprovals' job (T12), retry_task for
  // merge_conflicted (T17).
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'blocked', blockedReasonCode: 'approval_pending' },
  ]), spec)
  assert.deepEqual(readyIds(r), [])
  assert.deepEqual(blockedIds(r), [])
})

test('other blockedReasonCode (upstream_failed) is still re-evaluated', () => {
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'blocked', blockedReasonCode: 'upstream_failed' },
  ]), spec)
  assert.deepEqual(readyIds(r), ['a'])
})

// --- retry_wait backoff (source "retry_wait backoff" describe) --------------

test('retry_wait before backoff expiry is NOT ready and NOT blocked', () => {
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'retry_wait', retryNotBeforeMs: 5_000 },
  ]), spec, { now: 1_000 })
  assert.ok(!readyIds(r).includes('a'))
  assert.ok(!blockedIds(r).includes('a'))
})

test('retry_wait at backoff expiry + no deps → ready', () => {
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'retry_wait', retryNotBeforeMs: 5_000 },
  ]), spec, { now: 5_000 })
  assert.ok(readyIds(r).includes('a'))
})

test('retry_wait expired + control deps satisfied → ready', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'succeeded' },
    { taskId: 'b', state: 'retry_wait', retryNotBeforeMs: 8_000 },
  ]), spec, { now: 10_000 })
  assert.ok(readyIds(r).includes('b'))
})

test('retry_wait without retryNotBeforeMs is fail-closed (not ready)', () => {
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'retry_wait' },
  ]), spec, { now: 99_000 })
  assert.ok(!readyIds(r).includes('a'))
})

// --- inputs / outputResolver (source "artifact inputs" describe, adapted) ---

test('inputs: resolver returns true for every input → ready', () => {
  const spec = specShell([{ id: 'x', inputs: ['task://up/output'] }])
  const r = evaluateReady(snapshot([{ taskId: 'x', state: 'pending' }]), spec, { outputResolver: () => true })
  assert.ok(readyIds(r).includes('x'))
  assert.ok(!blockedIds(r).includes('x'))
})

test('inputs: resolver returns false → blocked dag.output_missing with missingInputs in details', () => {
  const spec = specShell([{ id: 'x', inputs: ['task://up/output'] }])
  const r = evaluateReady(snapshot([{ taskId: 'x', state: 'pending' }]), spec, { outputResolver: () => false })
  assert.ok(!readyIds(r).includes('x'))
  assert.equal(blockedCode(r, 'x'), 'dag.output_missing')
  assert.deepEqual(blockedReason(r, 'x').details.missingInputs, ['task://up/output'])
})

test('inputs: only the missing subset lands in missingInputs', () => {
  const spec = specShell([{ id: 'x', inputs: ['task://up/a', 'task://up/b', 'task://up/c'] }])
  const present = new Set(['task://up/a', 'task://up/c'])
  const r = evaluateReady(snapshot([{ taskId: 'x', state: 'pending' }]), spec, {
    outputResolver: (ref) => present.has(ref),
  })
  assert.equal(blockedCode(r, 'x'), 'dag.output_missing')
  assert.deepEqual(blockedReason(r, 'x').details.missingInputs, ['task://up/b'])
})

test('inputs fail-closed: resolver absent + declared inputs → blocked dag.output_missing', () => {
  const spec = specShell([{ id: 'x', inputs: ['task://up/output'] }])
  const r = evaluateReady(snapshot([{ taskId: 'x', state: 'pending' }]), spec)
  assert.ok(!readyIds(r).includes('x'))
  assert.equal(blockedCode(r, 'x'), 'dag.output_missing')
  assert.equal(blockedReason(r, 'x').details.reason, 'no output resolver wired')
  assert.deepEqual(blockedReason(r, 'x').details.inputs, ['task://up/output'])
})

test('inputs: no inputs + resolver absent → unaffected (ready)', () => {
  const spec = specShell([{ id: 'x' }])
  const r = evaluateReady(snapshot([{ taskId: 'x', state: 'pending' }]), spec)
  assert.ok(readyIds(r).includes('x'))
})

// --- multi-dependency combination -------------------------------------------

test('multi-dep: one satisfied + one waiting → waiting (skipped, not blocked)', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b' },
    { id: 'c', dependsOn: [
      { taskId: 'a', condition: 'succeeded' },
      { taskId: 'b', condition: 'succeeded' },
    ] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'succeeded' },
    { taskId: 'b', state: 'running' },
    { taskId: 'c', state: 'pending' },
  ]), spec)
  assert.ok(!readyIds(r).includes('c'))
  assert.ok(!blockedIds(r).includes('c'))
})

test('multi-dep: one satisfied + one blocked → blocked', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b' },
    { id: 'c', dependsOn: [
      { taskId: 'a', condition: 'succeeded' },
      { taskId: 'b', condition: 'succeeded' },
    ] },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'a', state: 'succeeded' },
    { taskId: 'b', state: 'blocked' },
    { taskId: 'c', state: 'pending' },
  ]), spec)
  assert.ok(!readyIds(r).includes('c'))
  assert.equal(blockedCode(r, 'c'), 'upstream_blocked')
})

test('multi-dep: blocked dominates waiting regardless of edge order', () => {
  // The dependency loop breaks on the first blocked verdict; a waiting edge
  // seen earlier must not soften it (source L368-374 semantics).
  for (const order of ['waiting-first', 'blocked-first']) {
    const deps = order === 'waiting-first'
      ? [{ taskId: 'a', condition: 'succeeded' }, { taskId: 'b', condition: 'succeeded' }]
      : [{ taskId: 'b', condition: 'succeeded' }, { taskId: 'a', condition: 'succeeded' }]
    const spec = specShell([
      { id: 'a' },
      { id: 'b' },
      { id: 'c', dependsOn: deps },
    ])
    const r = evaluateReady(snapshot([
      { taskId: 'a', state: 'running' },
      { taskId: 'b', state: 'failed' },
      { taskId: 'c', state: 'pending' },
    ]), spec)
    assert.equal(blockedCode(r, 'c'), 'upstream_failed', order)
  }
})

// --- upstream missing from snapshot (source L360-366) -----------------------

test('upstream missing from snapshot → blocked upstream_not_succeeded with explicit reason', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'b', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
  ])
  const r = evaluateReady(snapshot([{ taskId: 'b', state: 'pending' }]), spec)
  assert.equal(blockedCode(r, 'b'), 'upstream_not_succeeded')
  assert.equal(blockedReason(r, 'b').details.reason, 'upstream task missing from snapshot')
  assert.equal(blockedReason(r, 'b').details.upstreamTaskId, 'a')
})

test('spec task missing from snapshot is skipped entirely', () => {
  const spec = specShell([
    { id: 'a' },
    { id: 'ghost' },
  ])
  const r = evaluateReady(snapshot([{ taskId: 'a', state: 'pending' }]), spec)
  assert.deepEqual(readyIds(r), ['a'])
  assert.deepEqual(blockedIds(r), [])
})

// --- mixed-state snapshot: which states enter the evaluation ----------------

test('mixed snapshot: pending + due retry_wait + re-evaluable blocked all evaluated; succeeded/running/queued skipped', () => {
  const spec = specShell([
    { id: 'pend', dependsOn: [{ taskId: 'done', condition: 'succeeded' }] },
    { id: 'due', dependsOn: [{ taskId: 'done', condition: 'succeeded' }] },
    { id: 'blk', dependsOn: [{ taskId: 'done', condition: 'succeeded' }] },
    { id: 'done' },
    { id: 'gone' },
  ])
  const r = evaluateReady(snapshot([
    { taskId: 'pend', state: 'pending' },
    { taskId: 'due', state: 'retry_wait', retryNotBeforeMs: 1_000 },
    { taskId: 'blk', state: 'blocked', blockedReasonCode: 'upstream_blocked' },
    { taskId: 'done', state: 'succeeded' },
    { taskId: 'gone', state: 'running' },
  ]), spec, { now: 2_000 })
  // pending / due retry_wait / re-evaluable blocked → all ready (upstream done).
  assert.deepEqual(readyIds(r).sort(), ['blk', 'due', 'pend'])
  // succeeded / running → not re-evaluated (absent from output).
  assert.ok(!readyIds(r).includes('done'))
  assert.ok(!blockedIds(r).includes('done'))
  assert.ok(!readyIds(r).includes('gone'))
  assert.ok(!blockedIds(r).includes('gone'))
})

// --- kind-blindness (classifyM1Scope cut — narrowing manifest #4) -----------

test('kind-blind: approval / merge / agent kinds all evaluate identically (no scope gate here)', () => {
  for (const kind of ['agent', 'approval', 'merge']) {
    const spec = specShell([{ id: 'a', kind }])
    const r = evaluateReady(snapshot([{ taskId: 'a', state: 'pending' }]), spec)
    assert.deepEqual(readyIds(r), ['a'], `kind=${kind}`)
    assert.deepEqual(blockedIds(r), [], `kind=${kind}`)
  }
})

// --- result shape ------------------------------------------------------------

test('echoes runId', () => {
  const spec = specShell([{ id: 'a' }])
  const r = evaluateReady(snapshot([{ taskId: 'a', state: 'pending' }]), spec)
  assert.equal(r.runId, RUN_ID)
})

test('READY_BLOCKED_CODES exports the narrowed code vocabulary', () => {
  assert.deepEqual(READY_BLOCKED_CODES, {
    upstreamBlocked: 'upstream_blocked',
    upstreamFailed: 'upstream_failed',
    upstreamCancelled: 'upstream_cancelled',
    upstreamNotSucceeded: 'upstream_not_succeeded',
    outputMissing: 'dag.output_missing',
    dependencyGateNotMet: 'dependency_gate_not_met',
    dependencyGateEvaluatorUnavailable: 'dependency_gate_evaluator_unavailable',
  })
})
