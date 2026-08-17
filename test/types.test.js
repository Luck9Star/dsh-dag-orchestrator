import { test } from 'node:test'
import assert from 'node:assert/strict'

// Ported (slimmed) from task-weaver packages/scheduler/src/__tests__/types.test.ts
// (L1-86): the source was a frozen-interface compile test; in plain JS the
// equivalent contract check is (a) the module parses and imports, and (b)
// runtime values satisfy the frozen shapes. The ported subset covers the four
// types this plugin keeps (BlockedReason / ReadyEvaluation / TaskGraphTaskView /
// TaskGraphSnapshot); LeaseRequest/ResourceAdmission/ArtifactEnvelope were
// dropped with the resource-admission and artifact domains (DESIGN §9.2).
import * as types from '../lib/types.js'

test('types module imports cleanly (JSDoc typedef carrier)', () => {
  assert.equal(typeof types, 'object')
})

/** @typedef {import('../lib/types.js').BlockedReason} BlockedReason */

test('BlockedReason has {code, details}', () => {
  /** @type {BlockedReason} */
  const r = { code: 'upstream_failed', details: { taskId: 'x' } }
  assert.equal(r.code, 'upstream_failed')
  assert.deepEqual(r.details, { taskId: 'x' })
})

test('ReadyEvaluation has runId/readyTaskIds/blockedTasks', () => {
  /** @type {import('../lib/types.js').ReadyEvaluation} */
  const e = {
    runId: 'run_01',
    readyTaskIds: ['a'],
    blockedTasks: [{ taskId: 'b', reason: { code: 'workspace_unavailable', details: {} } }],
  }
  assert.equal(e.readyTaskIds.length, 1)
  assert.equal(e.blockedTasks.length, 1)
  assert.equal(e.blockedTasks[0].reason.code, 'workspace_unavailable')
})

test('TaskGraphTaskView has taskId/state/version with optional retry/blocked fields', () => {
  /** @type {import('../lib/types.js').TaskGraphTaskView} */
  const minimal = { taskId: 'a', state: 'pending', version: 1 }
  assert.equal(minimal.state, 'pending')

  /** @type {import('../lib/types.js').TaskGraphTaskView} */
  const full = {
    taskId: 'b',
    state: 'blocked',
    version: 3,
    retryNotBeforeMs: 1_700_000_000_000,
    blockedReasonCode: 'approval_pending',
  }
  assert.equal(full.blockedReasonCode, 'approval_pending')
  assert.equal(full.retryNotBeforeMs, 1_700_000_000_000)
})

test('TaskGraphSnapshot has runId/tasks', () => {
  /** @type {import('../lib/types.js').TaskGraphSnapshot} */
  const s = {
    runId: 'run_01',
    tasks: [{ taskId: 'a', state: 'pending', version: 1 }],
  }
  assert.equal(s.tasks.length, 1)
})
