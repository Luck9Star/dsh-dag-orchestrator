import { test } from 'node:test'
import assert from 'node:assert/strict'

import { criticalPathDepth } from '../lib/critical-path.js'

// Ported from task-weaver packages/scheduler/src/critical-path.ts —
// table-driven depth semantics. Spec shape is this plugin's (pre/post-zod):
// { tasks: [{ id, dependsOn: [{ taskId, condition }] }] } — see DESIGN §7.2.
//
// Edge direction: `t dependsOn X` makes t a DOWNSTREAM of X. Depth therefore
// flows A→B means "B dependsOn A": A=2 in the diamond, D (the leaf) = 0.

const dep = (taskId, condition = 'succeeded') => ({ taskId, condition })

test('critical-path: single task has depth 0', () => {
  const spec = { tasks: [{ id: 'only' }] }
  assert.equal(criticalPathDepth('only', spec), 0)
})

test('critical-path: diamond A→B,C→D gives A=2, B=C=1, D=0', () => {
  // A is the root (no dependsOn); B and C dependOn A; D dependsOn B and C.
  const spec = {
    tasks: [
      { id: 'a' },
      { id: 'b', dependsOn: [dep('a')] },
      { id: 'c', dependsOn: [dep('a')] },
      { id: 'd', dependsOn: [dep('b'), dep('c')] },
    ],
  }
  assert.equal(criticalPathDepth('a', spec), 2)
  assert.equal(criticalPathDepth('b', spec), 1)
  assert.equal(criticalPathDepth('c', spec), 1)
  assert.equal(criticalPathDepth('d', spec), 0)
})

test('critical-path: chain depth equals distance to deepest downstream leaf', () => {
  // t1 → t2 → t3 → t4 → t5 (each task dependsOn its predecessor).
  const spec = {
    tasks: [
      { id: 't1' },
      { id: 't2', dependsOn: [dep('t1')] },
      { id: 't3', dependsOn: [dep('t2')] },
      { id: 't4', dependsOn: [dep('t3')] },
      { id: 't5', dependsOn: [dep('t4')] },
    ],
  }
  assert.equal(criticalPathDepth('t1', spec), 4)
  assert.equal(criticalPathDepth('t3', spec), 2)
  assert.equal(criticalPathDepth('t5', spec), 0)
})

test('critical-path: task with neither dependsOn nor dependents has depth 0 (missing key tolerated)', () => {
  // Tasks in this plugin may omit `dependsOn` entirely (zod fills the default);
  // the calculator treats a missing key the same as an empty list. Depth runs
  // from execution source to final consumer: `y dependsOn x` ⇒ x=1, y=0, and
  // an isolated task (no key, no dependents) is a standalone leaf at 0.
  const spec = {
    tasks: [
      { id: 'solo' }, // no dependsOn key, nothing depends on it → 0
      { id: 'x' }, // no dependsOn key, but y depends on it → 1
      { id: 'y', dependsOn: [dep('x')] },
    ],
  }
  assert.equal(criticalPathDepth('solo', spec), 0)
  assert.equal(criticalPathDepth('x', spec), 1)
  assert.equal(criticalPathDepth('y', spec), 0)
})

test('critical-path: longest branch wins when chain lengths differ', () => {
  // root → { short: leaf1 (depth 1), long: mid → leaf2 (depth 2) } ⇒ root = 3
  const spec = {
    tasks: [
      { id: 'root' },
      { id: 'short', dependsOn: [dep('root')] },
      { id: 'long', dependsOn: [dep('root')] },
      { id: 'mid', dependsOn: [dep('long')] },
      { id: 'leaf1', dependsOn: [dep('short')] },
      { id: 'leaf2', dependsOn: [dep('mid')] },
    ],
  }
  assert.equal(criticalPathDepth('root', spec), 3)
  assert.equal(criticalPathDepth('short', spec), 1)
  assert.equal(criticalPathDepth('long', spec), 2)
})

test('critical-path: unknown taskId is a leaf (depth 0), matching source behavior', () => {
  // Source DFS returns 0 for ids with no downstream edges — an id absent from
  // the spec entirely behaves the same. Spec acyclicity/referential integrity
  // is spec-validate's job (precondition in lib/critical-path.js JSDoc).
  const spec = { tasks: [{ id: 'a' }, { id: 'b', dependsOn: [dep('a')] }] }
  assert.equal(criticalPathDepth('nonexistent', spec), 0)
})
