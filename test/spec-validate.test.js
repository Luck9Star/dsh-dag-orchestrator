import { test } from 'node:test'
import assert from 'node:assert/strict'

import { validateSpec, specHash, DAG_SPEC_ERROR_CODES } from '../lib/spec-validate.js'

// T04 — spec-validate (DESIGN §7.1/§7.2). Every negative case asserts the
// stable `code` AND the JSON path (dot/bracket, no leading $) per the
// workflow-spec.md L326-341 error shape.

const CODE = DAG_SPEC_ERROR_CODES

// ---------------------------------------------------------------------------
// §7.2 example spec (DESIGN L451-480) — analyze/impl-core/impl-docs/gate/
// integrate, five tasks. Prompt bodies abbreviated (the DESIGN elides them
// with "…"; agent-requires-prompt has its own dedicated negative test).
// ---------------------------------------------------------------------------

const EXAMPLE_SPEC = () => ({
  version: 1,
  name: 'refactor-auth',
  description: 'Parallel refactor with review gate and integration',
  project: { root: '/abs/repo', baseRef: 'HEAD' },
  limits: { maxRunningAgents: 3, queueCapacity: 16 },
  tasks: [
    {
      id: 'analyze', kind: 'agent', prompt: 'Analyze the auth module.',
      outputs: [{
        name: 'analysis',
        schema: {
          type: 'object', additionalProperties: false,
          properties: {
            summary: { type: 'string' },
            riskFiles: { type: 'array', items: { type: 'string' } },
          },
          required: ['summary'],
        },
      }],
    },
    {
      id: 'impl-core', kind: 'agent', prompt: 'Implement the core changes.',
      dependsOn: [{ taskId: 'analyze', condition: 'succeeded' }],
      inputs: ['task://analyze/analysis'],
      model: 'kimi-code/k3', persona: 'refactor-specialist', delegation: false,
      // T17: the merge node below integrates impl-core's worktree — the
      // example therefore declares one (the merge-source rule).
      worktree: { task: 'core-refactor', baseRef: 'main' },
      retry: { maxAttempts: 3, backoffMs: 5000, maxBackoffMs: 60000, retryOn: ['transient_network'] },
      timeoutMs: 1_800_000,
    },
    {
      id: 'impl-docs', kind: 'agent', prompt: 'Update the docs.',
      dependsOn: [{ taskId: 'analyze', condition: 'succeeded' }],
    },
    {
      id: 'gate', kind: 'approval',
      dependsOn: [{ taskId: 'impl-core', condition: 'succeeded' }],
      approval: { action: 'approve_integration', prompt: '两个实现分支已就绪，是否继续集成？' },
    },
    {
      id: 'integrate', kind: 'merge',
      dependsOn: [
        { taskId: 'impl-core', condition: 'succeeded' },
        { taskId: 'gate', condition: 'succeeded' },
      ],
      merge: { integrationBranch: 'dsh-wt/integration/<auto>' },
    },
  ],
})

/** Minimal single-agent spec (negative-case base). */
const ONE_TASK = (task = {}) => ({
  version: 1,
  name: 'solo',
  tasks: [{ id: 'only', kind: 'agent', prompt: 'do it', ...task }],
})

/** Assert the FIRST error with the given code matches {code, path}. */
const expectError = (result, code, path) => {
  assert.equal(result.ok, false, `expected ok:false, got ${JSON.stringify(result)}`)
  assert.ok(Array.isArray(result.errors) && result.errors.length > 0, 'errors array non-empty')
  const hit = result.errors.find((e) => e.code === code)
  assert.ok(hit !== undefined, `no error with code '${code}'; got ${JSON.stringify(result.errors)}`)
  if (path !== undefined) {
    assert.ok(
      result.errors.some((e) => e.code === code && e.path === path),
      `no '${code}' error at path '${path}'; got ${JSON.stringify(result.errors)}`,
    )
  }
  // Shape contract: every error carries {code, path?, message}.
  for (const e of result.errors) {
    assert.equal(typeof e.code, 'string')
    assert.equal(typeof e.message, 'string')
  }
  return hit
}

// ---------------------------------------------------------------------------
// §7.2 example — positive
// ---------------------------------------------------------------------------

test('example: DESIGN §7.2 five-task spec validates', () => {
  const r = validateSpec(EXAMPLE_SPEC())
  assert.equal(r.ok, true, JSON.stringify(r.errors))
  assert.equal(r.value.tasks.length, 5)
  const ids = r.value.tasks.map((t) => t.id)
  assert.deepEqual(ids, ['analyze', 'impl-core', 'impl-docs', 'gate', 'integrate'])
})

test('example: normalized value fills per-task defaults, keeps explicit ones', () => {
  const r = validateSpec(EXAMPLE_SPEC())
  assert.equal(r.ok, true)
  const [analyze, implCore, , gate] = r.value.tasks
  // defaults filled
  assert.deepEqual(analyze.dependsOn, [])
  assert.deepEqual(analyze.inputs, [])
  assert.deepEqual(implCore.outputs, [])
  assert.equal(analyze.timeoutMs, 1_800_000)
  assert.equal(analyze.priority, 0)
  assert.equal(analyze.failurePolicy, 'block_downstream')
  // explicit values kept
  assert.equal(implCore.timeoutMs, 1_800_000)
  assert.deepEqual(implCore.retry.retryOn, ['transient_network'])
  assert.equal(implCore.retry.maxAttempts, 3)
  // retry inner defaults fill when the block is present but partial
  const partial = validateSpec(ONE_TASK({ retry: { maxAttempts: 2 } }))
  assert.equal(partial.ok, true)
  assert.deepEqual(partial.value.tasks[0].retry, {
    maxAttempts: 2, backoffMs: 1000, maxBackoffMs: 60000, jitterRatio: 0.25, retryOn: [],
  })
  // approval task keeps its block verbatim
  assert.equal(gate.approval.action, 'approve_integration')
})

test('example: outputs schema passes through untouched (passthrough beyond type=object)', () => {
  const r = validateSpec(EXAMPLE_SPEC())
  assert.equal(r.ok, true)
  assert.deepEqual(r.value.tasks[0].outputs[0].schema.properties.riskFiles, {
    type: 'array', items: { type: 'string' },
  })
})

test('example: value is detached from the input object (deep clone)', () => {
  const spec = EXAMPLE_SPEC()
  const r = validateSpec(spec)
  assert.equal(r.ok, true)
  spec.tasks[0].outputs[0].schema.properties.summary = { type: 'number' }
  spec.tasks[1].retry.maxAttempts = 99
  assert.deepEqual(r.value.tasks[0].outputs[0].schema.properties.summary, { type: 'string' })
  assert.equal(r.value.tasks[1].retry.maxAttempts, 3)
})

// ---------------------------------------------------------------------------
// Root-level negatives
// ---------------------------------------------------------------------------

test('unknown root key → dag.unknown_field', () => {
  const r = validateSpec({ ...ONE_TASK(), labels: { env: 'prod' } })
  expectError(r, CODE.unknownField, 'labels')
})

test('unknown root key #2 (budget domain not migrated) → dag.unknown_field', () => {
  const r = validateSpec({ ...ONE_TASK(), budget: { maxTotalAttempts: 5 } })
  expectError(r, CODE.unknownField, 'budget')
})

test('version 2 → dag.unsupported_version (precheck before literal mismatch)', () => {
  const r = validateSpec({ ...ONE_TASK(), version: 2 })
  const hit = expectError(r, CODE.unsupportedVersion, 'version')
  assert.match(hit.message, /version 1 only/)
})

test('non-object spec → dag.schema_invalid, never throws', () => {
  for (const bad of [null, undefined, 42, 'text', [], true]) {
    const r = validateSpec(bad)
    assert.equal(r.ok, false, `expected rejection for ${JSON.stringify(bad)}`)
    assert.equal(r.errors[0].code, CODE.schemaInvalid)
  }
})

test('name longer than 80 chars → dag.schema_invalid at tasks-level path "name"', () => {
  const r = validateSpec(ONE_TASK({ id: 'ok' }))
  assert.equal(r.ok, true)
  const long = validateSpec({ ...ONE_TASK(), name: 'x'.repeat(81) })
  expectError(long, CODE.schemaInvalid, 'name')
  const edge = validateSpec({ ...ONE_TASK(), name: 'x'.repeat(80) })
  assert.equal(edge.ok, true)
})

test('description longer than 2000 chars → dag.schema_invalid', () => {
  const r = validateSpec({ ...ONE_TASK(), description: 'y'.repeat(2001) })
  expectError(r, CODE.schemaInvalid, 'description')
})

test('project.root relative → dag.schema_invalid (absolute path required)', () => {
  const r = validateSpec({ ...ONE_TASK(), project: { root: 'relative/path' } })
  expectError(r, CODE.schemaInvalid, 'project.root')
})

test('limits.maxRunningAgents=33 → dag.schema_invalid', () => {
  const r = validateSpec({ ...ONE_TASK(), limits: { maxRunningAgents: 33, queueCapacity: 16 } })
  expectError(r, CODE.schemaInvalid, 'limits.maxRunningAgents')
})

test('limits.queueCapacity=1025 → dag.schema_invalid', () => {
  const r = validateSpec({ ...ONE_TASK(), limits: { maxRunningAgents: 4, queueCapacity: 1025 } })
  expectError(r, CODE.schemaInvalid, 'limits.queueCapacity')
})

test('limits boundary values 32/1024 pass', () => {
  const r = validateSpec({ ...ONE_TASK(), limits: { maxRunningAgents: 32, queueCapacity: 1024 } })
  assert.equal(r.ok, true)
})

test('unknown key inside limits → dag.unknown_field', () => {
  const r = validateSpec({ ...ONE_TASK(), limits: { maxRunDurationMs: 5000 } })
  expectError(r, CODE.unknownField, 'limits.maxRunDurationMs')
})

test('empty tasks array → dag.schema_invalid', () => {
  const r = validateSpec({ version: 1, name: 'empty', tasks: [] })
  expectError(r, CODE.schemaInvalid, 'tasks')
})

test('257 tasks → dag.schema_invalid (max 256)', () => {
  const tasks = Array.from({ length: 257 }, (_, i) => ({ id: `t-${i}`, kind: 'agent', prompt: 'p' }))
  const r = validateSpec({ version: 1, name: 'too-many', tasks })
  expectError(r, CODE.schemaInvalid, 'tasks')
})

// ---------------------------------------------------------------------------
// Task-level shape negatives
// ---------------------------------------------------------------------------

test('unknown task key → dag.unknown_field with task path', () => {
  const r = validateSpec(ONE_TASK({ substrate: 'local-process' }))
  expectError(r, CODE.unknownField, 'tasks[0].substrate')
})

test('unknown nested key in dependsOn → dag.unknown_field (gate itself reopened at T18)', () => {
  const spec = {
    version: 1, name: 'dep-unknown',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      {
        id: 'b', kind: 'agent', prompt: 'p',
        dependsOn: [{ taskId: 'a', condition: 'succeeded', when: 'always' }],
      },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.unknownField, 'tasks[1].dependsOn[0].when')
})

test('id format violations → dag.schema_invalid at tasks[i].id', () => {
  for (const badId of ['Bad-upper', '1starts-digit', 'has_underscore', 'has.dot', '-leading', '']) {
    const r = validateSpec(ONE_TASK({ id: badId }))
    expectError(r, CODE.schemaInvalid, 'tasks[0].id')
  }
  // single lowercase char is the valid minimal id (regex: 1 char + 0..62 more)
  assert.equal(validateSpec(ONE_TASK({ id: 'a' })).ok, true)
})

test('id longer than 63 chars → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ id: `a${'b'.repeat(63)}` }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].id')
})

test('kind outside the enum → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ kind: 'verify' }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].kind')
})

test('dependsOn condition outside enum → dag.schema_invalid with element path', () => {
  const spec = {
    version: 1, name: 'cond',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'failed' }] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.schemaInvalid, 'tasks[1].dependsOn[0].condition')
})

test('input not a task:// URI → dag.schema_invalid', () => {
  const spec = {
    version: 1, name: 'uri',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'b', kind: 'agent', prompt: 'p', inputs: ['artifact://a/thing'] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.schemaInvalid, 'tasks[1].inputs[0]')
})

test('outputs schema not object-rooted → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({
    outputs: [{ name: 'o', schema: { type: 'string' } }],
  }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].outputs[0].schema.type')
})

test('outputs unknown key → dag.unknown_field', () => {
  const r = validateSpec(ONE_TASK({
    outputs: [{ name: 'o', schema: { type: 'object' }, mediaType: 'application/json' }],
  }))
  expectError(r, CODE.unknownField, 'tasks[0].outputs[0].mediaType')
})

test('cwd relative → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ cwd: 'relative/dir' }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].cwd')
})

test('maxDepth negative → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ maxDepth: -1 }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].maxDepth')
})

test('maxTokens non-positive → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ maxTokens: 0 }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].maxTokens')
})

test('timeoutMs non-positive → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ timeoutMs: 0 }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].timeoutMs')
})

test('priority 101 → dag.schema_invalid; -100..100 passes', () => {
  expectError(validateSpec(ONE_TASK({ priority: 101 })), CODE.schemaInvalid, 'tasks[0].priority')
  assert.equal(validateSpec(ONE_TASK({ priority: -100 })).ok, true)
  assert.equal(validateSpec(ONE_TASK({ priority: 100 })).ok, true)
})

test('failurePolicy outside enum → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ failurePolicy: 'propagate' }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].failurePolicy')
})

test('retryOn outside the plugin failure-type domain → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ retry: { retryOn: ['adapter_unhealthy'] } }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].retry.retryOn[0]')
})

test('M8: retryOn ["timeout"] is rejected as a dead key, message points at transient_network', () => {
  // failureTypeToPolicyKey maps timeout-class failures to the
  // 'transient_network' policy key — a 'timeout' retryOn entry could never
  // match (M1 review M8). The narrowed enum rejects it WITH guidance.
  const r = validateSpec(ONE_TASK({ retry: { retryOn: ['timeout'] } }))
  const hit = expectError(r, CODE.schemaInvalid, 'tasks[0].retry.retryOn[0]')
  assert.match(hit.message, /transient_network/, 'the rejection must say where timeout-class failures retry')
  assert.match(hit.message, /timeout/, 'the rejection must name the mapping')
})

test('M8: the surviving retryOn domain accepts exactly the three policy keys', () => {
  assert.equal(validateSpec(ONE_TASK({ retry: { retryOn: ['transient_network'] } })).ok, true)
  assert.equal(validateSpec(ONE_TASK({ retry: { retryOn: ['permanent', 'internal'] } })).ok, true)
  assert.equal(validateSpec(ONE_TASK({ retry: { retryOn: ['transient'] } })).ok, false)
  assert.equal(validateSpec(ONE_TASK({ retry: { retryOn: ['aborted'] } })).ok, false)
})

test('verify block missing expectStatus → dag.schema_invalid (M3 shape validated now)', () => {
  const r = validateSpec(ONE_TASK({ verify: { expectOutput: 'report' } }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].verify.expectStatus')
})

test('worktree block missing task → dag.schema_invalid', () => {
  const r = validateSpec(ONE_TASK({ worktree: { baseRef: 'HEAD' } }))
  expectError(r, CODE.schemaInvalid, 'tasks[0].worktree.task')
})

test('toolFilter unknown key → dag.unknown_field', () => {
  const r = validateSpec(ONE_TASK({ toolFilter: { allowlist: ['a'] } }))
  expectError(r, CODE.unknownField, 'tasks[0].toolFilter.allowlist')
})

// ---------------------------------------------------------------------------
// Structural (bridge gate)
// ---------------------------------------------------------------------------

test('permission_mode present → dag.bridge_unsupported', () => {
  const r = validateSpec(ONE_TASK({ permission_mode: 'full' }))
  const hit = expectError(r, CODE.bridgeUnsupported, 'tasks[0].permission_mode')
  assert.match(hit.message, /native/)
})

test('reasoning_effort present → dag.bridge_unsupported', () => {
  const r = validateSpec(ONE_TASK({ reasoning_effort: 'high' }))
  expectError(r, CODE.bridgeUnsupported, 'tasks[0].reasoning_effort')
})

test(`backend 'codex' → dag.bridge_unsupported (unknown bridge value)`, () => {
  const r = validateSpec(ONE_TASK({ backend: 'codex' }))
  const hit = expectError(r, CODE.bridgeUnsupported, 'tasks[0].backend')
  assert.match(hit.message, /native/)
  // P2 audit: the rejection names the native=spawn alias so a 'native'
  // spec author is never told the value is simply wrong.
  assert.match(hit.message, /'native' is an alias of 'spawn'/)
})

test('backend native|spawn|fork all pass', () => {
  for (const backend of ['native', 'spawn', 'fork']) {
    const r = validateSpec(ONE_TASK({ backend }))
    assert.equal(r.ok, true, `${backend}: ${JSON.stringify(r.errors)}`)
  }
})

test('P2: backend native and spawn hash DIFFERENTLY (spec value kept verbatim; the alias maps at dispatch only)', () => {
  // The alias mapping lives in the executor (native → the spawn provider
  // name); the spec value is NOT normalized — a stored spec re-hashes
  // identically across restarts (specHash stability).
  const a = specHash(validateSpec(ONE_TASK({ backend: 'native' })).value)
  const b = specHash(validateSpec(ONE_TASK({ backend: 'spawn' })).value)
  assert.notEqual(a, b)
})

// ---------------------------------------------------------------------------
// Structural (cross-task semantics)
// ---------------------------------------------------------------------------

test('duplicate task id → dag.duplicate_task_id at the SECOND occurrence', () => {
  const spec = {
    version: 1, name: 'dup',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'b', kind: 'agent', prompt: 'p' },
      { id: 'a', kind: 'agent', prompt: 'p' },
    ],
  }
  const r = validateSpec(spec)
  const hit = expectError(r, CODE.duplicateTaskId, 'tasks[2].id')
  assert.match(hit.message, /first declared at tasks\[0\]/)
})

test('cycle A→B→C→A → dag.cycle_detected with the cycle path in the message', () => {
  const spec = {
    version: 1, name: 'cycle',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'c', condition: 'succeeded' }] },
      { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
      { id: 'c', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  const hit = expectError(r, CODE.cycleDetected)
  assert.match(hit.message, /a → b → c → a|b → c → a → b|c → a → b → c/)
})

test('dangling dependency → dag.unknown_dependency', () => {
  const spec = {
    version: 1, name: 'dangling',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'ghost', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.unknownDependency, 'tasks[1].dependsOn[0].taskId')
})

test('self dependency → dag.self_dependency (not unknown_dependency)', () => {
  const spec = {
    version: 1, name: 'self',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  const hit = expectError(r, CODE.selfDependency, 'tasks[0].dependsOn[0].taskId')
  assert.match(hit.message, /itself/)
  // …and self_dependency does NOT also surface as unknown_dependency.
  assert.equal(r.errors.some((e) => e.code === CODE.unknownDependency), false)
})

test('input from a NON-upstream producer → dag.input_not_reachable', () => {
  const spec = {
    version: 1, name: 'sibling',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', outputs: [{ name: 'out', schema: { type: 'object' } }] },
      { id: 'b', kind: 'agent', prompt: 'p' },
      { id: 'c', kind: 'agent', prompt: 'p', inputs: ['task://a/out'] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.inputNotReachable, 'tasks[2].inputs[0]')
})

test('input from an unknown producer → dag.input_not_reachable', () => {
  const spec = {
    version: 1, name: 'ghost-producer',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', inputs: ['task://ghost/out'] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.inputNotReachable, 'tasks[0].inputs[0]')
})

test('input from a TRANSITIVE upstream (A→B→C) passes', () => {
  const spec = {
    version: 1, name: 'transitive',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', outputs: [{ name: 'base', schema: { type: 'object' } }] },
      { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
      { id: 'c', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'b', condition: 'succeeded' }], inputs: ['task://a/base'] },
    ],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, true, JSON.stringify(r.errors))
})

test('producer is upstream but lacks the named output → dag.output_not_declared', () => {
  const spec = {
    version: 1, name: 'no-output',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'b', kind: 'agent', prompt: 'p', dependsOn: [{ taskId: 'a', condition: 'succeeded' }], inputs: ['task://a/missing'] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.outputNotDeclared, 'tasks[1].inputs[0]')
})

// ---------------------------------------------------------------------------
// Kind field matrix
// ---------------------------------------------------------------------------

test('agent without prompt → dag.prompt_required', () => {
  const spec = { version: 1, name: 'no-prompt', tasks: [{ id: 'a', kind: 'agent' }] }
  const r = validateSpec(spec)
  expectError(r, CODE.promptRequired, 'tasks[0].prompt')
})

test('agent with whitespace-only prompt → dag.prompt_required', () => {
  const r = validateSpec(ONE_TASK({ prompt: '   ' }))
  expectError(r, CODE.promptRequired, 'tasks[0].prompt')
})

test('approval with retry.maxAttempts=3 → dag.kind_field_mismatch', () => {
  const spec = {
    version: 1, name: 'approval-retry',
    tasks: [{
      id: 'gate', kind: 'approval', approval: { action: 'go' },
      retry: { maxAttempts: 3 },
    }],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.kindFieldMismatch, 'tasks[0].retry.maxAttempts')
})

test('approval with retry.maxAttempts=1 passes', () => {
  const spec = {
    version: 1, name: 'approval-retry-1',
    tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' }, retry: { maxAttempts: 1 } }],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, true, JSON.stringify(r.errors))
})

test('approval with model → dag.kind_field_mismatch', () => {
  const spec = {
    version: 1, name: 'approval-model',
    tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' }, model: 'kimi-code/k3' }],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.kindFieldMismatch, 'tasks[0].model')
})

test('approval with each forbidden delegation field → dag.kind_field_mismatch', () => {
  const cases = {
    backend: 'native', provider: 'prov', persona: 'p',
    toolFilter: { allow: ['a'] }, cwd: '/abs', maxTokens: 100, maxDepth: 1,
    delegation: true, worktree: { task: 't' }, verify: { expectOutput: 'o', expectStatus: 's' },
  }
  for (const [field, value] of Object.entries(cases)) {
    const r = validateSpec({
      version: 1, name: 'approval-extra',
      tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' }, [field]: value }],
    })
    expectError(r, CODE.kindFieldMismatch, `tasks[0].${field}`)
  }
})

test('approval with outputs → dag.kind_field_mismatch', () => {
  const spec = {
    version: 1, name: 'approval-outputs',
    tasks: [{ id: 'gate', kind: 'approval', approval: { action: 'go' }, outputs: [{ name: 'o', schema: { type: 'object' } }] }],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.kindFieldMismatch, 'tasks[0].outputs')
})

test('approval missing the approval block → dag.kind_field_mismatch', () => {
  const spec = { version: 1, name: 'approval-bare', tasks: [{ id: 'gate', kind: 'approval' }] }
  const r = validateSpec(spec)
  expectError(r, CODE.kindFieldMismatch, 'tasks[0].approval')
})

test('merge without dependsOn → dag.kind_field_mismatch', () => {
  const spec = {
    version: 1, name: 'merge-bare',
    tasks: [{ id: 'm', kind: 'merge', merge: { integrationBranch: 'dsh-wt/x' } }],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.kindFieldMismatch, 'tasks[0].dependsOn')
})

test('merge with ≥1 worktree-declaring succeeded upstream passes', () => {
  const spec = {
    version: 1, name: 'merge-ok',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', worktree: { task: 'src-a' } },
      { id: 'm', kind: 'merge', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, true, JSON.stringify(r.errors))
})

test('merge with only non-worktree succeeded upstreams → dag.merge_source_missing', () => {
  const spec = {
    version: 1, name: 'merge-nosrc',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p' },
      { id: 'm', kind: 'merge', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ],
  }
  const r = validateSpec(spec)
  const hit = expectError(r, CODE.mergeSourceMissing, 'tasks[1].dependsOn')
  assert.match(hit.message, /declare worktree/)
})

test('merge whose only worktree upstream sits behind a completed condition → dag.merge_source_missing', () => {
  // A completed-condition edge admits failed upstreams — a merge source must
  // be a SUCCEEDED upstream, so the worktree declaration on a completed-only
  // edge does not count.
  const spec = {
    version: 1, name: 'merge-completed-only',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', worktree: { task: 'src-a' } },
      { id: 'm', kind: 'merge', dependsOn: [{ taskId: 'a', condition: 'completed' }] },
    ],
  }
  const r = validateSpec(spec)
  expectError(r, CODE.mergeSourceMissing, 'tasks[1].dependsOn')
})

// ---------------------------------------------------------------------------
// worktree.task slug uniqueness (M3 review M-A — §11.3 scopes reuse to the
// SAME task's re-dispatch; a shared slug is an isolation break, so it is
// structurally unreachable at plan time)
// ---------------------------------------------------------------------------

test('two tasks declaring the same worktree.task slug → dag.worktree_slug_conflict', () => {
  const spec = {
    version: 1, name: 'slug-conflict',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', worktree: { task: 'same-slug' } },
      { id: 'b', kind: 'agent', prompt: 'p', worktree: { task: 'same-slug' } },
    ],
  }
  const r = validateSpec(spec)
  const hit = expectError(r, CODE.worktreeSlugConflict, 'tasks[1].worktree.task')
  assert.match(hit.message, /first declared by task 'a'/)
  assert.match(hit.message, /unique slug/)
})

test('three tasks colliding on one slug → one error per later declaration, each at its own path', () => {
  const spec = {
    version: 1, name: 'slug-conflict-3',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', worktree: { task: 'dup' } },
      { id: 'b', kind: 'agent', prompt: 'p', worktree: { task: 'dup' } },
      { id: 'c', kind: 'agent', prompt: 'p', worktree: { task: 'dup' } },
    ],
  }
  const r = validateSpec(spec)
  const hits = r.errors.filter((e) => e.code === CODE.worktreeSlugConflict)
  assert.deepEqual(hits.map((e) => e.path), ['tasks[1].worktree.task', 'tasks[2].worktree.task'])
})

test('distinct worktree slugs across tasks still validate (parallel worktree tasks are legal)', () => {
  const spec = {
    version: 1, name: 'slug-ok',
    tasks: [
      { id: 'a', kind: 'agent', prompt: 'p', worktree: { task: 'slug-a' } },
      { id: 'b', kind: 'agent', prompt: 'p', worktree: { task: 'slug-b', baseRef: 'main' } },
    ],
  }
  const r = validateSpec(spec)
  assert.equal(r.ok, true, JSON.stringify(r.errors))
})

test('more than 1 output → dag.too_many_outputs', () => {
  const r = validateSpec(ONE_TASK({
    outputs: [
      { name: 'first', schema: { type: 'object' } },
      { name: 'second', schema: { type: 'object' } },
    ],
  }))
  const hit = expectError(r, CODE.tooManyOutputs, 'tasks[0].outputs')
  assert.match(hit.message, /at most 1/)
})

test('exactly 1 output passes', () => {
  const r = validateSpec(ONE_TASK({ outputs: [{ name: 'only-one', schema: { type: 'object' } }] }))
  assert.equal(r.ok, true, JSON.stringify(r.errors))
})

// ---------------------------------------------------------------------------
// Defaults — normalization for specHash
// ---------------------------------------------------------------------------

test('defaults: spec without limits gets maxRunningAgents=4, queueCapacity=16', () => {
  const r = validateSpec(ONE_TASK())
  assert.equal(r.ok, true)
  assert.equal(r.value.limits.maxRunningAgents, 4)
  assert.equal(r.value.limits.queueCapacity, 16)
})

test('defaults: partial limits fill only the missing number', () => {
  const r = validateSpec({ ...ONE_TASK(), limits: { maxRunningAgents: 7 } })
  assert.equal(r.ok, true)
  assert.deepEqual(r.value.limits, { maxRunningAgents: 7, queueCapacity: 16 })
})

// ---------------------------------------------------------------------------
// specHash
// ---------------------------------------------------------------------------

test('specHash: same spec with different key order → identical hash', () => {
  const a = EXAMPLE_SPEC()
  // b: every object rebuilt with reversed key order + tasks/dependsOn arrays
  // reversed where reversal is semantics-preserving (single-dep edges, and
  // tasks re-ordered WITH their objects — canonical JSON sorts keys, and task
  // ORDER is part of the hash, so keep the same task order here).
  const b = {
    tasks: a.tasks.map((t) => {
      const entries = Object.entries(t).reverse()
      const o = {}
      for (const [k, v] of entries) o[k] = v
      return o
    }),
    limits: { queueCapacity: 16, maxRunningAgents: 3 },
    project: { baseRef: 'HEAD', root: '/abs/repo' },
    description: 'Parallel refactor with review gate and integration',
    name: 'refactor-auth',
    version: 1,
  }
  const ha = specHash(a)
  const hb = specHash(b)
  assert.equal(typeof ha, 'string')
  assert.match(ha, /^[0-9a-f]{64}$/)
  assert.equal(ha, hb)
})

test('specHash: changed content → different hash', () => {
  const a = EXAMPLE_SPEC()
  const b = EXAMPLE_SPEC()
  b.tasks[1].retry.maxAttempts = 4 // content change inside a nested block
  assert.notEqual(specHash(a), specHash(b))

  const c = EXAMPLE_SPEC()
  c.tasks[2].prompt = 'Update the docs differently.'
  assert.notEqual(specHash(a), specHash(c))
})

test('specHash: omitting a default-valued field does not change the hash', () => {
  const a = EXAMPLE_SPEC() // timeoutMs explicit 1_800_000 on impl-core
  const b = EXAMPLE_SPEC()
  delete b.tasks[1].timeoutMs // same value via default
  assert.equal(specHash(a), specHash(b))
})

test('specHash: task ORDER changes the hash (array order is significant)', () => {
  const spec = {
    version: 1, name: 'order',
    tasks: [
      { id: 'a', kind: 'agent', prompt: '1' },
      { id: 'b', kind: 'agent', prompt: '2' },
    ],
  }
  const reordered = {
    version: 1, name: 'order',
    tasks: [
      { id: 'b', kind: 'agent', prompt: '2' },
      { id: 'a', kind: 'agent', prompt: '1' },
    ],
  }
  assert.notEqual(specHash(spec), specHash(reordered))
})

test('specHash: throws loud on an invalid spec', () => {
  assert.throws(() => specHash({ version: 2, name: 'x', tasks: [] }), /invalid spec/)
})
