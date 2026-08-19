// T08 tool tests — dag_plan / dag_status / dag_tick (DESIGN §8.1-§8.3).
//
// Focus: the TOOL layer. The engine and store are hand-rolled fakes (spies
// with fixed returns) so these tests assert the contract, not engine
// behavior (that is test/engine.test.js's job). The fake ctx is a
// tools.register collector.
//
// Covers: registration shape (name/description/parameters/output.schema),
// dag_plan return fields + validation errors + name_exists ± resume,
// dag_status detail passthrough + omit-run_id summary, dag_tick parameter
// passthrough + run_not_found, isConcurrencySafe (status true; plan/tick
// false), json-safe round-trips, and real schema-face rejection via
// dsh-tools' exported validators (validateArgs / validateJsonSchemaValue).
//
// NOTE on "extra parameter rejected": the defineTool DSL's implicit
// parameter root is OPEN (README: "an explicit object accepts extra keys
// only with additionalProperties: true" — the implicit root has no such
// declaration), so unknown top-level keys are NOT rejected at the DSL face
// by design; the enforced rejections are missing-required and wrong-type.
// The tests below assert exactly that enforced face, and additionally use
// validateJsonSchemaValue to prove the compiled parameter schema +
// additionalProperties:false output roots genuinely reject what they
// declare.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { validateArgs, validateJsonSchemaValue } from '@deepseek-ai/dsh-tools'

import { registerDagPlan, parseSpecArg } from '../lib/tools/dag-plan.js'
import { registerDagStatus } from '../lib/tools/dag-status.js'
import { registerDagTick } from '../lib/tools/dag-tick.js'
import { validateSpec } from '../lib/spec-validate.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/** Collector ctx: tools.register records definitions by name. */
function fakeCtx() {
  const registered = []
  return {
    registered,
    tools: {
      register(definition) {
        registered.push(definition)
      },
    },
  }
}

/** A minimal VALID spec (passes lib/spec-validate.js). */
function validSpec(overrides = {}) {
  return {
    version: 1,
    name: 'demo-run',
    tasks: [{ id: 'analyze', kind: 'agent', prompt: 'do the analysis' }],
    ...overrides,
  }
}

/** Fixed §8.3-shaped tickSummary the fake engine returns. */
function tickSummary(overrides = {}) {
  return {
    kind: 'tick',
    run_id: 'dag_20260827_00000001',
    run_state: 'running',
    rounds: 1,
    promoted: 1,
    dispatched: 1,
    terminal: 0,
    propagated: 0,
    in_flight: [{ task_id: 'analyze', attempt: 1, started_at: 1_700_000_000_000, elapsed_ms: 42 }],
    waiting_on: 'in_flight_attempts',
    next_hint: 'call dag_tick again after in-flight work settles',
    ...overrides,
  }
}

/** Fake engine — spies with fixed returns. */
function fakeEngine({ planResult, tickResult, statusResult } = {}) {
  const calls = { planRun: [], tick: [], status: [] }
  return {
    calls,
    planRun(spec, options) {
      calls.planRun.push({ spec, options })
      if (planResult instanceof Error) throw planResult
      return planResult ?? {
        runId: 'dag_20260827_00000001',
        specHash: 'a'.repeat(64),
        taskCount: 1,
      }
    },
    async tick(runId, options) {
      calls.tick.push({ runId, options })
      if (tickResult instanceof Error) throw tickResult
      return tickResult ?? tickSummary({ run_id: runId })
    },
    status(runId, options) {
      calls.status.push({ runId, options })
      if (statusResult instanceof Error) throw statusResult
      return statusResult ?? { kind: 'status', detail: options?.detail ?? 'tasks', run_id: runId, tasks: [] }
    },
  }
}

/** Fake store — findNonTerminalRuns / findRun over an in-memory array. */
function fakeStore({ nonTerminalRuns = [], runsById = new Map() } = {}) {
  return {
    findNonTerminalRuns: () => nonTerminalRuns.map((row) => ({ ...row })),
    findRun: (runId) => (runsById.has(runId) ? { ...runsById.get(runId) } : null),
  }
}

/** Register all three tools on one fake ctx; returns the definitions. */
function registerAll(ctx, { engine, store, config } = {}) {
  registerDagPlan(ctx, { engine, store, config })
  registerDagStatus(ctx, { engine, store, config })
  registerDagTick(ctx, { engine, store, config })
  const byName = Object.fromEntries(ctx.registered.map((tool) => [tool.name, tool]))
  return { plan: byName.dag_plan, status: byName.dag_status, tick: byName.dag_tick }
}

/** exec context carrying a live-agent-shaped session. */
function execCtx({ cwd = '/tmp/repo', id = 'sess-42' } = {}) {
  return { agent: { session: { id, header: { cwd } } } }
}

/** Assert a value is json-safe: JSON round-trip loses no key (deep). */
function assertJsonSafe(value, label = 'value') {
  const round = JSON.parse(JSON.stringify(value))
  assert.deepEqual(round, value, `${label} must survive a JSON round-trip with no key loss`)
}

/** Deep-collect every undefined-valued key path in an object. */
function undefinedKeyPaths(value, base = '') {
  const paths = []
  if (Array.isArray(value)) {
    value.forEach((entry, i) => paths.push(...undefinedKeyPaths(entry, `${base}[${i}]`)))
  } else if (value !== null && typeof value === 'object') {
    for (const [key, entry] of Object.entries(value)) {
      if (entry === undefined) paths.push(base === '' ? key : `${base}.${key}`)
      else paths.push(...undefinedKeyPaths(entry, base === '' ? key : `${base}.${key}`))
    }
  }
  return paths
}

// ---------------------------------------------------------------------------
// registration shape
// ---------------------------------------------------------------------------

test('tools: registration — names, key description fragments, closed output roots', () => {
  const ctx = fakeCtx()
  const { plan, status, tick } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })

  assert.deepEqual(ctx.registered.map((tool) => tool.name).sort(), ['dag_plan', 'dag_status', 'dag_tick'])
  assert.equal(typeof plan.execute, 'function')
  assert.equal(typeof status.execute, 'function')
  assert.equal(typeof tick.execute, 'function')

  // Description fragments (§8.1: the WorkflowSpec pointer; §8.6 sequence ①
  // embedded in dag_plan's description).
  assert.ok(plan.description.includes('WorkflowSpec'))
  assert.ok(plan.description.includes('dag_tick(run_id)'))
  assert.ok(status.description.includes('all-runs summary'))
  assert.ok(tick.description.includes('waiting_on'))

  // Compiled output schemas are object roots; dag_plan's is CLOSED and
  // carries the §8.1 fields (const kind + required annotations projected
  // into JSON Schema `required`).
  assert.equal(plan.output.schema.type, 'object')
  assert.equal(plan.output.schema.additionalProperties, false)
  assert.deepEqual(
    Object.keys(plan.output.schema.properties).sort(),
    ['initial_tick', 'kind', 'run_id', 'spec_hash', 'task_count', 'warnings'],
  )
  assert.deepEqual(plan.output.schema.properties.kind, { type: 'string', const: 'plan' })
  // §8.1 marks run_id/spec_hash/task_count required; initial_tick/warnings
  // stay optional properties (the tool always emits them — the schema face
  // just does not force the keys).
  assert.deepEqual(plan.output.schema.required, ['run_id', 'spec_hash', 'task_count'])

  assert.equal(status.output.schema.type, 'object')
  assert.equal(tick.output.schema.type, 'object')
})

test('tools: parameter faces — compiled shapes match DESIGN §8.1-§8.3', () => {
  const ctx = fakeCtx()
  const { plan, status, tick } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })

  // dag_plan: spec (open object, required) + resume (boolean).
  assert.deepEqual(plan.parameters.required, ['spec'])
  // spec compiles to a REAL `object` type (annotation-only `json` had no
  // wire constraint — the exact hole glossed by the meta/glossary); the open
  // additionalProperties keeps the WorkflowSpec face open for zod strict
  // down-field validation.
  assert.equal(plan.parameters.properties.spec.type, 'object')
  assert.equal(plan.parameters.properties.spec.additionalProperties, true)
  assert.ok(plan.parameters.properties.spec.description.includes('WorkflowSpec'))
  assert.equal(plan.parameters.properties.resume.type, 'boolean')

  // dag_status: run_id/detail/task_id/limit, detail enum, nothing required.
  assert.equal(status.parameters.required, undefined)
  assert.deepEqual(
    Object.keys(status.parameters.properties).sort(),
    ['detail', 'limit', 'run_id', 'task_id'],
  )
  assert.deepEqual(status.parameters.properties.detail.enum, ['summary', 'tasks', 'attempts', 'events'])
  assert.equal(status.parameters.properties.run_id.type, 'string')
  assert.equal(status.parameters.properties.limit.type, 'integer')

  // dag_tick: run_id required; max_rounds/settle_ms integers.
  assert.deepEqual(tick.parameters.required, ['run_id'])
  assert.equal(tick.parameters.properties.run_id.type, 'string')
  assert.equal(tick.parameters.properties.max_rounds.type, 'integer')
  assert.equal(tick.parameters.properties.settle_ms.type, 'integer')
})

test('tools: renderers produce JSON text blocks', () => {
  const ctx = fakeCtx()
  const { plan } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })
  const blocks = plan.output.render({}, { kind: 'plan', run_id: 'r' })
  assert.equal(blocks.length, 1)
  assert.equal(blocks[0].type, 'text')
  assert.equal(JSON.parse(blocks[0].text).run_id, 'r')
})

// ---------------------------------------------------------------------------
// dag_plan
// ---------------------------------------------------------------------------

test('dag_plan: full return shape, inline first tick, exec wiring', async () => {
  const engine = fakeEngine()
  const store = fakeStore()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store })

  const exec = execCtx()
  const result = await ctx.registered[0].execute({ spec: validSpec() }, exec)

  assert.deepEqual(
    { kind: result.kind, run_id: result.run_id, spec_hash: result.spec_hash, task_count: result.task_count },
    { kind: 'plan', run_id: 'dag_20260827_00000001', spec_hash: 'a'.repeat(64), task_count: 1 },
  )
  assert.deepEqual(result.initial_tick, tickSummary())
  assert.deepEqual(result.warnings, [])
  assertJsonSafe(result, 'dag_plan result')
  assert.deepEqual(undefinedKeyPaths(result), [])

  // planRun received the resolved base cwd + parent session + planner
  // session (same provenance: exec.agent.session.id) + agent.
  assert.equal(engine.calls.planRun.length, 1)
  const planned = engine.calls.planRun[0]
  assert.equal(planned.options.baseCwd, '/tmp/repo')
  assert.equal(planned.options.parentSession, 'sess-42')
  assert.equal(planned.options.plannerSession, 'sess-42')
  assert.equal(planned.options.execAgent, exec.agent)
  assert.equal(planned.spec.name, 'demo-run')

  // The inline first tick: maxRounds 1, settleMs 0, execAgent forwarded.
  assert.equal(engine.calls.tick.length, 1)
  assert.equal(engine.calls.tick[0].runId, 'dag_20260827_00000001')
  assert.deepEqual(
    { maxRounds: engine.calls.tick[0].options.maxRounds, settleMs: engine.calls.tick[0].options.settleMs },
    { maxRounds: 1, settleMs: 0 },
  )
})

test('dag_plan: baseCwd falls back to process.cwd without an agent session', async () => {
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store: fakeStore() })

  await ctx.registered[0].execute({ spec: validSpec() }, {})
  assert.equal(engine.calls.planRun[0].options.baseCwd, process.cwd())
  assert.equal(engine.calls.planRun[0].options.parentSession, undefined)
  assert.equal(engine.calls.planRun[0].options.plannerSession, undefined)
})

test('dag_plan: invalid spec throws with every error aggregated, dag_plan: prefix', async () => {
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store: fakeStore() })

  // Two faults: unknown root key + a dependency cycle (semantic pass) —
  // wait, unknown root key makes zod fail fast on the strict root; use a
  // spec that parses but fails semantics twice (cycle + missing prompt).
  const bad = {
    version: 1,
    name: 'bad',
    tasks: [
      { id: 'a', kind: 'agent', dependsOn: [{ taskId: 'b', condition: 'succeeded' }] },
      { id: 'b', kind: 'agent', dependsOn: [{ taskId: 'a', condition: 'succeeded' }] },
    ],
  }
  await assert.rejects(
    () => ctx.registered[0].execute({ spec: bad }, execCtx()),
    (error) => {
      assert.ok(error.message.startsWith('dag_plan: dag.'), `prefix wrong: ${error.message}`)
      assert.ok(error.message.includes('dag.cycle_detected'), error.message)
      assert.ok(error.message.includes('dag.prompt_required'), error.message)
      assert.equal(error.code, 'dag.schema_invalid')
      assert.ok(Array.isArray(error.errors))
      return true
    },
  )
  // Nothing persisted, nothing ticked.
  assert.equal(engine.calls.planRun.length, 0)
  assert.equal(engine.calls.tick.length, 0)
})

test('dag_plan: unknown keys are rejected with the path in the message', async () => {
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine: fakeEngine(), store: fakeStore() })
  await assert.rejects(
    () => ctx.registered[0].execute({ spec: { ...validSpec(), labels: ['x'] } }, execCtx()),
    (error) => {
      assert.ok(error.message.includes('dag.unknown_field'))
      assert.ok(error.message.includes('labels'))
      return true
    },
  )
})

test('dag_plan: name_exists — resume=false throws, resume=true reuses the stored run', async () => {
  const specJson = JSON.stringify({
    version: 1,
    name: 'demo-run',
    tasks: [{ id: 'analyze', kind: 'agent', prompt: 'x' }, { id: 'next', kind: 'agent', prompt: 'y' }],
  })
  const store = fakeStore({
    nonTerminalRuns: [
      { run_id: 'dag_20250101_abcdabcd', name: 'demo-run', spec_json: specJson, spec_hash: 'b'.repeat(64), state: 'running' },
    ],
  })
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store })

  await assert.rejects(
    () => ctx.registered[0].execute({ spec: validSpec() }, execCtx()),
    (error) => {
      assert.equal(error.message, 'dag_plan: dag.name_exists — run dag_20250101_abcdabcd')
      assert.equal(error.code, 'dag.name_exists')
      assert.equal(error.run_id, 'dag_20250101_abcdabcd')
      return true
    },
  )
  assert.equal(engine.calls.planRun.length, 0, 'no new run on name_exists')

  const resumed = await ctx.registered[0].execute({ spec: validSpec(), resume: true }, execCtx())
  assert.equal(resumed.run_id, 'dag_20250101_abcdabcd')
  assert.equal(resumed.spec_hash, 'b'.repeat(64), 'spec_hash from the STORED run')
  assert.equal(resumed.task_count, 2, 'task_count from the STORED run')
  assert.equal(resumed.initial_tick.run_id, 'dag_20250101_abcdabcd')
  assert.equal(engine.calls.planRun.length, 0, 'resume reuses; planRun not called')
  assert.equal(engine.calls.tick.length, 1)
  assertJsonSafe(resumed, 'resumed plan result')
})

test('dag_plan: same-name TERMINAL run does not block planning', async () => {
  // findNonTerminalRuns is the source — terminal runs are simply absent.
  const store = fakeStore({ nonTerminalRuns: [] })
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store })
  const result = await ctx.registered[0].execute({ spec: validSpec() }, execCtx())
  assert.equal(engine.calls.planRun.length, 1)
  assert.equal(result.run_id, 'dag_20260827_00000001')
})

test('dag_plan: the typed object face rejects a string-form spec at the argument boundary (schema layer)', async () => {
  // Schema-layer regression: spec is now `type:'object'` in the compiled wire
  // schema (previously the author-only `json` node had NO constraint there),
  // so a gateway that hands a JSON STRING spec is rejected AT THE FACE as a
  // ToolArgsError — it never reaches execute (the old leak path). This pins
  // the missing type constraint that let glm-5.3/newapi stringify the spec.
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store: fakeStore() })
  const plan = ctx.registered[0]

  for (const str of [JSON.stringify(validSpec()), '{not valid json']) {
    await assert.rejects(
      () => plan.execute({ spec: str, resume: true }, execCtx()),
      (error) => error.name === 'ToolArgsError' && /must be an object/.test(error.message),
    )
  }
  // Nothing persisted, nothing ticked for either rejected call.
  assert.equal(engine.calls.planRun.length, 0)
  assert.equal(engine.calls.tick.length, 0)
})

test('dag_plan: parseSpecArg tolerance — string spec parsed; malformed string kept for zod to reject', () => {
  // The execute-layer defense (parseSpecArg): a string is JSON.parsed into
  // the object that the full strict validateSpec then checks; a malformed
  // string is returned UNCHANGED so the strict path reports schema_invalid
  // with its original message (no weaker branch).
  assert.deepEqual(parseSpecArg(JSON.stringify(validSpec())), validSpec())
  assert.equal(parseSpecArg('{not valid json'), '{not valid json')
  // Non-string values pass through untouched (objects from the schema face).
  const specObj = validSpec()
  assert.equal(parseSpecArg(specObj), specObj)
  assert.equal(parseSpecArg(7), 7)
})

test('dag_plan: a malformed string reaching the execute body still reports dag.schema_invalid', async () => {
  // Belt-and-suspenders for the container path that bypasses the DSL wrapper:
  // parseSpecArg keeps a malformed string, and the strict zod path rejects it
  // with the original `Expected object, received string` → dag.schema_invalid
  // contract. (Through the compiled tool the wrapper rejects the string
  // first; this pins the容错 layer's own fallback semantics.)
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagPlan(ctx, { engine, store: fakeStore() })

  const validated = await validateSpec(parseSpecArg('{not valid json'))
  assert.equal(validated.ok, false)
  assert.ok(validated.errors.some((e) => e.code === 'dag.schema_invalid'))
})

// ---------------------------------------------------------------------------
// dag_status
// ---------------------------------------------------------------------------

test('dag_status: detail depths pass through; default tasks; omit run_id → summary', async () => {
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagStatus(ctx, { engine })

  const tool = ctx.registered[0]

  await tool.execute({ run_id: 'r1', detail: 'attempts', task_id: 't1', limit: 7 }, execCtx())
  assert.deepEqual(engine.calls.status.at(-1), {
    runId: 'r1',
    options: { detail: 'attempts', taskId: 't1', limit: 7 },
  })

  await tool.execute({ run_id: 'r1', detail: 'events' }, execCtx())
  assert.deepEqual(engine.calls.status.at(-1), { runId: 'r1', options: { detail: 'events' } })

  // Default depth: the tool passes NO detail key — the engine defaults to
  // 'tasks' (asserted via the fake engine's echoed result below).
  await tool.execute({ run_id: 'r1' }, execCtx())
  assert.deepEqual(engine.calls.status.at(-1), { runId: 'r1', options: {} })

  // Omit run_id → the all-runs summary arm (runId undefined at the tool
  // layer; the engine branches on it).
  await tool.execute({}, execCtx())
  assert.deepEqual(engine.calls.status.at(-1), { runId: undefined, options: {} })
})

test('dag_status: result is returned verbatim and json-safe', async () => {
  const fixed = {
    kind: 'status',
    detail: 'tasks',
    run_id: 'r1',
    name: 'demo-run',
    state: 'running',
    counts: { pending: 1, ready: 0, running: 0, succeeded: 0, failed: 0, blocked: 0 },
    created_at: 1,
    updated_at: 2,
    tasks: [{ id: 'analyze', state: 'pending', attempts: 0, ordinal: 0 }],
  }
  const engine = fakeEngine({ statusResult: fixed })
  const ctx = fakeCtx()
  registerDagStatus(ctx, { engine })
  const result = await ctx.registered[0].execute({ run_id: 'r1' }, execCtx())
  assert.deepEqual(result, fixed)
  assertJsonSafe(result, 'dag_status result')
})

// ---------------------------------------------------------------------------
// dag_tick
// ---------------------------------------------------------------------------

test('dag_tick: parameter passthrough (max_rounds/settle_ms/execAgent), tickSummary verbatim', async () => {
  const summary = tickSummary({ run_id: 'r1', run_state: 'succeeded', waiting_on: 'nothing' })
  const engine = fakeEngine({ tickResult: summary })
  const store = fakeStore({ runsById: new Map([['r1', { run_id: 'r1', state: 'running' }]]) })
  const ctx = fakeCtx()
  registerDagTick(ctx, { engine, store })

  const exec = execCtx()
  const result = await ctx.registered[0].execute({ run_id: 'r1', max_rounds: 8, settle_ms: 500 }, exec)
  assert.deepEqual(result, summary)
  assertJsonSafe(result, 'dag_tick result')
  assert.deepEqual(undefinedKeyPaths(result), [])

  assert.equal(engine.calls.tick.length, 1)
  assert.equal(engine.calls.tick[0].runId, 'r1')
  assert.equal(engine.calls.tick[0].options.maxRounds, 8)
  assert.equal(engine.calls.tick[0].options.settleMs, 500)
  assert.equal(engine.calls.tick[0].options.execAgent, exec.agent)
})

test('dag_tick: unknown run throws dag.run_not_found before touching the engine', async () => {
  const engine = fakeEngine()
  const ctx = fakeCtx()
  registerDagTick(ctx, { engine, store: fakeStore() })
  await assert.rejects(
    () => ctx.registered[0].execute({ run_id: 'nope' }, execCtx()),
    (error) => {
      assert.equal(error.message, 'dag_tick: dag.run_not_found — nope')
      assert.equal(error.code, 'dag.run_not_found')
      return true
    },
  )
  assert.equal(engine.calls.tick.length, 0)
})

// ---------------------------------------------------------------------------
// isConcurrencySafe
// ---------------------------------------------------------------------------

test('tools: isConcurrencySafe — dag_status true, dag_plan/dag_tick false', () => {
  const ctx = fakeCtx()
  const { plan, status, tick } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })
  assert.equal(status.isConcurrencySafe({}), true)
  assert.equal(plan.isConcurrencySafe({ spec: {} }), false)
  assert.equal(tick.isConcurrencySafe({ run_id: 'r1' }), false)
})

// ---------------------------------------------------------------------------
// json-safe discipline (all three happy paths)
// ---------------------------------------------------------------------------

test('tools: all happy-path returns survive the JSON round-trip', async () => {
  const runId = 'dag_20260827_00000001'
  const engine = fakeEngine()
  const ctx = fakeCtx()
  const { plan, status, tick } = registerAll(ctx, {
    engine,
    store: fakeStore({ runsById: new Map([[runId, { run_id: runId, state: 'running' }]]) }),
  })

  const planResult = await plan.execute({ spec: validSpec() }, execCtx())
  const statusResult = await status.execute({ run_id: runId, detail: 'tasks' }, execCtx())
  const tickResult = await tick.execute({ run_id: runId }, execCtx())

  for (const [label, value] of [['plan', planResult], ['status', statusResult], ['tick', tickResult]]) {
    assertJsonSafe(value, label)
    assert.deepEqual(undefinedKeyPaths(value), [], `${label} has undefined-valued keys`)
  }
})

// ---------------------------------------------------------------------------
// real schema-face validation (dsh-tools exported validators)
// ---------------------------------------------------------------------------

test('tools: the enforced argument face rejects missing-required and wrong-type args', async () => {
  const ctx = fakeCtx()
  const { plan, tick } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })

  // via the exported validateArgs on the DECLARED spec shapes.
  const planSpec = {
    spec: { type: 'object', additionalProperties: true, required: true },
    resume: { type: 'boolean' },
  }
  assert.ok(validateArgs(planSpec, {}).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(planSpec, { spec: {}, resume: 'yes' }).some((v) => v.includes('boolean')))

  const tickSpec = { run_id: { type: 'string', required: true } }
  assert.ok(validateArgs(tickSpec, {}).some((v) => v.includes('missing required')))
  assert.ok(validateArgs(tickSpec, { run_id: 7 }).some((v) => v.includes('string')))

  // And THROUGH the compiled tool definition: defineTool's execute wrapper
  // validates before the body runs (wrong type → ToolArgsError; the body's
  // engine must stay untouched).
  await assert.rejects(
    () => plan.execute({ spec: {}, resume: 'yes' }, execCtx()),
    (error) => error.name === 'ToolArgsError',
  )
  await assert.rejects(
    () => tick.execute({ run_id: 7 }, execCtx()),
    (error) => error.name === 'ToolArgsError',
  )
})

test('tools: unknown top-level keys are NOT rejected by the DSL face (open implicit root) — documented behavior', async () => {
  // The defineTool DSL's implicit parameter root is open by design (the
  // closed-object rejection needs an EXPLICIT object node with
  // additionalProperties:false, which the parameter DSL does not produce at
  // the root). This test pins that fact so a future dsh-tools behavior
  // change is noticed: today an extra key passes validateArgs and reaches
  // execute, where it is simply ignored (only DECLARED keys are read).
  const ctx = fakeCtx()
  const { tick } = registerAll(ctx, {
    engine: fakeEngine(),
    store: fakeStore({ runsById: new Map([['r1', { run_id: 'r1' }]]) }),
  })
  assert.deepEqual(validateArgs({ run_id: { type: 'string', required: true } }, { run_id: 'r1', bogus: 1 }), [])
  const result = await tick.execute({ run_id: 'r1', bogus: 1 }, execCtx())
  assert.equal(result.run_id, 'r1')
})

test('tools: compiled schemas genuinely close what they declare', () => {
  const ctx = fakeCtx()
  const { plan } = registerAll(ctx, { engine: fakeEngine(), store: fakeStore() })

  // The compiled parameter schema (raw JSON Schema) validates good args and
  // rejects the declared wrong type.
  assert.deepEqual(validateJsonSchemaValue(plan.parameters, { spec: {}, resume: true }, 'arguments'), [])
  assert.ok(validateJsonSchemaValue(plan.parameters, { spec: {}, resume: 1 }, 'arguments').length > 0)

  // The additionalProperties:false output root rejects an undeclared key —
  // proving the tool's RETURN values must stick to the §8.1 field set.
  assert.deepEqual(
    validateJsonSchemaValue(plan.output.schema, {
      kind: 'plan', run_id: 'r', spec_hash: 'h', task_count: 1, initial_tick: {}, warnings: [],
    }, 'value'),
    [],
  )
  assert.ok(
    validateJsonSchemaValue(plan.output.schema, {
      kind: 'plan', run_id: 'r', spec_hash: 'h', task_count: 1, initial_tick: {}, warnings: [], rogue: 1,
    }, 'value').some((v) => v.includes('rogue')),
  )
})

// ---------------------------------------------------------------------------
// deps contract
// ---------------------------------------------------------------------------

test('tools: register functions throw loud on missing deps', () => {
  const ctx = fakeCtx()
  assert.throws(() => registerDagPlan(ctx, {}), /dag_plan: requires deps\.engine/)
  assert.throws(() => registerDagStatus(ctx, {}), /dag_status: requires deps\.engine/)
  assert.throws(() => registerDagTick(ctx, { engine: fakeEngine() }), /dag_tick: requires deps\.store/)
})
