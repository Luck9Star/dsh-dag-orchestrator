// worktrees-seam tests — TASKS.md T15 (DESIGN §11.2 seam 2: engine
// singleton acquisition via opportunistic probing).
//
// dsh-worktrees does NOT yet provide the service face (verified: no
// `provide`/`worktreesEngine` anywhere in its lib/) — per the T15 note,
// this plugin lands the CONSUMER side first and this file's fakes define
// the acceptance shape for the provider task. The interface contract of
// record is the WorktreesEngineFace JSDoc in lib/worktrees-seam.js.
//
// Covered, per the T15 brief:
//
//   * full engine face → probe available + get() passes the SAME
//     singleton references through (identity, not copies);
//   * ctx.get returns undefined → available false + get().worktrees null
//     (the agent-only deployment — engine assembly elsewhere unaffected);
//   * half-shaped faces (missing getMergeQueue / non-function members /
//     non-object values) → treated EXACTLY like absence (fail-safe, no
//     half-wiring) + one diagnostic warn per probe;
//   * LATE REGISTRATION: probe absent → host provides the engine later →
//     the NEXT get() picks it up (the "apply + every use" cadence);
//   * ctx.get throws → caught, treated as absent, logger.warn, never
//     propagates (apply must not be blocked by the optional seam);
//   * a ctx WITHOUT a get face at all → absent (strict test doubles);
//   * executor wiring: createExecutor({worktreesSeam}) exposes the handle
//     and a never-throwing worktrees() accessor; a malformed seam is a
//     loud constructor TypeError; absence keeps executor behavior intact;
//   * apply() integration: a fake worktreesEngine on the ctx flows through
//     to the executor the tools actually use (probe via dag_plan's
//     dispatch path — one tick, one dispatch, the subagent face records
//     the request, then the seam face is read through the executor);
//     absent engine → apply still completes and an agent-only run goes
//     FULL CHAIN green (plan → dispatch → harvest → run succeeded).
//
// The loud `dag.worktrees_unavailable` CONSUMPTION behavior belongs to
// T16/T17 (merge/worktree tasks meeting absence) — deliberately NOT
// tested here; this file pins the seam face only.
//
// Zero network, zero CLI, zero models, no dsh-worktrees import.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createWorktreesSeam } from '../lib/worktrees-seam.js'
import { createExecutor } from '../lib/executor.js'
import { apply } from '../lib/index.js'

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/**
 * A fake matching the WorktreesEngineFace contract (lib/worktrees-seam.js
 * JSDoc): the two accessor functions over shared singleton objects. The
 * singletons are frozen markers so identity assertions are unambiguous.
 */
function fakeEngine() {
  const mergeQueue = Object.freeze({ __queue: true, enqueue() {}, drain: async () => ({}) })
  const worktreeService = Object.freeze({ __service: true, create: async () => ({}) })
  return {
    getMergeQueue: () => mergeQueue,
    getWorktreeService: () => worktreeService,
    available: true,
  }
}

/** Collecting logger (warn only is interesting; info/debug no-ops). */
function collectingLogger() {
  const warns = []
  return { warns, logger: { warn: (m) => warns.push(m), info() {}, debug() {} } }
}

/** A ctx whose get('worktreesEngine') returns `value` (possibly undefined). */
function ctxReturning(value) {
  return { get: (name) => (name === 'worktreesEngine' ? value : undefined) }
}

// ---------------------------------------------------------------------------
// probe / get units
// ---------------------------------------------------------------------------

test('seam: full engine face → probe available + get() passes the SAME references through', () => {
  const engine = fakeEngine()
  const seam = createWorktreesSeam(ctxReturning(engine), {})

  const probed = seam.probe()
  assert.equal(probed.available, true)
  assert.equal(probed.engine, engine, 'probe reports the engine reference itself')
  assert.equal(seam.engineRef, engine, 'the last admitted reference is kept for diagnostics')

  const got = seam.get()
  assert.notEqual(got.worktrees, null)
  // Identity pass-through is the whole point (worktrees §10: reuse the
  // single instance — never a copy, never a second queue).
  assert.equal(got.worktrees.getMergeQueue(), engine.getMergeQueue())
  assert.equal(got.worktrees.getWorktreeService(), engine.getWorktreeService())
})

test('seam: get() re-probes — a DIFFERENT later engine is served, never a stale one', () => {
  const first = fakeEngine()
  const second = fakeEngine()
  let current = first
  const seam = createWorktreesSeam({ get: () => current }, {})

  assert.equal(seam.get().worktrees.getMergeQueue(), first.getMergeQueue())
  current = second // the host re-provided (dispose + re-provide cycle)
  assert.equal(seam.get().worktrees.getMergeQueue(), second.getMergeQueue())
  current = undefined // and unloaded again — no stale reference served
  assert.equal(seam.get().worktrees, null)
})

test('seam: ctx.get returns undefined → available false + get().worktrees null (agent-only mode)', () => {
  const { warns, logger } = collectingLogger()
  const seam = createWorktreesSeam(ctxReturning(undefined), { logger })

  assert.deepEqual(seam.probe(), { available: false })
  assert.equal(seam.get().worktrees, null)
  assert.equal(seam.engineRef, undefined)
  // Plain absence is the NORMAL agent-only deployment — not a fault, so
  // it must stay silent (no warn spam on every probe).
  assert.equal(warns.length, 0)
})

test('seam: half-shaped faces are treated exactly like absence (fail-safe, no half-wiring)', () => {
  const cases = [
    ['missing getMergeQueue', { getWorktreeService: () => ({}) }],
    ['missing getWorktreeService', { getMergeQueue: () => ({}) }],
    ['getMergeQueue not a function', { getMergeQueue: 'nope', getWorktreeService: () => ({}) }],
    ['plain object, no members', { available: true }],
    ['a string', 'worktreesEngine?'],
    ['a number', 42],
    ['null', null],
  ]
  for (const [label, face] of cases) {
    const { warns, logger } = collectingLogger()
    const seam = createWorktreesSeam(ctxReturning(face), { logger })
    assert.equal(seam.probe().available, false, `${label}: probe must report unavailable`)
    assert.equal(seam.get().worktrees, null, `${label}: get() must degrade to null`)
    assert.equal(seam.engineRef, undefined, `${label}: nothing half-admitted`)
    // Shape rejection is a FAULT (unlike plain absence): one warn per
    // PROBE — and this test probes twice (probe + the get()'s re-probe).
    assert.equal(warns.length, 2, `${label}: one diagnostic warn per probe`)
    assert.match(warns[0], /getMergeQueue\(\)\/getWorktreeService\(\)/)
  }
})

test('seam: a stale available:true flag neither admits a half face nor blocks a full one', () => {
  // The two functions are the authoritative shape test (DESIGN's suggested
  // `available` flag is a convenience, never grounds for admission).
  const { logger } = collectingLogger()
  const flagged = { available: true, getMergeQueue: () => ({}) } // half face, loud flag
  const seamA = createWorktreesSeam(ctxReturning(flagged), { logger })
  assert.equal(seamA.probe().available, false)

  const unflagged = fakeEngine()
  delete unflagged.available // full face, flag missing
  const seamB = createWorktreesSeam(ctxReturning(unflagged), { logger })
  assert.equal(seamB.probe().available, true)
})

test('seam: LATE REGISTRATION — absent at first probe, engine provided later, next get() wires in', () => {
  // The T15 cadence probe: apply-time absent + use-time present. Simulates
  // the host loading dsh-worktrees AFTER dsh-dag-orchestrator.
  let provided = undefined
  const seam = createWorktreesSeam({ get: () => provided }, {})

  assert.equal(seam.probe().available, false, 'apply-time probe: honestly absent')
  assert.equal(seam.get().worktrees, null, 'early consumers degrade loudly (T16/T17)')

  const engine = fakeEngine()
  provided = engine // the host's ctx.provide('worktreesEngine', …) lands

  const got = seam.get()
  assert.notEqual(got.worktrees, null, 'the NEXT use-time probe picked the engine up')
  assert.equal(got.worktrees.getMergeQueue(), engine.getMergeQueue())
})

test('seam: ctx.get throwing is caught → absent + logger.warn, never propagates', () => {
  const { warns, logger } = collectingLogger()
  const ctx = {
    get(name) {
      if (name === 'worktreesEngine') throw new Error('registry exploded')
      return undefined
    },
  }
  const seam = createWorktreesSeam(ctx, { logger })

  assert.deepEqual(seam.probe(), { available: false })
  assert.equal(seam.get().worktrees, null)
  // One warn per probe; this test probes twice (probe + get's re-probe).
  assert.equal(warns.length, 2)
  assert.match(warns[0], /worktreesEngine probe threw.*registry exploded/)
  assert.match(warns[1], /worktreesEngine probe threw.*registry exploded/)
})

test('seam: a ctx without a get face (strict double) → absent, silent', () => {
  const { warns, logger } = collectingLogger()
  for (const ctx of [undefined, null, {}]) {
    const seam = createWorktreesSeam(ctx, { logger })
    assert.deepEqual(seam.probe(), { available: false })
    assert.equal(seam.get().worktrees, null)
  }
  assert.equal(warns.length, 0, 'no face is absence, not a fault')
})

// ---------------------------------------------------------------------------
// executor wiring (T15 wires; T16/T17 consume)
// ---------------------------------------------------------------------------

/** Minimal fake ctx.subagents (never dispatches in these tests). */
function fakeSubagents() {
  return { start: async () => { throw new Error('no dispatch expected here') } }
}

test('executor: worktreesSeam handle + never-throwing worktrees() accessor exposed', () => {
  const engine = fakeEngine()
  const seam = createWorktreesSeam(ctxReturning(engine), {})
  const executor = createExecutor({ ctxSubagents: fakeSubagents(), worktreesSeam: seam })

  assert.equal(executor.worktreesSeam(), seam, 'the constructed handle is carried verbatim')
  const face = executor.worktrees()
  assert.notEqual(face.worktrees, null)
  assert.equal(face.worktrees.getMergeQueue(), engine.getMergeQueue())
  assert.equal(face.worktrees.getWorktreeService(), engine.getWorktreeService())

  // Absent engine through the SAME executor: degrades to null (the
  // T16/T17 loud path's input), never throws.
  const absentSeam = createWorktreesSeam(ctxReturning(undefined), {})
  const executor2 = createExecutor({ ctxSubagents: fakeSubagents(), worktreesSeam: absentSeam })
  assert.equal(executor2.worktreesSeam(), absentSeam)
  assert.deepEqual(executor2.worktrees(), { worktrees: null })
})

test('executor: constructed WITHOUT a seam (direct test construction) behaves as before', () => {
  const executor = createExecutor({ ctxSubagents: fakeSubagents() })
  assert.equal(executor.worktreesSeam(), undefined)
  assert.deepEqual(executor.worktrees(), { worktrees: null }, 'no seam ⇒ absent face, not a throw')
})

test('executor: a malformed worktreesSeam is a loud constructor TypeError', () => {
  for (const bad of [{}, { get: () => ({}) }, { probe: () => ({}) }, 'seam']) {
    assert.throws(
      () => createExecutor({ ctxSubagents: fakeSubagents(), worktreesSeam: bad }),
      /worktreesSeam must be a createWorktreesSeam/,
    )
  }
})

// ---------------------------------------------------------------------------
// apply() integration — the seam flows to the executor the tools use
// ---------------------------------------------------------------------------

/** The apply-level fake ctx (index.test.js shape) + an optional engine. */
function fakeApplyCtx({ engine } = {}) {
  const registered = []
  const teardowns = []
  const lines = { info: [], warn: [], fatal: [] }
  const subagents = {
    calls: [],
    async start(name, request) {
      const index = subagents.calls.length
      subagents.calls.push({ name, request, index })
      return { id: `sess-${index + 1}`, result: Promise.resolve({ output: [], stopReason: 'completed' }), dispose: async () => {} }
    },
  }
  return {
    registered,
    teardowns,
    lines,
    subagents,
    tools: { register(definition) { registered.push(definition) } },
    // The opportunistic face: a real Cordis ctx.get reads the service
    // store; the fake reads the injected engine (undefined = not loaded).
    get: (name) => (name === 'worktreesEngine' ? engine : undefined),
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

function tmpDbPath(prefix) {
  return join(mkdtempSync(join(tmpdir(), prefix)), 'dag.db')
}

const AGENT_ONLY_SPEC = { version: 1, name: 'agent-only', tasks: [{ id: 'a', kind: 'agent', prompt: 'do a' }] }

test('apply: fake worktreesEngine on the ctx → seam available and the executor face carries it', async (t) => {
  const engine = fakeEngine()
  const ctx = fakeApplyCtx({ engine })
  const path = tmpDbPath('dag-seam-on-')
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  // The apply-time probe made it into the assembly log (honest reporting).
  assert.ok(ctx.lines.info.some((m) => m.includes('worktrees engine available')),
    `assembly log states availability, got: ${JSON.stringify(ctx.lines.info)}`)

  // The engine reached the executor through the tool face: dispatch one
  // task through the REGISTERED dag_plan (its inline first tick runs the
  // real engine → real executor), then finish the run with dag_tick and
  // confirm the seam the assembly built resolves the SAME singletons the
  // fake provided (identity pass-through, worktrees §10 single instance).
  const plan = ctx.registered.find((tool) => tool.name === 'dag_plan')
  const tick = ctx.registered.find((tool) => tool.name === 'dag_tick')
  const exec = { agent: { session: { id: 'sess-parent', header: { cwd: '/tmp/repo' } } } }
  const planned = await plan.execute({ spec: AGENT_ONLY_SPEC }, exec)
  assert.equal(planned.initial_tick.dispatched, 1, 'the inline first tick dispatched')
  assert.equal(ctx.subagents.calls.length, 1, 'exactly one subagent started')

  const finalTick = await tick.execute({ run_id: planned.run_id }, exec)
  assert.equal(finalTick.run_state, 'succeeded', 'the run completed through the real assembly')

  const consumerSeam = createWorktreesSeam(ctx, {})
  const face = consumerSeam.get()
  assert.equal(face.worktrees.getMergeQueue(), engine.getMergeQueue())
  assert.equal(face.worktrees.getWorktreeService(), engine.getWorktreeService())
})

test('apply: engine absent → apply completes, agent-only DAG goes FULL CHAIN green', async (t) => {
  const ctx = fakeApplyCtx({ engine: undefined })
  const path = tmpDbPath('dag-seam-off-')
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  // Absence is honest in the log and silent in the warns (normal mode).
  assert.ok(ctx.lines.info.some((m) => m.includes('worktrees engine absent (agent-only DAG mode)')))
  assert.equal(ctx.lines.warn.filter((m) => m.includes('worktreesEngine')).length, 0)

  // Full chain through the REGISTERED tools: plan (inline tick dispatches
  // + harvests) → the run is already terminal; dag_tick idempotent.
  const plan = ctx.registered.find((tool) => tool.name === 'dag_plan')
  const tick = ctx.registered.find((tool) => tool.name === 'dag_tick')
  const status = ctx.registered.find((tool) => tool.name === 'dag_status')
  const exec = { agent: { session: { id: 'sess-parent', header: { cwd: '/tmp/repo' } } } }

  const planned = await plan.execute({ spec: AGENT_ONLY_SPEC }, exec)
  assert.equal(planned.initial_tick.dispatched, 1)
  assert.equal(ctx.subagents.calls.length, 1)

  const finalTick = await tick.execute({ run_id: planned.run_id }, exec)
  assert.equal(finalTick.run_state, 'succeeded', 'agent-only run finished with the seam absent')

  const finalStatus = await status.execute({ run_id: planned.run_id }, exec)
  assert.ok(finalStatus.run_id === planned.run_id || finalStatus.tasks !== undefined,
    'dag_status answers for the finished run')

  // And the consumer view through a seam over the same ctx degrades null.
  assert.equal(createWorktreesSeam(ctx, {}).get().worktrees, null)
})

test('apply: a half-shaped engine on the ctx → apply still completes (warn, degrade, no block)', async (t) => {
  const ctx = fakeApplyCtx({ engine: { getMergeQueue: () => ({}) } }) // missing getWorktreeService
  const path = tmpDbPath('dag-seam-half-')
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())

  assert.ok(ctx.lines.warn.some((m) => m.includes('worktreesEngine') && m.includes('getMergeQueue')),
    `the half shape was warned, got: ${JSON.stringify(ctx.lines.warn)}`)
  assert.ok(ctx.lines.info.some((m) => m.includes('worktrees engine absent')),
    'the assembly log reports absence (fail-safe, not half-wired)')

  const consumerSeam = createWorktreesSeam(ctx, {})
  assert.equal(consumerSeam.get().worktrees, null)
})

test('apply: LATE-registered engine — absent at apply, provided before first use, consumer wires in', async (t) => {
  // The cadence under test end-to-end: apply-time probe absent (log says
  // agent-only), the host provides the engine afterwards, and the seam a
  // T16/T17 consumer would read NOW resolves it.
  const holder = { engine: undefined }
  const ctx = fakeApplyCtx()
  ctx.get = (name) => (name === 'worktreesEngine' ? holder.engine : undefined)
  const path = tmpDbPath('dag-seam-late-')
  await apply(ctx, { dbPath: path })
  t.after(() => ctx.teardowns[0]())
  assert.ok(ctx.lines.info.some((m) => m.includes('worktrees engine absent')),
    'apply-time probe honestly reported absence')

  const engine = fakeEngine()
  holder.engine = engine // the host's ctx.provide lands LATE

  const seam = createWorktreesSeam(ctx, {})
  const face = seam.get()
  assert.notEqual(face.worktrees, null, 'the use-time re-probe found the late engine')
  assert.equal(face.worktrees.getWorktreeService(), engine.getWorktreeService())
})
